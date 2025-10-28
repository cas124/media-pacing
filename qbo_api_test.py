# ==============================================================================
# 0. IMPORTS AND INITIAL CONFIGURATION
# ==============================================================================
import json
import requests
import pandas as pd
from google.cloud import bigquery 
import os
import sys 

# --- Required Intuit Libraries (for token management) ---
from intuitlib.client import AuthClient 
# NOTE: The quickbooks.client is NOT used for querying in this final version 
# because of the ModuleNotFoundError, but we keep the import if you want to fix it later.
from quickbooks.client import QuickBooks 

# --- Configuration File ---
CONFIG_FILE = 'qbo_config.json'

# Load credentials
try:
    with open(CONFIG_FILE, 'r') as f:
        config = json.load(f)
except FileNotFoundError:
    print(f"❌ ERROR: Config file '{CONFIG_FILE}' not found. Please create it.")
    sys.exit(1)


# ==============================================================================
# 1. QBO AUTHENTICATION & TOKEN REFRESH (E)
# ==============================================================================

ENV = 'sandbox' 
AUTH_REDIRECT_URI = 'https://developer.intuit.com/v2/OAuth2Playground/RedirectUrl' 

auth_client = AuthClient(
    config['client_id'],
    config['client_secret'],
    AUTH_REDIRECT_URI,
    ENV 
)

try:
    auth_client.refresh(refresh_token=config['refresh_token'])
    
    # CRITICAL: Save the new Refresh Token
    new_refresh_token = auth_client.refresh_token
    if new_refresh_token != config['refresh_token']:
        config['refresh_token'] = new_refresh_token
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=4)
        print("✅ New Refresh Token successfully saved.")

except Exception as e:
    print(f"❌ QBO Authentication Failed during refresh. Check token in {CONFIG_FILE}: {e}")
    sys.exit(1)

# Variables needed for the API call
access_token = auth_client.access_token
company_id = config['company_id']
env_base = "https://sandbox-quickbooks.api.intuit.com" if ENV == 'sandbox' else "https://quickbooks.api.intuit.com"

print("✅ QBO Client authenticated and tokens refreshed.")


# ==============================================================================
# 2. QBO DATA EXTRACTION (E) - Using Raw Requests
# ==============================================================================

def fetch_qbo_payments_raw(access_token, company_id, base_url):
    """Fetches Payment data directly using the requests library and handles pagination."""
    all_records = []
    start_pos = 1
    max_results = 1000
    
    print(f"\nStarting raw extraction for Payment...")

    while True:
        qbo_query = f"SELECT * FROM Payment STARTPOSITION {start_pos} MAXRESULTS {max_results}"
        api_url = f"{base_url}/v3/company/{company_id}/query"

        headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/json',
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        response = requests.get(
            api_url, 
            headers=headers, 
            params={'query': qbo_query}
        )
        
        if response.status_code == 401:
            print("❌ ERROR 401: Token expired. Restart script.")
            raise Exception("Unauthorized API Call.")

        if response.status_code != 200:
            print(f"❌ API Error {response.status_code}: {response.text}")
            raise Exception("QBO API Request Failed.")

        data = response.json()
        payments = data.get('QueryResponse', {}).get('Payment', [])
        all_records.extend(payments)
        
        if len(payments) < max_results:
            break
        
        start_pos += max_results
        print(f"Fetched {len(all_records)} total records, continuing...")

    print(f"✅ Extraction complete. Total {len(all_records)} records found.")
    return pd.DataFrame(all_records) 

# --- Execute the Extraction ---
df_payments_raw = fetch_qbo_payments_raw(access_token, company_id, env_base)


# ==============================================================================
# 3. TRANSFORMATION (T)
# ==============================================================================

# 1. Flatten Customer ID
df_payments_raw['customer_id'] = df_payments_raw['CustomerRef'].apply(
    lambda x: x.get('value') if isinstance(x, dict) else None
)

# 2. Clean and convert data types
df_payments_raw['transaction_date'] = pd.to_datetime(
    df_payments_raw['TxnDate'], errors='coerce'
).dt.date
df_payments_raw['total_amount'] = pd.to_numeric(
    df_payments_raw['TotalAmt'], errors='coerce'
)

# 3. Select final columns and rename for BigQuery standards
df_payments_final = df_payments_raw[[
    'Id',
    'customer_id',
    'transaction_date',
    'total_amount',
    # Include other desired columns (e.g., CurrencyRef, SyncToken)
]].copy()

df_payments_final.rename(columns={'Id': 'payment_id'}, inplace=True)
print("✅ Data transformation complete.")


# ==============================================================================
# 4. BIGQUERY LOADING (L)
# ==============================================================================

# --- BigQuery Credentials ---
# NOTE: Ensure this file is uploaded and the path is correct
BQ_KEY_PATH = 'we_are_hipaa_smart_google_key.json' 

# Initialize the BQ client using your key file
try:
    bq_client = bigquery.Client.from_service_account_json(BQ_KEY_PATH) 
    print("✅ BigQuery Client authenticated.")
except Exception as e:
    print(f"❌ BigQuery Authentication Failed. Check key path/content: {e}")
    sys.exit(1)


# Define your target table path
PROJECT_ID = bq_client.project 
DATASET_ID = 'quickbooks_data'  # <-- UPDATE THIS DATASET NAME
TABLE_ID = 'payments_header'
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# Configure the load job
job_config = bigquery.LoadJobConfig(
    write_disposition='WRITE_TRUNCATE', # Overwrites the table
)

try:
    # Execute the load job
    job = bq_client.load_table_from_dataframe(
        df_payments_final, 
        table_ref, 
        job_config=job_config
    )

    job.result() # Wait for the job to complete

    print(f"\n🚀 Success! Loaded {job.output_rows} rows into BigQuery table: {table_ref}")

except Exception as e:
    print(f"❌ BigQuery Load Failed: {e}")