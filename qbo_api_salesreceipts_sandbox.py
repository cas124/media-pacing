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
TARGET_PRODUCT = 'We Are, HIPAA Smart'
TARGET_PRODUCT = TARGET_PRODUCT.strip() 

def clean_and_lower(text):
    if pd.isna(text): return None
    # Replace all whitespace characters (spaces, tabs, newlines, etc.) with a single space, then lower.
    return ' '.join(str(text).split()).lower()

# Apply this cleaning to your target:
TARGET_PRODUCT_CLEAN = clean_and_lower(TARGET_PRODUCT) 

print("✅ QBO Client authenticated and tokens refreshed.")


# ==============================================================================
# 2. QBO DATA EXTRACTION (E) - CORRECTED
# ==============================================================================

def fetch_qbo_sales_receipts_raw(access_token, company_id, base_url, product_name):
    all_records = []
    start_pos = 1
    max_results = 1000
    
    qbo_base_query = (
        f"SELECT * FROM SalesReceipt "
    )
    
    # --- FIX 2: Corrected print statement ---
    print(f"\nStarting raw extraction for SALES RECEIPTS (Target: {product_name})...")

    while True:
        qbo_query = f"{qbo_base_query} STARTPOSITION {start_pos} MAXRESULTS {max_results}"
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
        
        if response.status_code != 200:
            print("\n🚨 API REQUEST FAILED DETAILS 🚨")
            print(f"Status Code: {response.status_code}")
            print(f"Response Body: {response.text}")
            print("-----------------------------------")
            raise Exception("QBO API Request Failed.")

        data = response.json()
        
        # --- FIX 1: Extract SalesReceipts, NOT Invoices ---
        receipts = data.get('QueryResponse', {}).get('SalesReceipt', [])
        all_records.extend(receipts)
        
        if len(receipts) < max_results:
            break
        
        start_pos += max_results
        print(f"Fetched {len(all_records)} total records, continuing...")

    # --- FIX 2: Corrected print statement ---
    print(f"✅ Extraction complete. Total {len(all_records)} Sales Receipt records found.")
    return pd.DataFrame(all_records)

def get_item_name(line):
    """Safely extracts and strips whitespace from the item name."""
    if isinstance(line, dict) and line.get('SalesItemLineDetail'):
        name = line['SalesItemLineDetail'].get('ItemRef', {}).get('name')
        if name:
            return str(name).strip() # <--- CRITICAL: Strips whitespace
    return None


#  ==============================================================================
# 3. EXECUTION AND TRANSFORMATION (E & T)
# ==============================================================================

# --- EXECUTION (This runs first, creating df_receipts_raw) ---
df_receipts_raw = fetch_qbo_sales_receipts_raw(access_token, company_id, env_base, TARGET_PRODUCT) 


# --- TRANSFORMATION STARTS HERE ---

if df_receipts_raw.empty:
    print("⚠️ WARNING: No Sales Receipts found in the QBO Sandbox. Loading 0 rows to BQ.")
    df_payments_final = pd.DataFrame(columns=['sales_receipt_id', 'customer_name', 'transaction_date', 'product_name', 'line_amount'])

else:
    # 1. Flatten Header Data
    df_receipts_raw['customer_name'] = df_receipts_raw['CustomerRef'].apply(
        lambda x: x.get('name') if isinstance(x, dict) else None
    )
    df_receipts_raw['transaction_date'] = pd.to_datetime(
        df_receipts_raw['TxnDate'], errors='coerce'
    ).dt.date

    # 2. Explode the line items (Creates df_lines)
    df_lines = df_receipts_raw.explode('Line', ignore_index=True)

    # 3. Extract Item Name
    df_lines['item_name'] = df_lines['Line'].apply(get_item_name)
    
    # --- STEP 2: THE CASE-INSENSITIVE FILTER GOES HERE ---
    
    # 4. Apply Case-Insensitive Filter
    
    # Convert target product name and item names to lowercase for comparison
    target_lower = TARGET_PRODUCT.lower()
    df_lines['item_name_lower'] = df_lines['item_name'].apply(clean_and_lower)
    df_product_lines = df_lines[df_lines['item_name_lower'] == TARGET_PRODUCT_CLEAN].copy()
    
    # Check if the filtered result is empty (critical check after filtering)
    if df_product_lines.empty:
        print(f"⚠️ WARNING: Filtered DataFrame is EMPTY after checking for product '{TARGET_PRODUCT}'.")
        df_payments_final = pd.DataFrame(columns=['sales_receipt_id', 'customer_name', 'transaction_date', 'product_name', 'line_amount'])
        
    else:
        # 5. Final Selection and Rename
        
        # --- Safely find the amount key (either 'Amount' or 'amount') ---
        # Assuming the amount key is 'Amount' (capital A) based on QBO JSON structure 
        # for line items. If this fails, the error will tell you the exact name.
        amount_key = 'amount' 
        
        df_payments_final = df_product_lines[[
            'Id', # Sales Receipt ID
            'customer_name',
            'transaction_date',
            'item_name',  
            amount_key, 
        ]].rename(columns={
            'Id': 'sales_receipt_id', 
            amount_key: 'line_amount',
            'item_name': 'product_name' 
        })
        
        # 6. Final Cleaning
        df_payments_final['line_amount'] = pd.to_numeric(
            df_payments_final['line_amount'], errors='coerce'
        )

print("✅ Data transformation complete.")


# ==============================================================================
# 4. BIGQUERY LOADING (L)
# ==============================================================================

# --- BigQuery Credentials ---
BQ_KEY_PATH = 'we_are_hipaa_smart_google_key.json' 

try:
    bq_client = bigquery.Client.from_service_account_json(BQ_KEY_PATH) 
    print("✅ BigQuery Client authenticated.")
except Exception as e:
    print(f"❌ BigQuery Authentication Failed. Check key path/content: {e}")
    sys.exit(1)


# Define your target table path
PROJECT_ID = bq_client.project 
DATASET_ID = 'quickbooks_data'  
TABLE_ID = 'invoices_filtered' # Changed table name to reflect Invoice data
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# Configure the load job
job_config = bigquery.LoadJobConfig(
    write_disposition='WRITE_TRUNCATE', 
)

try:
    # Execute the load job
    job = bq_client.load_table_from_dataframe(
        df_payments_final, 
        table_ref, 
        job_config=job_config
    )

    job.result() 

    print(f"\n🚀 Success! Loaded {job.output_rows} rows into BigQuery table: {table_ref}")

except Exception as e:
    print(f"❌ BigQuery Load Failed: {e}")