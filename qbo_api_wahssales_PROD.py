# ==============================================================================
# 0. IMPORTS AND INITIAL CONFIGURATION
# ==============================================================================
import json
import requests
import pandas as pd
from google.cloud import bigquery 
import os
import sys 
import numpy as np

# --- Required Intuit Libraries (for token management) ---
from intuitlib.client import AuthClient 
from quickbooks.client import QuickBooks 

# --- Configuration File ---
CONFIG_FILE = 'qbo_config_PROD.json'

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

ENV = 'production' 
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
env_base = "https://quickbooks.api.intuit.com" # Production base URL
TARGET_PRODUCT = 'Products:We Are, HIPAA Smart'
TARGET_PRODUCT = TARGET_PRODUCT.strip() 

def clean_and_lower(text):
    """Robustly cleans input, ensuring it's a string, and converts to lowercase."""
    s = str(text) if not pd.isna(text) and text is not None else ""
    return ' '.join(s.split()).lower()

TARGET_PRODUCT_CLEAN = clean_and_lower(TARGET_PRODUCT) 

print("✅ QBO Client authenticated and tokens refreshed.")


# ==============================================================================
# 2. QBO DATA EXTRACTION (E) & HELPER FUNCTIONS
# ==============================================================================

def fetch_qbo_sales_receipts_raw(access_token, company_id, base_url, product_name):
    all_records = []
    start_pos = 1
    max_results = 1000
    
    qbo_base_query = "SELECT * FROM SalesReceipt "
    
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
            print(f"\n🚨 API REQUEST FAILED DETAILS 🚨")
            print(f"Status Code: {response.status_code}")
            print(f"Response Body: {response.text}")
            print("-----------------------------------")
            raise Exception("QBO API Request Failed during Sales Receipt fetch.")

        data = response.json()
        
        receipts = data.get('QueryResponse', {}).get('SalesReceipt', [])
        all_records.extend(receipts)
        
        if len(receipts) < max_results:
            break
        
        start_pos += max_results
        print(f"Fetched {len(all_records)} total records, continuing...")

    df_raw = pd.DataFrame(all_records)
    
    if not df_raw.empty:
        df_raw['transaction_type'] = 'Sales Receipt'
        
    print(f"✅ Extraction complete. Total {len(all_records)} Sales Receipt records found.")
    return df_raw

def fetch_qbo_invoices_raw(access_token, company_id, base_url, product_name):
    """Fetches INVOICES (unfiltered) up to 1000 records for filtering."""
    all_records = []
    start_pos = 1
    max_results = 1000
    
    qbo_base_query = "SELECT * FROM Invoice " 
    
    print(f"\nStarting raw extraction for INVOICES (FULL FETCH for filtering)...")

    while start_pos <= 1000: # Limit fetch to 1000 records total
        
        qbo_query = f"{qbo_base_query} STARTPOSITION {start_pos} MAXRESULTS {max_results}"
        api_url = f"{base_url}/v3/company/{company_id}/query" 

        headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/json',
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        response = requests.get(api_url, headers=headers, params={'query': qbo_query})
        
        if response.status_code != 200:
            print(f"❌ API Error {response.status_code}: {response.text}")
            raise Exception("QBO API Request Failed during Invoice fetch.")

        data = response.json()
        invoices = data.get('QueryResponse', {}).get('Invoice', [])
        all_records.extend(invoices)
        
        if len(invoices) < max_results:
            break
        
        start_pos += max_results
        print(f"Fetched {len(all_records)} total invoice records, continuing...")

    df_raw = pd.DataFrame(all_records)
    
    if not df_raw.empty:
        df_raw['transaction_type'] = 'Invoice' 

    print(f"✅ Extraction complete. Total {len(all_records)} Invoice records retrieved for filtering.")
    return df_raw

def get_item_name(line):
    """Safely extracts and strips whitespace from the item name."""
    if isinstance(line, dict) and line.get('SalesItemLineDetail'):
        name = line['SalesItemLineDetail'].get('ItemRef', {}).get('name')
        if name:
            return str(name).strip() 
    return None

def get_line_detail(line, key):
    """Safely extracts a specific value (Qty, Rate) from SalesItemLineDetail."""
    if isinstance(line, dict) and line.get('SalesItemLineDetail'):
        return line['SalesItemLineDetail'].get(key)
    return None

def process_and_filter_df(df_raw, target_product_clean):
    """Processes, flattens, and filters a single DataFrame."""
    
    #1: If input is empty, return an empty DataFrame with the full schema
    if df_raw.empty:
        return pd.DataFrame(columns=['Id', 'customer_name', 'transaction_date', 'item_name_raw', 'transaction_type', 'TotalAmt']) 

    # 1. Flatten Header Data
    df_raw['customer_name'] = df_raw['CustomerRef'].apply(
        lambda x: x.get('name') if isinstance(x, dict) else None
    )
    df_raw['transaction_date'] = pd.to_datetime(
        df_raw['TxnDate'], errors='coerce'
    ).dt.date

    # 2. Explode the line items
    df_lines = df_raw.explode('Line', ignore_index=True) 

    # 3. Extract Item Name and Apply Filter (Creation of item_name_raw and item_name_lower)
    df_lines['item_name_raw'] = df_lines['Line'].apply(get_item_name) 
    df_lines['item_name_lower'] = df_lines['item_name_raw'].apply(clean_and_lower)
    df_lines['quantity'] = df_lines['Line'].apply(lambda x: get_line_detail(x, 'Qty'))
    df_lines['sales_price'] = df_lines['Line'].apply(lambda x: get_line_detail(x, 'UnitPrice'))


    # ------------------------------------------------------------------
    # --- TEMPORARY FIX: BYPASS FILTER TO DUMP ALL DATA ---
    #df_product_lines = df_lines.copy() # <--- Use ALL lines for the dump
    # -----------------------------------------------------
    
    # Filter the data frame 
    df_product_lines = df_lines[df_lines['item_name_lower'] == target_product_clean].copy()
    
    # Check 2: If the filtered result is empty, return an empty DataFrame with final schema
    if df_product_lines.empty:
        return pd.DataFrame(columns=['Id', 'customer_name', 'transaction_date', 'item_name_raw', 'transaction_type', 'TotalAmt'])
    
    # 4. RFinal selection of needed columns for the combined frame
    df_final_cols = df_product_lines[[
        'Id', 
        'customer_name',
        'transaction_date',
        'item_name_raw', 
        'transaction_type', 
        'TotalAmt',
        'Balance',           # <--- ADDED BALANCE (Header Field)
        'quantity',          # <--- ADDED QUANTITY (Line Detail)
        'sales_price'        # <--- ADDED SALES PRICE (Line Detail)
    ]].copy()

    return df_final_cols


#  ==============================================================================
# 3. EXECUTION AND TRANSFORMATION (E & T)
# ==============================================================================

# --- EXECUTION: Runs both extraction functions ---
df_receipts_raw = fetch_qbo_sales_receipts_raw(access_token, company_id, env_base, TARGET_PRODUCT) 
df_invoices_raw = fetch_qbo_invoices_raw(access_token, company_id, env_base, TARGET_PRODUCT)


# --- Process Each DataFrame Separately and Filter ---

# Apply the common processing to both sets of data
df_filtered_receipts = process_and_filter_df(df_receipts_raw, TARGET_PRODUCT_CLEAN)
df_filtered_invoices = process_and_filter_df(df_invoices_raw, TARGET_PRODUCT_CLEAN)


# --------------------------------------------------------
# 4. COMBINE AND FINAL CLEANUP
# --------------------------------------------------------

# Filter out any resulting None values and ensure we have an iterable of DataFrames
dfs_to_concat = [df_filtered_receipts, df_filtered_invoices]
dfs_to_concat = [df for df in dfs_to_concat if not df.empty]


if not dfs_to_concat:
    print(f"⚠️ WARNING: No transactions found matching '{TARGET_PRODUCT}'. Loading 0 rows to BQ.")
    df_payments_final = pd.DataFrame(columns=['transaction_id', 'customer_name', 'transaction_date', 'product_name', 'line_amount', 'transaction_type'])

else:
    # Concatenate the standardized DataFrames
    df_combined_filtered = pd.concat(dfs_to_concat, ignore_index=True)

    # --- Final Selection and Rename ---
    amount_key = 'TotalAmt' 
    
    # Select and rename final columns 
    df_payments_final = df_combined_filtered[[
        'Id', 
        'customer_name',
        'transaction_date',
        'item_name_raw', 
        'transaction_type', 
        'quantity',
        'sales_price',
        amount_key, 
    ]].rename(columns={
        'Id': 'transaction_id', 
        amount_key: 'total_amount', 
        'item_name_raw': 'product_name'
    })
    
    # Final Cleaning (Ensure numeric conversion)
    df_payments_final['total_amount'] = pd.to_numeric(
        df_payments_final['total_amount'], errors='coerce'
    )
    df_payments_final['quantity'] = pd.to_numeric(
        df_payments_final['quantity'], errors='coerce'
    )
    df_payments_final['sales_price'] = pd.to_numeric(
        df_payments_final['sales_price'], errors='coerce'
    )

print("✅ Data transformation complete.")


# ==============================================================================
# 5. BIGQUERY LOADING (L)
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
TABLE_ID = 'wahs_qbo_sales' 
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# Configure the load job
job_config = bigquery.LoadJobConfig(
    write_disposition='WRITE_TRUNCATE', 
)

try:
    df_to_load = df_payments_final 

    # Execute the load job
    job = bq_client.load_table_from_dataframe(
        df_to_load, 
        table_ref, 
        job_config=job_config
    )

    job.result() 

    print(f"\n🚀 Success! Loaded {job.output_rows} rows into BigQuery table: {table_ref}")

except Exception as e:
    print(f"❌ BigQuery Load Failed: {e}")