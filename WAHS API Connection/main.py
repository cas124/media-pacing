# ==============================================================================
# 0. IMPORTS AND GLOBAL DEFINITIONS
# ==============================================================================
import json
import requests
import pandas as pd
from google.cloud import bigquery 
from google.cloud import secretmanager 
import os
import sys 
import numpy as np

# --- Required Intuit Libraries ---
from intuitlib.client import AuthClient 
from quickbooks.client import QuickBooks 

# --- Global Constants ---

# --- CRITICAL FIX: Use the absolute path inside the container ---
# The Dockerfile copies this file to /app/
BQ_KEY_FILE = '/secrets/bigquery-sa-key'

# --- Use the exact, literal string you found in your BQ diagnostic query ---
TARGET_PRODUCT = 'Products:We Are HIPAA Smart' 

# ==============================================================================
# GOOGLE CLOUD SECRET MANAGER HELPER FUNCTIONS
# ==============================================================================

# Initialize Secret Manager Client globally
SECRET_CLIENT = secretmanager.SecretManagerServiceClient() 

def get_latest_refresh_token(project_id, secret_name):
    """Retrieves the latest version of the Refresh Token from Secret Manager."""
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    try:
        response = SECRET_CLIENT.access_secret_version(name=name)
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        raise Exception(f"Failed to access secret '{secret_name}': {e}")

def update_refresh_token(project_id, secret_name, new_token):
    """Creates a new secret version with the latest Refresh Token."""
    parent = f"projects/{project_id}/secrets/{secret_name}"
    try:
        SECRET_CLIENT.add_secret_version(
            parent=parent,
            payload={"data": new_token.encode("UTF-8")}
        )
        print(f"🔥 NEW REFRESH TOKEN SAVED to Secret Manager.")
    except Exception as e:
        print(f"❌ WARNING: Failed to update token in Secret Manager: {e}")


# ==============================================================================
# CLOUD FUNCTION ENTRY POINT
# ==============================================================================

def run_pipeline(request=None):
    
    # 1. READ CREDENTIALS FROM ENVIRONMENT (Set during deployment)
    try:
        # Static QBO Credentials
        QB_CLIENT_ID = os.environ['QB_CLIENT_ID']
        QB_CLIENT_SECRET = os.environ['QB_CLIENT_SECRET']
        QB_REDIRECT_URI = os.environ['QB_REDIRECT_URI']
        
        # Dynamic Secrets and Project IDs
        QB_SECRET_NAME = os.environ['QB_SECRET_NAME'] # Name of the secret holding the Refresh Token
        COMPANY_ID = os.environ['QB_COMPANY_ID']     # Realm ID
        BQ_PROJECT_ID = os.environ['BQ_PROJECT_ID']
        
    except KeyError as e:
        print(f"❌ ERROR: Missing required environment variable: {e}")
        return f"Pipeline failed: Missing environment variable {e}", 500

    # Static Variables
    ENV = 'production' 
    env_base = "https://quickbooks.api.intuit.com" 
    PROJECT_ID_FOR_SECRETS = BQ_PROJECT_ID


    # ==============================================================================
    # 2. QBO AUTHENTICATION & TOKEN REFRESH (E)
    # ==============================================================================

    # Retrieve current token from Secret Manager
    try:
        QB_REFRESH_TOKEN_INITIAL = get_latest_refresh_token(PROJECT_ID_FOR_SECRETS, QB_SECRET_NAME)
    except Exception as e:
        print(f"❌ ERROR: Could not retrieve initial token: {e}")
        return "QBO Authentication Failed: Token retrieval error.", 500

    auth_client = AuthClient(
        QB_CLIENT_ID,
        QB_CLIENT_SECRET,
        QB_REDIRECT_URI,
        ENV 
    )

    try:
        auth_client.refresh(refresh_token=QB_REFRESH_TOKEN_INITIAL)
        new_refresh_token = auth_client.refresh_token
        
        if new_refresh_token != QB_REFRESH_TOKEN_INITIAL:
            update_refresh_token(PROJECT_ID_FOR_SECRETS, QB_SECRET_NAME, new_refresh_token)

    except Exception as e:
        print(f"❌ QBO Authentication Failed during refresh: {e}")
        return f"QBO Authentication Failed: {e}", 500

    access_token = auth_client.access_token
    print("--- Authentication Success. Starting Pipeline Execution ---")
    
    print("✅ QBO Client authenticated and tokens refreshed. Ready for BQ sync.") 


    # ==============================================================================
    # 3. QBO DATA EXTRACTION (E) & TRANSFORMATION (T)
    # ==============================================================================
    
    # --- Function Definitions (Moved inside for cleaner global namespace) ---
    def fetch_qbo_sales_receipts_raw(access_token, COMPANY_ID, base_url, product_name):
        all_records = []
        start_pos = 1
        max_results = 1000
        qbo_base_query = "SELECT * FROM SalesReceipt "
        
        print(f"\nStarting raw extraction for SALES RECEIPTS (Target: {product_name})...")

        while True: 
            qbo_query = f"{qbo_base_query} STARTPOSITION {start_pos} MAXRESULTS {max_results}"
            api_url = f"{base_url}/v3/company/{COMPANY_ID}/query"

            headers = {
                'Authorization': f'Bearer {access_token}',
                'Accept': 'application/json',
                'Content-Type': 'application/x-www-form-urlencoded'
            }

            response = requests.get(api_url, headers=headers, params={'query': qbo_query})
            
            if response.status_code != 200:
                print(f"\n🚨 API REQUEST FAILED DETAILS 🚨")
                print(f"Status Code: {response.status_code}")
                print(f"Response Body: {response.text}")
                print("-----------------------------------")
                raise Exception("QBO API Request Failed during Sales Receipt fetch.")

            data = response.json()
            receipts = data.get('QueryResponse', {}).get('SalesReceipt', [])

            if not receipts:
                print("No more sales receipts found. Ending fetch.")
                break 

            all_records.extend(receipts)
            
            if len(receipts) < max_results: 
                print(f"Last page reached. Total {len(all_records)} sales receipts.")
                break
            
            start_pos += max_results
            print(f"Fetched {len(all_records)} total sales receipt records, continuing to next page...")

        df_raw = pd.DataFrame(all_records)
        if not df_raw.empty:
            df_raw['transaction_type'] = 'Sales Receipt'
            
        print(f"✅ Extraction complete. Total {len(all_records)} Sales Receipt records found.")
        return df_raw

    def fetch_qbo_invoices_raw(access_token, COMPANY_ID, base_url, product_name):
        all_records = []
        start_pos = 1
        max_results = 1000
        
        qbo_base_query = "SELECT * FROM Invoice " 
        
        print(f"\nStarting raw extraction for INVOICES (FULL FETCH for filtering)...")

        while True: 
            qbo_query = f"{qbo_base_query} STARTPOSITION {start_pos} MAXRESULTS {max_results}"
            api_url = f"{base_url}/v3/company/{COMPANY_ID}/query" 

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
            
            if not invoices:
                print("No more invoices found. Ending fetch.")
                break 

            all_records.extend(invoices)
            
            if len(invoices) < max_results: 
                print(f"Last page reached. Total {len(all_records)} invoices.")
                break
            
            start_pos += max_results
            print(f"Fetched {len(all_records)} total invoice records, continuing to next page...")

        df_raw = pd.DataFrame(all_records)
        
        if not df_raw.empty:
            df_raw['transaction_type'] = 'Invoice'

        print(f"✅ Extraction complete. Total {len(all_records)} Invoice records retrieved for filtering.")
        return df_raw

    def get_item_name(line):
        """
        Safely extracts the item name.
        Checks the Description field first, then falls back to ItemRef.name.
        """
        if isinstance(line, dict):
            # 1. Check the 'Description' field first.
            # This is often where "Category:Product" strings are stored.
            name = line.get('Description')
            
            # 2. If Description is empty, check the ItemRef.name
            if not name and line.get('SalesItemLineDetail'):
                name = line['SalesItemLineDetail'].get('ItemRef', {}).get('name')
                
            if name:
                return str(name).strip() 
        return None

    def process_and_filter_df(df_raw, target_product_string):
        
        # Define the schema for empty DataFrames
        EMPTY_COLS = ['Id', 'customer_name', 'transaction_date', 'item_name_raw', 'transaction_type', 'Amount']
        
        if df_raw.empty:
            return pd.DataFrame(columns=EMPTY_COLS) 

        # 1. Flatten Header Data
        df_raw['customer_name'] = df_raw['CustomerRef'].apply(lambda x: x.get('name') if isinstance(x, dict) else None)
        df_raw['transaction_date'] = pd.to_datetime(df_raw['TxnDate'], errors='coerce').dt.date

        # 2. Explode the line items
        df_lines = df_raw.explode('Line', ignore_index=True) 

        # 3. Extract Item Name
        df_lines['item_name_raw'] = df_lines['Line'].apply(get_item_name) 

        # --- NEW ROBUST FILTER LOGIC ---
        
        # 4. Normalize the target string (aggressive clean)
        # Replaces all whitespace with a single space, lowers, and strips.
        target_clean = ' '.join(str(target_product_string).split()).lower()

        # 5. Normalize the data column (aggressive clean)
        # We use .astype(str) to force everything to a string, then clean and lower.
        df_lines['item_name_lower'] = df_lines['item_name_raw'].astype(str).apply(lambda x: ' '.join(x.split()).lower())
        
        # 6. Apply the filter
        df_product_lines = df_lines[df_lines['item_name_lower'] == target_clean].copy()
        
        # -------------------------------
        
        # Check 2: If the filtered result is empty, return an empty DataFrame with final schema
        if df_product_lines.empty:
            return pd.DataFrame(columns=EMPTY_COLS)
        
        # 7. Add the line-item Amount column
        df_product_lines['Amount'] = df_product_lines['Line'].apply(lambda x: x.get('Amount') if isinstance(x, dict) else 0)
        
        # 8. Return the filtered DataFrame with the required final columns
        return df_product_lines[['Id', 'customer_name', 'transaction_date', 'item_name_raw', 'transaction_type', 'Amount']].copy()
    

    # --- EXECUTION: Runs both extraction functions ---
    print("Checkpoint A: Starting Sales Receipts Fetch")
    df_receipts_raw = fetch_qbo_sales_receipts_raw(access_token, COMPANY_ID, env_base, TARGET_PRODUCT)
    print("Checkpoint B: Sales Receipts Fetch Complete")

    print("Checkpoint C: Starting Invoices Fetch")
    df_invoices_raw = fetch_qbo_invoices_raw(access_token, COMPANY_ID, env_base, TARGET_PRODUCT)
    print("Checkpoint D: Invoices Fetch Complete")


    # --- Process Each DataFrame Separately and Filter ---
    print("Checkpoint E: Starting Filtering (Receipts)")
    df_filtered_receipts = process_and_filter_df(df_receipts_raw, TARGET_PRODUCT) # <-- Pass original variable
    print("Checkpoint F: Starting Filtering (Invoices)")
    df_filtered_invoices = process_and_filter_df(df_invoices_raw, TARGET_PRODUCT) # <-- Pass original variable
    print("Checkpoint G: Filtering Complete. Starting Concat.")



    # --------------------------------------------------------
    # 4. COMBINE AND FINAL CLEANUP
    # --------------------------------------------------------

    dfs_to_concat = [df_filtered_receipts, df_filtered_invoices]
    dfs_to_concat = [df for df in dfs_to_concat if not df.empty]


    if not dfs_to_concat:
        print(f"⚠️ WARNING: No transactions found matching '{TARGET_PRODUCT}'. Loading 0 rows to BQ.")
        df_payments_final = pd.DataFrame(columns=['transaction_id', 'customer_name', 'transaction_date', 'product_name', 'total_amount', 'transaction_type'])

    else:
        # Concatenate the standardized DataFrames
        df_combined_filtered = pd.concat(dfs_to_concat, ignore_index=True)

        # --- Final Selection and Rename ---
        amount_key = 'Amount' # Use the line-item amount key
        
        df_payments_final = df_combined_filtered[[
            'Id', 
            'customer_name',
            'transaction_date',
            'item_name_raw', 
            'transaction_type', 
            amount_key, 
        ]].rename(columns={
            'Id': 'transaction_id', 
            amount_key: 'total_amount', 
            'item_name_raw': 'product_name' 
        })
        
        # Final Cleaning
        df_payments_final['total_amount'] = pd.to_numeric(df_payments_final['total_amount'], errors='coerce')

    print("✅ Data transformation complete.")


    # ==============================================================================
    # 5. BIGQUERY LOADING (L)
    # ==============================================================================
    
    # Authenticate BigQuery using the Service Account file deployed with the function
    try:
        bq_client = bigquery.Client.from_service_account_json(BQ_KEY_FILE) 
        print("✅ BigQuery Client authenticated.")
    except Exception as e:
        print(f"❌ BigQuery Auth Failed (Key File): {e}")
        return f"BigQuery Auth Failed (Key File): {e}", 500

    # Define Target and Execute Load Job
    PROJECT_ID = BQ_PROJECT_ID
    DATASET_ID = 'quickbooks_data'  
    TABLE_ID = 'wahs_qbo_sales' 
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    job_config = bigquery.LoadJobConfig(write_disposition='WRITE_TRUNCATE') 

    try:
        df_to_load = df_payments_final 

        job = bq_client.load_table_from_dataframe(df_to_load, table_ref, job_config=job_config)
        job.result() 
        
        success_message = f"QuickBooks data loaded successfully! Loaded {job.output_rows} rows."
        print(f"\n🚀 {success_message}\n")
        return success_message, 200
    
    except Exception as e:
        return f"BigQuery Load Failed: {e}", 500

# ==============================================================================
# LOCAL EXECUTION ENTRY POINT (Run this function)
# ==============================================================================

if __name__ == "__main__":
    run_pipeline()