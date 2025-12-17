import json
import io
import datetime
from google.cloud import bigquery
from google.cloud import secretmanager
from google.oauth2 import service_account

# ---------------------------------------------------------
# 1. CONFIGURATION
# ---------------------------------------------------------
PROJECT_ID = 'media-pacing'
DATASET_ID = 'marketing_data'
TABLE_ID = 'daily_spend'  

SECRET_ID = 'media_pacing-bigquery-key' 
VERSION_ID = 'latest'

# Construct the full table reference (project.dataset.table)
FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# ---------------------------------------------------------
# 2. AUTHENTICATION (The Secure Handshake)
# ---------------------------------------------------------
def get_creds_from_secret_manager():
    """
    Fetches the private key JSON from Secret Manager and 
    converts it into credentials Python can use.
    """
    try:
        # Create the Secret Manager client
        # (This uses your local 'gcloud auth' login to prove identity)
        client = secretmanager.SecretManagerServiceClient()
        
        # Build the resource name
        name = f"projects/{PROJECT_ID}/secrets/{SECRET_ID}/versions/{VERSION_ID}"
        
        # Access the secret
        print(f"🔐 Accessing Secret Manager for {SECRET_ID}...")
        response = client.access_secret_version(request={"name": name})
        secret_payload = response.payload.data.decode("UTF-8")
        
        # Convert the string payload back to a JSON dictionary
        key_info = json.loads(secret_payload)
        
        # Create credentials object
        creds = service_account.Credentials.from_service_account_info(key_info)
        return creds
        
    except Exception as e:
        print(f"❌ Failed to fetch credentials from Secret Manager: {e}")
        raise

# ---------------------------------------------------------
# 3. THE LOAD LAYER (The "Unbreakable" Loader)
# ---------------------------------------------------------
def load_to_bigquery(data):
    """
    Loads JSON data into BigQuery using credentials from the vault.
    """
    # Get the credentials safely
    credentials = get_creds_from_secret_manager()
    
    # Authenticate BigQuery Client with these credentials
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

    # Configure the Load Job
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    # Convert data to Newline Delimited JSON
    ndjson_data = '\n'.join([json.dumps(record) for record in data])
    source_file = io.StringIO(ndjson_data)

    try:
        print(f"🚀 Pushing data to {FULL_TABLE_ID}...")
        job = client.load_table_from_file(
            source_file,
            FULL_TABLE_ID,
            job_config=job_config