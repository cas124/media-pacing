import json
import io
import datetime
import requests
from google.cloud import bigquery
from google.cloud import secretmanager
from google.oauth2 import service_account
from google.ads.googleads.client import GoogleAdsClient

# ---------------------------------------------------------
# 1. CONFIGURATION
# ---------------------------------------------------------
PROJECT_ID = 'media-pacing'
DATASET_ID = 'marketing_data'
TABLE_ID = 'daily_spend'
SECRET_ID = 'marketing-bigquery-key' 
VERSION_ID = 'latest'
FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# --- AD PLATFORM CONFIG (You need to fill these in!) ---
# Ideally, put these in Secret Manager too, but for now, we define them here.

# META (FACEBOOK) CREDENTIALS
META_ACCESS_TOKEN = "YOUR_LONG_ACCESS_TOKEN_HERE"
META_AD_ACCOUNT_ID = "act_123456789" # Must start with 'act_'

# GOOGLE ADS CONFIG
GOOGLE_ADS_CUSTOMER_ID = "123-456-7890"
# Google Ads also requires a 'google-ads.yaml' file in your folder
# or a dict configuration.

# MICROSOFT ADS CREDENTIALS
MS_CLIENT_ID = "YOUR_CLIENT_ID"
MS_REFRESH_TOKEN = "YOUR_REFRESH_TOKEN"
MS_ACCOUNT_ID = "12345678"

# ---------------------------------------------------------
# 2. AUTHENTICATION (BigQuery)
# ---------------------------------------------------------
def get_creds_from_secret_manager():
    """Fetches BigQuery credentials from the vault."""
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{PROJECT_ID}/secrets/{SECRET_ID}/versions/{VERSION_ID}"
        response = client.access_secret_version(request={"name": name})
        secret_payload = response.payload.data.decode("UTF-8")
        key_info = json.loads(secret_payload)
        return service_account.Credentials.from_service_account_info(key_info)
    except Exception as e:
        print(f"❌ Failed to fetch credentials: {e}")
        raise

# ---------------------------------------------------------
# 3. THE FETCHERS (The "Spokes")
# ---------------------------------------------------------

def fetch_meta_data():
    """Fetches yesterday's spend from Meta (Facebook/Instagram)."""
    print("Fetching Meta Ads...")
    try:
        # Define the date range (Yesterday)
        yesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
        
        url = f"https://graph.facebook.com/v19.0/{META_AD_ACCOUNT_ID}/insights"
        params = {
            'access_token': META_ACCESS_TOKEN,
            'level': 'campaign',
            'time_range': json.dumps({'since': yesterday, 'until': yesterday}),
            'fields': 'campaign_name,spend,impressions,clicks,actions'
        }
        
        response = requests.get(url, params=params)
        data = response.json()
        
        if 'error' in data:
            print(f"⚠️ Meta Error: {data['error']['message']}")
            return []

        formatted_rows = []
        for item in data.get('data', []):
            formatted_rows.append({
                "date": yesterday,
                "platform": "Meta",
                "campaign_name": item.get('campaign_name'),
                "spend": float(item.get('spend', 0)),
                "impressions": int(item.get('impressions', 0)),
                "clicks": int(item.get('clicks', 0))
            })
        return formatted_rows
    except Exception as e:
        print(f"⚠️ Meta Exception: {e}")
        return []

def fetch_google_ads_data():
    """Fetches yesterday's spend from Google Ads."""
    print("Fetching Google Ads...")
    # NOTE: This requires a 'google-ads.yaml' file in your directory
    # or a properly configured client dict.
    try:
        client = GoogleAdsClient.load_from_storage("google-ads.yaml")
        ga_service = client.get_service("GoogleAdsService")
        
        yesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
        
        query = f"""
            SELECT 
                campaign.name, 
                metrics.cost_micros, 
                metrics.impressions, 
                metrics.clicks 
            FROM campaign 
            WHERE segments.date = '{yesterday}'
        """
        
        stream = ga_service.search_stream(customer_id=GOOGLE_ADS_CUSTOMER_ID, query=query)
        
        formatted_rows = []
        for batch in stream:
            for row in batch.results:
                formatted_rows.append({
                    "date": yesterday,
                    "platform": "Google Ads",
                    "campaign_name": row.campaign.name,
                    "spend": row.metrics.cost_micros / 1000000.0, # Convert micros to currency
                    "impressions": row.metrics.impressions,
                    "clicks": row.metrics.clicks
                })
        return formatted_rows
    except Exception as e:
        print(f"⚠️ Google Ads Exception: {e} (Do you have google-ads.yaml?)")
        return []

def fetch_microsoft_data():
    """
    Placeholder for Microsoft Ads.
    Microsoft requires a complex OAuth refresh flow that is too large for this snippet.
    This is a 'Stub' to remind you to build it.
    """
    print("Fetching Microsoft Ads (Skipped - Needs OAuth Setup)...")
    # You would use the 'bingads' SDK here.
    return []

# ---------------------------------------------------------
# 4. THE LOADER
# ---------------------------------------------------------
def load_to_bigquery(data):
    if not data:
        print("❌ No data to load.")
        return

    credentials = get_creds_from_secret_manager()
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    ndjson_data = '\n'.join([json.dumps(record) for record in data])
    source_file = io.StringIO(ndjson_data)

    try:
        print(f"🚀 Pushing {len(data)} rows to {FULL_TABLE_ID}...")
        job = client.load_table_from_file(source_file, FULL_TABLE_ID, job_config=job_config)
        job.result()
        print(f"✅ Success! Loaded {job.output_rows} rows.")
    except Exception as e:
        print(f"❌ Error loading data to BigQuery: {e}")

# ---------------------------------------------------------
# 5. MAIN EXECUTION
# ---------------------------------------------------------
if __name__ == "__main__":
    
    all_data = []
    
    # 1. Fetch Meta
    meta_rows = fetch_meta_data()
    all_data.extend(meta_rows)
    
    # 2. Fetch Google (Requires google-ads.yaml)
    # google_rows = fetch_google_ads_data() 
    # all_data.extend(google_rows)
    
    # 3. Fetch Microsoft (Requires BingAds SDK)
    # ms_rows = fetch_microsoft_data()
    # all_data.extend(ms_rows)
    
    # 4. Push everything to BigQuery
    load_to_bigquery(all_data)