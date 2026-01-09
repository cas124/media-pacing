import os
import requests
from google.cloud import bigquery
from datetime import datetime

# 1. Configuration
# We read the password from the environment variable we will set up in the deploy step
WP_PASSWORD = os.environ.get('WP_PASSWORD') 
WP_USER = "cassie.haxton@hedyandhopp.com"
WP_URL = "https://wearehipaasmart.com/"

# BigQuery Config
PROJECT_ID = "325576423919"
DATASET_ID = "learndash_stats"
TABLE_ID = "daily_student_count"

def sync_learndash_data():
    if not WP_PASSWORD:
        raise ValueError("WP_PASSWORD not found. Check Secret Manager mounting.")

    print("Fetching data from WordPress...")
    
    # 2. Get Data
    params = {'roles': 'subscriber', 'per_page': 1}
    response = requests.get(WP_URL, auth=(WP_USER, WP_PASSWORD), params=params)
    
    if response.status_code != 200:
        raise Exception(f"WP Error: {response.text}")

    # Extract total
    total_students = int(response.headers.get('X-WP-Total', 0))
    print(f"Found {total_students} students.")

    # 3. Push to BigQuery
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    rows = [{
        "date": datetime.now().date().isoformat(),
        "total_students": total_students
    }]
    
    errors = client.insert_rows_json(table_ref, rows)
    
    if errors == []:
        print("Success! BigQuery updated.")
    else:
        print(f"BigQuery Errors: {errors}")

if __name__ == "__main__":
    sync_learndash_data()