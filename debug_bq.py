import json
from google.cloud import bigquery

KEY_FILE = 'service-account-key.json'

def check_tables():
    with open(KEY_FILE, 'r') as f:
        project_id = json.load(f)['project_id']
        
    client = bigquery.Client.from_service_account_json(KEY_FILE)
    dataset_id = f"{project_id}.supermarket_analytics"
    
    print(f"Checking dataset: {dataset_id}")
    
    try:
        tables = list(client.list_tables(dataset_id))
        if tables:
            print("Found tables:")
            for t in tables:
                print(f" - {t.table_id} (Type: {t.table_type})")
        else:
            print("Dataset exists but is EMPTY.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_tables()
