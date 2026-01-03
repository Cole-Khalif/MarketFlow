import json
from google.cloud import bigquery

# Configuration
KEY_FILE = 'service-account-key.json'

def get_project_id():
    with open(KEY_FILE, 'r') as f:
        return json.load(f)['project_id']

def materialize_tables():
    project_id = get_project_id()
    client = bigquery.Client.from_service_account_json(KEY_FILE)
    dataset_id = f"{project_id}.supermarket_analytics"
    
    print(f"Materializing Native Tables in {dataset_id}...")
    
    # We will create copies: orders -> orders_native
    tables = ['orders', 'products', 'stores']
    
    for table in tables:
        source_id = f"{dataset_id}.{table}"
        target_id = f"{dataset_id}.{table}_native"
        
        print(f" - Converting {table} to Native Table...", end="")
        
        # SQL to copy data
        query = f"""
            CREATE OR REPLACE TABLE `{target_id}` AS
            SELECT * FROM `{source_id}`
        """
        
        try:
            job = client.query(query)
            job.result() # Wait for completion
            print(f" ✅ Created {table}_native")
        except Exception as e:
            print(f" ❌ Failed: {e}")

    print("\nDone! Try refreshing Power BI and looking for 'orders_native' etc.")

if __name__ == "__main__":
    materialize_tables()
