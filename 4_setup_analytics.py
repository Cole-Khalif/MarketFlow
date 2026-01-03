import json
from google.cloud import bigquery

# Configuration
KEY_FILE = 'service-account-key.json'

def get_project_id():
    with open(KEY_FILE, 'r') as f:
        return json.load(f)['project_id']

def setup_analytics():
    project_id = get_project_id()
    # Construct GCS URI base (using the unique bucket name created earlier)
    # The ETL script uploaded to: {project_id}-processed
    bucket_name = f"{project_id}-processed"
    
    client = bigquery.Client.from_service_account_json(KEY_FILE)
    dataset_id = f"{project_id}.supermarket_analytics"
    
    # 1. Create Dataset
    print(f"Creating Dataset: {dataset_id}...")
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US" # Must match GCS location (we used us-central1 which is US compatible for multi-region or similar)
    # Note: If GCS is US-CENTRAL1, Dataset should be US-CENTRAL1 or US (multi-region). Let's try US.
    
    try:
        client.create_dataset(dataset, exists_ok=True)
        print("✅ Dataset created/exists.")
    except Exception as e:
        print(f"❌ Error creating dataset: {e}")
        return

    # 2. Configure External Tables
    tables = {
        'orders': f'gs://{bucket_name}/orders/*', # Recursive match for all files in orders/
        'products': f'gs://{bucket_name}/products/ref_data/products.parquet',
        'stores': f'gs://{bucket_name}/stores/ref_data/stores.parquet'
    }

    for table_name, gcs_uri in tables.items():
        table_id = f"{dataset_id}.{table_name}"
        print(f"Creating External Table: {table_name} -> {gcs_uri}")
        
        table = bigquery.Table(table_id)
        
        external_config = bigquery.ExternalConfig("PARQUET")
        external_config.source_uris = [gcs_uri]
        # Auto-detect schema from Parquet headers
        external_config.autodetect = True
        
        table.external_data_configuration = external_config
        
        try:
            client.create_table(table, exists_ok=True)
            print(f"✅ Table {table_name} ready.")
        except Exception as e:
            print(f"❌ Error creating table {table_name}: {e}")

    # 3. Run Analysis Query
    print("\nRunning Analysis Query: [Total Revenue by Product Config]")
    query = f"""
        SELECT 
            p.category,
            p.product_name,
            COUNT(o.order_id) as total_orders,
            SUM(o.quantity) as total_units_sold,
            ROUND(SUM(o.quantity * o.unit_price), 2) as total_revenue
        FROM `{dataset_id}.orders` o
        JOIN `{dataset_id}.products` p ON o.product_id = p.product_id
        GROUP BY 1, 2
        ORDER BY 5 DESC
    """
    
    try:
        query_job = client.query(query)
        results = query_job.result()  # Waits for job to complete.
        
        print("\n--- RESULTS ---")
        print(f"{'Category':<15} | {'Product':<15} | {'Orders':<6} | {'Units':<5} | {'Revenue':<10}")
        print("-" * 65)
        for row in results:
            print(f"{row.category:<15} | {row.product_name:<15} | {row.total_orders:<6} | {row.total_units_sold:<5} | ${row.total_revenue:<10}")
            
    except Exception as e:
        print(f"❌ Query failed: {e}")

if __name__ == "__main__":
    setup_analytics()
