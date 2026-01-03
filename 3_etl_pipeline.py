import os
import json
import pandas as pd
from io import BytesIO
from google.cloud import storage

# Configuration
KEY_FILE = 'service-account-key.json'

def get_project_id():
    with open(KEY_FILE, 'r') as f:
        return json.load(f)['project_id']

def process_and_upload(client, raw_bucket_name, processed_bucket_name, prefix, schema_map=None):
    """
    Reads CSVs from Raw, converts to Parquet, Uploads to Processed.
    """
    raw_bucket = client.bucket(raw_bucket_name)
    processed_bucket = client.bucket(processed_bucket_name)

    print(f"\nProcessing prefix: {prefix}...")
    
    blobs = raw_bucket.list_blobs(prefix=prefix)
    count = 0
    
    for blob in blobs:
        if not blob.name.endswith('.csv'):
            continue
            
        print(f" - Reading {blob.name}...", end="")
        
        # 1. Download to Memory (Buffer)
        data = blob.download_as_bytes()
        df = pd.read_csv(BytesIO(data))
        
        # 2. Transform (Simple type casting / cleaning could go here)
        if schema_map:
            df = df.astype(schema_map)
            
        print(f" Rows: {len(df)}...", end="")

        # 3. Convert to Parquet (Buffer)
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        
        # 4. Construct New Path
        # raw/orders/2025-01-01/orders.csv -> orders/2025-01-01/orders.parquet
        new_name = blob.name.replace('raw/', '').replace('.csv', '.parquet')
        # Actually our raw path is 'orders/...' (no raw prefix in the key itself, just in bucket name)
        # But we might want to keep the same structure found in raw bucket?
        # Yes: 'orders/2025-01-01/orders.csv' -> 'orders/2025-01-01/orders.parquet'
        new_name = blob.name.replace('.csv', '.parquet')
        
        # 5. Upload to Processed
        new_blob = processed_bucket.blob(new_name)
        new_blob.upload_from_file(parquet_buffer, content_type='application/octet-stream', rewind=True)
        print(f" ✅ Uploaded to {processed_bucket_name}/{new_name}")
        count += 1
        
    if count == 0:
        print("   No files found.")

def run_etl():
    project_id = get_project_id()
    raw_bucket = f"{project_id}-raw"
    processed_bucket = f"{project_id}-processed"
    
    client = storage.Client.from_service_account_json(KEY_FILE)
    
    print(f"Starting ETL Job...")
    print(f"Source: {raw_bucket}")
    print(f"Target: {processed_bucket}")

    # 1. Process Dimensions (products, stores)
    # They are small, so straightforward processing
    process_and_upload(client, raw_bucket, processed_bucket, 'stores/')
    process_and_upload(client, raw_bucket, processed_bucket, 'products/')

    # 2. Process Facts (orders)
    # We enforce schema for orders to ensure dates are correct for partitioning later
    orders_schema = {
        'order_id': 'string',
        'store_id': 'string',
        'product_id': 'string',
        'quantity': 'int64',
        'unit_price': 'float64'
    }
    # Note: timestamp needs special handling usually, but pandas infers well. 
    # Validating it is 'object' or 'datetime' is good.
    
    process_and_upload(client, raw_bucket, processed_bucket, 'orders/', schema_map=orders_schema)
    
    print("\nETL Job Complete!")

if __name__ == "__main__":
    run_etl()
