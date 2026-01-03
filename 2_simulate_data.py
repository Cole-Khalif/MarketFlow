import os
import json
import random
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import storage

# Configuration
KEY_FILE = 'service-account-key.json'
LOCAL_DATA_DIR = 'data'

def get_project_id():
    with open(KEY_FILE, 'r') as f:
        return json.load(f)['project_id']

def generate_data():
    print("Generating simulation data...")
    if not os.path.exists(LOCAL_DATA_DIR):
        os.makedirs(LOCAL_DATA_DIR)

    # 1. Stores
    stores_data = [
        {'store_id': 'S001', 'location': 'New York - Downtown'},
        {'store_id': 'S002', 'location': 'New York - Queens'},
        {'store_id': 'S003', 'location': 'Los Angeles - Beverly Hills'},
        {'store_id': 'S004', 'location': 'Chicago - Loop'}
    ]
    df_stores = pd.DataFrame(stores_data)
    df_stores.to_csv(f'{LOCAL_DATA_DIR}/stores.csv', index=False)
    print(f" - Generated {len(df_stores)} stores.")

    # 2. Products
    products_data = [
        {'product_id': 'P001', 'product_name': 'Apple', 'category': 'Fruit', 'price': 0.50},
        {'product_id': 'P002', 'product_name': 'Banana', 'category': 'Fruit', 'price': 0.30},
        {'product_id': 'P003', 'product_name': 'Milk', 'category': 'Dairy', 'price': 3.50},
        {'product_id': 'P004', 'product_name': 'Bread', 'category': 'Bakery', 'price': 2.50},
        {'product_id': 'P005', 'product_name': 'Eggs', 'category': 'Dairy', 'price': 4.00},
        {'product_id': 'P006', 'product_name': 'Chicken Breast', 'category': 'Meat', 'price': 9.00}
    ]
    df_products = pd.DataFrame(products_data)
    df_products.to_csv(f'{LOCAL_DATA_DIR}/products.csv', index=False)
    print(f" - Generated {len(df_products)} products.")

    # 3. Orders (Simulate 100 orders for today)
    orders = []
    start_date = datetime.now()
    for i in range(100):
        order_time = start_date - timedelta(minutes=random.randint(0, 1440))
        store = random.choice(stores_data)
        product = random.choice(products_data)
        quantity = random.randint(1, 5)
        
        orders.append({
            'order_id': f'ORD-{1000+i}',
            'order_timestamp': order_time.strftime('%Y-%m-%d %H:%M:%S'),
            'store_id': store['store_id'],
            'product_id': product['product_id'],
            'quantity': quantity,
            'unit_price': product['price'] # Denormalized price at time of purchase
        })
    df_orders = pd.DataFrame(orders)
    
    # Save partitioned by date
    today_str = datetime.now().strftime('%Y-%m-%d')
    orders_file = f'{LOCAL_DATA_DIR}/orders_{today_str}.csv'
    df_orders.to_csv(orders_file, index=False)
    print(f" - Generated {len(df_orders)} orders.")
    
    return today_str

def upload_to_gcs(today_str):
    project_id = get_project_id()
    bucket_name = f"{project_id}-raw"
    client = storage.Client.from_service_account_json(KEY_FILE)
    bucket = client.bucket(bucket_name)

    print(f"\nUploading to GCS Bucket: {bucket_name}...")

    # Upload Schema: 
    # raw/stores/ref_data/stores.csv
    # raw/products/ref_data/products.csv
    # raw/orders/yyyy-mm-dd/orders.csv

    uploads = [
        (f'{LOCAL_DATA_DIR}/stores.csv', 'stores/ref_data/stores.csv'),
        (f'{LOCAL_DATA_DIR}/products.csv', 'products/ref_data/products.csv'),
        (f'{LOCAL_DATA_DIR}/orders_{today_str}.csv', f'orders/{today_str}/orders.csv')
    ]

    for local_path, gcs_path in uploads:
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_path)
        print(f" ✅ Uploaded: {gcs_path}")

if __name__ == "__main__":
    today = generate_data()
    upload_to_gcs(today)
