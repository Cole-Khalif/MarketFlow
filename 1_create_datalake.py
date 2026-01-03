import os
import json
from google.cloud import storage

# Configuration
KEY_FILE = 'service-account-key.json'
REGION = 'us-central1' # Free Tier eligible region

def create_datalake():
    # 1. Load Project ID from Key File
    if not os.path.exists(KEY_FILE):
        print(f"Error: {KEY_FILE} not found. Please ensure you downloaded it.")
        return

    with open(KEY_FILE, 'r') as f:
        key_data = json.load(f)
        project_id = key_data['project_id']

    print(f"Authenticated as Service Account for Project: {project_id}")
    
    # 2. Define Bucket Names (Globally Unique)
    # We use the project_id as a prefix because it's already globally unique.
    buckets = {
        'raw': f"{project_id}-raw",
        'processed': f"{project_id}-processed"
    }

    # 3. Initialize Client
    # logic: if env var is set, it uses it, but passing credentials explicitly 
    # from the file is safer given we have the file right here.
    client = storage.Client.from_service_account_json(KEY_FILE)

    # 4. Create Buckets
    for zone, bucket_name in buckets.items():
        try:
            print(f"Creating {zone} bucket: {bucket_name} in {REGION}...")
            bucket = client.bucket(bucket_name)
            
            # Check if exists first to avoid invalid request
            if not bucket.exists():
                bucket.create(location=REGION)
                print(f"✅ Success: {bucket_name} created.")
            else:
                print(f"ℹ️  Info: {bucket_name} already exists.")
                
        except Exception as e:
            print(f"❌ Error creating {bucket_name}: {e}")

    print("\nData Lake Setup Complete!")
    print("--------------------------------")
    print("Current Buckets:")
    for b in client.list_buckets():
        print(f"- {b.name}")

if __name__ == "__main__":
    create_datalake()
