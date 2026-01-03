import json

# Read the key file
with open('service-account-key.json', 'r') as f:
    key_data = json.load(f)

# Convert back to string, but compact (no indent)
# This removes newlines and spaces, making it safe for the Power BI single-line input
compact_json = json.dumps(key_data)

print("\n--- COPY THE TEXT BELOW THIS LINE ---")
print(compact_json)
print("--- END OF TEXT ---\n")
print(f"Key email: {key_data['client_email']}")
