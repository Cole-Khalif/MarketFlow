try:
    print("Python works.")
    import google.cloud.storage
    print("Import successful.")
except Exception as e:
    print(f"Import failed: {e}")
