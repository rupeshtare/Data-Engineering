# cloud_storage_basics.py
# Beginner Level: Understanding how code talks to the Cloud (S3/ADLS)

# In a real project, you would use 'boto3' for AWS or 'azure-storage-blob' for Azure.
# This script simulates the logic of uploading a file to a Data Lake.

def upload_to_datalake(file_path, bucket_name):
    # 1. ARCHITECT'S CHECK: Does the file exist?
    print(f"--- [LOCAL] Checking file: {file_path} ---")
    
    # 2. THE CONNECTION (Simulated)
    print(f"--- [CLOUD] Connecting to Bucket: {bucket_name} ---")
    
    # 3. THE UPLOAD
    print(f"--- [ACTION] Uploading {file_path} to {bucket_name}/raw_zone/ ---")
    print("SUCCESS: File is now safely stored in the Cloud.")

# Run the simulation
upload_to_datalake("daily_sales.parquet", "my-company-datalake")

# 🏛️ Architect's Tip:
# "In production, never hardcode your 'Access Keys'. Use IAM Roles 
# or Managed Identities. This way, your code has permission to 
# write to the cloud without you ever typing a password."
