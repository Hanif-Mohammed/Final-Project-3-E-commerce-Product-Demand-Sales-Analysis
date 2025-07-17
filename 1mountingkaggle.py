#mountingkaggle

%pip install kaggle
import shutil, os

#  Use the corrected uploaded file path
uploaded_path = "/dbfs/FileStore/tables/kaggle_json-1.json"
target_path = "/root/.kaggle/kaggle.json"

# Ensure the ~/.kaggle directory exists
os.makedirs("/root/.kaggle", exist_ok=True)

#  Copy the file to the Kaggle expected location
shutil.copy(uploaded_path, target_path)

#  Set required permissions
os.chmod(target_path, 600)

print(" kaggle.json is moved and configured successfully!")

import shutil, os

# Make sure the config directory exists
os.makedirs("/root/.config/kaggle", exist_ok=True)

# Copy kaggle.json to the expected location
shutil.copy("/root/.kaggle/kaggle.json", "/root/.config/kaggle/kaggle.json")

# Optional: reset permissions again
os.chmod("/root/.config/kaggle/kaggle.json", 600)

print(" kaggle.json copied to /root/.config/kaggle/")

import kaggle

# Re-authenticate
kaggle.api.authenticate()

# Download RetailRocket dataset
kaggle.api.dataset_download_files(
    'retailrocket/ecommerce-dataset',
    path='/dbfs/tmp/',
    unzip=False
)

print(" Dataset downloaded successfully!")

import zipfile

zip_path = "/dbfs/tmp/ecommerce-dataset.zip"
extract_path = "/dbfs/tmp/ecommerce_dataset"

# Unzip all contents
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(extract_path)

print(" Files extracted to", extract_path)

# Set your Azure Storage config
storage_account_name = "storageecommerce"
storage_account_key = "CLZSIPRieeBvBCfFN8yljWxg9Tp28faTS2nlO9LrBnfA0tFgc5bW0XZKAPFLhO1XZkBymz80mZsq+AStuywHWg=="

# Spark config to allow writing to ADLS Gen2
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
    storage_account_key
)

# Define local-to-ADLS path mappings
files_to_upload = {
    "events.csv": "events",
    "item_properties_part1.csv": "item_properties",
    "item_properties_part2.csv": "item_properties",
    "category_tree.csv": "category"
}

# Upload each CSV to the correct Bronze folder
for filename, folder in files_to_upload.items():
    local_path = f"/dbfs/tmp/ecommerce_dataset/{filename}"
    adls_path = f"wasbs://bronzelayer@{storage_account_name}.blob.core.windows.net/{folder}/{filename}"

    print(f" Uploading {filename} → {adls_path}")
    dbutils.fs.cp(f"file:{local_path}", adls_path)
