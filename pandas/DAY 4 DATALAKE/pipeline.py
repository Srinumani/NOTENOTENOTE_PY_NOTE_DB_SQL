from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
import os

CONNECTION_STRING =   "<YOUR_CONNECTION_STRING_HERE>"
client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
print("Connected to Azure Data Lake!")

# ══════════════════════════════════════════════
# STEP 1 — READ FROM BRONZE
# ══════════════════════════════════════════════
print("\n=== STEP 1: Reading from Bronze ===")

blob_client = client.get_blob_client(
    container="bronze",
    blob="orders/orders_raw.parquet"
)

# Download and read directly into Pandas
download = blob_client.download_blob()
data     = download.readall()
df       = pd.read_parquet(io.BytesIO(data))

print("Read from Azure successfully!")
print(df)

# ══════════════════════════════════════════════
# STEP 2 — CLEAN THE DATA (Silver logic)
# ══════════════════════════════════════════════
print("\n=== STEP 2: Cleaning Data ===")

# Keep only completed orders
df_clean = df[df['status'] == 'completed'].copy()

# Add a revenue column
df_clean['revenue'] = df_clean['amount']

# Add audit column — when was this processed
df_clean['processed_at'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')

print("Cleaned data:")
print(df_clean)

# ══════════════════════════════════════════════
# STEP 3 — UPLOAD CLEAN DATA TO SILVER
# ══════════════════════════════════════════════
print("\n=== STEP 3: Uploading to Silver ===")

# Convert DataFrame to parquet bytes in memory
# No need to save locally — goes straight to Azure!
buffer = io.BytesIO()
df_clean.to_parquet(buffer, index=False)
buffer.seek(0)

blob_client = client.get_blob_client(
    container="silver",
    blob="orders/orders_clean.parquet"
)

blob_client.upload_blob(buffer, overwrite=True)
print("Uploaded to silver/orders/orders_clean.parquet ✅")

# ══════════════════════════════════════════════
# STEP 4 — CREATE GOLD SUMMARY + UPLOAD
# ══════════════════════════════════════════════
print("\n=== STEP 4: Creating Gold Summary ===")

df_gold = df_clean.groupby('product').agg(
    total_revenue = ('revenue', 'sum'),
    total_orders  = ('order_id', 'count')
).reset_index()

print("Gold summary:")
print(df_gold)

# Upload gold
buffer_gold = io.BytesIO()
df_gold.to_parquet(buffer_gold, index=False)
buffer_gold.seek(0)

blob_client = client.get_blob_client(
    container="gold",
    blob="reports/product_summary.parquet"
)
blob_client.upload_blob(buffer_gold, overwrite=True)
print("Uploaded to gold/reports/product_summary.parquet ✅")

# ══════════════════════════════════════════════
# STEP 5 — VERIFY ALL 3 LAYERS
# ══════════════════════════════════════════════
print("\n=== YOUR DATA LAKE RIGHT NOW ===")
for container_name in ['bronze', 'silver', 'gold']:
    container_client = client.get_container_client(container_name)
    blobs = list(container_client.list_blobs())
    print(f"\n  {container_name.upper()}/")
    for blob in blobs:
        size = blob.size / 1024
        print(f"    {blob.name}  ({size:.1f} KB)")

print("\nBronze to Silver to Gold pipeline complete!")
print("Go check your Azure portal — all 3 containers have files now! 🎉")




# ### What Just Happened — In Simple Words

# Azure Bronze  →  Python read it  →  Python cleaned it
# →  Azure Silver  →  Python summarized it  →  Azure Gold