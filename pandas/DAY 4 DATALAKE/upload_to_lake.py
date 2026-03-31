from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
import os

# ── PASTE YOUR CONNECTION STRING HERE ────────────────────
CONNECTION_STRING =   "<YOUR_CONNECTION_STRING_HERE>"
# ── STEP 1: CREATE SMALL SAMPLE DATA ─────────────────────
# Simple orders data — like a small shop
data = {
    'order_id':   [1, 2, 3, 4, 5],
    'customer':   ['Raj', 'Priya', 'Kumar', 'Anita', 'Ravi'],
    'product':    ['Laptop', 'Phone', 'Tablet', 'Mouse', 'Keyboard'],
    'amount':     [75000, 25000, 35000, 1500, 2000],
    'status':     ['completed', 'completed', 'pending', 'completed', 'cancelled']
}

df= pd.DataFrame(data)

print("Sample Data:")
print(df)



# ── STEP 2: SAVE AS PARQUET LOCALLY ──────────────────────
df.to_parquet('orders_raw.parquet', index=False)
print("\nSaved locally as orders_raw.parquet")

# ── STEP 3: CONNECT TO YOUR LAKE ─────────────────────────
client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
print("\nConnected to Azure Data Lake!")

# ── STEP 4: UPLOAD TO BRONZE ──────────────────────────────
blob_client = client.get_blob_client(
    container="bronze",
    blob="orders/orders_raw.parquet"
    #     👆 container   👆 folder/filename
)

with open("orders_raw.parquet", "rb") as f:
    blob_client.upload_blob(f, overwrite=True)

print("Uploaded to bronze/orders/orders_raw.parquet ✅")

# ── STEP 5: VERIFY IT'S THERE ────────────────────────────
print("\n=== FILES IN YOUR BRONZE CONTAINER ===")
container_client = client.get_container_client("bronze")
for blob in container_client.list_blobs():
    print(f"  {blob.name}")

print("\nYour file is now in Azure Data Lake buddy!")
print("Go to portal.azure.com → your storage account → bronze → orders")
print("You will see orders_raw.parquet sitting there!")