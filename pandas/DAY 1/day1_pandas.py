import pandas as pd

# In real DE jobs, data comes from CSV, Excel, APIs, databases
# Today we simulate a raw messy sales file — exactly like real world

data = {
    'order_id':    [101, 102, 103, 104, 105, 106, 107, 108],
    'customer':    ['Alice', 'Bob', None, 'Diana', 'Bob', 'Alice', None, 'Eve'],
    'product':     ['Laptop', 'Mouse', 'Keyboard', 'Laptop', 'Monitor', 'Mouse', 'Keyboard', 'Laptop'],
    'category':    ['Electronics', 'Electronics', 'Electronics', 'Electronics', 'Electronics', 'Electronics', 'Electronics', 'Electronics'],
    'amount':      [75000, 500, 1500, 80000, 12000, 450, 1500, 90000],
    'quantity':    [1, 2, 1, 1, 1, 3, 2, 1],
    'order_date':  ['2024-01-15', '2024-01-15', '2024-01-16', None, '2024-01-17', '2024-01-17', '2024-01-18', '2024-01-18'],
    'status':      ['completed', 'completed', 'pending', 'completed', 'cancelled', 'completed', 'pending', 'completed']
}

# data frame is the core data structure in pandas — like a table in SQL or Excel

df=pd.DataFrame(data)
print("=== RAW DATA ===")

print(df)

# STEP 1 — EXPLORATION ──────────────────────────────────────────
# First thing any DE does — understand the data shape

print("\n=== SHAPE (rows, columns) ===")
print(df.shape)

print("\n=== COLUMN TYPES ===")
print(df.dtypes)
# Notice: order_date is 'object' not datetime — this is a real problem!

print("\n=== BASIC STATS ===")
print(df.describe())
# This gives you min, max, mean of numeric columns instantly

print("\n=== NULL CHECK — most important! ===")
print(df.isnull().sum())
# In real projects, nulls cause pipeline failures — you always check this first




# ── STEP 2 — CLEANING ─────────────────────────────────────────────
# This is the most important DE skill — garbage in, garbage out

# 1. Fix date column — always convert strings to proper datetime
df['order_date'] = pd.to_datetime(df['order_date'])
print("\n=== AFTER DATE FIX ===")
print(df.dtypes)
# Now order_date is datetime64 — correct!

# 2. Handle nulls — real DE decision: fill or drop?
# For customer — we fill with 'Unknown' (business rule: don't lose the order)
df['customer'] = df['customer'].fillna('Unknown')

# For order_date — we drop because we can't guess the date 
# dropna with subset means we only drop rows where order_date is null, not other columns
df = df.dropna(subset=['order_date'])

print("\n=== AFTER CLEANING ===")
print(df.isnull().sum())
# All zeros now — clean data!

# 3. Add a calculated column — revenue = amount * quantity
df['revenue'] = df['amount'] * df['quantity']

# 4. Filter only completed orders — like a WHERE clause in SQL
completed = df[df['status'] == 'completed']
print("\n=== COMPLETED ORDERS ONLY ===")
print(completed[['order_id', 'customer', 'product', 'revenue']])






# ── STEP 3 — ANALYSIS ─────────────────────────────────────────────
# This is what business teams ask DE to deliver every single day

# 1. Total revenue by product — same as SQL GROUP BY
#reset index means we want the result to be a clean dataframe, not a series with product as index
print("\n=== REVENUE BY PRODUCT ===")
product_revenue = df.groupby('product')['revenue'].sum().reset_index()
product_revenue = product_revenue.sort_values('revenue', ascending=False)
print(product_revenue)

# 2. Order count by customer
print("\n=== ORDERS PER CUSTOMER ===")
customer_orders = df.groupby('customer')['order_id'].count().reset_index()
customer_orders.columns = ['customer', 'total_orders']
print(customer_orders)

# 3. Daily revenue trend
print("\n=== DAILY REVENUE ===")
daily = df.groupby('order_date')['revenue'].sum().reset_index()
print(daily)

# 4. Status breakdown — how many orders in each status
print("\n=== STATUS BREAKDOWN ===")
print(df['status'].value_counts())



# ── STEP 4 — SAVE OUTPUT ──────────────────────────────────────────
# In real DE pipelines, you always save cleaned data for next layer

# Save as CSV
df.to_csv('cleaned_sales.csv', index=False)

# Save as Parquet — this is what you'll use in Azure Data Lake!
df.to_parquet('cleaned_sales.parquet', index=False)

print("\n=== FILES SAVED ===")
print("cleaned_sales.csv — for humans to read")
print("cleaned_sales.parquet — for Azure Data Lake (efficient, compressed)")

# Read back parquet to confirm it works
df_check = pd.read_parquet('cleaned_sales.parquet')
print("\n=== PARQUET READ BACK ===")
print(df_check.head())