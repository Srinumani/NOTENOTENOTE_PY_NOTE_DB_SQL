import pandas as pd
import duckdb
from datetime import datetime, timedelta
import random

# ── CREATE REALISTIC DATA ─────────────────────────────────
# Simulating an e-commerce company's orders table
# 50 rows — realistic enough to see patterns
# what is random seed?
#  Random seed is a value that initializes the random number generator. It ensures that the sequence of random numbers generated is reproducible. By setting the same seed, you can get the same sequence of random numbers every time you run the code.

random.seed(42)  # so everyone gets same data

customers = ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve',
             'Frank', 'Grace', 'Henry', 'Iris', 'Jack']
products  = ['Laptop', 'Phone', 'Tablet', 'Mouse',
             'Keyboard', 'Monitor', 'Headphones', 'Charger']
regions   = ['North', 'South', 'East', 'West']
statuses  = ['completed', 'completed', 'completed', 'pending', 'cancelled']

orders = []
base_date = datetime(2024, 1, 1)

for i in range(1, 51):
    orders.append({
        'order_id':   i,
        'customer':   random.choice(customers),
        'product':    random.choice(products),
        'region':     random.choice(regions),
        'amount':     round(random.uniform(500, 80000), 2),
        'quantity':   random.randint(1, 5),
        'status':     random.choice(statuses),
        'order_date': (base_date + timedelta(days=random.randint(0, 89))).strftime('%Y-%m-%d')
    })

df_orders = pd.DataFrame(orders)
df_orders['order_date'] = pd.to_datetime(df_orders['order_date'])
df_orders['revenue'] = (df_orders['amount'] * df_orders['quantity']).round(2)

print("=== ORDERS DATA CREATED ===")
print(df_orders.head())
print(f"Shape: {df_orders.shape}")




# ── DATABASE CONNECTION ───────────────────────────────────
# This pattern is IDENTICAL to connecting to Azure SQL
# Only the connection string changes — everything else is same

# Create a local DuckDB database
conn = duckdb.connect('ecommerce.db')

# Write DataFrame to database — like loading into a data warehouse
conn.execute("DROP TABLE IF EXISTS orders")
conn.execute("""
    CREATE TABLE orders AS
    SELECT * FROM df_orders
""")

print("\n=== DATA LOADED TO DATABASE ===")

# Verify it loaded correctly
result = conn.execute("SELECT COUNT(*) as total_rows FROM orders").fetchdf()
print(result)

# ── THIS IS THE KEY PATTERN ───────────────────────────────
# In real DE jobs:
# Source DB → pandas DataFrame → Target DB / Data Lake
# You just did: DataFrame → Database
# Tomorrow in Azure: DataFrame → Parquet → Data Lake





# ── SQL FROM PYTHON ───────────────────────────────────────
# Real DE skill — sometimes SQL is faster than Pandas
# Good DE knows WHEN to use SQL vs Pandas

# 1. Basic query — read SQL result into DataFrame
print("\n=== QUERY 1: Completed orders only ===")
df_completed = conn.execute("""
    SELECT order_id, customer, product, revenue
    FROM orders
    WHERE status = 'completed'
    ORDER BY revenue DESC
    LIMIT 10
""").fetchdf()
print(df_completed)

# 2. Aggregation in SQL
print("\n=== QUERY 2: Revenue by region ===")
df_region = conn.execute("""
    SELECT
        region,
        COUNT(order_id)     AS total_orders,
        SUM(revenue)        AS total_revenue,
        ROUND(AVG(revenue), 2) AS avg_revenue
    FROM orders
    WHERE status = 'completed'
    GROUP BY region
    ORDER BY total_revenue DESC
""").fetchdf()
print(df_region)

# 3. Monthly trend
print("\n=== QUERY 3: Monthly revenue trend ===")
df_monthly = conn.execute("""
    SELECT
        STRFTIME(order_date, '%Y-%m') AS month,
        COUNT(order_id)               AS orders,
        ROUND(SUM(revenue), 2)        AS revenue
    FROM orders
    WHERE status = 'completed'
    GROUP BY month
    ORDER BY month
""").fetchdf()
print(df_monthly)

# 4. Top customer per region
print("\n=== QUERY 4: Top customer per region ===")
df_top_customer = conn.execute("""
    SELECT region, customer, total_revenue
    FROM (
        SELECT
            region,
            customer,
            ROUND(SUM(revenue), 2) AS total_revenue,
            RANK() OVER (PARTITION BY region ORDER BY SUM(revenue) DESC) AS rnk
        FROM orders
        WHERE status = 'completed'
        GROUP BY region, customer
    )
    WHERE rnk = 1
""").fetchdf()
print(df_top_customer)





# ── WINDOW FUNCTIONS ──────────────────────────────────────
# You already know these in SQL buddy — now in Pandas
# This is asked in EVERY DE interview — "do window functions in Pandas"

df = df_orders[df_orders['status'] == 'completed'].copy()
df = df.sort_values(['customer', 'order_date']).reset_index(drop=True)

# 1. RANK — rank orders by revenue per customer
# SQL: RANK() OVER (PARTITION BY customer ORDER BY revenue DESC)
df['revenue_rank'] = df.groupby('customer')['revenue'] \
                       .rank(method='dense', ascending=False)

print("\n=== WINDOW 1: Revenue rank per customer ===")
print(df[['customer', 'order_date', 'revenue', 'revenue_rank']]
      .sort_values(['customer', 'revenue_rank'])
      .head(15))

# 2. RUNNING TOTAL — cumulative revenue per customer
# SQL: SUM(revenue) OVER (PARTITION BY customer ORDER BY order_date)
df['running_total'] = df.groupby('customer')['revenue'] \
                        .cumsum()

print("\n=== WINDOW 2: Running total per customer ===")
print(df[['customer', 'order_date', 'revenue', 'running_total']]
      .sort_values(['customer', 'order_date'])
      .head(15))

# 3. LAG — previous order amount (did customer spend more or less?)
# SQL: LAG(revenue) OVER (PARTITION BY customer ORDER BY order_date)
df['prev_order_revenue'] = df.groupby('customer')['revenue'] \
                             .shift(1)

df['revenue_change'] = (df['revenue'] - df['prev_order_revenue']).round(2)
df['trend'] = df['revenue_change'].apply(
    lambda x: 'UP' if pd.notna(x) and x > 0
    else ('DOWN' if pd.notna(x) and x < 0
    else 'FIRST ORDER')
)

print("\n=== WINDOW 3: Revenue trend per customer ===")
print(df[['customer', 'order_date', 'revenue', 'prev_order_revenue', 'trend']]
      .sort_values(['customer', 'order_date'])
      .head(15))

# 4. MOVING AVERAGE — 3 order rolling average per customer
# SQL: AVG(revenue) OVER (PARTITION BY customer ORDER BY order_date ROWS 2 PRECEDING)
df['moving_avg_3'] = df.groupby('customer')['revenue'] \
                       .transform(lambda x: x.rolling(3, min_periods=1).mean()) \
                       .round(2)

print("\n=== WINDOW 4: 3-order moving average per customer ===")
print(df[['customer', 'order_date', 'revenue', 'moving_avg_3']]
      .sort_values(['customer', 'order_date'])
      .head(15))

# 5. PERCENT OF TOTAL — each order's % of customer's total revenue
# SQL: revenue / SUM(revenue) OVER (PARTITION BY customer)
df['customer_total'] = df.groupby('customer')['revenue'] \
                         .transform('sum')
df['pct_of_customer_total'] = \
    ((df['revenue'] / df['customer_total']) * 100).round(2)

print("\n=== WINDOW 5: % of customer total ===")
print(df[['customer', 'revenue', 'customer_total', 'pct_of_customer_total']]
      .sort_values(['customer', 'pct_of_customer_total'], ascending=[True, False])
      .head(15))




# ── WHEN SQL vs WHEN PANDAS ───────────────────────────────
# This is what separates senior DEs from juniors

print("\n=== SENIOR DECISION GUIDE ===")
print("""
USE SQL WHEN:
  - Data is already in a database
  - Simple aggregations, filters, joins
  - Team knows SQL better than Python
  - Performance matters on large data

USE PANDAS WHEN:
  - Data comes from files, APIs
  - Complex transformations (reshape, pivot)
  - Machine learning feature engineering
  - Data exploration and profiling
  - Writing to multiple output formats

USE BOTH (most common in real jobs):
  - SQL to extract and filter from DB
  - Pandas to transform and enrich
  - Save back to DB or Data Lake
""")

# Close connection — always clean up!
conn.close()
print("Database connection closed.")








# Re-open to read final clean data
conn = duckdb.connect('ecommerce.db')

df_final = conn.execute("SELECT * FROM orders").fetchdf()
conn.close()

# Save all three layers
df_final.to_parquet('bronze_orders.parquet', index=False)

# Silver — completed only with window functions
df_silver = df.copy()
df_silver.to_parquet('silver_orders_windowed.parquet', index=False)

# Gold — region summary
df_region.to_parquet('gold_region_summary.parquet', index=False)

print("\n=== ALL FILES SAVED ===")
print("bronze_orders.parquet")
print("silver_orders_windowed.parquet")
print("gold_region_summary.parquet")
print("\nDay 3 done buddy! You're building like a real DE now!")


df_check = pd.read_parquet('bronze_orders.parquet')
print("\n=== CHECK BRONZE PARQUET ===")
print(df_check.head())

df_check = pd.read_parquet('silver_orders_windowed.parquet')
print("\n=== CHECK SILVER PARQUET ===")     
print(df_check.head())

df_check = pd.read_parquet('gold_region_summary.parquet')
print("\n=== CHECK GOLD PARQUET ===")       
print(df_check.head())