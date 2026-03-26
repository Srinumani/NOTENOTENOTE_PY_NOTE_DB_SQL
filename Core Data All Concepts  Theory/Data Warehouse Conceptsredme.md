Data Warehousing Concepts
	• OLTP vs OLAP  
	• Fact Table
	• Dimension Table
	• Star Schema
	• Snowflake Schema
	• Surrogate Keys --don’t no
	• Grain Definition --don’t no
	• Slowly Changing Dimensions (SCD Type 1, Type 2)
	• Data Mart vs Data Warehouse--don’t no
 




Fact Table & Dimension Table

🚀 1️⃣️ ⃣  What is Fact Table?
👉 Simple:

Fact table = stores measurable data (numbers, metrics)

🧠 Examples

Sales amount
Order quantity
Revenue
Clicks

🧾 Example: fact_orders

CREATE TABLE fact_orders (
    order_id INT,
    customer_id INT,
    product_id INT,
    amount INT,
    order_date DATE
);

🚀 2️⃣️ ⃣   What is Dimension Table?
👉 Simple:

Dimension table = stores descriptive data (details)

🧠 Examples

Customer name
Product name
City
Category

🧾 Example: dim_customer

CREATE TABLE dim_customer (
    customer_id INT,
    customer_name TEXT,
    city TEXT
);

🧾 Example: dim_product

CREATE TABLE dim_product (
    product_id INT,
    product_name TEXT,
    category TEXT
);

🎯 REAL WORLD SCENARIO
Company like Amazon

📦 Raw Orders Data

INSERT INTO fact_orders VALUES
(1,101,1001,500,'2026-03-01'),
(2,102,1002,300,'2026-03-01'),
(3,101,1003,700,'2026-03-02');

👤 Customer Data

INSERT INTO dim_customer VALUES
(101,'Srinu','Chennai'),
(102,'Ravi','Hyderabad');

📦 Product Data

INSERT INTO dim_product VALUES
(1001,'Mobile','Electronics'),
(1002,'Shoes','Fashion'),
(1003,'Laptop','Electronics');

🔗 How They Connect

fact_orders.customer_id → dim_customer.customer_id
fact_orders.product_id → dim_product.product_id

🔥 REAL QUERY (IMPORTANT)
🎯 Business Question:
👉 “Total sales by customer name”

SELECT c.customer_name, SUM(f.amount) AS total_sales
FROM fact_orders f
JOIN dim_customer c 
ON f.customer_id = c.customer_id
GROUP BY c.customer_name;

💥 FAILURE SCENARIO
❌ Wrong Design

Store everything in one table

🚨 Problem

Data duplication ❌
Hard to maintain ❌
Slow queries ❌

✅ SUCCESS SCENARIO
✔️ Proper Star Schema

Fact table → metrics
Dimension tables → details

🎉 Result

Fast queries ✅
Clean data model ✅
Easy reporting ✅

⭐ STAR SCHEMA (IMPORTANT)

        dim_customer
             |
dim_product — fact_orders — dim_date
             |
        dim_location
👉 Fact table is center ⭐

🧠 HOW TO IDENTIFY (INTERVIEW TRICK 🔥)
👉 Fact Table
Ask:

Is this measurable?
Can we SUM it?
✔️ YES → Fact table

👉 Dimension Table
Ask:

Is this descriptive?
Used for filtering/grouping?
✔️ YES → Dimension table

🧠 REAL ENGINEER THINKING
👉 Example:

amount → Fact ✅
customer_name → Dimension ✅
city → Dimension ✅
order_date → Dimension (dim_date) ✅

🎯 INTERVIEW ANSWER

Fact tables store measurable metrics like sales or revenue, 
while dimension tables store descriptive attributes like customer or product details. 
They are connected using keys to form a star schema for efficient analytics.

 SCD 
1️⃣ What is SCD (Slowly Changing Dimension)?
👉 Simple:

SCD = Handling changes in dimension data over time

🎯 Real Problem
Company like Amazon
👉 Customer moves city:

Old → Chennai
New → Bangalore

❓ Question

Should we update or keep history?
👉 That’s where SCD comes.

🚀 2️⃣️⃣ SCD Type 1 (Overwrite)
👉 Simple:

Update old data (no history)

🧾 Example Table

CREATE TABLE dim_customer (
    customer_id INT,
    name TEXT,
    city TEXT
);

🧪 Before

101 | Srinu | Chennai

🔄 Change

City → Bangalore

✅ Query (Type 1)

UPDATE dim_customer
SET city = 'Bangalore'
WHERE customer_id = 101;

🎯 Result

101 | Srinu | Bangalore

❗ Problem

Old data lost ❌

🚀 3️⃣️⃣ SCD Type 2 (History Maintai🔥🔥)
👉 Simple:

Keep old + new records (track history)

🧾 Table Design

CREATE TABLE dim_customer (
    customer_id INT,
    name TEXT,
    city TEXT,
    start_date DATE,
    end_date DATE,
    is_active INT
);

🧪 Before

101 | Srinu | Chennai | 2025-01-01 | NULL | 1

🔄 Change (City updated)
Step 1: Expire old record

UPDATE dim_customer
SET end_date = '2026-03-01',
    is_active = 0
WHERE customer_id = 101 AND is_active = 1;

Step 2: Insert new record

INSERT INTO dim_customer
VALUES (101, 'Srinu', 'Bangalore', '2026-03-01', NULL, 1);

✅ Final Data

101 | Srinu | Chennai   | 2025-01-01 | 2026-03-01 | 0
101 | Srinu | Bangalore | 2026-03-01 | NULL       | 1

💥 FAILURE SCENARIO
❌ Using Type 1 when history needed

Customer moved → old data lost ❌
Reports become wrong ❌

✅ SUCCESS SCENARIO
✔️ Using Type 2

Full history available ✅
Time-based analysis possible ✅

🧠 WHEN TO USE WHAT?
👉 SCD Type 1

No need history
Example: typo correction, small fixes

👉 SCD Type 2

History required
Example: address, salary, status

🧠 SENIOR ENGINEER TRICK 🔥

Always ask:
Does business need history?
YES → Type 2
NO → Type 1

🎯 INTERVIEW ANSWER

SCD Type 1 overwrites old data without maintaining history, 
while SCD Type 2 preserves historical changes by inserting new records 
and marking old ones as inactive using date ranges or flags.
 




