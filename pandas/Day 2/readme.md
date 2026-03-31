Setup (5 mins)

pip install requests


Interview Questions


What is the difference between LEFT JOIN and INNER JOIN — give a real example?
Why did we add a fetched_at column when pulling from API?
What is a business key and why do we dedupe on it?
What is Bronze / Silver / Gold — explain in your own words?
Why Parquet over CSV for saving?

1. LEFT JOIN vs INNER JOIN — Real Example
Imagine you work at Swiggy as a Data Engineer.
You have two tables:
Table A — All customers who registered (500 users)
Table B — Customers who placed orders (320 users)
INNER JOIN → gives you only 320 users (who registered AND ordered)
LEFT JOIN  → gives you all 500 users (ordered ones have order data, rest have NULL)
When does this matter in real life?
Your manager asks — "How many users registered but NEVER ordered?"
If you used INNER JOIN — those 180 users are gone from your result. You'd tell your manager 0 users never ordered. Wrong answer. Pipeline bug. You're in trouble.
If you used LEFT JOIN — you see all 500, filter where order data is NULL = 180 users never ordered. Correct answer.

Senior rule: When your LEFT table is your source of truth — always LEFT JOIN. INNER JOIN only when both sides must exist.


2. Why fetched_at column?
Three real reasons buddy:
Reason 1 — Debugging
Your pipeline ran at 9am and 9pm. Data looks different. How do you know which run caused the problem? fetched_at tells you exactly.
Reason 2 — Incremental loading
Tomorrow you only want to load new data since last run. You query WHERE fetched_at > last_run_time. Without this column — you reload everything every day. Expensive and slow.
Reason 3 — Audit trail
In banking, healthcare, any regulated industry — auditors ask "when did this data enter your system?" Without fetched_at you have no answer. Companies have been fined for this.

Senior rule: Every table that gets data from outside — always add fetched_at, created_at, source_system. These are called audit columns. Add them by habit, not by reminder.


3. Business Key — What & Why
Business key = the column that makes a record unique in the real world. Not a database ID — a real world identifier.
Examples:
TableBusiness KeyOrdersorder_idCustomersemail or phoneCountriescountry_name or country_codeProductsproduct_skuEmployeesemployee_id
Why dedupe on business key and not all columns?
Imagine same order came twice — once with fetched_at = 9am and once with fetched_at = 9pm. If you dedupe on ALL columns — both rows survive because fetched_at is different. You now have duplicate orders in your warehouse. Finance team double counts revenue. Big problem.
If you dedupe on order_id (business key) — first occurrence kept, duplicate removed. Correct.

Senior rule: Always ask — "What makes this record unique in the real world?" That's your business key. Dedupe on that, not on all columns.


4. Bronze / Silver / Gold — In Your Own Words
Think of it like processing raw gold ore in a mine:
Bronze = Raw, untouched
Exactly as it came from the source. API response, CSV dump, database export. No changes. No cleaning. You keep this forever as your backup. If something goes wrong downstream — you come back here and reprocess. In Azure this lives in your Data Lake raw folder.
Silver = Cleaned, trusted
You cleaned nulls, fixed data types, removed duplicates, joined tables, added audit columns. Data is now reliable and consistent. Not yet shaped for business questions — but clean enough that any team can use it. In Azure this is your Data Lake processed folder.
Gold = Business ready
Aggregated, summarized, shaped for specific business questions. Revenue by product. Orders by region. Monthly trends. This is what Power BI dashboards, reports, and analysts consume directly. In Azure this lives in Synapse or a data warehouse.
Real example from our Day 2 code:
Bronze → raw API countries data         (as-is from internet)
Silver → joined with GDP, cleaned nulls (trustworthy)
Gold   → subregion summary aggregated   (business ready)

Senior rule: Never skip Bronze. Even if data looks clean — store raw first. You'll thank yourself 6 months later when source system changes and you need to reprocess.


5. Parquet vs CSV
Let me show you with real numbers buddy:
FactorCSVParquetFile size (1M rows)~500 MB~50 MBRead speedSlow10x fasterColumn selectReads entire fileReads only needed columnsData typesEverything is textPreserves int, float, dateCompressionNone built-inBuilt-in (snappy, gzip)Azure Data Lake costHigh (big files)Low (small files)
Real world impact:
Your pipeline runs daily on 10 million rows. CSV = reads 5GB every run. Parquet = reads 500MB. Over a month that's 150GB vs 15GB of data scanned. In Azure you pay per GB scanned. CSV costs 10x more money.
Also — CSV stores everything as text. When you read a date column from CSV, Pandas reads it as string, you must convert it. Parquet stores it as actual date. Zero conversion needed.

Senior rule: CSV is for humans to read. Parquet is for machines to process. In any pipeline that runs at scale — always Parquet.









-----------------Medallion Architecture-----------------------------

What actually happens on your computer / Azure right now 👇
🥉
Bronze Layer — Raw Storage
Save everything exactly as it came. Touch nothing.
What goes in
Raw API response, CSV dump from client, database export — exactly as received. Nulls, duplicates, wrong types — all kept.
What you do to it
Nothing. Zero changes. Just save it as-is. This is your safety net forever.
📄
bronze_countries.parquet
rawuntouched
# Our Day 2 code — we just saved raw API data
df_countries.to_parquet('bronze_countries.parquet')
Senior rule 💬
If your Silver or Gold pipeline breaks 3 months later, you come back to Bronze and reprocess. Without Bronze — you have to call the API again, pray the data is same. It never is. Bronze saves your job.
↓
🥈
Silver Layer — Cleaned & Trusted
Clean, join, fix types. Data is now reliable.
What goes in
Bronze data — messy, raw, untrusted.
What you do to it
Remove nulls, fix dates, remove duplicates, join tables, add audit columns, standardize formats.
📄
silver_countries.parquet
cleanedjoinedtrusted
# Our Day 2 code — cleaned + joined with GDP
df_left = pd.merge(df_countries, df_gdp, on='country_name', how='left')
df_left['gdp_usd_billion'] = df_left['gdp_usd_billion'].fillna(0)
df_left.to_parquet('silver_countries.parquet')
Senior rule 💬
Silver is where data quality lives. Any analyst, any team can trust Silver data. But it's not shaped for any specific question yet — it's just clean and complete.
↓
🥇
Gold Layer — Business Ready
Aggregated, summarized, shaped for reports & dashboards.
What goes in
Silver data — clean and trusted.
What you do to it
GROUP BY, SUM, AVG, COUNT. Shape it to answer specific business questions. Power BI reads from here.
📄
gold_subregion_summary.parquet
aggregateddashboard-ready
# Our Day 2 code — grouped by subregion
subregion_summary = df.groupby('subregion').agg(
  country_count=('country_name','count'),
  total_population=('population','sum')
).reset_index()
subregion_summary.to_parquet('gold_subregion_summary.parquet')
Senior rule 💬
Gold is what business teams see. Manager opens Power BI — they're reading Gold. You can have multiple Gold tables for different teams — finance gold, sales gold, operations gold — all from same Silver.
Where these files actually live — Local vs Azure
1
Right now on your computer 💻
bronze_countries.parquet, silver_countries.parquet, gold_subregion_summary.parquet — sitting in your de_journey folder. That IS a medallion architecture. Small scale, but exact same concept.
2
In Azure Data Lake (next week) ☁️
Same files, same names — but stored in Azure Blob folders: /bronze/, /silver/, /gold/. ADF moves data in. Databricks transforms Bronze → Silver → Gold. Synapse reads Gold.
3
In a real company project 🏢
Swiggy example — Bronze: raw orders from app. Silver: cleaned orders, joined with restaurant data. Gold: daily revenue by city, top restaurants, delivery time averages. Power BI reads Gold.