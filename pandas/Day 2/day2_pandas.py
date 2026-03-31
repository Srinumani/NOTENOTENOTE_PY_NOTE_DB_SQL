import pandas as pd
import requests
from datetime import datetime

# ── REAL API CALL ─────────────────────────────────────────
# We're hitting a real public API — same pattern used in every DE project
# This gives us real country data — population, area, region etc.

print("=== PULLING DATA FROM API ===")

url = "https://restcountries.com/v3.1/region/asia"
response = requests.get(url)

# Always check status — in real pipelines, failed API = pipeline failure
if response.status_code == 200:
    print(f"API call successful! Status: {response.status_code}")
    raw_data = response.json()
    print(f"Total countries fetched: {len(raw_data)}")
else:
    print(f"API failed! Status: {response.status_code}")

# ── PARSE THE RESPONSE ────────────────────────────────────
# Real API responses are nested JSON — you must flatten them
# This is one of the most common DE tasks

countries = []
for country in raw_data:
    countries.append({
        'country_name': country.get('name', {}).get('common', 'Unknown'),
        'capital':      country.get('capital', ['Unknown'])[0] if country.get('capital') else 'Unknown',
        'population':   country.get('population', 0),
        'area_km2':     country.get('area', 0),
        'region':       country.get('region', 'Unknown'),
        'subregion':    country.get('subregion', 'Unknown'),
        'currency':     list(country.get('currencies', {}).keys())[0] if country.get('currencies') else 'Unknown',
        'timezones':    country.get('timezones', ['Unknown'])[0],
        'fetched_at':   datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # audit column — always add this!
    })

df_countries = pd.DataFrame(countries)
print("\n=== RAW API DATA ===")
print(df_countries.head(20))
print(f"\nShape: {df_countries.shape}")





# ── SECOND DATA SOURCE ────────────────────────────────────
# Simulate a second table — like coming from a different database
# This has GDP data for some countries (intentionally incomplete — real world!)

gdp_data = {
    'country_name': ['India', 'China', 'Japan', 'South Korea', 'Indonesia',
                     'Saudi Arabia', 'Turkey', 'Thailand', 'Pakistan', 'Bangladesh',
                     'FakeCountry1', 'FakeCountry2'],  # extra rows that won't match
    'gdp_usd_billion': [3750, 17700, 4200, 1700, 1320,
                        1100, 905, 544, 376, 460,
                        999, 888],
    'gdp_growth_pct':  [6.8, 5.2, 1.9, 2.6, 5.3,
                        8.7, 4.5, 3.8, 2.4, 6.0,
                        1.0, 2.0]
}

df_gdp = pd.DataFrame(gdp_data)
print("\n=== GDP DATA (second source) ===")
print(df_gdp)





# ── JOINS — exactly like SQL ──────────────────────────────
# This is what separates a good DE from a bad one
# Know WHEN to use which join — interviewers love this question

# INNER JOIN — only countries that exist in BOTH sources
df_inner = pd.merge(df_countries, df_gdp, on='country_name', how='inner')
print("\n=== INNER JOIN (countries with GDP data) ===")
print(f"Rows: {df_inner.shape[0]}")
print(df_inner[['country_name', 'population', 'gdp_usd_billion']].head())

# LEFT JOIN — all countries from API, GDP if available else NaN
df_left = pd.merge(df_countries, df_gdp, on='country_name', how='left')
print("\n=== LEFT JOIN (all countries, GDP where available) ===")
print(f"Rows: {df_left.shape[0]}")
# Check how many countries are missing GDP
missing_gdp = df_left[df_left['gdp_usd_billion'].isnull()]
print(f"Countries without GDP data: {len(missing_gdp)}")

# RIGHT JOIN — all GDP rows, country info if available
df_right = pd.merge(df_countries, df_gdp, on='country_name', how='right')
print("\n=== RIGHT JOIN ===")
print(f"Rows: {df_right.shape[0]}")
# FakeCountry rows will show NaN for country columns — they don't exist in API
print(df_right[df_right['population'].isnull()])

# OUTER JOIN — everything from both sources
df_outer = pd.merge(df_countries, df_gdp, on='country_name', how='outer')
print("\n=== OUTER JOIN (everything) ===")
print(f"Rows: {df_outer.shape[0]}")

# ── SENIOR TIP ────────────────────────────────────────────
print("\n=== INTERVIEW ANSWER: When to use which join ===")
print("INNER  → When you only want matched records (strictest)")
print("LEFT   → When left table is your main source, right is optional")
print("RIGHT  → Rare — just flip tables and use LEFT instead")
print("OUTER  → When you want everything and handle nulls yourself")





# ── DUPLICATES ────────────────────────────────────────────
# Real data ALWAYS has duplicates — pipelines running twice, system bugs etc.

# Simulate duplicates coming in — exactly like a real pipeline re-run
duplicate_rows = df_countries.head(5).copy()
df_with_dupes = pd.concat([df_countries, duplicate_rows], ignore_index=True)

print(f"\n=== WITH DUPLICATES ===")
print(f"Rows before: {df_countries.shape[0]}")
print(f"Rows after adding dupes: {df_with_dupes.shape[0]}")

# Check duplicates
print(f"\nDuplicate rows found: {df_with_dupes.duplicated().sum()}")

# Remove duplicates — keep first occurrence
df_deduped = df_with_dupes.drop_duplicates(subset=['country_name'], keep='first')
print(f"Rows after dedup: {df_deduped.shape[0]}")

# ── SENIOR TIP ────────────────────────────────────────────
# In real pipelines you dedupe on business key — not all columns
# Business key = the column that uniquely identifies a record
# Here it's country_name — in sales it would be order_id
print("\nAlways dedupe on BUSINESS KEY — not all columns!")




# ── REAL WORLD TRANSFORMS ────────────────────────────────
# Things you will do in every single DE project

# Work with the left joined df — most realistic
df = df_left.copy()

# 1. Population density — new calculated column
df['pop_density'] = (df['population'] / df['area_km2']).round(2)

# 2. Categorize countries by population size — like CASE WHEN in SQL
def population_category(pop):
    if pop >= 100_000_000:
        return 'Large'
    elif pop >= 10_000_000:
        return 'Medium'
    else:
        return 'Small'

df['pop_category'] = df['population'].apply(population_category)

# 3. Fill missing GDP with 0 and flag it — never just leave nulls
df['gdp_available'] = df['gdp_usd_billion'].notnull()
df['gdp_usd_billion'] = df['gdp_usd_billion'].fillna(0)
df['gdp_growth_pct'] = df['gdp_growth_pct'].fillna(0)

# 4. Subregion summary — GROUP BY like SQL
print("\n=== COUNTRIES BY SUBREGION ===")
subregion_summary = df.groupby('subregion').agg(
    country_count   = ('country_name', 'count'),
    total_population= ('population', 'sum'),
    avg_gdp         = ('gdp_usd_billion', 'mean')
).round(2).reset_index()
subregion_summary = subregion_summary.sort_values('total_population', ascending=False)
print(subregion_summary)

# 5. Top 5 most populated countries
print("\n=== TOP 5 MOST POPULATED ===")
top5 = df.nlargest(5, 'population')[['country_name', 'population', 'pop_density', 'pop_category']]
print(top5)




# ── SAVE ─────────────────────────────────────────────────
# Save in layers — exactly like Bronze/Silver/Gold in Azure

# Bronze = raw API data (as-is)
df_countries.to_parquet('bronze_countries.parquet', index=False)

# Silver = cleaned, joined, transformed
df.to_parquet('silver_countries.parquet', index=False)

# Gold = aggregated, business-ready
subregion_summary.to_parquet('gold_subregion_summary.parquet', index=False)

print("\n=== SAVED ===")
print("bronze_countries.parquet   — raw from API")
print("silver_countries.parquet   — cleaned + joined + transformed")
print("gold_subregion_summary.parquet — business ready aggregation")
print("\nBuddy — you just built a Bronze/Silver/Gold pipeline in Python!")
print("This is EXACTLY what Azure Databricks does — same concept, bigger scale!")