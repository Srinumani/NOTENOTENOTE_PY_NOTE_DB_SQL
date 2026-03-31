pip install sqlalchemy duckdb


Why DuckDB buddy? DuckDB is a lightweight database that runs locally — no server setup, no Azure needed today. But the connection pattern is identical to Azure SQL, PostgreSQL, any database. Learn it once, use it everywhere.


# ── WHY CHUNKING MATTERS ──────────────────────────────────
# Real DE problem: you get a 5GB CSV file
# If you do pd.read_csv('huge_file.csv') — Python crashes
# Solution: read in chunks — process piece by piece

# First let's CREATE a large-ish file to simulate this