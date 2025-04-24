import duckdb

# Connect to your warehouse
con = duckdb.connect("data/warehouse.duckdb")

# List all available tables
tables = con.execute("SHOW TABLES").fetchall()
print("📋 Tables in DB:", tables)

# Preview a specific table
df = con.execute("SELECT * FROM KNS_Person LIMIT 10").fetchdf()
print(df)
