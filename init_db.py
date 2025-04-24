import duckdb

# Create a connection to a local database file
con = duckdb.connect("data/warehouse.duckdb")

# Run a basic sanity check
result = con.execute("SELECT 1 AS hello").fetchdf()
print(result)
