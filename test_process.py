import duckdb

# Path to your CSV file
overall_priority_ids = "full_products.csv"
loaded_ids = "loaded_products.csv"

# Open a DuckDB connection
con = duckdb.connect()

# Query the CSV file directly
query = f"""
SELECT o.id as id
FROM read_csv_auto('{overall_priority_ids}') as o
LEFT JOIN read_csv_auto('{loaded_ids}') as l
ON o.id = l.id
where l.id is null
"""

# Execute the query and fetch the results as a pandas DataFrame
result = con.execute(query).fetchdf()
result.to_csv('result.csv', index=False)
# result = con.execute(query)

# Display the result
print(len(result))
