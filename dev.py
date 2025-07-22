# -*- coding: utf-8 -*-
"""
This script uses DuckDB to analyze a network edges CSV file.
It specifically checks whether the Source and Target columns have values that appear in both columns,
or whether the values in each column don't intersect.

The script:
1. Loads the CSV file into a DuckDB table
2. Displays the table structure and a sample of data
3. Checks if there are values that appear in both source and target columns
4. Lists the specific values that appear in both columns
5. Checks if source and target columns have non-intersecting values

Results interpretation:
- If values appear in both columns, the network has nodes that can be both sources and targets
- If the columns don't intersect, the network might be strictly directional or bipartite
"""
import duckdb
import pandas as pd

# Path to the CSV file
csv_file = "/home/markus/projects/kiara/plugins/kiara_plugin.network_analysis/examples/data/simple_networks/bipartite/SampleEdges.csv"

# Connect to an in-memory DuckDB database
con = duckdb.connect(database=':memory:')

# Load the CSV file into DuckDB
con.execute(f"CREATE TABLE edges AS SELECT * FROM read_csv_auto('{csv_file}')")

# Print the table structure and a sample of data
print("Table structure:")
con.execute("DESCRIBE edges").fetchdf().to_string(index=False)
print("\nSample data:")
print(con.execute("SELECT * FROM edges LIMIT 5").fetchdf().to_string(index=False))

# Query 1: Check if there are values that appear in both source and target columns
print("\nQuery 1: Values that appear in both source and target columns:")
overlap_query = """
SELECT
    CASE
        WHEN COUNT(*) > 0 THEN 'YES, there are values that appear in both source and target columns'
        ELSE 'NO, there are no values that appear in both source and target columns'
    END as result
FROM (
    SELECT value FROM (
        SELECT DISTINCT source as value FROM edges
        INTERSECT
        SELECT DISTINCT target as value FROM edges
    )
)
"""
overlap_result = con.execute(overlap_query).fetchone()[0]
print(overlap_result)

# Query 2: List the values that appear in both columns
print("\nValues that appear in both source and target columns:")
common_values_query = """
SELECT value FROM (
    SELECT DISTINCT source as value FROM edges
    INTERSECT
    SELECT DISTINCT target as value FROM edges
)
ORDER BY value
"""
common_values = con.execute(common_values_query).fetchdf()
if len(common_values) > 0:
    print(common_values.to_string(index=False))
else:
    print("None")

# Query 3: Check if source and target columns have non-intersecting values
print("\nQuery 2: Do source and target columns have non-intersecting values?")
disjoint_query = """
SELECT
    CASE
        WHEN COUNT(*) = 0 THEN 'YES, source and target columns have non-intersecting values'
        ELSE 'NO, source and target columns have some overlapping values'
    END as result
FROM (
    SELECT value FROM (
        SELECT DISTINCT source as value FROM edges
        INTERSECT
        SELECT DISTINCT target as value FROM edges
    )
)
"""
disjoint_result = con.execute(disjoint_query).fetchone()[0]
print(disjoint_result)

# Close the connection
con.close()
