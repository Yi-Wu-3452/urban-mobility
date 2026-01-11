import ibis
from config import DUCKDB_PATH, SILVER_TABLE

conn = ibis.duckdb.connect(str(DUCKDB_PATH))

table = conn.table(SILVER_TABLE)

print(f"Table: {SILVER_TABLE}")
print("\nSchema:")
print(table.schema())

print("\nColumn Summary:")
print(table.describe())

conn.disconnect()
