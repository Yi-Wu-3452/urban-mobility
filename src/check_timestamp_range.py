import duckdb
from config import DUCKDB_PATH, SILVER_TABLE, TIME_COL

conn = duckdb.connect(str(DUCKDB_PATH))

result = conn.execute(f"""
    SELECT
        MIN({TIME_COL}) as min_timestamp,
        MAX({TIME_COL}) as max_timestamp,
        COUNT(*) as total_rows
    FROM {SILVER_TABLE}
""").fetchone()

print(f"Silver Table: {SILVER_TABLE}")
print(f"Min Timestamp: {result[0]}")
print(f"Max Timestamp: {result[1]}")
print(f"Total Rows: {result[2]:,}")

conn.close()
