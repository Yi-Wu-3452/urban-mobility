import ibis
from config import DUCKDB_PATH, TIME_COL

conn = ibis.duckdb.connect(str(DUCKDB_PATH))

model_table = conn.table("silver_trips_yellow_model")

# Get latest 50K rows by pickup timestamp
sample_size = 50000
smoke_test = model_table.order_by(ibis.desc(TIME_COL)).limit(sample_size)

print(f"Sampling latest {sample_size:,} rows for smoke test...")
result = smoke_test.execute()

print(f"Sampled rows: {len(result):,}")
print(f"Date range: {result[TIME_COL].min()} to {result[TIME_COL].max()}")

# Create smoke test table
smoke_table = "silver_trips_yellow_smoke"
print(f"\nCreating smoke test table: {smoke_table}...")
conn.create_table(smoke_table, result, overwrite=True)

print("Done!")

conn.disconnect()
