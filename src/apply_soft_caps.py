import ibis
from config import DUCKDB_PATH, SILVER_TABLE, TARGET_COL

conn = ibis.duckdb.connect(str(DUCKDB_PATH))

qc_table = conn.table(f"{SILVER_TABLE}_qc")

# Count before soft caps
total_before = qc_table.count().execute()
print(f"Total rows before soft caps: {total_before:,}")

# Apply soft caps
soft_capped = qc_table.filter(
    [
        qc_table[TARGET_COL] <= 500,  # Total amount cap
        qc_table.duration_sec <= 6 * 3600  # 6 hours in seconds
    ]
)

# Count after soft caps
total_after = soft_capped.count().execute()
removed = total_before - total_after

print(f"Total rows after soft caps: {total_after:,}")
print(f"Rows removed: {removed:,} ({removed/total_before*100:.2f}%)")

# Show breakdown of violations
print("\nSoft cap violations:")
over_amount = qc_table.filter(qc_table[TARGET_COL] > 500).count().execute()
over_duration = qc_table.filter(qc_table.duration_sec > 6 * 3600).count().execute()

print(f"  {TARGET_COL} > $500: {over_amount:,}")
print(f"  duration_sec > 6 hrs: {over_duration:,}")

# Create model table
model_table = "silver_trips_yellow_model"
print(f"\nCreating model table: {model_table}...")
conn.create_table(model_table, soft_capped.execute(), overwrite=True)
print("Done!")

conn.disconnect()
