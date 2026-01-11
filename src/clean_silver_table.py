import ibis
from config import DUCKDB_PATH, SILVER_TABLE

conn = ibis.duckdb.connect(str(DUCKDB_PATH))

table = conn.table(SILVER_TABLE)

# Count before QC
total_before = table.count().execute()
print(f"Total rows before QC: {total_before:,}")

# Apply QC rules
qc = table.filter(
    [
        table.pickup_ts < ibis.timestamp('2025-01-01 00:00:00'),  # Remove pickups after 2024
        table.dropoff_ts > table.pickup_ts,  # Dropoff after pickup
        table.total_amount >= 0  # Non-negative total amount
    ]
)

# Count after QC
total_after = qc.count().execute()
removed = total_before - total_after

print(f"Total rows after QC: {total_after:,}")
print(f"Rows removed: {removed:,} ({removed/total_before*100:.2f}%)")

# Show breakdown of violations
print("\nViolation breakdown:")
invalid_pickup = table.filter(table.pickup_ts >= ibis.timestamp('2025-01-01 00:00:00')).count().execute()
invalid_dropoff = table.filter(table.dropoff_ts <= table.pickup_ts).count().execute()
invalid_amount = table.filter(table.total_amount < 0).count().execute()

print(f"  Pickup time >= 2025: {invalid_pickup:,}")
print(f"  Dropoff <= Pickup: {invalid_dropoff:,}")
print(f"  Negative total_amount: {invalid_amount:,}")

# Create QC table
print(f"\nCreating QC table: {SILVER_TABLE}_qc...")
conn.create_table(f"{SILVER_TABLE}_qc", qc.execute(), overwrite=True)
print("Done!")

conn.disconnect()
