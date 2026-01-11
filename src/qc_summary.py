import ibis
from config import DUCKDB_PATH, SILVER_TABLE, TIME_COL, TARGET_COL

conn = ibis.duckdb.connect(str(DUCKDB_PATH))

original = conn.table(SILVER_TABLE)
qc = conn.table(f"{SILVER_TABLE}_qc")

print("=" * 60)
print("QUALITY CONTROL SUMMARY")
print("=" * 60)

# Row counts
original_count = original.count().execute()
qc_count = qc.count().execute()
removed = original_count - qc_count

print(f"\nOriginal table: {SILVER_TABLE}")
print(f"  Total rows: {original_count:,}")
print(f"\nQC table: {SILVER_TABLE}_qc")
print(f"  Total rows: {qc_count:,}")
print(f"  Removed: {removed:,} ({removed/original_count*100:.2f}%)")

# Timestamp ranges
print(f"\n{'-'*60}")
print("TIMESTAMP RANGE")
print(f"{'-'*60}")

orig_time = original.aggregate([
    original[TIME_COL].min().name('min_ts'),
    original[TIME_COL].max().name('max_ts')
]).execute().iloc[0]

qc_time = qc.aggregate([
    qc[TIME_COL].min().name('min_ts'),
    qc[TIME_COL].max().name('max_ts')
]).execute().iloc[0]

print(f"\nOriginal:")
print(f"  Min: {orig_time['min_ts']}")
print(f"  Max: {orig_time['max_ts']}")

print(f"\nQC:")
print(f"  Min: {qc_time['min_ts']}")
print(f"  Max: {qc_time['max_ts']}")

# Target column stats
print(f"\n{'-'*60}")
print(f"{TARGET_COL.upper()} STATISTICS")
print(f"{'-'*60}")

orig_stats = original.aggregate([
    original[TARGET_COL].min().name('min'),
    original[TARGET_COL].max().name('max'),
    original[TARGET_COL].mean().name('mean'),
    original[TARGET_COL].median().name('median')
]).execute().iloc[0]

qc_stats = qc.aggregate([
    qc[TARGET_COL].min().name('min'),
    qc[TARGET_COL].max().name('max'),
    qc[TARGET_COL].mean().name('mean'),
    qc[TARGET_COL].median().name('median')
]).execute().iloc[0]

print(f"\nOriginal:")
print(f"  Min: ${orig_stats['min']:.2f}")
print(f"  Max: ${orig_stats['max']:.2f}")
print(f"  Mean: ${orig_stats['mean']:.2f}")
print(f"  Median: ${orig_stats['median']:.2f}")

print(f"\nQC:")
print(f"  Min: ${qc_stats['min']:.2f}")
print(f"  Max: ${qc_stats['max']:.2f}")
print(f"  Mean: ${qc_stats['mean']:.2f}")
print(f"  Median: ${qc_stats['median']:.2f}")

# Violations breakdown
print(f"\n{'-'*60}")
print("VIOLATIONS REMOVED")
print(f"{'-'*60}")

invalid_pickup = original.filter(original.pickup_ts >= ibis.timestamp('2025-01-01 00:00:00')).count().execute()
invalid_dropoff = original.filter(original.dropoff_ts <= original.pickup_ts).count().execute()
invalid_amount = original.filter(original[TARGET_COL] < 0).count().execute()

print(f"\n  Pickup time >= 2025: {invalid_pickup:,}")
print(f"  Dropoff <= Pickup: {invalid_dropoff:,}")
print(f"  Negative {TARGET_COL}: {invalid_amount:,}")
print(f"  Total violations: {removed:,}")

print("\n" + "=" * 60)

conn.disconnect()
