import duckdb
import pandas as pd
from pycaret.regression import setup, compare_models, finalize_model, save_model, pull
from pathlib import Path

DUCKDB_PATH = "data/duckdb/urban_mobility.duckdb"
SILVER_TABLE = "silver_trips_yellow"

TARGET = "total_amount"
TIME_COL = "pickup_ts"

MODEL_DIR = Path("models")
MODEL_DIR.mkdir(parents=True, exist_ok=True)

# 1) Extract from DuckDB (ONLY the columns we want)
query = f"""
SELECT
  vendor_id,
  pu_location_id,
  do_location_id,
  passenger_count,
  congestion_surcharge,
  Airport_fee,
  pickup_dow,
  pickup_hour_of_day,
  {TIME_COL},
  {TARGET}
FROM {SILVER_TABLE}
WHERE {TARGET} IS NOT NULL AND {TIME_COL} IS NOT NULL
"""

df = duckdb.connect(DUCKDB_PATH, read_only=True).execute(query).fetchdf()

# sort by time 
df[TIME_COL] = pd.to_datetime(df[TIME_COL])
df = df.sort_values(TIME_COL).reset_index(drop=True)

# 2) Setup: time-aware split (keeps chronology)
exp = setup(
    data=df,
    target=TARGET,
    fold_strategy="timeseries",    # time series CV
    fold=3,
    data_split_shuffle=False,
    fold_shuffle=False,
    session_id=42,
    verbose=False
)

# 3) Baseline + strong defaults (PyCaret will try many and rank)
best = compare_models(sort="MAE")

# 4) Finalize & package
final = finalize_model(best)
save_model(final, str(MODEL_DIR / "trip_total_amount_model"))

# 5) Quick report
leaderboard = pull()
print(leaderboard.head(10))
print("\nSaved model to:", MODEL_DIR / "trip_total_amount_model.pkl")
