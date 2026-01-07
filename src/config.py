from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DUCKDB_PATH = PROJECT_ROOT / "data" / "duckdb" / "urban_mobility.duckdb"

SILVER_TABLE = "silver_trips_yellow"

GOLD_PRED_TABLE = "gold_trip_total_amount_predictions"

GOLD_METRICS_TABLE = "gold_model_metrics_daily"

TARGET_COL = "total_amount"
TIME_COL = "pickup_ts"

