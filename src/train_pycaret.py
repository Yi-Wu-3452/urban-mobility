import ibis
from pycaret.regression import *
from config import DUCKDB_PATH, TARGET_COL, DEFAULT_FEATURE_COLS

# Load smoke test data
conn = ibis.duckdb.connect(str(DUCKDB_PATH))
smoke_table = conn.table("silver_trips_yellow_smoke")
df = smoke_table.execute()
conn.disconnect()

print(f"Loaded {len(df):,} rows for training")
print(f"Target: {TARGET_COL}")
print(f"Features: {DEFAULT_FEATURE_COLS}")

# Setup PyCaret
print("\nSetting up PyCaret...")
exp = setup(
    data=df,
    target=TARGET_COL,
    train_size=0.7,
    test_data=None,
    fold=3,  # Reduced from 5 for speed
    session_id=42,
    verbose=False,
    normalize=True,
    ignore_features=['pickup_ts', 'dropoff_ts', 'pickup_hour'],  # Exclude timestamp columns
)

# Train Linear Regression model
print("\nTraining Linear Regression model...")
model = create_model('lr', verbose=True)

# Make predictions on hold-out test set
print("\nPredicting on hold-out test set...")
predictions = predict_model(model)

# Show metrics
print("\nModel Performance:")
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import numpy as np

y_true = predictions['total_amount']
y_pred = predictions['prediction_label']

mae = mean_absolute_error(y_true, y_pred)
rmse = np.sqrt(mean_squared_error(y_true, y_pred))
r2 = r2_score(y_true, y_pred)

print(f"  MAE:  ${mae:.2f}")
print(f"  RMSE: ${rmse:.2f}")
print(f"  R²:   {r2:.4f}")
print(f"\nPredictions sample:")
print(predictions[['total_amount', 'prediction_label']].head(10))

# Save model
print("\nSaving model...")
save_model(model, 'models/trip_amount_model')

print("\nTraining complete!")
print(f"Model saved to: models/trip_amount_model.pkl")
