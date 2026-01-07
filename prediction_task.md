# Define prediction task
## Task: predict the total amount at trip start
## Prevent leakage: do not use columns that are only known after the trip ends or that directly compose the target
- OK features (known at/near pickup):
vendor_id, pu_location_id, do_location_id, passenger_count, pickup_dow, pickup_hour_of_day, pickup_hour, congestion_surchrage, Airport_fee

- Careful/ usually leakage:
trip_distance (known after trip), duration_sec(after_trip), dropoff_ts(after_trip), and any component of the total(e.g., fare_amount, tip_amoutn) depends on what you consider "available"