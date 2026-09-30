-- Task: staging_fuel (partitioned by date)
SELECT date, fuel_surcharge_cents_per_mile
FROM raw_fuel
WHERE date = CAST($date AS DATE)
