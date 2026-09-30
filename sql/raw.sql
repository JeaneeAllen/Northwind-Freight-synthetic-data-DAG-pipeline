-- Task: raw
-- Global (not date-partitioned): a full reload of every source file.
-- Shipments are filtered to the --as-of cutoff; lanes and fuel are static
-- reference data, loaded in full. Each statement below is executed
-- separately (DuckDB only allows bound parameters on a single statement).

CREATE OR REPLACE TABLE raw_shipments AS
SELECT shipment_id, shipment_date, arrival_date, lane_id,
       freight_charge_cents, fuel_surcharge_cents, total_charge_cents
FROM read_csv($shipments_path, header = true)
WHERE arrival_date <= CAST($as_of AS DATE);

CREATE OR REPLACE TABLE raw_lanes AS
SELECT lane_id, origin, destination, distance_miles,
       rate_cents_per_mile, standard_transit_days
FROM read_csv($lanes_path, header = true);

CREATE OR REPLACE TABLE raw_fuel AS
SELECT date, fuel_surcharge_cents_per_mile
FROM read_csv($fuel_path, header = true);
