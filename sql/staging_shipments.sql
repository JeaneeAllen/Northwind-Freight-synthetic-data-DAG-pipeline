-- Task: staging_shipments (partitioned by shipment_date)
-- Deterministic dedup: MIN() over an exact duplicate group returns the
-- same value regardless of row order, so re-running never changes output.
SELECT
    shipment_id,
    MIN(shipment_date) AS shipment_date,
    MIN(arrival_date) AS arrival_date,
    MIN(lane_id) AS lane_id,
    MIN(freight_charge_cents) AS freight_charge_cents,
    MIN(fuel_surcharge_cents) AS fuel_surcharge_cents,
    MIN(total_charge_cents) AS total_charge_cents
FROM raw_shipments
WHERE shipment_date = CAST($date AS DATE)
GROUP BY shipment_id
