-- Task: export (partitioned by shipment_date)
-- Sorted by lane_id for a consistent, diffable CSV; no timestamp column.
SELECT
    shipment_date,
    lane_id,
    shipment_count,
    revenue_cents,
    cost_cents,
    margin_cents
FROM mart
WHERE shipment_date = CAST($date AS DATE)
ORDER BY lane_id
