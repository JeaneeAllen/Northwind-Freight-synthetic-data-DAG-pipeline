-- Task: mart (partitioned by shipment_date)
SELECT
    shipment_date,
    lane_id,
    COUNT(*) AS shipment_count,
    SUM(revenue_cents) AS revenue_cents,
    SUM(cost_cents) AS cost_cents,
    SUM(margin_cents) AS margin_cents
FROM intermediate
WHERE shipment_date = CAST($date AS DATE)
GROUP BY shipment_date, lane_id
