-- Task: intermediate (partitioned by shipment_date)
-- Revenue and cost are recomputed from staging_lanes/staging_fuel (rather
-- than reused from staging_shipments) so the declared dependency on all
-- three staging tasks is load-bearing, not decorative.
--
-- Cost model (no separate carrier-cost source exists, so this is an
-- explicit assumption): the fuel surcharge is a pass-through cost with no
-- markup, and the freight charge carries an 80% carrier-cost ratio, i.e. a
-- flat 20% margin on the freight portion of revenue.
WITH calc AS (
    SELECT
        s.shipment_id,
        s.shipment_date,
        s.lane_id,
        l.distance_miles * l.rate_cents_per_mile AS freight_charge_cents,
        l.distance_miles * f.fuel_surcharge_cents_per_mile AS fuel_surcharge_cents
    FROM staging_shipments AS s
    JOIN staging_lanes AS l ON l.lane_id = s.lane_id
    JOIN staging_fuel AS f ON f.date = s.shipment_date
    WHERE s.shipment_date = CAST($date AS DATE)
)
SELECT
    shipment_id,
    shipment_date,
    lane_id,
    freight_charge_cents + fuel_surcharge_cents AS revenue_cents,
    CAST(ROUND(freight_charge_cents * 0.80) AS BIGINT) + fuel_surcharge_cents AS cost_cents,
    (freight_charge_cents + fuel_surcharge_cents)
        - (CAST(ROUND(freight_charge_cents * 0.80) AS BIGINT) + fuel_surcharge_cents) AS margin_cents
FROM calc
