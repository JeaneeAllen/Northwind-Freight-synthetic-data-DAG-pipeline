-- Bootstrap DDL. Not a DAG task: run once at startup so every managed
-- table exists before a task's DELETE/INSERT or CREATE OR REPLACE runs.

CREATE TABLE IF NOT EXISTS raw_shipments (
    shipment_id VARCHAR,
    shipment_date DATE,
    arrival_date DATE,
    lane_id VARCHAR,
    freight_charge_cents BIGINT,
    fuel_surcharge_cents BIGINT,
    total_charge_cents BIGINT
);

CREATE TABLE IF NOT EXISTS raw_lanes (
    lane_id VARCHAR,
    origin VARCHAR,
    destination VARCHAR,
    distance_miles BIGINT,
    rate_cents_per_mile BIGINT,
    standard_transit_days BIGINT
);

CREATE TABLE IF NOT EXISTS raw_fuel (
    date DATE,
    fuel_surcharge_cents_per_mile BIGINT
);

CREATE TABLE IF NOT EXISTS staging_shipments (
    shipment_id VARCHAR,
    shipment_date DATE,
    arrival_date DATE,
    lane_id VARCHAR,
    freight_charge_cents BIGINT,
    fuel_surcharge_cents BIGINT,
    total_charge_cents BIGINT
);

CREATE TABLE IF NOT EXISTS staging_lanes (
    lane_id VARCHAR,
    origin VARCHAR,
    destination VARCHAR,
    distance_miles BIGINT,
    rate_cents_per_mile BIGINT,
    standard_transit_days BIGINT
);

CREATE TABLE IF NOT EXISTS staging_fuel (
    date DATE,
    fuel_surcharge_cents_per_mile BIGINT
);

CREATE TABLE IF NOT EXISTS intermediate (
    shipment_id VARCHAR,
    shipment_date DATE,
    lane_id VARCHAR,
    revenue_cents BIGINT,
    cost_cents BIGINT,
    margin_cents BIGINT
);

CREATE TABLE IF NOT EXISTS mart (
    shipment_date DATE,
    lane_id VARCHAR,
    shipment_count BIGINT,
    revenue_cents BIGINT,
    cost_cents BIGINT,
    margin_cents BIGINT
);

CREATE TABLE IF NOT EXISTS pipeline_checkpoints (
    task VARCHAR,
    scope_key VARCHAR,
    checkpoint_hash VARCHAR,
    status VARCHAR,
    updated_at TIMESTAMP,
    PRIMARY KEY (task, scope_key)
);
