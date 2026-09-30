-- Task: staging_lanes (global: lanes are static reference data, not date-partitioned)
SELECT DISTINCT lane_id, origin, destination, distance_miles,
       rate_cents_per_mile, standard_transit_days
FROM raw_lanes
ORDER BY lane_id
