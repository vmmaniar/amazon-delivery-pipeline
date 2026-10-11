-- Reporting layer: flat views over the star schema.

CREATE OR REPLACE VIEW marts.v_trip AS
SELECT f.trip_key, f.order_id, f.order_ts, f.pickup_ts, d.full_date, d.day_name, d.is_weekend,
       extract(hour FROM f.order_ts)::int AS order_hour,
       ci.city_name, s.lat AS store_lat, s.lon AS store_lon,
       ve.vehicle_type, w.weather, t.traffic, a.area_type, ca.category_name,
       f.agent_age,
       CASE WHEN f.agent_age < 25 THEN '18-24' WHEN f.agent_age < 35 THEN '25-34'
            WHEN f.agent_age < 45 THEN '35-44' ELSE '45+' END AS agent_age_band,
       f.agent_rating, f.distance_km, f.pickup_delay_min, f.delivery_minutes, f.speed_kmph
FROM core.fact_trip f
JOIN core.dim_date d      ON d.date_key = f.date_key
JOIN core.dim_store s     ON s.store_key = f.store_key
JOIN core.dim_city ci     ON ci.city_key = s.city_key
JOIN core.dim_vehicle ve  ON ve.vehicle_key = f.vehicle_key
JOIN core.dim_weather w   ON w.weather_key = f.weather_key
JOIN core.dim_traffic t   ON t.traffic_key = f.traffic_key
JOIN core.dim_area a      ON a.area_key = f.area_key
JOIN core.dim_category ca ON ca.category_key = f.category_key;

CREATE OR REPLACE VIEW marts.daily_kpis AS
SELECT full_date, count(*) AS trips,
       round(avg(delivery_minutes), 1) AS avg_delivery_min,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY delivery_minutes) AS median_delivery_min,
       percentile_cont(0.9) WITHIN GROUP (ORDER BY delivery_minutes) AS p90_delivery_min,
       round(avg(distance_km), 2) AS avg_distance_km,
       round(avg(agent_rating), 2) AS avg_agent_rating
FROM marts.v_trip GROUP BY full_date;

CREATE OR REPLACE VIEW marts.vehicle_performance AS
SELECT vehicle_type, count(*) AS trips,
       round(avg(delivery_minutes), 1) AS avg_delivery_min,
       round(avg(distance_km), 2) AS avg_distance_km,
       round(avg(speed_kmph), 2) AS avg_speed_kmph,
       round(avg(agent_rating), 2) AS avg_rating
FROM marts.v_trip GROUP BY vehicle_type;

CREATE OR REPLACE VIEW marts.traffic_weather_impact AS
SELECT traffic, weather, count(*) AS trips,
       round(avg(delivery_minutes), 1) AS avg_delivery_min,
       round(avg(speed_kmph), 2) AS avg_speed_kmph
FROM marts.v_trip GROUP BY traffic, weather;

CREATE OR REPLACE VIEW marts.city_performance AS
SELECT city_name, count(*) AS trips, count(DISTINCT (store_lat, store_lon)) AS stores,
       round(avg(delivery_minutes), 1) AS avg_delivery_min,
       percentile_cont(0.9) WITHIN GROUP (ORDER BY delivery_minutes) AS p90_delivery_min,
       round(avg(distance_km), 2) AS avg_distance_km
FROM marts.v_trip GROUP BY city_name;

CREATE OR REPLACE VIEW marts.hourly_demand AS
SELECT order_hour, is_weekend, count(*) AS trips, round(avg(delivery_minutes), 1) AS avg_delivery_min
FROM marts.v_trip GROUP BY order_hour, is_weekend;

CREATE OR REPLACE VIEW marts.dq_summary AS
SELECT r.batch_id, r.rule_code, k.severity, k.description, r.rows_failed, r.rows_total,
       round(100.0 * r.rows_failed / nullif(r.rows_total, 0), 2) AS pct_failed
FROM ops.dq_result r JOIN ops.dq_rule k USING (rule_code);

-- ---- Analysis views (answer the questions a delivery-ops team asks) ----

-- Does agent age relate to rating? Bands start at 18 because the DQ rules already removed under-18s.
CREATE OR REPLACE VIEW marts.age_band_rating AS
SELECT CASE WHEN agent_age < 25 THEN '18-24' WHEN agent_age < 30 THEN '25-29'
            WHEN agent_age < 35 THEN '30-34' WHEN agent_age < 40 THEN '35-39' ELSE '40+' END AS age_band,
       count(*) AS trips, round(avg(agent_rating), 3) AS avg_rating,
       round(avg(delivery_minutes), 1) AS avg_delivery_min
FROM marts.v_trip WHERE agent_rating IS NOT NULL GROUP BY 1;

CREATE OR REPLACE VIEW marts.rating_distribution AS
SELECT agent_rating, count(*) AS trips FROM marts.v_trip WHERE agent_rating IS NOT NULL GROUP BY 1;

CREATE OR REPLACE VIEW marts.area_traffic AS
SELECT area_type, traffic, count(*) AS trips, round(avg(delivery_minutes), 1) AS avg_delivery_min
FROM marts.v_trip GROUP BY 1, 2;

CREATE OR REPLACE VIEW marts.distance_band AS
SELECT CASE WHEN distance_km < 3 THEN '0-3' WHEN distance_km < 6 THEN '3-6' WHEN distance_km < 9 THEN '6-9'
            WHEN distance_km < 12 THEN '9-12' WHEN distance_km < 15 THEN '12-15' ELSE '15+' END AS km_band,
       min(distance_km) AS band_min_km, count(*) AS trips,
       round(avg(delivery_minutes), 1) AS avg_delivery_min
FROM marts.v_trip GROUP BY 1;

CREATE OR REPLACE VIEW marts.category_performance AS
SELECT category_name, count(*) AS trips, round(avg(delivery_minutes), 1) AS avg_delivery_min,
       round(avg(distance_km), 2) AS avg_distance_km
FROM marts.v_trip GROUP BY 1;

-- One row of headline numbers for the dashboard.
CREATE OR REPLACE VIEW marts.headline AS
SELECT count(*) AS trips, round(avg(delivery_minutes), 1) AS avg_delivery_min,
       round(avg(agent_rating), 2) AS avg_rating, round(avg(distance_km), 2) AS avg_distance_km,
       round(corr(distance_km, delivery_minutes)::numeric, 3) AS corr_distance_time,
       round(corr(agent_rating, delivery_minutes)::numeric, 3) AS corr_rating_time
FROM marts.v_trip;
