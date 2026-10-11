-- Load clean rows into the star schema. Param: %(batch_id)s. Idempotent (upserts on natural keys).

CREATE TEMP TABLE _good ON COMMIT DROP AS
SELECT * FROM staging.trip_clean s
WHERE s.batch_id = %(batch_id)s
  AND NOT EXISTS (SELECT 1 FROM unnest(s.violations) v WHERE v LIKE 'E\_%%');

INSERT INTO core.dim_date
SELECT to_char(d, 'YYYYMMDD')::int, d, extract(year FROM d), extract(month FROM d), extract(day FROM d),
       extract(isodow FROM d), trim(to_char(d, 'Day')), extract(isodow FROM d) >= 6
FROM (SELECT DISTINCT order_ts::date AS d FROM _good UNION SELECT DISTINCT pickup_ts::date FROM _good) x
ON CONFLICT DO NOTHING;

INSERT INTO core.dim_vehicle  (vehicle_type)  SELECT DISTINCT coalesce(vehicle, 'unknown') FROM _good ON CONFLICT DO NOTHING;
INSERT INTO core.dim_weather  (weather)       SELECT DISTINCT coalesce(weather, 'Unknown') FROM _good ON CONFLICT DO NOTHING;
INSERT INTO core.dim_traffic  (traffic)       SELECT DISTINCT coalesce(traffic, 'Unknown') FROM _good ON CONFLICT DO NOTHING;
INSERT INTO core.dim_area     (area_type)     SELECT DISTINCT coalesce(area, 'Unknown')    FROM _good ON CONFLICT DO NOTHING;
INSERT INTO core.dim_category (category_name) SELECT DISTINCT coalesce(category, 'Unknown') FROM _good ON CONFLICT DO NOTHING;

INSERT INTO core.dim_store (lat, lon, city_key)
SELECT g.lat, g.lon,
       coalesce((SELECT c.city_key FROM core.dim_city c
                 WHERE c.lat IS NOT NULL AND util.haversine_km(g.lat, g.lon, c.lat, c.lon) <= 60
                 ORDER BY util.haversine_km(g.lat, g.lon, c.lat, c.lon) LIMIT 1),
                (SELECT city_key FROM core.dim_city WHERE city_name = 'Other'))
FROM (SELECT DISTINCT round(store_lat, 6) AS lat, round(store_lon, 6) AS lon FROM _good) g
ON CONFLICT (lat, lon) DO NOTHING;

INSERT INTO core.fact_trip (
  order_id, order_ts, pickup_ts, date_key, store_key, vehicle_key, weather_key, traffic_key,
  area_key, category_key, agent_age, agent_rating, drop_lat, drop_lon, distance_km,
  pickup_delay_min, delivery_minutes, speed_kmph, dq_warnings, batch_id)
SELECT g.order_id, g.order_ts, g.pickup_ts, to_char(g.order_ts, 'YYYYMMDD')::int,
       st.store_key, v.vehicle_key, w.weather_key, t.traffic_key, a.area_key, c.category_key,
       g.agent_age, g.agent_rating, g.drop_lat, g.drop_lon, g.distance_km,
       g.pickup_delay_min, g.delivery_minutes,
       round(g.distance_km / (g.delivery_minutes / 60.0), 2),
       ARRAY(SELECT x FROM unnest(g.violations) x WHERE x LIKE 'W\_%%'),
       g.batch_id
FROM _good g
JOIN core.dim_store    st ON st.lat = round(g.store_lat, 6) AND st.lon = round(g.store_lon, 6)
JOIN core.dim_vehicle  v  ON v.vehicle_type  = coalesce(g.vehicle, 'unknown')
JOIN core.dim_weather  w  ON w.weather       = coalesce(g.weather, 'Unknown')
JOIN core.dim_traffic  t  ON t.traffic       = coalesce(g.traffic, 'Unknown')
JOIN core.dim_area     a  ON a.area_type     = coalesce(g.area, 'Unknown')
JOIN core.dim_category c  ON c.category_name = coalesce(g.category, 'Unknown')
ON CONFLICT (order_id) DO UPDATE SET
  order_ts = EXCLUDED.order_ts, pickup_ts = EXCLUDED.pickup_ts, date_key = EXCLUDED.date_key,
  store_key = EXCLUDED.store_key, vehicle_key = EXCLUDED.vehicle_key, weather_key = EXCLUDED.weather_key,
  traffic_key = EXCLUDED.traffic_key, area_key = EXCLUDED.area_key, category_key = EXCLUDED.category_key,
  agent_age = EXCLUDED.agent_age, agent_rating = EXCLUDED.agent_rating,
  drop_lat = EXCLUDED.drop_lat, drop_lon = EXCLUDED.drop_lon, distance_km = EXCLUDED.distance_km,
  pickup_delay_min = EXCLUDED.pickup_delay_min, delivery_minutes = EXCLUDED.delivery_minutes,
  speed_kmph = EXCLUDED.speed_kmph, dq_warnings = EXCLUDED.dq_warnings,
  batch_id = EXCLUDED.batch_id, loaded_at = now();
