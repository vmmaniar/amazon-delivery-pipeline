-- Typed + normalised copy of one raw batch, with the list of DQ rule codes each row violates.
-- Param: %(batch_id)s. Safe to re-run: the batch's staging rows are replaced.
CREATE TABLE IF NOT EXISTS staging.trip_clean (
  batch_id integer NOT NULL,
  row_num  integer NOT NULL,
  order_id text,
  agent_age integer,
  agent_rating numeric,
  store_lat numeric, store_lon numeric, drop_lat numeric, drop_lon numeric,
  order_ts timestamp, pickup_ts timestamp,
  weather text, traffic text, vehicle text, area text, category text,
  delivery_minutes integer,
  pickup_delay_min numeric,
  distance_km numeric,
  violations text[] NOT NULL DEFAULT '{}',
  PRIMARY KEY (batch_id, row_num)
);

DELETE FROM staging.trip_clean WHERE batch_id = %(batch_id)s;

INSERT INTO staging.trip_clean
WITH typed AS (
  SELECT
    r.batch_id, r.row_num,
    util.clean_text(r.order_id)                               AS order_id,
    util.to_numeric(r.agent_age)::integer                     AS agent_age,
    util.to_numeric(r.agent_rating)                           AS agent_rating,
    util.to_numeric(r.store_latitude)                         AS raw_store_lat,
    util.to_numeric(r.store_longitude)                        AS raw_store_lon,
    util.to_numeric(r.drop_latitude)                          AS raw_drop_lat,
    util.to_numeric(r.drop_longitude)                         AS raw_drop_lon,
    util.to_date(r.order_date)                                AS order_date,
    util.to_time(r.order_time)                                AS order_time,
    util.to_time(r.pickup_time)                               AS pickup_time,
    initcap(util.clean_text(r.weather))                       AS weather,
    initcap(util.clean_text(r.traffic))                       AS traffic,
    lower(util.clean_text(r.vehicle))                         AS vehicle,
    -- source spells it "Metropolitian"
    initcap(replace(lower(coalesce(util.clean_text(r.area), '')), 'metropolitian', 'metropolitan')) AS area,
    initcap(util.clean_text(r.category))                      AS category,
    util.to_numeric(r.delivery_time)::integer                 AS delivery_minutes
  FROM raw.amazon_delivery r
  WHERE r.batch_id = %(batch_id)s
),
derived AS (
  SELECT t.*,
    -- negative coords in the source are sign errors; repair with abs(), but 0 stays 0
    abs(raw_store_lat) AS store_lat, abs(raw_store_lon) AS store_lon,
    abs(raw_drop_lat)  AS drop_lat,  abs(raw_drop_lon)  AS drop_lon,
    (raw_store_lat < 0 OR raw_store_lon < 0 OR raw_drop_lat < 0 OR raw_drop_lon < 0) AS sign_fixed,
    (order_date + order_time) AS order_ts,
    CASE WHEN order_time IS NOT NULL AND pickup_time IS NOT NULL THEN
      CASE WHEN pickup_time >= order_time
           THEN extract(epoch FROM (pickup_time - order_time)) / 60
           ELSE extract(epoch FROM (pickup_time - order_time)) / 60 + 1440 END
    END AS pickup_delay_min,
    (pickup_time < order_time) AS pickup_next_day,
    row_number() OVER (PARTITION BY order_id ORDER BY row_num) AS dup_seq
  FROM typed t
),
measured AS (
  SELECT d.*,
    util.haversine_km(store_lat, store_lon, drop_lat, drop_lon) AS distance_km
  FROM derived d
)
SELECT
  batch_id, row_num, order_id, agent_age, agent_rating,
  store_lat, store_lon, drop_lat, drop_lon,
  order_ts,
  order_ts + make_interval(mins => pickup_delay_min::int) AS pickup_ts,
  weather, traffic, vehicle, NULLIF(area, '') AS area, category,
  delivery_minutes, round(pickup_delay_min, 1), distance_km,
  array_remove(ARRAY[
    CASE WHEN order_id IS NULL THEN 'E_ORDER_ID_NULL' END,
    CASE WHEN order_id IS NOT NULL AND dup_seq > 1 THEN 'E_DUPLICATE_ORDER' END,
    CASE WHEN agent_age IS NULL OR agent_age NOT BETWEEN 18 AND 65 THEN 'E_AGENT_AGE_RANGE' END,
    CASE WHEN agent_rating IS NOT NULL AND agent_rating NOT BETWEEN 1 AND 5 THEN 'E_RATING_RANGE' END,
    CASE WHEN raw_store_lat IS NULL OR raw_store_lon IS NULL OR raw_store_lat = 0 OR raw_store_lon = 0
         THEN 'E_STORE_COORD_ZERO' END,
    CASE WHEN NOT (coalesce(store_lat BETWEEN 6 AND 38 AND store_lon BETWEEN 68 AND 98, false)
                   AND coalesce(drop_lat BETWEEN 6 AND 38 AND drop_lon BETWEEN 68 AND 98, false))
              AND raw_store_lat <> 0 AND raw_store_lon <> 0
         THEN 'E_COORD_OUTSIDE_INDIA' END,
    CASE WHEN order_ts IS NULL THEN 'E_ORDER_TS_INVALID' END,
    CASE WHEN pickup_time IS NULL THEN 'E_PICKUP_TS_INVALID' END,
    CASE WHEN pickup_delay_min > 180 THEN 'E_PICKUP_DELAY_RANGE' END,
    CASE WHEN delivery_minutes IS NULL OR delivery_minutes NOT BETWEEN 1 AND 600 THEN 'E_DELIVERY_TIME_RANGE' END,
    CASE WHEN distance_km > 100 THEN 'E_DISTANCE_IMPLAUSIBLE' END,
    CASE WHEN agent_rating IS NULL THEN 'W_RATING_MISSING' END,
    CASE WHEN weather IS NULL THEN 'W_WEATHER_MISSING' END,
    CASE WHEN traffic IS NULL THEN 'W_TRAFFIC_MISSING' END,
    CASE WHEN sign_fixed THEN 'W_COORD_SIGN_FIXED' END,
    CASE WHEN pickup_next_day THEN 'W_PICKUP_NEXT_DAY' END,
    CASE WHEN vehicle IS NOT NULL AND vehicle NOT IN ('motorcycle', 'scooter', 'van', 'bicycle')
         THEN 'W_VEHICLE_UNKNOWN' END
  ], NULL)
FROM measured;
