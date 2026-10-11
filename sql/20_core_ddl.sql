-- Star schema around core.fact_trip (one row per delivery trip).

CREATE TABLE IF NOT EXISTS core.dim_date (
  date_key    integer PRIMARY KEY,         -- yyyymmdd
  full_date   date    NOT NULL UNIQUE,
  year        smallint NOT NULL,
  month       smallint NOT NULL,
  day         smallint NOT NULL,
  dow         smallint NOT NULL,           -- 1=Mon .. 7=Sun (ISO)
  day_name    text     NOT NULL,
  is_weekend  boolean  NOT NULL
);

CREATE TABLE IF NOT EXISTS core.dim_vehicle  (vehicle_key  serial PRIMARY KEY, vehicle_type  text NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS core.dim_weather  (weather_key  serial PRIMARY KEY, weather       text NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS core.dim_traffic  (traffic_key  serial PRIMARY KEY, traffic       text NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS core.dim_area     (area_key     serial PRIMARY KEY, area_type     text NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS core.dim_category (category_key serial PRIMARY KEY, category_name text NOT NULL UNIQUE);

-- Reference cities; a store is assigned to the nearest centroid within 60 km, else 'Other'.
CREATE TABLE IF NOT EXISTS core.dim_city (
  city_key  serial PRIMARY KEY,
  city_name text NOT NULL UNIQUE,
  lat       numeric(9,6),
  lon       numeric(9,6)
);
INSERT INTO core.dim_city (city_name, lat, lon) VALUES
  ('Bengaluru', 12.9716, 77.5946), ('Mumbai', 19.0760, 72.8777), ('Delhi', 28.6139, 77.2090),
  ('Chennai', 13.0827, 80.2707), ('Hyderabad', 17.3850, 78.4867), ('Pune', 18.5204, 73.8567),
  ('Kolkata', 22.5726, 88.3639), ('Ahmedabad', 23.0225, 72.5714), ('Jaipur', 26.9124, 75.7873),
  ('Indore', 22.7196, 75.8577), ('Kochi', 9.9312, 76.2673), ('Goa', 15.4909, 73.8278),
  ('Ranchi', 23.3441, 85.3096), ('Surat', 21.1702, 72.8311), ('Coimbatore', 11.0168, 76.9558),
  ('Mysuru', 12.2958, 76.6394), ('Bhopal', 23.2599, 77.4126), ('Kanpur', 26.4499, 80.3319),
  ('Nagpur', 21.1458, 79.0882), ('Vadodara', 22.3072, 73.1812), ('Aurangabad', 19.8762, 75.3433),
  ('Dehradun', 30.3165, 78.0322), ('Allahabad', 25.4358, 81.8463), ('Agra', 27.1767, 78.0081),
  ('Mangalore', 12.9141, 74.8560), ('Other', NULL, NULL)
ON CONFLICT (city_name) DO NOTHING;

CREATE TABLE IF NOT EXISTS core.dim_store (
  store_key serial PRIMARY KEY,
  lat       numeric(9,6) NOT NULL,
  lon       numeric(9,6) NOT NULL,
  city_key  integer NOT NULL REFERENCES core.dim_city(city_key),
  UNIQUE (lat, lon)
);

CREATE TABLE IF NOT EXISTS core.fact_trip (
  trip_key          bigserial PRIMARY KEY,
  order_id          text    NOT NULL UNIQUE,         -- natural key => upserts are idempotent
  order_ts          timestamp NOT NULL,
  pickup_ts         timestamp NOT NULL,
  date_key          integer NOT NULL REFERENCES core.dim_date(date_key),
  store_key         integer NOT NULL REFERENCES core.dim_store(store_key),
  vehicle_key       integer NOT NULL REFERENCES core.dim_vehicle(vehicle_key),
  weather_key       integer NOT NULL REFERENCES core.dim_weather(weather_key),
  traffic_key       integer NOT NULL REFERENCES core.dim_traffic(traffic_key),
  area_key          integer NOT NULL REFERENCES core.dim_area(area_key),
  category_key      integer NOT NULL REFERENCES core.dim_category(category_key),
  agent_age         smallint NOT NULL CHECK (agent_age BETWEEN 18 AND 65),
  agent_rating      numeric(2,1) CHECK (agent_rating BETWEEN 1 AND 5),
  drop_lat          numeric(9,6) NOT NULL,
  drop_lon          numeric(9,6) NOT NULL,
  distance_km       numeric(8,3) NOT NULL,
  pickup_delay_min  numeric(6,1) NOT NULL CHECK (pickup_delay_min >= 0),
  delivery_minutes  integer NOT NULL CHECK (delivery_minutes > 0),
  speed_kmph        numeric(8,2),
  dq_warnings       text[] NOT NULL DEFAULT '{}',
  batch_id          integer NOT NULL REFERENCES ops.load_batch(batch_id),
  loaded_at         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_fact_trip_date    ON core.fact_trip (date_key);
CREATE INDEX IF NOT EXISTS ix_fact_trip_store   ON core.fact_trip (store_key);
CREATE INDEX IF NOT EXISTS ix_fact_trip_vehicle ON core.fact_trip (vehicle_key);

-- Rows that failed an error-severity rule. Nothing is dropped silently: the raw record is kept as JSON.
CREATE TABLE IF NOT EXISTS quarantine.rejected_trip (
  batch_id       integer NOT NULL REFERENCES ops.load_batch(batch_id),
  row_num        integer NOT NULL,
  order_id       text,
  error_codes    text[]  NOT NULL,
  raw_record     jsonb   NOT NULL,
  quarantined_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (batch_id, row_num)
);
CREATE INDEX IF NOT EXISTS ix_rejected_codes ON quarantine.rejected_trip USING gin (error_codes);
