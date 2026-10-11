-- Run metadata
CREATE TABLE IF NOT EXISTS ops.load_batch (
  batch_id     serial PRIMARY KEY,
  source_file  text        NOT NULL,
  file_sha256  text        NOT NULL,
  row_count    integer,
  status       text        NOT NULL DEFAULT 'loading'
               CHECK (status IN ('loading', 'loaded', 'transformed', 'failed')),
  started_at   timestamptz NOT NULL DEFAULT now(),
  finished_at  timestamptz,
  UNIQUE (file_sha256)          -- same file is never loaded twice
);

-- Raw landing zone: every column verbatim as text, exactly as in the CSV.
CREATE TABLE IF NOT EXISTS raw.amazon_delivery (
  batch_id       integer NOT NULL REFERENCES ops.load_batch(batch_id),
  row_num        integer NOT NULL,                -- 1-based line within the file (excl. header)
  order_id       text, agent_age text, agent_rating text,
  store_latitude text, store_longitude text, drop_latitude text, drop_longitude text,
  order_date     text, order_time text, pickup_time text,
  weather text, traffic text, vehicle text, area text,
  delivery_time  text, category text,
  PRIMARY KEY (batch_id, row_num)
);

-- Data-quality rule catalogue. 'error' => row is quarantined. 'warn' => row loads, issue is counted.
CREATE TABLE IF NOT EXISTS ops.dq_rule (
  rule_code   text PRIMARY KEY,
  severity    text NOT NULL CHECK (severity IN ('error', 'warn')),
  description text NOT NULL
);

INSERT INTO ops.dq_rule (rule_code, severity, description) VALUES
  ('E_ORDER_ID_NULL',        'error', 'order_id is missing'),
  ('E_DUPLICATE_ORDER',      'error', 'order_id already seen earlier in this batch'),
  ('E_AGENT_AGE_RANGE',      'error', 'agent age missing or outside 18-65'),
  ('E_RATING_RANGE',         'error', 'agent rating outside the 1-5 scale'),
  ('E_STORE_COORD_ZERO',     'error', 'store latitude/longitude is 0 or missing'),
  ('E_COORD_OUTSIDE_INDIA',  'error', 'store or drop coordinates outside the India bounding box'),
  ('E_ORDER_TS_INVALID',     'error', 'order date/time missing or unparseable'),
  ('E_PICKUP_TS_INVALID',    'error', 'pickup time missing or unparseable'),
  ('E_PICKUP_DELAY_RANGE',   'error', 'pickup more than 3 hours after the order'),
  ('E_DELIVERY_TIME_RANGE',  'error', 'delivery time missing or outside 1-600 minutes'),
  ('E_DISTANCE_IMPLAUSIBLE', 'error', 'store-to-drop distance over 100 km for a last-mile trip'),
  ('W_RATING_MISSING',       'warn',  'agent rating missing (kept as NULL)'),
  ('W_WEATHER_MISSING',      'warn',  'weather missing (loaded as Unknown)'),
  ('W_TRAFFIC_MISSING',      'warn',  'traffic missing (loaded as Unknown)'),
  ('W_COORD_SIGN_FIXED',     'warn',  'negative coordinate sign repaired (abs value used)'),
  ('W_PICKUP_NEXT_DAY',      'warn',  'pickup is after midnight; rolled to the next day'),
  ('W_VEHICLE_UNKNOWN',      'warn',  'vehicle type not in the known list')
ON CONFLICT (rule_code) DO UPDATE
  SET severity = EXCLUDED.severity, description = EXCLUDED.description;

CREATE TABLE IF NOT EXISTS ops.dq_result (
  batch_id    integer NOT NULL REFERENCES ops.load_batch(batch_id),
  rule_code   text    NOT NULL REFERENCES ops.dq_rule(rule_code),
  rows_failed integer NOT NULL,
  rows_total  integer NOT NULL,
  checked_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (batch_id, rule_code)
);
