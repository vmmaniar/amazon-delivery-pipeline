-- Layers: raw (verbatim text) -> staging (typed + cleaned) -> core (star schema)
-- Side schemas: quarantine (rows that failed DQ), ops (run metadata), marts (reporting views)
CREATE SCHEMA IF NOT EXISTS util;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS quarantine;
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS marts;
