-- Null-safe cleaning helpers. Postgres has no TRY_CAST, so each returns NULL on bad input.

-- Trim + collapse inner whitespace; the text tokens that mean "missing" become NULL.
CREATE OR REPLACE FUNCTION util.clean_text(v text) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE
    WHEN lower(btrim(regexp_replace(coalesce(v, ''), '\s+', ' ', 'g')))
         IN ('', 'nan', 'null', 'none', 'n/a', 'na') THEN NULL
    ELSE btrim(regexp_replace(v, '\s+', ' ', 'g'))
  END
$$;

CREATE OR REPLACE FUNCTION util.to_numeric(v text) RETURNS numeric
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE s text := util.clean_text(v);
BEGIN
  IF s IS NULL THEN RETURN NULL; END IF;
  RETURN s::numeric;
EXCEPTION WHEN others THEN
  RETURN NULL;
END
$$;

CREATE OR REPLACE FUNCTION util.to_date(v text) RETURNS date
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE s text := util.clean_text(v);
BEGIN
  IF s IS NULL THEN RETURN NULL; END IF;
  RETURN s::date;
EXCEPTION WHEN others THEN
  RETURN NULL;
END
$$;

CREATE OR REPLACE FUNCTION util.to_time(v text) RETURNS time
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE s text := util.clean_text(v);
BEGIN
  IF s IS NULL THEN RETURN NULL; END IF;
  RETURN s::time;
EXCEPTION WHEN others THEN
  RETURN NULL;
END
$$;

-- Great-circle distance in km.
CREATE OR REPLACE FUNCTION util.haversine_km(lat1 numeric, lon1 numeric, lat2 numeric, lon2 numeric)
RETURNS numeric LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE WHEN lat1 IS NULL OR lon1 IS NULL OR lat2 IS NULL OR lon2 IS NULL THEN NULL
    ELSE round((2 * 6371 * asin(sqrt(
           power(sin(radians((lat2 - lat1)::float8) / 2), 2) +
           cos(radians(lat1::float8)) * cos(radians(lat2::float8)) *
           power(sin(radians((lon2 - lon1)::float8) / 2), 2)
         )))::numeric, 3)
  END
$$;
