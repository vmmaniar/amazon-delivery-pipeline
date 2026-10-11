-- Apply the DQ verdicts for one batch. Param: %(batch_id)s. Re-runnable.

DELETE FROM quarantine.rejected_trip WHERE batch_id = %(batch_id)s;
DELETE FROM ops.dq_result            WHERE batch_id = %(batch_id)s;

-- 1. Quarantine every row with at least one error-severity violation, keeping the raw record.
INSERT INTO quarantine.rejected_trip (batch_id, row_num, order_id, error_codes, raw_record)
SELECT s.batch_id, s.row_num, s.order_id,
       ARRAY(SELECT v FROM unnest(s.violations) v WHERE v LIKE 'E\_%%'),
       to_jsonb(r) - 'batch_id' - 'row_num'
FROM staging.trip_clean s
JOIN raw.amazon_delivery r USING (batch_id, row_num)
WHERE s.batch_id = %(batch_id)s
  AND EXISTS (SELECT 1 FROM unnest(s.violations) v WHERE v LIKE 'E\_%%');

-- 2. One result row per rule, including rules with zero failures.
INSERT INTO ops.dq_result (batch_id, rule_code, rows_failed, rows_total)
SELECT %(batch_id)s, k.rule_code,
       count(s.row_num) FILTER (WHERE k.rule_code = ANY (s.violations)),
       count(s.row_num)
FROM ops.dq_rule k
CROSS JOIN staging.trip_clean s
WHERE s.batch_id = %(batch_id)s
GROUP BY k.rule_code;
