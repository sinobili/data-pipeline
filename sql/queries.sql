-- 4.a) Driver Performance Report
SELECT
  driver_id AS "DRIVER",
  COUNT(DISTINCT DATE(trip_date)) AS "TOTAL_DAYS",
  ROUND(
     100.0 * SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) / COUNT(*)
  )::INT AS "SUCCESS_RATE"
FROM trips
GROUP BY driver_id
ORDER BY "SUCCESS_RATE" DESC, "TOTAL_DAYS" DESC;

-- 4.b) Loyal Customer Analysis
SELECT
  client_id AS "CLIENT_ID",
  driver_id AS "DRIVER_ID",
  COUNT(*) AS "TRIP_COUNT"
FROM trips
GROUP BY client_id, driver_id
HAVING COUNT(DISTINCT DATE(trip_date)) > 1
ORDER BY "TRIP_COUNT" DESC, "CLIENT_ID", "DRIVER_ID";
