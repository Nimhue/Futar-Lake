-- Starter queries for an AI/BI dashboard on top of the gold layer.
-- Replace main.bkk_transit with your catalog.schema.

-- 1. Delay heatmap: line x hour of day (weekday)
SELECT line, mode, hour_local, avg_delay_s, on_time_share, observations
FROM main.bkk_transit.gold_delay_by_line_hour
WHERE day_type = 'weekday' AND observations >= 20
ORDER BY mode, line, hour_local;

-- 2. Worst 15 lines at evening peak (16-18h), by p90 delay
SELECT mode, line,
       sum(observations)                                         AS observations,
       round(sum(avg_delay_s * observations) / sum(observations), 1) AS avg_delay_s,
       max(p90_delay_s)                                          AS worst_hour_p90_s
FROM main.bkk_transit.gold_delay_by_line_hour
WHERE day_type = 'weekday' AND hour_local BETWEEN 16 AND 18
GROUP BY mode, line
HAVING sum(observations) >= 100
ORDER BY worst_hour_p90_s DESC
LIMIT 15;

-- 3. Stop hotspots for a map visual
SELECT stop_name, stop_lat, stop_lon, mode, avg_delay_s, late_over_5min_share, observations
FROM main.bkk_transit.gold_stop_delay_hotspots
ORDER BY avg_delay_s DESC
LIMIT 200;

-- 4. Reliability trend per mode
SELECT service_date, mode,
       round(sum(on_time_share * observations) / sum(observations), 4) AS on_time_share
FROM main.bkk_transit.gold_line_daily_reliability
GROUP BY service_date, mode
ORDER BY service_date, mode;

-- 5. Is the feed alive? (alert if latest snapshot is older than 10 minutes)
SELECT feed, max(latest_feed_ts) AS latest_feed_ts,
       timestampdiff(MINUTE, max(latest_feed_ts), current_timestamp()) AS minutes_stale
FROM main.bkk_transit.gold_feed_health
GROUP BY feed;

-- 6. How good are BKK's predictions? Error of the prediction made N minutes before arrival.
WITH final AS (
  SELECT service_date, trip_id, stop_sequence, event_epoch AS actual_epoch
  FROM main.bkk_transit.silver_stop_arrivals
)
SELECT floor((f.actual_epoch - u.feed_epoch) / 60 / 5) * 5 AS minutes_ahead_bucket,
       count(*)                                          AS predictions,
       round(avg(abs(u.event_epoch - f.actual_epoch)), 1) AS mean_abs_error_s
FROM main.bkk_transit.silver_rt_stop_updates u
JOIN final f USING (service_date, trip_id, stop_sequence)
WHERE NOT u.is_realized AND f.actual_epoch - u.feed_epoch BETWEEN 0 AND 30 * 60
GROUP BY 1
ORDER BY 1;
