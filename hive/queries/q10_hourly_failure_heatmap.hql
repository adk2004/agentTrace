-- ====================================================================
-- Q10: Hour-of-day failure heatmap data
-- Question: How do failure rates vary across days of the week and hours of the day?
-- Dataset: runs_orc
-- ====================================================================

SET hive.execution.engine=mr;
SET mapreduce.job.reduces=2;
USE ai_obs;

SELECT
    from_unixtime(CAST(start_ts / 1000 AS BIGINT), 'u') AS day_of_week,
    from_unixtime(CAST(start_ts / 1000 AS BIGINT), 'EEEE') AS day_name,
    from_unixtime(CAST(start_ts / 1000 AS BIGINT), 'HH') AS hour_of_day,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN LOWER(final_status) != 'success' THEN 1 ELSE 0 END) AS failed_runs,
    ROUND(100.0 * CAST(SUM(CASE WHEN LOWER(final_status) != 'success' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*), 2) AS failure_rate_pct
FROM runs_orc
WHERE start_ts IS NOT NULL
GROUP BY
    from_unixtime(CAST(start_ts / 1000 AS BIGINT), 'u'),
    from_unixtime(CAST(start_ts / 1000 AS BIGINT), 'EEEE'),
    from_unixtime(CAST(start_ts / 1000 AS BIGINT), 'HH')
ORDER BY day_of_week ASC, hour_of_day ASC;
