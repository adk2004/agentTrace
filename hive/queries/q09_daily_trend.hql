-- ====================================================================
-- Q09: Daily trend: runs, failure rate, total tokens by dt (7-day moving average window)
-- Question: What are the daily volume trends, failure rates, and token consumption
-- with 7-day trailing moving averages?
-- Dataset: runs_orc
-- ====================================================================

SET hive.execution.engine=mr;
SET mapreduce.job.reduces=2;
USE ai_obs;

WITH daily_agg AS (
    SELECT
        dt,
        COUNT(*) AS daily_runs,
        ROUND(CAST(SUM(CASE WHEN LOWER(final_status) != 'success' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*), 4) AS daily_failure_rate,
        SUM(prompt_tokens_total + completion_tokens_total) AS daily_tokens
    FROM runs_orc
    WHERE dt IS NOT NULL AND dt != 'unknown'
    GROUP BY dt
)
SELECT
    dt,
    daily_runs,
    daily_failure_rate,
    daily_tokens,
    ROUND(AVG(daily_runs) OVER (
        ORDER BY dt
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS ma7_runs,
    ROUND(AVG(daily_failure_rate) OVER (
        ORDER BY dt
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 4) AS ma7_failure_rate,
    ROUND(AVG(daily_tokens) OVER (
        ORDER BY dt
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS ma7_tokens
FROM daily_agg
ORDER BY dt ASC;
