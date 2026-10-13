-- ====================================================================
-- Q08: Slowest tools: p95 latency by tool and share of total run time
-- Question: Which tools exhibit the highest 95th percentile latency, and what
-- proportion of total agent execution time do they consume?
-- Dataset: steps_orc and runs_orc
-- ====================================================================

SET hive.execution.engine=mr;
SET mapreduce.job.reduces=2;
USE ai_obs;

WITH tool_agg AS (
    SELECT
        tool_name,
        COUNT(*) AS total_calls,
        ROUND(percentile_approx(latency_ms, 0.95), 2) AS p95_latency_ms,
        ROUND(AVG(latency_ms), 2) AS avg_latency_ms,
        SUM(latency_ms) AS tool_total_latency_ms
    FROM steps_orc
    WHERE step_type = 'tool_call'
      AND tool_name IS NOT NULL
      AND latency_ms >= 0
    GROUP BY tool_name
),
overall_totals AS (
    SELECT
        SUM(duration_ms) AS total_run_duration_ms
    FROM runs_orc
)
SELECT
    t.tool_name,
    t.total_calls,
    t.p95_latency_ms,
    t.avg_latency_ms,
    t.tool_total_latency_ms,
    ROUND(100.0 * t.tool_total_latency_ms / o.total_run_duration_ms, 4) AS pct_of_total_run_time
FROM tool_agg t
CROSS JOIN overall_totals o
ORDER BY t.p95_latency_ms DESC;
