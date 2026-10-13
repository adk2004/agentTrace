-- ====================================================================
-- Q07: Latency percentiles by model and step_type
-- Question: What are the latency distributions (p50, p90, p95, p99) across
-- different models and operation step types?
-- Dataset: steps_orc
-- Note: EXPLAIN and runtime comparison with steps (text) generated in scripts/06_run_queries.sh
-- ====================================================================

SET hive.execution.engine=mr;
SET mapreduce.job.reduces=2;
USE ai_obs;

SELECT
    model,
    step_type,
    COUNT(*) AS total_samples,
    ROUND(percentile_approx(latency_ms, 0.50), 2) AS p50_latency_ms,
    ROUND(percentile_approx(latency_ms, 0.90), 2) AS p90_latency_ms,
    ROUND(percentile_approx(latency_ms, 0.95), 2) AS p95_latency_ms,
    ROUND(percentile_approx(latency_ms, 0.99), 2) AS p99_latency_ms,
    ROUND(AVG(latency_ms), 2) AS avg_latency_ms
FROM steps_orc
WHERE latency_ms IS NOT NULL
  AND latency_ms >= 0
GROUP BY model, step_type
ORDER BY model ASC, step_type ASC;
