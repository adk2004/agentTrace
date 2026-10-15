-- ====================================================================
-- Q11: Run success rate by model x task_type (conditional aggregation)
-- Question: What are the success, failure, and timeout rates across all
-- combinations of model and task domain?
-- Dataset: runs_orc
-- ====================================================================

SET hive.execution.engine=mr;
SET mapreduce.job.reduces=2;
USE ai_obs;

SELECT
    model,
    task_type,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN LOWER(final_status) = 'success' THEN 1 ELSE 0 END) AS successful_runs,
    SUM(CASE WHEN LOWER(final_status) = 'failed' THEN 1 ELSE 0 END) AS failed_runs,
    SUM(CASE WHEN LOWER(final_status) = 'timeout' THEN 1 ELSE 0 END) AS timeout_runs,
    ROUND(100.0 * CAST(SUM(CASE WHEN LOWER(final_status) = 'success' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*), 2) AS success_rate_pct,
    ROUND(100.0 * CAST(SUM(CASE WHEN LOWER(final_status) = 'failed' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*), 2) AS failure_rate_pct,
    ROUND(100.0 * CAST(SUM(CASE WHEN LOWER(final_status) = 'timeout' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*), 2) AS timeout_rate_pct
FROM runs_orc
GROUP BY model, task_type
ORDER BY model ASC, success_rate_pct DESC;
