-- ====================================================================
-- Q03: Retry-loop analysis: % of runs with is_retry_loop by model and tool
-- Question: What percentage of agent runs encounter retry loops (same tool failed
-- >= 3 times consecutively) broken down by model and tool?
-- Dataset: runs_orc joined with steps_orc
-- ====================================================================

SET hive.execution.engine=mr;
SET mapreduce.job.reduces=2;
USE ai_obs;

SELECT
    COALESCE(r.model, s.model) AS model,
    s.tool_name,
    COUNT(DISTINCT s.run_id) AS total_runs,
    COUNT(DISTINCT CASE WHEN r.is_retry_loop = 1 THEN s.run_id END) AS retry_loop_runs,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN r.is_retry_loop = 1 THEN s.run_id END) / COUNT(DISTINCT s.run_id), 2) AS retry_loop_pct
FROM steps_orc s
LEFT JOIN runs_orc r ON s.run_id = r.run_id
WHERE s.step_type = 'tool_call'
  AND s.tool_name IS NOT NULL
GROUP BY COALESCE(r.model, s.model), s.tool_name
ORDER BY retry_loop_pct DESC, total_runs DESC;
