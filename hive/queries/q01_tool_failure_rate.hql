-- ====================================================================
-- Q01: Tool failure rate by tool and task_type (with rank)
-- Question: What is the failure rate of each tool across different task types,
-- and how do they rank by failure rate?
-- Dataset: steps_orc (filtered for tool_call operations)
-- ====================================================================

SET hive.execution.engine=mr;
SET mapreduce.job.reduces=2;
USE ai_obs;

SELECT
    tool_name,
    task_type,
    COUNT(*) AS total_calls,
    SUM(CASE WHEN LOWER(tool_status) = 'ok' THEN 1 ELSE 0 END) AS ok_calls,
    SUM(CASE WHEN LOWER(tool_status) = 'error' THEN 1 ELSE 0 END) AS error_calls,
    SUM(CASE WHEN LOWER(tool_status) = 'timeout' THEN 1 ELSE 0 END) AS timeout_calls,
    ROUND(CAST(SUM(CASE WHEN LOWER(tool_status) IN ('error', 'timeout') THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*), 4) AS failure_rate,
    DENSE_RANK() OVER (
        ORDER BY CAST(SUM(CASE WHEN LOWER(tool_status) IN ('error', 'timeout') THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) DESC,
                 COUNT(*) DESC
    ) AS failure_rank
FROM steps_orc
WHERE step_type = 'tool_call'
  AND tool_name IS NOT NULL
GROUP BY tool_name, task_type
ORDER BY failure_rank ASC, total_calls DESC;
