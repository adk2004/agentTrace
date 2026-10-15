-- ====================================================================
-- Q12: Most common failing tool sequences: LAG() to get (previous tool -> failing tool) top 10
-- Question: What are the top 10 most common tool transition sequences where a tool
-- execution fails after a preceding tool call?
-- Dataset: steps_orc
-- ====================================================================

SET hive.execution.engine=mr;
SET mapreduce.job.reduces=2;
USE ai_obs;

WITH tool_transitions AS (
    SELECT
        run_id,
        step_index,
        tool_name AS current_tool,
        tool_status,
        error_type,
        LAG(tool_name, 1) OVER (
            PARTITION BY run_id
            ORDER BY step_index ASC
        ) AS previous_tool
    FROM steps_orc
    WHERE step_type = 'tool_call'
      AND tool_name IS NOT NULL
)
SELECT
    previous_tool,
    current_tool AS failing_tool,
    COALESCE(error_type, 'unspecified_error') AS error_type,
    COUNT(*) AS occurrence_count
FROM tool_transitions
WHERE LOWER(tool_status) IN ('error', 'timeout')
  AND previous_tool IS NOT NULL
GROUP BY previous_tool, current_tool, COALESCE(error_type, 'unspecified_error')
ORDER BY occurrence_count DESC
LIMIT 10;
