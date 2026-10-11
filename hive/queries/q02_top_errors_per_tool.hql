-- ====================================================================
-- Q02: Top 5 error types per tool (ROW_NUMBER)
-- Question: What are the top 5 most frequent error types encountered by each tool?
-- Dataset: steps_orc
-- ====================================================================

SET hive.execution.engine=mr;
SET mapreduce.job.reduces=2;
USE ai_obs;

WITH tool_error_counts AS (
    SELECT
        tool_name,
        error_type,
        COUNT(*) AS error_occurrences
    FROM steps_orc
    WHERE step_type = 'tool_call'
      AND tool_name IS NOT NULL
      AND error_type IS NOT NULL
      AND error_type != '\\N'
      AND error_type != ''
    GROUP BY tool_name, error_type
),
ranked_errors AS (
    SELECT
        tool_name,
        error_type,
        error_occurrences,
        ROW_NUMBER() OVER (
            PARTITION BY tool_name
            ORDER BY error_occurrences DESC, error_type ASC
        ) AS error_rank
    FROM tool_error_counts
)
SELECT
    tool_name,
    error_rank,
    error_type,
    error_occurrences
FROM ranked_errors
WHERE error_rank <= 5
ORDER BY tool_name ASC, error_rank ASC;
