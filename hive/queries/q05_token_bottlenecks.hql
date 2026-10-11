-- ====================================================================
-- Q05: Token bottlenecks: top 1 percent of runs by prompt tokens (NTILE) and their task types
-- Question: Which task types and models represent the top 1% heaviest token bottleneck runs?
-- Dataset: runs_orc
-- ====================================================================

SET hive.execution.engine=mr;
SET mapreduce.job.reduces=2;
USE ai_obs;

WITH ranked_runs AS (
    SELECT
        run_id,
        task_type,
        model,
        prompt_tokens_total,
        completion_tokens_total,
        (prompt_tokens_total + completion_tokens_total) AS total_tokens,
        NTILE(100) OVER (ORDER BY prompt_tokens_total DESC) AS pct_tile
    FROM runs_orc
    WHERE prompt_tokens_total IS NOT NULL
)
SELECT
    task_type,
    model,
    COUNT(*) AS top_1pct_run_count,
    MIN(prompt_tokens_total) AS min_prompt_tokens,
    MAX(prompt_tokens_total) AS max_prompt_tokens,
    ROUND(AVG(prompt_tokens_total), 2) AS avg_prompt_tokens,
    ROUND(AVG(total_tokens), 2) AS avg_total_tokens
FROM ranked_runs
WHERE pct_tile = 1
GROUP BY task_type, model
ORDER BY top_1pct_run_count DESC, avg_prompt_tokens DESC;
