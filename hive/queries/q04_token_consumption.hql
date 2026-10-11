-- ====================================================================
-- Q04: Token consumption by model: total, avg per run, avg per llm_call
-- Question: How do different LLM models compare in total token consumption,
-- average token usage per run, and average tokens consumed per LLM invocation?
-- Dataset: runs_orc
-- ====================================================================

SET hive.execution.engine=mr;
SET mapreduce.job.reduces=2;
USE ai_obs;

SELECT
    model,
    COUNT(*) AS total_runs,
    SUM(llm_calls) AS total_llm_calls,
    SUM(prompt_tokens_total) AS total_prompt_tokens,
    SUM(completion_tokens_total) AS total_completion_tokens,
    SUM(prompt_tokens_total + completion_tokens_total) AS total_tokens,
    ROUND(AVG(prompt_tokens_total + completion_tokens_total), 2) AS avg_tokens_per_run,
    ROUND(CAST(SUM(prompt_tokens_total + completion_tokens_total) AS DOUBLE) / NULLIF(SUM(llm_calls), 0), 2) AS avg_tokens_per_llm_call
FROM runs_orc
GROUP BY model
ORDER BY total_tokens DESC;
