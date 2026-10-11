-- ====================================================================
-- Q06: Context growth: avg prompt_tokens by step_index
-- Question: How do prompt token requirements grow across successive step indices
-- due to conversation history accumulation?
-- Dataset: steps_orc (filtered for llm_call)
-- ====================================================================

SET hive.execution.engine=mr;
SET mapreduce.job.reduces=2;
USE ai_obs;

SELECT
    step_index,
    COUNT(*) AS llm_call_count,
    ROUND(AVG(prompt_tokens), 2) AS avg_prompt_tokens,
    MIN(prompt_tokens) AS min_prompt_tokens,
    MAX(prompt_tokens) AS max_prompt_tokens,
    ROUND(AVG(completion_tokens), 2) AS avg_completion_tokens
FROM steps_orc
WHERE step_type = 'llm_call'
  AND prompt_tokens IS NOT NULL
GROUP BY step_index
ORDER BY step_index ASC;
