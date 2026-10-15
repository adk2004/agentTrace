-- ====================================================================
-- Q13: Cost estimate by model using a small pricing lookup table created in Hive (plain arithmetic)
-- Question: What is the total and per-run financial cost estimated for each model
-- based on standard industry token pricing?
-- Dataset: runs_orc and model_pricing lookup table
-- ====================================================================

SET hive.execution.engine=mr;
SET mapreduce.job.reduces=2;
USE ai_obs;

-- Ensure pricing lookup table exists
CREATE TABLE IF NOT EXISTS model_pricing (
    model STRING COMMENT 'Model identifier',
    prompt_price_per_1m DOUBLE COMMENT 'Price in USD per 1M prompt tokens',
    completion_price_per_1m DOUBLE COMMENT 'Price in USD per 1M completion tokens'
)
COMMENT 'Pricing reference table for LLM token costs'
STORED AS ORC;

-- Compute cost telemetry per model
SELECT
    r.model,
    COUNT(*) AS total_runs,
    SUM(r.prompt_tokens_total) AS total_prompt_tokens,
    SUM(r.completion_tokens_total) AS total_completion_tokens,
    p.prompt_price_per_1m,
    p.completion_price_per_1m,
    ROUND((SUM(r.prompt_tokens_total) / 1000000.0) * p.prompt_price_per_1m, 4) AS prompt_cost_usd,
    ROUND((SUM(r.completion_tokens_total) / 1000000.0) * p.completion_price_per_1m, 4) AS completion_cost_usd,
    ROUND(
        ((SUM(r.prompt_tokens_total) / 1000000.0) * p.prompt_price_per_1m) +
        ((SUM(r.completion_tokens_total) / 1000000.0) * p.completion_price_per_1m),
        4
    ) AS total_cost_usd,
    ROUND(
        (((SUM(r.prompt_tokens_total) / 1000000.0) * p.prompt_price_per_1m) +
         ((SUM(r.completion_tokens_total) / 1000000.0) * p.completion_price_per_1m)) / COUNT(*),
        6
    ) AS avg_cost_per_run_usd
FROM runs_orc r
JOIN model_pricing p ON r.model = p.model
GROUP BY r.model, p.prompt_price_per_1m, p.completion_price_per_1m
ORDER BY total_cost_usd DESC;
