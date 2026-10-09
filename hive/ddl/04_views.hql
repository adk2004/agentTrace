-- ====================================================================
-- 04_views.hql
-- Analytical views over curated ORC tables for focused analysis
-- ====================================================================

SET hive.execution.engine=mr;
SET mapreduce.job.reduces=2;

USE ai_obs;

-- --------------------------------------------------------------------
-- View: v_tool_calls
-- Filters curated steps to only tool_call operations with tool metadata
-- --------------------------------------------------------------------
CREATE OR REPLACE VIEW v_tool_calls
COMMENT 'Filtered view of curated steps for tool_call operations'
AS
SELECT
    run_id,
    session_id,
    trace_id,
    agent_name,
    task_type,
    model,
    run_status,
    user_tier,
    region,
    step_index,
    step_timestamp,
    dt,
    latency_ms,
    tool_name,
    tool_status,
    error_type,
    retry_count,
    args_size_bytes
FROM steps_orc
WHERE step_type = 'tool_call';

-- --------------------------------------------------------------------
-- View: v_llm_calls
-- Filters curated steps to only llm_call operations with token telemetry
-- --------------------------------------------------------------------
CREATE OR REPLACE VIEW v_llm_calls
COMMENT 'Filtered view of curated steps for llm_call operations with token telemetry'
AS
SELECT
    run_id,
    session_id,
    trace_id,
    agent_name,
    task_type,
    model,
    run_status,
    user_tier,
    region,
    step_index,
    step_timestamp,
    dt,
    latency_ms,
    prompt_tokens,
    completion_tokens,
    (COALESCE(prompt_tokens, 0) + COALESCE(completion_tokens, 0)) AS total_tokens,
    finish_reason
FROM steps_orc
WHERE step_type = 'llm_call';
