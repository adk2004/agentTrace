-- ====================================================================
-- Q15: Data-quality report: malformed ratio and null rate per column
-- Question: What is the malformed record ratio across raw inputs, and what is
-- the null value rate across every telemetry attribute in the curated dataset?
-- Dataset: steps_orc and dq_pipeline_counters
-- ====================================================================

SET hive.execution.engine=mr;
SET mapreduce.job.reduces=2;
USE ai_obs;

-- Ensure pipeline quality counters table exists
CREATE TABLE IF NOT EXISTS dq_pipeline_counters (
    metric_name STRING COMMENT 'Name of pipeline quality counter',
    metric_value BIGINT COMMENT 'Recorded value of counter'
)
COMMENT 'Small pipeline telemetry counters table for quality reporting'
STORED AS ORC;

-- Data quality metrics: null rates per column alongside pipeline malformed ratio
WITH step_metrics AS (
    SELECT
        COUNT(*) AS total_rows,
        COUNT(run_id) AS nn_run_id,
        COUNT(session_id) AS nn_session_id,
        COUNT(trace_id) AS nn_trace_id,
        COUNT(agent_name) AS nn_agent_name,
        COUNT(task_type) AS nn_task_type,
        COUNT(model) AS nn_model,
        COUNT(run_status) AS nn_run_status,
        COUNT(user_tier) AS nn_user_tier,
        COUNT(region) AS nn_region,
        COUNT(agent_version) AS nn_agent_version,
        COUNT(step_index) AS nn_step_index,
        COUNT(step_type) AS nn_step_type,
        COUNT(step_timestamp) AS nn_step_timestamp,
        COUNT(dt) AS nn_dt,
        COUNT(latency_ms) AS nn_latency_ms,
        COUNT(prompt_tokens) AS nn_prompt_tokens,
        COUNT(completion_tokens) AS nn_completion_tokens,
        COUNT(finish_reason) AS nn_finish_reason,
        COUNT(tool_name) AS nn_tool_name,
        COUNT(tool_status) AS nn_tool_status,
        COUNT(error_type) AS nn_error_type,
        COUNT(retry_count) AS nn_retry_count,
        COUNT(args_size_bytes) AS nn_args_size_bytes
    FROM steps_orc
),
counter_stats AS (
    SELECT
        ROUND(CAST(MAX(CASE WHEN metric_name = 'MALFORMED_LINES' THEN metric_value END) AS DOUBLE) /
              MAX(CASE WHEN metric_name = 'TOTAL_LINES' THEN metric_value END), 4) AS malformed_ratio,
        MAX(CASE WHEN metric_name = 'TOTAL_LINES' THEN metric_value END) AS total_raw_lines,
        MAX(CASE WHEN metric_name = 'MALFORMED_LINES' THEN metric_value END) AS total_malformed_lines
    FROM dq_pipeline_counters
)
SELECT
    'all_columns' AS scope,
    s.total_rows,
    c.total_raw_lines,
    c.total_malformed_lines,
    c.malformed_ratio,
    ROUND(CAST(s.total_rows - s.nn_prompt_tokens AS DOUBLE) / s.total_rows, 4) AS null_rate_prompt_tokens,
    ROUND(CAST(s.total_rows - s.nn_completion_tokens AS DOUBLE) / s.total_rows, 4) AS null_rate_completion_tokens,
    ROUND(CAST(s.total_rows - s.nn_tool_name AS DOUBLE) / s.total_rows, 4) AS null_rate_tool_name,
    ROUND(CAST(s.total_rows - s.nn_error_type AS DOUBLE) / s.total_rows, 4) AS null_rate_error_type,
    ROUND(CAST(s.total_rows - s.nn_finish_reason AS DOUBLE) / s.total_rows, 4) AS null_rate_finish_reason,
    ROUND(CAST(s.total_rows - s.nn_latency_ms AS DOUBLE) / s.total_rows, 4) AS null_rate_latency_ms
FROM step_metrics s
CROSS JOIN counter_stats c;
