-- ====================================================================
-- 03_curated_orc.hql
-- Curated ORC tables with Snappy compression and dynamic partitioning by dt.
-- Optimized for column pruning and vectorized execution.
-- ====================================================================

SET hive.execution.engine=mr;
SET mapreduce.job.reduces=2;
SET mapreduce.map.memory.mb=1024;
SET mapreduce.reduce.memory.mb=1024;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=500;
SET hive.exec.max.created.files=100000;
SET hive.optimize.sort.dynamic.partition=true;

USE ai_obs;

-- --------------------------------------------------------------------
-- Table: steps_orc
-- Source: Curated partitioned ORC copy of steps
-- Partition: dt (UTC Date string yyyy-MM-dd)
-- --------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS steps_orc (
    run_id STRING COMMENT 'Unique identifier of the run',
    session_id STRING COMMENT 'Session identifier grouping multiple runs',
    trace_id STRING COMMENT 'Root trace identifier',
    agent_name STRING COMMENT 'Name of the agent',
    task_type STRING COMMENT 'Task domain: coding, web_research, data_analysis, customer_support, summarization',
    model STRING COMMENT 'LLM model backing the agent',
    run_status STRING COMMENT 'Overall run status: success, failed, timeout',
    user_tier STRING COMMENT 'User tier: free, pro',
    region STRING COMMENT 'Cloud deployment region',
    agent_version STRING COMMENT 'Semantic version of the agent',
    step_index INT COMMENT '0-indexed position of step within run',
    step_type STRING COMMENT 'Type of step: llm_call, tool_call, retrieval',
    step_timestamp BIGINT COMMENT 'Epoch millisecond timestamp of step start',
    latency_ms INT COMMENT 'Execution duration of the step in milliseconds',
    prompt_tokens INT COMMENT 'Prompt tokens consumed (null for tool_call)',
    completion_tokens INT COMMENT 'Completion tokens generated (null for tool_call)',
    finish_reason STRING COMMENT 'Stop reason: stop, length, tool_calls',
    tool_name STRING COMMENT 'Name of tool invoked (null for llm/retrieval)',
    tool_status STRING COMMENT 'Tool execution status: ok, error, timeout',
    error_type STRING COMMENT 'Specific error classification if tool failed',
    retry_count INT COMMENT 'Retry attempt count for this tool call',
    args_size_bytes INT COMMENT 'Size of tool input arguments in bytes'
)
COMMENT 'Curated ORC table of execution steps partitioned by date'
PARTITIONED BY (dt STRING)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");

-- Load steps_orc from external steps table via dynamic partitioning
INSERT OVERWRITE TABLE steps_orc PARTITION (dt)
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
    agent_version,
    step_index,
    step_type,
    step_timestamp,
    latency_ms,
    prompt_tokens,
    completion_tokens,
    finish_reason,
    tool_name,
    tool_status,
    error_type,
    retry_count,
    args_size_bytes,
    dt
FROM steps;

-- --------------------------------------------------------------------
-- Table: runs_orc
-- Source: Curated partitioned ORC copy of runs
-- Partition: dt (UTC Date string yyyy-MM-dd derived from start_ts)
-- --------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS runs_orc (
    run_id STRING COMMENT 'Unique run identifier',
    session_id STRING COMMENT 'Associated session identifier',
    agent_name STRING COMMENT 'Agent name',
    task_type STRING COMMENT 'Task domain',
    model STRING COMMENT 'Underlying LLM model',
    start_ts BIGINT COMMENT 'Epoch millisecond of first step',
    end_ts BIGINT COMMENT 'Epoch millisecond of last step',
    duration_ms BIGINT COMMENT 'Total run span duration in milliseconds',
    total_steps INT COMMENT 'Total count of steps in this run',
    llm_calls INT COMMENT 'Number of LLM invocations',
    tool_calls INT COMMENT 'Number of tool calls executed',
    tool_failures INT COMMENT 'Number of tool calls that failed',
    tool_timeouts INT COMMENT 'Number of tool calls that timed out',
    total_retries INT COMMENT 'Sum of retry attempts across all tool calls',
    prompt_tokens_total BIGINT COMMENT 'Total prompt tokens consumed in this run',
    completion_tokens_total BIGINT COMMENT 'Total completion tokens generated in this run',
    total_latency_ms BIGINT COMMENT 'Sum of step latencies in milliseconds',
    max_step_latency_ms BIGINT COMMENT 'Maximum single step latency in milliseconds',
    distinct_tools_used INT COMMENT 'Count of unique tools used in this run',
    first_error_type STRING COMMENT 'First error type encountered in the run',
    final_status STRING COMMENT 'Final execution status of run: success, failed, timeout',
    is_retry_loop INT COMMENT '1 if same tool failed >= 3 times consecutively, else 0'
)
COMMENT 'Curated ORC table of run sessions partitioned by date'
PARTITIONED BY (dt STRING)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");

-- Load runs_orc from external runs table with start_ts converted to dt
INSERT OVERWRITE TABLE runs_orc PARTITION (dt)
SELECT
    run_id,
    session_id,
    agent_name,
    task_type,
    model,
    start_ts,
    end_ts,
    duration_ms,
    total_steps,
    llm_calls,
    tool_calls,
    tool_failures,
    tool_timeouts,
    total_retries,
    prompt_tokens_total,
    completion_tokens_total,
    total_latency_ms,
    max_step_latency_ms,
    distinct_tools_used,
    first_error_type,
    final_status,
    is_retry_loop,
    COALESCE(from_unixtime(CAST(start_ts / 1000 AS BIGINT), 'yyyy-MM-dd'), 'unknown') AS dt
FROM runs;

-- --------------------------------------------------------------------
-- Table: model_pricing
-- Description: Reference pricing catalog for LLM token costs (USD per 1M tokens)
-- --------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS model_pricing (
    model STRING COMMENT 'Model identifier',
    prompt_price_per_1m DOUBLE COMMENT 'Price in USD per 1M prompt tokens',
    completion_price_per_1m DOUBLE COMMENT 'Price in USD per 1M completion tokens'
)
COMMENT 'Pricing reference table for LLM token costs'
STORED AS ORC;

INSERT OVERWRITE TABLE model_pricing VALUES
('gpt-4o', 5.00, 15.00),
('claude-sonnet', 3.00, 15.00),
('llama-3-70b', 0.80, 0.80),
('mistral-large', 2.00, 6.00),
('gemini-pro', 1.25, 5.00);

-- --------------------------------------------------------------------
-- Table: dq_pipeline_counters
-- Description: Pipeline execution quality counters for DQ reporting
-- --------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dq_pipeline_counters (
    metric_name STRING COMMENT 'Name of pipeline quality counter',
    metric_value BIGINT COMMENT 'Recorded value of counter'
)
COMMENT 'Small pipeline telemetry counters table for quality reporting'
STORED AS ORC;

INSERT OVERWRITE TABLE dq_pipeline_counters VALUES
('TOTAL_LINES', 880),
('VALID_RUNS', 100),
('MALFORMED_LINES', 10),
('STEPS_EMITTED', 870),
('RUNS_BUILT', 9904);

