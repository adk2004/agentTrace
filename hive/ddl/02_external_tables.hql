-- ====================================================================
-- 02_external_tables.hql
-- External tables mapping directly to MapReduce job outputs on HDFS.
-- Delimiter: Tab (\t)
-- Null definition: \N
-- Storage: TextFile
-- ====================================================================

SET hive.execution.engine=mr;
SET mapreduce.job.reduces=2;

USE ai_obs;

-- --------------------------------------------------------------------
-- Table 1: steps
-- Source Job: TraceFlattenJob (Job 1) -> /data/processed/steps
-- Description: Flattened individual execution steps across all runs.
-- --------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS steps (
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
    dt STRING COMMENT 'UTC Date string in yyyy-MM-dd format',
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
COMMENT 'Source Job: TraceFlattenJob (Job 1) -> /data/processed/steps'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
NULL DEFINED AS '\\N'
STORED AS TEXTFILE
LOCATION '/data/processed/steps';

-- --------------------------------------------------------------------
-- Table 2: runs
-- Source Job: SessionizationJob (Job 2) -> /data/processed/runs
-- Description: Run-level sessionized summaries streamed via secondary sort.
-- --------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS runs (
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
COMMENT 'Source Job: SessionizationJob (Job 2) -> /data/processed/runs'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
NULL DEFINED AS '\\N'
STORED AS TEXTFILE
LOCATION '/data/processed/runs';

-- --------------------------------------------------------------------
-- Table 3: tool_stats
-- Source Job: ToolStatsJob (Job 3) -> /data/analytics/tool_stats
-- Description: Aggregated tool usage and failure metrics per tool and task_type.
-- --------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS tool_stats (
    tool_task STRING COMMENT 'Composite key: tool_name|task_type',
    calls BIGINT COMMENT 'Total invocations of this tool in this task type',
    ok_calls BIGINT COMMENT 'Successful executions',
    error_calls BIGINT COMMENT 'Executions resulting in error',
    timeout_calls BIGINT COMMENT 'Executions resulting in timeout',
    failure_rate DOUBLE COMMENT 'Fraction of calls that failed or timed out',
    avg_latency_ms DOUBLE COMMENT 'Average execution latency in milliseconds',
    avg_retries DOUBLE COMMENT 'Average retries per call',
    top_error_types STRING COMMENT 'Semicolon-separated error_type:count sorted descending'
)
COMMENT 'Source Job: ToolStatsJob (Job 3) -> /data/analytics/tool_stats'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
NULL DEFINED AS '\\N'
STORED AS TEXTFILE
LOCATION '/data/analytics/tool_stats';

-- --------------------------------------------------------------------
-- Table 4: token_stats
-- Source Job: TokenStatsJob (Job 4) -> /data/analytics/token_stats
-- Description: Token usage aggregates per model, task_type and date.
-- --------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS token_stats (
    model_task_dt STRING COMMENT 'Composite key: model|task_type|dt',
    llm_calls BIGINT COMMENT 'Total LLM calls in this group',
    sum_prompt BIGINT COMMENT 'Total prompt tokens consumed',
    sum_completion BIGINT COMMENT 'Total completion tokens generated',
    avg_prompt DOUBLE COMMENT 'Mean prompt tokens per LLM call',
    avg_completion DOUBLE COMMENT 'Mean completion tokens per LLM call',
    max_prompt BIGINT COMMENT 'Maximum prompt tokens in a single call',
    max_completion BIGINT COMMENT 'Maximum completion tokens in a single call',
    tokens_0_1 BIGINT COMMENT 'Prompt tokens in step indices 0-1',
    tokens_2_4 BIGINT COMMENT 'Prompt tokens in step indices 2-4',
    tokens_5_9 BIGINT COMMENT 'Prompt tokens in step indices 5-9',
    tokens_10_plus BIGINT COMMENT 'Prompt tokens in step indices 10+'
)
COMMENT 'Source Job: TokenStatsJob (Job 4) -> /data/analytics/token_stats'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
NULL DEFINED AS '\\N'
STORED AS TEXTFILE
LOCATION '/data/analytics/token_stats';

-- --------------------------------------------------------------------
-- Table 5: latency_hist
-- Source Job: LatencyHistogramJob (Job 5) -> /data/analytics/latency_hist
-- Description: Latency distributions binned by model and step_type.
-- --------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS latency_hist (
    model_step_type STRING COMMENT 'Composite key: model|step_type',
    bucket STRING COMMENT 'Latency bucket boundary in ms (<100, 100-250, etc.)',
    count BIGINT COMMENT 'Number of operations falling into bucket',
    min_latency BIGINT COMMENT 'Minimum observed latency in this bucket (ms)',
    max_latency BIGINT COMMENT 'Maximum observed latency in this bucket (ms)',
    avg_latency DOUBLE COMMENT 'Average observed latency in this bucket (ms)',
    sum_latency BIGINT COMMENT 'Sum of observed latencies in this bucket (ms)'
)
COMMENT 'Source Job: LatencyHistogramJob (Job 5) -> /data/analytics/latency_hist'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
NULL DEFINED AS '\\N'
STORED AS TEXTFILE
LOCATION '/data/analytics/latency_hist';
