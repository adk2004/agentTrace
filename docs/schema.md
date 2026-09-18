# Data Schema Documentation

**Project:** Large-Scale Distributed Analysis of AI Agent Execution Traces and LLM Telemetry Logs  
**Author:** Saurav Patel  
**Last Updated:** 2026-10-04

---

## Overview

The pipeline ingests agent execution traces in two complementary formats:

| Property | Format A — Nested Run Record | Format B — Flat Span Event |
|---|---|---|
| Granularity | One JSON object per complete agent run | One JSON object per step/span |
| Locality | All steps for a run are co-located in a single line | Events of the same `run_id` are scattered across files |
| Primary use | Run-level aggregation, step arrays | Step-level joins, time-series analysis |
| HDFS path | `/data/raw/nested/` | `/data/raw/flat/` |
| File naming | `nested_part_NNNNN.jsonl` | `flat_part_NNNNN.jsonl` |

---

## Enumerated Values

### `task_type`
| Value | Description |
|---|---|
| `coding` | Code generation and debugging tasks |
| `web_research` | Internet search and summarisation |
| `data_analysis` | Structured data querying and insight generation |
| `customer_support` | Conversational support interactions |
| `summarization` | Document / thread summarisation |

### `model`
| Value | Provider |
|---|---|
| `gpt-4o` | OpenAI |
| `claude-sonnet` | Anthropic |
| `llama-3-70b` | Meta (open-weight) |
| `mistral-large` | Mistral AI |
| `gemini-pro` | Google DeepMind |

### `status` (run level)
`success` · `failed` · `timeout`

### `step.type`
| Value | Has llm block | Has tool block |
|---|---|---|
| `llm_call` | YES | NO |
| `retrieval` | YES | NO |
| `tool_call` | NO | YES |

### `llm.finish_reason`
`stop` · `length` · `tool_calls`

### `tool.name`
`web_search` · `code_exec` · `sql_query` · `file_read` · `http_get` · `calculator`

### `tool.status`
`ok` · `error` · `timeout`

### `tool.error_type` (null when status = ok)
`rate_limit` · `timeout` · `invalid_args` · `network_error` · `parse_error`

### `metadata.user_tier`
`free` · `pro`

### `metadata.region`
`us-east-1` · `us-west-2` · `eu-west-1` · `ap-southeast-1` · `ap-northeast-1`

---

## Format A — Nested Run Record

### Field Reference

| Field | Type | Nullable | Notes |
|---|---|---|---|
| `trace_id` | string (UUID v4) | No | Unique per trace session |
| `session_id` | string (UUID v4) | No | Groups runs within a user session |
| `run_id` | string (UUID v4) | No | Primary key for a single run |
| `agent_name` | string | No | e.g. "ResearchAgent-v2" |
| `task_type` | enum | No | See enumerated values |
| `model` | enum | No | See enumerated values |
| `start_time` | ISO-8601 UTC | No | e.g. "2026-01-15T08:23:11.452Z" |
| `end_time` | ISO-8601 UTC | No | Always >= start_time |
| `status` | enum | No | Overall run outcome |
| `metadata` | object | ~3% missing | Nullable sub-object |
| `metadata.user_tier` | enum | ~3% null | |
| `metadata.region` | string | ~3% null | AWS-style region code |
| `metadata.agent_version` | string | ~3% null | Semver e.g. "2.1.0" |
| `steps` | array | No | At least 1 element |
| `steps[].step_index` | int | No | 0-based, monotonically increasing |
| `steps[].type` | enum | No | llm_call, tool_call, retrieval |
| `steps[].timestamp` | ISO-8601 UTC | No | Within [start_time, end_time] |
| `steps[].latency_ms` | int | No | >= 0; clamped if raw value negative |
| `steps[].llm` | object | Conditional | Present iff type in {llm_call, retrieval} |
| `steps[].llm.prompt_tokens` | int | No | Grows with step_index |
| `steps[].llm.completion_tokens` | int | No | Log-normal |
| `steps[].llm.finish_reason` | enum | No | |
| `steps[].tool` | object | Conditional | Present iff type = tool_call |
| `steps[].tool.name` | enum | No | |
| `steps[].tool.status` | enum | No | |
| `steps[].tool.error_type` | string | Yes | Null when status = ok |
| `steps[].tool.retry_count` | int | No | 0–3 |
| `steps[].tool.args_size_bytes` | int | No | Serialised argument payload size |

### Intentional Imperfections (~3% of records)
- `metadata` block entirely absent
- Individual fields inside `metadata` set to JSON null
- `steps[].llm` or `steps[].tool` block absent even when expected
- Malformed injection lines (truncated JSON, empty line, wrong field types) at configurable --malformed-pct

### Full Example

```json
{
  "trace_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "session_id": "f0e1d2c3-b4a5-6789-0abc-def123456789",
  "run_id":     "11223344-5566-7788-99aa-bbccddeeff00",
  "agent_name": "ResearchAgent-v2",
  "task_type":  "web_research",
  "model":      "gpt-4o",
  "start_time": "2026-01-15T08:23:11.452Z",
  "end_time":   "2026-01-15T08:24:57.891Z",
  "status":     "success",
  "metadata": {
    "user_tier":     "pro",
    "region":        "us-east-1",
    "agent_version": "2.1.0"
  },
  "steps": [
    {
      "step_index": 0,
      "type":       "llm_call",
      "timestamp":  "2026-01-15T08:23:11.500Z",
      "latency_ms": 823,
      "llm": {
        "prompt_tokens":     512,
        "completion_tokens": 147,
        "finish_reason":     "tool_calls"
      }
    },
    {
      "step_index": 1,
      "type":       "tool_call",
      "timestamp":  "2026-01-15T08:23:12.330Z",
      "latency_ms": 4210,
      "tool": {
        "name":            "web_search",
        "status":          "error",
        "error_type":      "network_error",
        "retry_count":     1,
        "args_size_bytes": 248
      }
    },
    {
      "step_index": 2,
      "type":       "tool_call",
      "timestamp":  "2026-01-15T08:23:16.680Z",
      "latency_ms": 3890,
      "tool": {
        "name":            "web_search",
        "status":          "ok",
        "error_type":      null,
        "retry_count":     0,
        "args_size_bytes": 248
      }
    },
    {
      "step_index": 3,
      "type":       "retrieval",
      "timestamp":  "2026-01-15T08:23:20.580Z",
      "latency_ms": 312,
      "llm": {
        "prompt_tokens":     2048,
        "completion_tokens": 64,
        "finish_reason":     "stop"
      }
    },
    {
      "step_index": 4,
      "type":       "llm_call",
      "timestamp":  "2026-01-15T08:23:21.010Z",
      "latency_ms": 1540,
      "llm": {
        "prompt_tokens":     3200,
        "completion_tokens": 412,
        "finish_reason":     "stop"
      }
    }
  ]
}
```

---

## Format B — Flat Span Event

Each step within a run becomes an independent JSON line. Events belonging to the same run are intentionally scattered across multiple files to simulate realistic distributed ingestion.

### Field Reference

| Field | Type | Nullable | Notes |
|---|---|---|---|
| `trace_id` | string (UUID v4) | No | Same as parent run |
| `session_id` | string (UUID v4) | No | Same as parent run |
| `run_id` | string (UUID v4) | No | Join key back to nested record |
| `span_id` | string (UUID v4) | No | Unique per span/step |
| `parent_span_id` | string (UUID v4) | Yes | Null for first step; previous span_id otherwise |
| `event_type` | enum | No | llm_call, tool_call, retrieval |
| `timestamp` | ISO-8601 UTC | No | Step-level timestamp |
| `agent_name` | string | No | Denormalised from run |
| `task_type` | enum | No | Denormalised from run |
| `model` | enum | No | Denormalised from run |
| `run_status` | enum | No | Denormalised from run |
| `step_index` | int | No | 0-based |
| `latency_ms` | int | No | >= 0 |
| `llm_prompt_tokens` | int | Yes | Null when event_type = tool_call |
| `llm_completion_tokens` | int | Yes | Null when event_type = tool_call |
| `llm_finish_reason` | string | Yes | Null when event_type = tool_call |
| `tool_name` | string | Yes | Null when event_type != tool_call |
| `tool_status` | string | Yes | Null when event_type != tool_call |
| `tool_error_type` | string | Yes | Always nullable |
| `tool_retry_count` | int | Yes | Null when event_type != tool_call |
| `tool_args_size_bytes` | int | Yes | Null when event_type != tool_call |
| `user_tier` | string | Yes | From metadata; ~3% null |
| `region` | string | Yes | From metadata; ~3% null |
| `agent_version` | string | Yes | From metadata; ~3% null |

### Full Example — Two spans from the same run

Line 1 (llm_call span):
```json
{"trace_id":"a1b2c3d4-e5f6-7890-abcd-ef1234567890","session_id":"f0e1d2c3-b4a5-6789-0abc-def123456789","run_id":"11223344-5566-7788-99aa-bbccddeeff00","span_id":"aaaabbbb-cccc-dddd-eeee-ffff00001111","parent_span_id":null,"event_type":"llm_call","timestamp":"2026-01-15T08:23:11.500Z","agent_name":"ResearchAgent-v2","task_type":"web_research","model":"gpt-4o","run_status":"success","step_index":0,"latency_ms":823,"llm_prompt_tokens":512,"llm_completion_tokens":147,"llm_finish_reason":"tool_calls","tool_name":null,"tool_status":null,"tool_error_type":null,"tool_retry_count":null,"tool_args_size_bytes":null,"user_tier":"pro","region":"us-east-1","agent_version":"2.1.0"}
```

Line 2 (tool_call span — may appear in a different file):
```json
{"trace_id":"a1b2c3d4-e5f6-7890-abcd-ef1234567890","session_id":"f0e1d2c3-b4a5-6789-0abc-def123456789","run_id":"11223344-5566-7788-99aa-bbccddeeff00","span_id":"bbbbcccc-dddd-eeee-ffff-000011112222","parent_span_id":"aaaabbbb-cccc-dddd-eeee-ffff00001111","event_type":"tool_call","timestamp":"2026-01-15T08:23:12.330Z","agent_name":"ResearchAgent-v2","task_type":"web_research","model":"gpt-4o","run_status":"success","step_index":1,"latency_ms":4210,"llm_prompt_tokens":null,"llm_completion_tokens":null,"llm_finish_reason":null,"tool_name":"web_search","tool_status":"error","tool_error_type":"network_error","tool_retry_count":1,"tool_args_size_bytes":248,"user_tier":"pro","region":"us-east-1","agent_version":"2.1.0"}
```

---

## Sizing Estimates

| Dataset | Runs | Avg steps/run | Approx nested size | Approx flat size |
|---|---|---|---|---|
| small | 100,000 | 8 | ~1.5 GB | ~2.2 GB |
| medium | 500,000 | 8 | ~7.5 GB | ~11 GB |
| large | 1,000,000 | 8 | ~15 GB | ~22 GB |

NOTE: Flat format is larger because every span duplicates run-level fields.

---

## Malformed Line Types (injected at --malformed-pct)

| Type | Example |
|---|---|
| Truncated JSON | {"trace_id":"abc","steps":[{"step_index":0 |
| Empty line | (blank) |
| Wrong field type | {"trace_id":12345,"steps":"not-an-array"} |
| Missing required fields | {"agent_name":"Bot"} |

MapReduce jobs count these via a dedicated Hadoop counter (MALFORMED_RECORDS) and skip them without crashing.
