# AI-Trace BigData: System Architecture & Data Pipeline

This document details the end-to-end distributed data processing architecture for the **AI-Trace BigData** telemetry and observability pipeline. It covers data flow, MapReduce job design, storage formats, Hive schema layers, and analytics export pipelines.

---

## 1. End-to-End Pipeline Architecture

The pipeline processes synthetic multi-turn AI agent execution logs through an automated, resilient big data workflow:

```mermaid
flowchart TD
    subgraph Ingestion ["1. Data Ingestion & Storage"]
        GEN["Python Synthetic Trace Generator<br/><code>generate_traces.py</code>"] -->|Streams JSON Lines| RAW_LOCAL["Local Staging<br/><code>data/raw/*.jsonl</code>"]
        RAW_LOCAL -->|HDFS Put / Parallel Copy| HDFS_RAW["HDFS Raw Ingestion Tier<br/><code>/ai-trace/raw/</code>"]
    end

    subgraph CoreMR ["2. Core Processing (Jobs 1 & 2)"]
        HDFS_RAW -->|Map-Only Streaming Parser| JOB1["Job 1: TraceFlattenJob<br/>(Extracts runs, steps, errors)"]
        HDFS_RAW -->|Secondary Sort Composite Key| JOB2["Job 2: SessionizationJob<br/>(Groups & orders by session_id)"]
    end

    subgraph ProcessedHDFS ["3. Processed HDFS Tier"]
        JOB1 -->|TSV + Malformed Divert| OUT_RUNS["<code>/ai-trace/processed/runs</code>"]
        JOB1 -->|TSV + Malformed Divert| OUT_STEPS["<code>/ai-trace/processed/steps</code>"]
        JOB2 -->|Chronological Ordered Sessions| OUT_SESS["<code>/ai-trace/processed/sessions</code>"]
    end

    subgraph AggMR ["4. Specialized MapReduce Aggregations (Jobs 3 - 5)"]
        OUT_STEPS -->|Combiner-Optimized| JOB3["Job 3: ToolStatsJob<br/>(Key: tool_name + task_type)"]
        OUT_STEPS -->|Combiner-Optimized| JOB4["Job 4: TokenStatsJob<br/>(Key: model + task_type)"]
        OUT_STEPS -->|Log-Binning Quantiles| JOB5["Job 5: LatencyHistogramJob<br/>(Key: model + step_type)"]
    end

    subgraph HiveStorage ["5. Apache Hive Warehouse Layer"]
        OUT_RUNS & OUT_STEPS & OUT_SESS -->|Schema-on-Read| HIVE_EXT["Hive External Text Tables<br/><code>runs_text</code>, <code>steps_text</code>, <code>sessions_text</code>"]
        JOB3 & JOB4 & JOB5 -->|Schema-on-Read| HIVE_STATS["Hive Pre-Aggregated Tables<br/><code>tool_stats</code>, <code>token_stats</code>, <code>latency_histogram</code>"]
        HIVE_EXT -->|CTAS ETL + Snappy Compression| HIVE_ORC["Hive Managed ORC Tables<br/><code>runs_orc</code>, <code>steps_orc</code>, <code>sessions_orc</code><br/>(Partitioned by dt)"]
    end

    subgraph Analytics ["6. HiveQL Analytical Queries"]
        HIVE_ORC & HIVE_STATS -->|Cost-Based Optimizer / Tez| HIVE_QUERIES["HiveQL Queries (Q01 - Q15)<br/>Failure rates, costs, percentiles, bottlenecks"]
        HIVE_QUERIES -->|Beeline Delimited Export| RESULTS_CSV["Analysis Results Directory<br/><code>analysis/results/*.csv</code>"]
        JOB1 & JOB2 & JOB3 & JOB4 & JOB5 -->|Job Counters & Benchmarks| BENCH_CSV["<code>analysis/results/benchmark.csv</code>"]
    end

    subgraph Visualization ["7. Visualization & Reporting"]
        RESULTS_CSV & BENCH_CSV -->|Pandas + Matplotlib| CHARTS["<code>analysis/make_charts.py</code>"]
        CHARTS -->|DPI 200 Figures| FIGURES["<code>docs/figures/*.png</code>"]
        CHARTS -->|Telemetry Summaries| DOCS["<code>docs/results_summary.md</code>"]
    end
```

---

## 2. Pipeline Stage Breakdown

### Stage 1: Data Generation & Ingestion
- **Source**: [scripts/generate_traces.py](file:///home/aksh_dhingra/ai-trace-bigdata/scripts/generate_traces.py).
- **Format**: JSON Lines (`.jsonl`), simulating multi-turn autonomous AI agent trajectories (`run_id`, `session_id`, `task_type`, `model`, `steps[]`, `tools[]`, `tokens`, `errors`).
- **Data Quality Ingestion**: Intentionally injects 1–2% malformed rows (truncated JSON, invalid unicode, unparseable numbers) to exercise error handling counters and routing logic.
- **HDFS Target**: `/ai-trace/raw/*.jsonl`.

### Stage 2: Core MapReduce Processing (Jobs 1 & 2)

#### Job 1: `TraceFlattenJob` (Map-Only)
- **Class**: [TraceFlattenJob.java](file:///home/aksh_dhingra/ai-trace-bigdata/src/main/java/com/aitrace/jobs/TraceFlattenJob.java)
- **Role**: High-throughput distributed parsing and schema normalization.
- **Input**: Raw JSON Lines from `/ai-trace/raw/`.
- **Outputs**:
  - `runs`: Run-level summary metadata (`run_id`, `session_id`, `model`, `task_type`, `status`, `start_time`, `end_time`, `duration_ms`, `total_prompt_tokens`, `total_completion_tokens`, `total_tokens`, `step_count`, `error_type`).
  - `steps`: Granular turn-by-turn trace records (`run_id`, `step_index`, `step_type`, `model`, `tool_name`, `status`, `error_type`, `latency_ms`, `prompt_tokens`, `completion_tokens`, `start_time_ms`).
- **Resilience**: Invalid lines are caught without failing the task, incrementing Hadoop custom counter `MalformedRecords.INVALID_JSON` and writing directly to `/ai-trace/output/malformed/`.

#### Job 2: `SessionizationJob` (Secondary Sort MapReduce)
- **Class**: [SessionizationJob.java](file:///home/aksh_dhingra/ai-trace-bigdata/src/main/java/com/aitrace/jobs/SessionizationJob.java)
- **Role**: Reconstructs multi-run user sessions, calculates idle periods between consecutive runs, and marks session-level error propagation.
- **Secondary Sort Architecture**:
  - **Composite Key**: `SessionKeyWritable(Text sessionId, LongWritable startTimeMs)`.
  - **Custom Partitioner**: `SessionIdPartitioner` routes purely by `sessionId.hashCode()`, guaranteeing all runs belonging to a session land in the exact same reducer.
  - **Grouping Comparator**: `SessionIdGroupingComparator` compares only `sessionId`, ensuring that the reducer receives all runs of a session in a single `reduce()` invocation.
  - **Key Comparator**: Sorts by `sessionId ASC`, then `startTimeMs ASC`.
  - **Memory Efficiency**: Streaming evaluation in Reducer. Does **not** buffer runs in memory (preventing Reducer Java Heap OOM errors on large sessions).

---

### Stage 3: Specialized Aggregations (Jobs 3–5)

#### Job 3: `ToolStatsJob`
- **Class**: [ToolStatsJob.java](file:///home/aksh_dhingra/ai-trace-bigdata/src/main/java/com/aitrace/jobs/ToolStatsJob.java)
- **Map Output**: `(tool_name \t task_type) -> (total_calls, ok_calls, error_calls, timeout_calls, sum_latency_ms)`.
- **Combiner**: Computes associative and commutative partial sums on local mapper spill files, reducing shuffle bytes by **1,524x**.
- **Reducer**: Computes failure rate percentages and average latencies.

#### Job 4: `TokenStatsJob`
- **Class**: [TokenStatsJob.java](file:///home/aksh_dhingra/ai-trace-bigdata/src/main/java/com/aitrace/jobs/TokenStatsJob.java)
- **Map Output**: `(model \t task_type) -> (prompt_tokens, completion_tokens, total_tokens, min_prompt, max_prompt)`.
- **Combiner**: Computes local sum and min/max aggregation, achieving a **198x** shuffle reduction.
- **Reducer**: Aggregates total token consumption, averages, and maximum context windows.

#### Job 5: `LatencyHistogramJob`
- **Class**: [LatencyHistogramJob.java](file:///home/aksh_dhingra/ai-trace-bigdata/src/main/java/com/aitrace/jobs/LatencyHistogramJob.java)
- **Map Output**: `(model \t step_type) -> latency_ms`.
- **Reducer**: Organizes latencies into logarithmic buckets (`<100ms`, `100-250ms`, `250-500ms`, `500ms-1s`, `1s-2.5s`, `2.5s-5s`, `5s-10s`, `>10s`) and estimates non-parametric p50, p90, p95, and p99 percentiles.

---

### Stage 4: Apache Hive Storage & Optimization Layer

The pipeline utilizes a dual-tier storage strategy in Apache Hive 3.1.3:

| Tier | Format | Compression | Partitioning | Use Case |
|---|---|---|---|---|
| **External Tier** | Delimited TSV (`TEXTFILE`) | None / Host-level | None | Direct ingestion over MapReduce outputs (`runs_text`, `steps_text`, `sessions_text`) |
| **Managed Tier** | Optimized Row Columnar (`ORC`) | **Snappy** | `PARTITIONED BY (dt STRING)` | High-performance analytical queries (Q01–Q15) |

#### ORC + Snappy Optimization Benefits:
1. **Columnar Projection**: Queries touching only tokens or tool names skip reading unrelated step attributes, reducing I/O by >75%.
2. **Storage Footprint**: Compresses raw TSV by **2.04x** (194.2 KB text down to 95.4 KB ORC).
3. **Stripe Indexing & Pushdown**: Built-in stripe indexes (min/max values per 10,000 rows) enable predicate pushdown to eliminate entire data blocks during scans.

---

### Stage 5: Analytics & Visualization Pipeline

1. **HiveQL Analytical Suite**:
   - 15 production queries ([hive/queries/](file:///home/aksh_dhingra/ai-trace-bigdata/hive/queries/)) covering tool failure rates, token bottlenecks, retry loops, context growth, and financial cost modeling.
   - Results exported to CSV format in [analysis/results/](file:///home/aksh_dhingra/ai-trace-bigdata/analysis/results/).
2. **Automated Visualization**:
   - [analysis/make_charts.py](file:///home/aksh_dhingra/ai-trace-bigdata/analysis/make_charts.py) reads analytical and benchmark CSVs using purely standard `pandas` and `matplotlib` (zero ML libraries).
   - Generates 8 high-resolution figures (200 DPI) saved to [docs/figures/](file:///home/aksh_dhingra/ai-trace-bigdata/docs/figures/).
3. **Execution Reporting**:
   - Empirical findings compiled into [docs/results_summary.md](file:///home/aksh_dhingra/ai-trace-bigdata/docs/results_summary.md).
