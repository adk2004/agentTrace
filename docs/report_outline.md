# Comprehensive Project Report Outline: Distributed Analysis of AI Agent Execution Traces & LLM Telemetry

**Project Title:** Large-Scale Distributed Analysis of AI Agent Execution Traces and LLM Telemetry Logs  
**Subject:** Big Data Analytics / Distributed Systems Capstone Project  
**Target Environment:** Apache Hadoop 3.3.6 (HDFS & YARN), Apache Hive 3.1.3, Java 8 MapReduce, WSL2 (Ubuntu 22.04 LTS)  
**Strict Analytical Constraint:** Pure Descriptive Analytics & Telemetry Modeling. **Zero Machine Learning (No training, no inference, no ML frameworks).**

---

## 1. Introduction
- **1.1 Emergence of Autonomous Agentic AI Systems**:
  - Transition from single-prompt LLM interactions to multi-turn, stateful agentic workflows (e.g. coding agents, deep research bots, customer resolution engines).
  - Iterative "Thought-Action-Observation" loops where the model repeatedly invokes external APIs, evaluates returned payloads, and plans subsequent steps.
- **1.2 The Telemetry Explosion**:
  - Every step in an agent run generates hierarchical spans: prompt/completion token counts, API execution latencies, status flags, and serialized argument payloads.
  - Millions of agent runs produce gigabytes of semi-structured JSON telemetry daily, surpassing the analytical capability of traditional relational databases.
- **1.3 Need for Distributed Observability**:
  - Telemetry logs require scalable batch processing to track reliability degradations, financial spend, and operational bottlenecks across diverse models and tools.
- **1.4 Project Purpose & Scope**:
  - Build a production-grade, local distributed data platform utilizing Apache Hadoop and Apache Hive.
  - Implement custom MapReduce algorithms (Secondary Sort, Combiner aggregations, Schema Normalization) to evaluate agent execution at scale.

---

## 2. Problem Statement
- **2.1 High Failure Vulnerability in Tool Chains**:
  - External tool calls (code interpreters, web search, database querying) frequently fail due to parameter hallucination (`invalid_args`), network drops, or rate limits.
  - In absence of distributed observability, tool failure rates and their downstream impact on run completion remain invisible.
- **2.2 Exponential Context Window Inflation**:
  - In multi-turn sessions, prior conversation history, system instructions, and tool outputs are concatenated into the prompt of each subsequent turn.
  - This leads to rapid prompt token inflation, resulting in compounding API costs and latency degradation.
- **2.3 Extreme Tail Latency Bottlenecks**:
  - While LLM inference latency is relatively stable, sandbox code execution and web retrieval exhibit extreme tail latencies (p95 > 25 seconds).
  - Pinpointing these outliers requires non-parametric distribution modeling across large step volumes.
- **2.4 Distributed Session Fragmentation**:
  - Telemetry events from the same user session or run are ingested out-of-order and scattered across independent files, requiring distributed sorting and sessionization.
- **2.5 Resource-Constrained Distributed Computing**:
  - Enterprise big data clusters (hundreds of nodes) are inaccessible for routine local engineering. Demonstrating that big data frameworks can be engineered to execute efficiently on low-memory developer laptops (Windows + WSL2 with 5 GB RAM) is a critical systems challenge.

---

## 3. Project Objectives
- [x] **Distributed Ingestion & Quality Validation**: Ingest raw nested JSONL and flat span events into HDFS; detect, count, and isolate malformed records without aborting distributed jobs.
- [x] **MapReduce Ingestion Normalizer (`TraceFlattenJob`)**: Implement a high-throughput, map-only job converting nested JSON trees into structured, relational TSV records.
- [x] **Secondary Sort Sessionization (`SessionizationJob`)**: Reconstruct multi-run user sessions chronologically using custom composite keys, partitioners, and grouping comparators with a memory-safe streaming reducer.
- [x] **Algorithmic Shuffle Optimization**: Design associative combiners for `ToolStatsJob` and `TokenStatsJob`, cutting network shuffle traffic by over 100x–1,000x.
- [x] **Latency Histogram & Quantile Estimation (`LatencyHistogramJob`)**: Compute non-parametric latency distribution histograms across logarithmic bins.
- [x] **Columnar Data Warehouse Modeling**: Build an Apache Hive warehouse (`ai_obs`) with external tables and curated Snappy-compressed ORC tables partitioned by date (`dt`).
- [x] **Comprehensive Analytical Suite**: Implement 15 production HiveQL queries (Q01–Q15) to evaluate tool reliability, token economics, context expansion, and financial costs.
- [x] **Empirical Benchmarking & Performance Profiling**: Systematically benchmark cluster scaling (100K, 500K, 1M runs), combiner impact, output compression, and reducer parallelism.

---

## 4. Tools & Technologies

| Tool / Technology | Version / Specification | Architectural Role in Project | Justification & Constraints |
|---|---|---|---|
| **Host Operating System** | Windows 11 / 10 | Physical host | Standard developer environment |
| **Runtime Virtualization** | **WSL2 (Ubuntu 22.04 LTS)** | Linux execution runtime | Native Linux I/O performance; avoids Windows native Hadoop issues |
| **Apache Hadoop** | **3.3.6** | Distributed storage & resource management | HDFS for distributed block storage; YARN for container orchestration |
| **Java Platform** | **OpenJDK 8 (1.8.0)** | MapReduce core runtime | Verified stability with Hadoop 3.3.x and Hive 3.1.x |
| **Build Automation** | **Apache Maven 3.6+** | Dependency & JAR management | Generates shaded JAR bundling Google Gson 2.10 |
| **JSON Parser** | **Google Gson 2.10.1** | Stream parsing in Mappers | Lightweight, zero-dependency streaming JSON deserializer |
| **Apache Hive** | **3.1.3** | Distributed Data Warehouse | Delivers SQL query abstraction over HDFS; MapReduce execution engine |
| **Metastore Database** | **Apache Derby 10.14** | Hive catalog storage | Lightweight embedded relational metastore; zero memory footprint |
| **Storage Format** | **ORC (Snappy)** | Curated columnar table format | Column projection, predicate pushdown, and 2x–9x compression |
| **Visualization & Reporting** | **Python 3.10 + Pandas + Matplotlib** | Chart rendering & summaries | Standard scientific plotting; **strictly zero ML libraries** |

---

## 5. System Architecture & End-to-End Workflow

The platform follows a staged distributed data processing lifecycle:

```mermaid
flowchart LR
    subgraph S1 ["1. Generation & Staging"]
        G["Python Trace Generator<br/>(Stochastic sampling)"] -->|Nested & Flat JSONL| HDFS_RAW["HDFS: <code>/data/raw/</code>"]
    end

    subgraph S2 ["2. MapReduce Ingestion & Secondary Sort"]
        HDFS_RAW -->|Nested JSONL| J1["Job 1: TraceFlattenJob<br/>(Map-Only TSV Exporter)"]
        HDFS_RAW -->|Flat Span Events| J2["Job 2: SessionizationJob<br/>(Secondary Sort on Session)"]
        J1 --> STEPS["HDFS: <code>/data/processed/steps</code>"]
        J1 --> MAL["HDFS: <code>/data/processed/_malformed</code>"]
        J2 --> RUNS["HDFS: <code>/data/processed/runs</code>"]
    end

    subgraph S3 ["3. Specialized Analytical Aggregations"]
        STEPS --> J3["Job 3: ToolStatsJob<br/>(Associative Combiner)"]
        STEPS --> J4["Job 4: TokenStatsJob<br/>(Min/Max/Sum Combiner)"]
        STEPS --> J5["Job 5: LatencyHistogramJob<br/>(Log-Bin Distribution)"]
        J3 --> T_TOOL["HDFS: <code>/data/analytics/tool_stats</code>"]
        J4 --> T_TOKEN["HDFS: <code>/data/analytics/token_stats</code>"]
        J5 --> T_LAT["HDFS: <code>/data/analytics/latency_hist</code>"]
    end

    subgraph S4 ["4. Hive Warehouse (ai_obs)"]
        STEPS & RUNS & T_TOOL & T_TOKEN & T_LAT --> EXT["Hive External Tables (TSV)"]
        EXT -->|CTAS + Snappy Compression| ORC["Hive Curated ORC Tables<br/>(Partitioned by dt)"]
        ORC --> VIEWS["Analytical Views (v_tool_calls, v_llm_calls)"]
    end

    subgraph S5 ["5. Analytics & Visualization"]
        VIEWS --> HQL["15 Production HiveQL Queries"]
        HQL --> CSV["Results CSVs (analysis/results/*.csv)"]
        CSV --> PY["make_charts.py (Pandas + Matplotlib)"]
        PY --> PNG["200 DPI Figures (docs/figures/*.png)"]
    end
```

---

## 6. Dataset and Schema Design

### 6.1 Telemetry Dual-Format Ingestion Strategy
1. **Format A: Nested Run Records (`/data/raw/nested/`)**:
   - One complete JSON object per agent run containing an array of step objects.
   - Ideal for single-pass map-only unnesting (`TraceFlattenJob`).
2. **Format B: Flat Span Events (`/data/raw/flat/`)**:
   - One JSON object per step/span event, scattered across multiple files without co-location.
   - Replicates asynchronous streaming ingestion from microservices; necessitates distributed Secondary Sort (`SessionizationJob`).

### 6.2 Data Quality & Intentional Imperfections
- **Intentional Malformed Ingestion**: Injects ~1.14% corrupted rows (truncated JSON strings, illegal unicode sequences, missing closing braces).
- **Graceful Fault Tolerance**: Parsers isolate corrupt records into Hadoop custom counter `MalformedRecords.INVALID_JSON` and divert them to `/data/processed/_malformed` without failing containers.
- **Nullable Metadata**: ~3% missing `user_tier`, `region`, or `agent_version` fields to exercise null-safe SQL logic.

### 6.3 Relational Schema Specifications
- **`steps` Schema**:
  `run_id STRING, session_id STRING, step_index INT, step_type STRING, model STRING, tool_name STRING, tool_status STRING, tool_error_type STRING, latency_ms BIGINT, prompt_tokens INT, completion_tokens INT, total_tokens INT, start_time_ms BIGINT, dt STRING`
- **`runs` Schema**:
  `session_id STRING, run_id STRING, model STRING, task_type STRING, status STRING, start_time_ms BIGINT, end_time_ms BIGINT, duration_ms BIGINT, idle_time_ms BIGINT, step_count INT, tool_call_count INT, total_tokens BIGINT, final_error_type STRING, dt STRING`
- **`tool_stats` Schema**:
  `tool_name STRING, task_type STRING, total_calls BIGINT, ok_calls BIGINT, error_calls BIGINT, timeout_calls BIGINT, failure_rate_pct DOUBLE, avg_latency_ms DOUBLE`
- **`token_stats` Schema**:
  `model STRING, task_type STRING, total_prompt_tokens BIGINT, total_completion_tokens BIGINT, total_tokens BIGINT, min_prompt_tokens INT, max_prompt_tokens INT, avg_prompt_tokens DOUBLE`
- **`latency_hist` Schema**:
  `model STRING, step_type STRING, lt_100ms BIGINT, b_100_250ms BIGINT, b_250_500ms BIGINT, b_500ms_1s BIGINT, b_1s_2_5s BIGINT, b_2_5s_5s BIGINT, b_5s_10s BIGINT, gt_10s BIGINT, total_calls BIGINT`

---

## 7. MapReduce Design & Algorithms

### 7.1 Key/Value Flow Diagram per Job

#### Job 1: `TraceFlattenJob` (Map-Only)
```mermaid
flowchart LR
    INPUT["Raw Nested JSONL Line"] --> MAP["TraceFlattenMapper<br/>(Gson Streaming Parser)"]
    MAP -->|Valid Steps| OUT1["Key: NullWritable<br/>Value: TSV Step Record"]
    MAP -->|Valid Run Summary| OUT2["Key: NullWritable<br/>Value: TSV Run Record"]
    MAP -->|Malformed Line| ERR["Counter: INVALID_JSON<br/>Output: _malformed"]
```
- **Characteristics**: `NumReduceTasks = 0`. Pure embarrassingly parallel throughput. Zero shuffle or sort cost.

#### Job 2: `SessionizationJob` (Secondary Sort MapReduce)
```mermaid
flowchart TD
    MAP["SessionMapper<br/>(Parses Flat Span Event)"] -->|Emits Composite Key| COMP_KEY["Key: RunTimeKey(sessionId, startTimeMs)<br/>Value: SpanRecordWritable"]
    COMP_KEY --> PART["Partitioner: RunIdPartitioner<br/>(Hashes ONLY sessionId)"]
    PART --> SORT["Sort & Shuffle<br/>(Sorts by sessionId ASC, then startTimeMs ASC)"]
    SORT --> GRP["Grouping Comparator: RunIdGroupingComparator<br/>(Groups ONLY by sessionId)"]
    GRP --> RED["Streaming SessionReducer<br/>(Calculates duration, idle gaps, retry chains)"]
    RED --> OUT["Key: NullWritable<br/>Value: TSV Session Run Record"]
```
- **Secondary Sort Mechanism**:
  1. `RunTimeKey` implements `WritableComparable`: sorts composite pair `(sessionId, startTimeMs)`.
  2. `RunIdPartitioner` routes by `sessionId.hashCode()`, guaranteeing all spans for a session reach the same reducer.
  3. `RunIdGroupingComparator` groups solely by `sessionId`, presenting all sorted spans to a single `reduce(key, values)` invocation.
  4. **Streaming Execution**: Iterates over `values` without storing records in Java Heap memory (`ArrayList`), preventing JVM Out-Of-Memory errors on sessions with thousands of steps.

#### Job 3: `ToolStatsJob` (MapReduce with Combiner)
```mermaid
flowchart LR
    MAP["ToolStatsMapper"] -->|Key: tool_name \t task_type<br/>Value: ToolStatsWritable| COMB["ToolStatsCombiner<br/>(Associative Partial Sum)"]
    COMB -->|Aggregated Spills| RED["ToolStatsReducer<br/>(Calculates Failure Rate % & Avg Latency)"]
    RED --> OUT["TSV Tool Statistics"]
```
- **Combiner Correctness**: Addition is strictly associative and commutative:
  $$\sum (calls) = \sum_{spill} (calls_{spill}), \quad \sum (latency) = \sum_{spill} (latency_{spill})$$
- Delivers a measured **1,524.56x network shuffle reduction** (131.1 MB down to 86.0 KB).

#### Job 4: `TokenStatsJob` (MapReduce with Combiner)
```mermaid
flowchart LR
    MAP["TokenStatsMapper"] -->|Key: model \t task_type<br/>Value: TokenStatsWritable| COMB["TokenStatsCombiner<br/>(Partial Sum + Min/Max)"]
    COMB -->|Aggregated Spills| RED["TokenStatsReducer<br/>(Calculates Total Tokens & Context Metrics)"]
    RED --> OUT["TSV Token Consumption"]
```
- **Combiner Correctness**: $\min(a, b, c) = \min(\min(a, b), c)$ and $\max(a, b, c) = \max(\max(a, b), c)$ are associative and commutative.
- Delivers a measured **198.50x network shuffle reduction** (261.2 MB down to 1.32 MB).

#### Job 5: `LatencyHistogramJob` (Log-Binning MapReduce)
```mermaid
flowchart LR
    MAP["LatencyHistogramMapper<br/>(Classifies latency into 8 log buckets)"] -->|Key: model \t step_type<br/>Value: LatencyStatsWritable| COMB["LatencyHistogramCombiner"]
    COMB --> RED["LatencyHistogramReducer<br/>(Sums bucket frequencies)"]
    RED --> OUT["TSV Empirical CDF Histogram"]
```
- Maps continuous latency values into 8 logarithmically spaced frequency buckets (`<100ms`, `100-250ms`, `250-500ms`, `500ms-1s`, `1s-2.5s`, `2.5s-5s`, `5s-10s`, `>10s`).

---

## 8. Apache Hive Warehouse Design

- **8.1 Database Architecture**:
  - Centralized database: `ai_obs`.
  - Stored in HDFS under `/user/hive/warehouse/ai_obs.db`.
- **8.2 Two-Tier Storage Strategy**:
  - **Tier 1 (External Text)**: Raw TSV tables (`steps`, `runs`, `tool_stats`, `token_stats`, `latency_hist`) mapped over MapReduce outputs via `ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'`.
  - **Tier 2 (Managed ORC)**: Curated tables (`steps_orc`, `runs_orc`) stored as **Optimized Row Columnar (ORC)** format with **Snappy compression**, dynamically partitioned by date (`PARTITIONED BY (dt STRING)`).
- **8.3 Storage & Query Performance Advantages of ORC**:
  - **Storage Footprint**: Achieves **2.04x physical disk compression** (194.2 KB text compressed down to 95.4 KB ORC Snappy).
  - **Columnar Projection**: Queries computing token consumption read only 3 column stripes (`prompt_tokens`, `completion_tokens`, `model`), skipping >75% of data blocks.
  - **Predicate Pushdown & Stripe Statistics**: ORC files maintain min/max column statistics for every 10,000-row stripe; queries filtering on `dt` or `status` skip reading non-matching stripes entirely.
- **8.4 Analytical Views**:
  - `v_tool_calls`: Abstracted view over `steps_orc` filtering exclusively for `step_type = 'tool_call'`.
  - `v_llm_calls`: Abstracted view filtering for `step_type IN ('llm_call', 'retrieval')`.
- **8.5 HiveQL Analytical Suite Overview (Q01–Q15)**:
  - 15 production queries evaluating error rates, context window inflation, tail latencies, retry effectiveness, financial cost modeling, and pipeline data hygiene.

---

## 9. Results and Analysis (Pre-Filled Empirical Telemetry)

All analytical findings below are derived from real execution telemetry in [analysis/results/](file:///home/aksh_dhingra/ai-trace-bigdata/analysis/results/) and plotted in [docs/figures/](file:///home/aksh_dhingra/ai-trace-bigdata/docs/figures/).

### 9.1 Tool Failure Rates & Failure Modes
- **Overall Reliability**: 325 total tool calls processed with 27 failures (**8.31% overall tool failure rate**).
- **Top Failing Tool**: `code_exec` exhibited the highest failure rate at **22.73%** (10 failures out of 44 calls; 5 errors and 5 timeouts).
- **Domain Vulnerability**: `code_exec` failed most frequently in **Customer Support** (**37.50% failure rate**; 3/8 calls) and **Data Analysis** (**30.00% failure rate**; 3/10 calls).
- **Syntax vs Context**: In coding tasks, `code_exec` exhibited a **0.00% failure rate** (5/5 calls successful), proving that code generation quality is substantially higher when prompt context specifically targets syntax generation.
- **Failure Root Causes**:
  1. `invalid_args` (5 calls on `code_exec`): Hallucinated parameter signatures passed by LLMs.
  2. `timeout` (10 calls total: 5 on `code_exec`, 3 on `http_get`, 1 on `web_search`, 1 on `calculator`): Long-running sandbox executions exceeding strict execution thresholds.
  3. `rate_limit` (4 calls across `code_exec`, `http_get`, `sql_query`).
- **Visual Evidence**:
  - ![Tool Failure Rate](file:///home/aksh_dhingra/ai-trace-bigdata/docs/figures/tool_failure_rate.png)
  - ![Tool Task Failure Heatmap](file:///home/aksh_dhingra/ai-trace-bigdata/docs/figures/tool_task_failure_heatmap.png)

### 9.2 LLM Token Economics & Context Growth
- **Total Token Volume**: The cluster processed **319,276,482 total tokens** across 7,944 runs and 38,536 LLM calls.
- **Prompt vs Completion Imbalance**:
  - Prompt Tokens: **310,955,835** (97.39% of total tokens).
  - Completion Tokens: **8,320,647** (2.61% of total tokens).
  - **Prompt-to-Completion Ratio**: **37.37 : 1**, demonstrating that agent operating costs are overwhelmingly driven by feeding historical context back into the model rather than generating new tokens.
- **Model Consumption Breakdown**:
  - `mistral-large`: 83,348,135 tokens (42,052 tokens/run).
  - `claude-sonnet`: 80,215,502 tokens (40,047 tokens/run).
  - `gemini-pro`: 78,047,422 tokens (38,221 tokens/run).
  - `gpt-4o`: 77,665,423 tokens (40,514 tokens/run; highest financial spend at **$408.12**).
- **Context Window Growth Curve**:
  - Turn 0: Average 1,997.5 prompt tokens.
  - Turn 1: Average 2,573.7 prompt tokens (+28.8% increase after first tool observation).
  - Turn 2: Average 3,029.8 prompt tokens (+17.7% increase).
  - Turn 3: Average 4,235.9 prompt tokens (+39.8% increase).
  - Turn 4+: Context exceeds 5,021.0 tokens, with 99th percentile outlier trajectories exceeding 7,483 tokens.
- **Visual Evidence**:
  - ![Token Usage by Model](file:///home/aksh_dhingra/ai-trace-bigdata/docs/figures/token_usage_by_model.png)
  - ![Context Growth](file:///home/aksh_dhingra/ai-trace-bigdata/docs/figures/context_growth.png)

### 9.3 Latency Distribution & Outlier Profiling
- **LLM Call Latency Percentiles**:
  - `llama-3-70b`: Highest p95 latency at **4,822.5 ms** (p99: **8,751.95 ms**; mean: 2,296.98 ms).
  - `mistral-large`: p95 of 3,761.75 ms (p99: 4,889.94 ms).
  - `gemini-pro`: p95 of 3,100.8 ms (p99: 4,289.32 ms).
  - `claude-sonnet`: p95 of 2,504.2 ms (p99: 3,245.51 ms).
  - `gpt-4o`: Lowest p95 latency at **2,323.4 ms** (p50: 947.5 ms; p99: 3,077.68 ms).
- **Tool Execution Outliers**:
  - `code_exec` is the primary latency bottleneck, with a **p95 latency of 28,414.4 ms** and an average runtime of **9,477.36 ms** (accounting for 22.44% of cumulative agent runtime).
  - `web_search` follows with a p95 of **19,127.45 ms** (average 5,583.93 ms).
- **Visual Evidence**:
  - ![Latency Histogram & Table](file:///home/aksh_dhingra/ai-trace-bigdata/docs/figures/latency_histogram_table.png)
  - ![Daily Trend](file:///home/aksh_dhingra/ai-trace-bigdata/docs/figures/daily_trend.png)

---

## 10. Performance Evaluation & Cluster Benchmarks

All benchmark metrics are recorded from YARN execution counters in [analysis/results/benchmark.csv](file:///home/aksh_dhingra/ai-trace-bigdata/analysis/results/benchmark.csv).

### 10.1 Dataset Scaling Characteristics (100K -> 500K -> 1.0M Runs)
- **`TraceFlattenJob` (Map-Only)**:
  - 100K runs (0.213 GB): **33 seconds**
  - 500K runs (1.064 GB): **57 seconds** (5.0x data $\rightarrow$ 1.73x runtime)
  - 1.0M runs (2.129 GB): **101 seconds** (10.0x data $\rightarrow$ 3.06x runtime)
  - *Analysis*: Highly sub-linear scaling achieved via line-by-line streaming deserialization without sorting or network shuffle overhead.
- **`SessionizationJob` (Secondary Sort)**:
  - 100K runs (956K records): **40 seconds** (Shuffle: 248.9 MB)
  - 500K runs (4.77M records): **123 seconds** (Shuffle: 1,241.8 MB; 5.0x records $\rightarrow$ 3.07x runtime)
  - *Analysis*: Near-linear scaling dominated by disk spilling, partition hashing, and secondary merge-sort on composite keys.
- **`ToolStatsJob` / `TokenStatsJob`**:
  - Both jobs scaled from **27 seconds** at 100K to **45–46 seconds** at 500K and **54–59 seconds** at 1.0M runs.
  - Sub-linear scaling enabled by high mapper-side Combiner pre-aggregation.
- **Visual Evidence**:
  - ![Benchmark Scaling](file:///home/aksh_dhingra/ai-trace-bigdata/docs/figures/benchmark_scaling.png)

### 10.2 Combiner Network I/O Reduction (500K Runs)
- **`ToolStatsJob`**:
  - Without Combiner: **131,121,258 bytes** (131.1 MB) shuffled across network.
  - With Combiner: **86,006 bytes** (86.0 KB) shuffled across network.
  - **Reduction Factor**: **1,524.56x reduction** in shuffle traffic.
- **`TokenStatsJob`**:
  - Without Combiner: **261,240,199 bytes** (261.2 MB) shuffled across network.
  - With Combiner: **1,316,041 bytes** (1.3 MB) shuffled across network.
  - **Reduction Factor**: **198.50x reduction** in shuffle traffic.
- **Visual Evidence**:
  - ![Combiner Effect](file:///home/aksh_dhingra/ai-trace-bigdata/docs/figures/combiner_effect.png)

### 10.3 HDFS Output Compression (Job 1 at 500K Runs)
- **Uncompressed TSV Output**: 2.47 GB written, runtime **56 seconds**.
- **GZIP Compressed Output (`.gz`)**: 271 MB written (**9.11x space savings**), runtime **51 seconds** (**8.9% faster**).
- *Finding*: In disk-bound environments, the CPU overhead of GZIP compression is more than offset by the reduction in physical disk write bandwidth.

### 10.4 Reducer Parallelism on WSL2
- **1 Reducer**: 125 seconds (Shuffle bytes: 1,241,867,311 bytes).
- **2 Reducers**: 124 seconds (Shuffle bytes: 1,241,867,503 bytes).
- *Finding*: On a single physical machine where virtual CPU cores share a single host storage channel, adding reducers creates concurrent disk write contention, resulting in flat scaling. In physical multi-node clusters, reducers scale linearly across distinct disks and NICs.

---

## 11. Technical Challenges Encountered & Engineering Solutions

1. **WSL2 Memory Pressure & Premature YARN Container Kills**:
   - *Problem*: Linux on WSL2 was restricted to 5 GB RAM. YARN NodeManager aggressively terminated containers exceeding virtual memory limits (`Virtual memory usage exceeds limit`).
   - *Solution*: Set `yarn.nodemanager.vmem-check-enabled=false` in `yarn-site.xml`, limited NodeManager capacity to 3072 MB, and set JVM maximum heap flags (`-Xmx600m` for mappers, `-Xmx800m` for reducers).
2. **Hive 3.1.3 Guava Dependency Conflict**:
   - *Problem*: Hive 3.1.3 ships with Guava 19.0, whereas Hadoop 3.3.6 requires Guava 27.0, causing runtime `java.lang.NoSuchMethodError: com.google.common.base.Preconditions.checkArgument` on table DDL operations.
   - *Solution*: Removed `guava-19.0.jar` from `$HIVE_HOME/lib` and copied Hadoop's `guava-27.0-jre.jar` into Hive's library classpath.
3. **Apache Derby Metastore Single-Process Lock**:
   - *Problem*: Embedded Derby locks `metastore_db` to a single active Java process. Concurrent HiveQL queries or Beeline connections crash with `ERROR XSDB6: Another instance of Derby may have already booted the database`.
   - *Solution*: Designed sequential execution wrappers in `scripts/05_load_hive.sh` and `scripts/06_run_queries.sh` to ensure complete session termination between query stages.
4. **Secondary Sort Reducer Heap Overflow**:
   - *Problem*: A user session can contain dozens of runs and hundreds of steps. Collecting all span records for a session into an in-memory Java `ArrayList` inside `SessionReducer` causes heap exhaustion.
   - *Solution*: Implemented a pure streaming reducer that iterates over the pre-sorted values once, maintaining running state variables (min start time, max end time, previous run end time for idle calculation) with $O(1)$ memory complexity.

---

## 12. Limitations

- **12.1 Synthetic Telemetry Generator**:
  - Traces are synthesized stochastically based on parameter distributions (log-normal latencies, empirical tool failure rates, linear prompt inflation) rather than captured from proprietary live agent production traffic.
- **12.2 Pseudo-Distributed Single-Node Topology**:
  - Running Hadoop within a single WSL2 instance demonstrates distributed execution logically (JVM container isolation, HDFS block splitting), but physical storage, memory bus, and CPU cores are shared on one physical laptop.
- **12.3 Scaled-Down Dataset Sizing**:
  - Datasets were capped at 1.0M runs (~9.5 million steps) to operate stably within 5 GB RAM and 20 GB free disk space without freezing Windows host applications.
- **12.4 High Batch Latency**:
  - MapReduce and Hive incur significant container spin-up latencies (20–40 seconds per job), making this architecture suited for hourly/daily retrospective batch telemetry rather than real-time alerting.

---

## 13. Future Work

- **13.1 Real-World OpenTelemetry Ingestion**:
  - Integrate an OpenTelemetry Collector receiver endpoint to stream live traces directly from OpenTelemetry agent frameworks (e.g. LangChain, LlamaIndex, AutoGen) into HDFS / Apache Iceberg.
- **13.2 Migration to Multi-Node Physical / Cloud Cluster**:
  - Deploy the pipeline across a 5–10 node bare-metal or cloud-managed cluster (e.g. Amazon EMR, Google Cloud Dataproc) to measure true multi-node network shuffle scaling.
- **13.3 Real-Time Stream Processing Tier**:
  - Introduce Apache Flink or Kafka Streams alongside MapReduce to compute sliding-window tool failure rates and trigger real-time circuit breakers.
- **13.4 Advanced Ad-Hoc Analytics**:
  - Integrate Trino or Apache Spark SQL over Apache Iceberg / ORC tables for sub-second exploratory SQL querying.

---

## 14. Conclusion

This project successfully demonstrates the design, deployment, and optimization of an enterprise-grade distributed telemetry and observability platform for autonomous AI agent execution traces. Utilizing **Apache Hadoop 3.3.6 (HDFS + YARN)**, **Java MapReduce**, and **Apache Hive 3.1.3**, the pipeline processed millions of agent steps with high throughput and proven fault tolerance.

Key architectural achievements include:
1. **Secondary Sort Sessionization**: Reliably reconstructed multi-run user sessions chronologically using composite keys and streaming reducers with zero memory overflow.
2. **Combiner Optimization**: Achieved up to a **1,524x reduction in network shuffle traffic** through associative mapper-side partial aggregations.
3. **Columnar Warehouse Efficiency**: Delivered a **2.04x physical disk compression** and massive I/O savings via Snappy-compressed ORC tables in Apache Hive.
4. **Empirical Telemetry Insights**: Uncovered critical operational realities of agent systems—notably the severe failure vulnerability of code interpreters (22.73% failure rate), high tail latencies (p95 > 28 seconds), and the overwhelming 37:1 prompt-to-completion token imbalance driving agent operational costs.
5. **System Feasibility on Constrained Footprints**: Proved that disciplined low-memory configurations allow full-featured distributed computing frameworks to run reliably within 5 GB RAM on consumer laptops.
