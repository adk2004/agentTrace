# AI-Trace BigData: Distributed Telemetry & Observability Pipeline for Multi-Turn AI Agents

[![Hadoop](https://img.shields.io/badge/Apache%20Hadoop-3.3.6-red.svg)](https://hadoop.apache.org/)
[![Hive](https://img.shields.io/badge/Apache%20Hive-3.1.3-yellow.svg)](https://hive.apache.org/)
[![Java](https://img.shields.io/badge/Java-OpenJDK%208-blue.svg)](https://openjdk.org/)
[![Maven](https://img.shields.io/badge/Build-Maven%203.6+-orange.svg)](https://maven.apache.org/)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)
[![Constraint](https://img.shields.io/badge/ML%20Constraint-Zero%20ML%20(Pure%20Descriptive)-purple.svg)](#constraints--design-philosophy)

> **Large-Scale Distributed Analysis of AI Agent Execution Traces and LLM Telemetry Logs**  
> A production-grade, distributed big data telemetry pipeline built with **Apache Hadoop 3.3.6 (HDFS + YARN)**, **Java MapReduce**, and **Apache Hive 3.1.3 (ORC + Snappy)**. Designed specifically for low-overhead descriptive telemetry analysis of autonomous AI agents across tool invocations, token economics, latency percentiles, and multi-turn sessionization.

---

## Table of Contents
- [1. Overview & Problem Statement](#1-overview--problem-statement)
- [2. System Architecture](#2-system-architecture)
- [3. Hardware Assumptions & Machine Constraints](#3-hardware-assumptions--machine-constraints)
- [4. Prerequisites & Environment Setup](#4-prerequisites--environment-setup)
- [5. Quick Start (3 Commands)](#5-quick-start-3-commands)
- [6. Repository Directory Tree](#6-repository-directory-tree)
- [7. MapReduce Job Descriptions & Schemas](#7-mapreduce-job-descriptions--schemas)
- [8. Apache Hive Data Warehouse & Analytics Suite](#8-apache-hive-data-warehouse--analytics-suite)
- [9. How to Reproduce Benchmarks](#9-how-to-reproduce-benchmarks)
- [10. Key Empirical Findings](#10-key-empirical-findings)
- [11. Technical Challenges & Limitations](#11-technical-challenges--limitations)

---

## 1. Overview & Problem Statement

Autonomous AI agents (such as code generators, automated customer assistants, and multi-step research bots) execute iteratively through complex loops: prompting Large Language Models (LLMs), executing external tools (SQL databases, code sandboxes, web search), observing responses, and re-prompting. 

In production, these multi-turn workflows generate massive volumes of hierarchical telemetry data. Observing and operating agent systems at scale introduces unique operational challenges:
1. **Silent Failure Propagation**: Failures in external tool invocations (`invalid_args`, timeouts, API rate limits) cascade into wasted downstream model calls and truncated workflows.
2. **Context Window Inflation**: As conversation history and tool outputs accumulate over turns, prompt token consumption grows exponentially, driving non-linear billing costs.
3. **Severe Tail Latency**: Heavy tools (e.g. sandbox code execution) introduce high p95/p99 tail latencies that dominate user wait time.
4. **Session Fragmentation**: User interactions span multiple related runs that must be reconstructed chronologically to observe retry behavior and session-level degradation.

This project delivers an end-to-end distributed telemetry and observability platform to ingest, parse, sessionize, aggregate, and query millions of AI agent execution traces using the classic **Apache Hadoop** ecosystem.

### Constraints & Design Philosophy
- **Descriptive Analytics Only**: Strictly **NO machine learning libraries** (e.g., PyTorch, TensorFlow, Scikit-learn, XGBoost) are utilized anywhere in this repository. All insights are derived via distributed deterministic aggregation, percentiles, and analytical SQL.
- **Resource-Constrained Optimization**: Engineered to execute stably on standard developer hardware (Windows + WSL2 with only 5 GB RAM allocated), demonstrating that big data engineering principles (Combiners, Secondary Sort, Columnar ORC compression, Map-only pipelines) allow significant scale on modest footprints.
- **Data Quality Ingestion**: Intentionally injects and isolates corrupted, malformed, and out-of-schema records using Hadoop custom counters and error routing without crashing batch jobs.

---

## 2. System Architecture

The end-to-end data processing workflow spans synthetic ingestion, two-tier MapReduce batch processing, columnar storage with Apache Hive, and automated telemetry visualization:

```mermaid
flowchart TD
    subgraph S1 ["1. Ingestion Tier"]
        GEN["Synthetic Trace Generator<br/><code>datagen/generate_traces.py</code>"]
        RAW_LOCAL["Local Staging<br/><code>data/raw/{nested,flat}</code>"]
        HDFS_RAW["HDFS Raw Tier<br/><code>/data/raw/</code>"]
        
        GEN -->|Stream JSONL| RAW_LOCAL
        RAW_LOCAL -->|HDFS Put / Parallel Copy| HDFS_RAW
    end

    subgraph S2 ["2. Distributed Processing (Hadoop MapReduce)"]
        JOB1["Job 1: TraceFlattenJob<br/>(Map-Only, Gson Streaming)"]
        JOB2["Job 2: SessionizationJob<br/>(Secondary Sort Composite Key)"]
        
        HDFS_RAW -->|Format A: Nested JSONL| JOB1
        HDFS_RAW -->|Format B: Flat Span Events| JOB2
        
        OUT_STEPS["HDFS: <code>/data/processed/steps</code>"]
        OUT_RUNS["HDFS: <code>/data/processed/runs</code>"]
        OUT_MAL["HDFS: <code>/data/processed/_malformed</code>"]
        
        JOB1 -->|TSV Turn Records| OUT_STEPS
        JOB1 -->|Malformed Divert| OUT_MAL
        JOB2 -->|Chronological Ordered Sessions| OUT_RUNS
    end

    subgraph S3 ["3. Specialized Analytical Aggregations"]
        JOB3["Job 3: ToolStatsJob<br/>(Combiner: 1,524x Shuffle Cut)"]
        JOB4["Job 4: TokenStatsJob<br/>(Combiner: 198x Shuffle Cut)"]
        JOB5["Job 5: LatencyHistogramJob<br/>(Log-Binning Quantiles)"]
        
        OUT_STEPS --> JOB3
        OUT_STEPS --> JOB4
        OUT_STEPS --> JOB5
        
        AGG_TOOL["HDFS: <code>/data/analytics/tool_stats</code>"]
        AGG_TOKEN["HDFS: <code>/data/analytics/token_stats</code>"]
        AGG_LAT["HDFS: <code>/data/analytics/latency_hist</code>"]
        
        JOB3 --> AGG_TOOL
        JOB4 --> AGG_TOKEN
        JOB5 --> AGG_LAT
    end

    subgraph S4 ["4. Data Warehouse Tier (Apache Hive 3.1.3)"]
        HIVE_EXT["External Tables (Delimited TSV)<br/><code>steps</code>, <code>runs</code>, <code>tool_stats</code>, <code>token_stats</code>"]
        HIVE_ORC["Managed ORC Tables (Snappy Compressed)<br/><code>steps_orc</code>, <code>runs_orc</code> (Partitioned by dt)"]
        HIVE_VIEWS["Analytical Views<br/><code>v_tool_calls</code>, <code>v_llm_calls</code>"]
        
        OUT_STEPS & OUT_RUNS & AGG_TOOL & AGG_TOKEN & AGG_LAT --> HIVE_EXT
        HIVE_EXT -->|CTAS ETL + Dynamic Partitioning| HIVE_ORC
        HIVE_ORC --> HIVE_VIEWS
    end

    subgraph S5 ["5. Analytics & Visualization Tier"]
        HIVE_QL["15 Analytical HiveQL Queries (Q01 - Q15)<br/>Failure Rates, Token Costs, Latency Percentiles"]
        CSV_OUT["Delimited Analysis Outputs<br/><code>analysis/results/*.csv</code>"]
        CHARTS["Visualization Engine<br/><code>analysis/make_charts.py</code> (Pandas + Matplotlib)"]
        FIGS["Report Figures (200 DPI PNG)<br/><code>docs/figures/*.png</code>"]
        
        HIVE_VIEWS --> HIVE_QL
        HIVE_QL --> CSV_OUT
        CSV_OUT --> CHARTS
        CHARTS --> FIGS
    end
```

---

## 3. Hardware Assumptions & Machine Constraints

This system is deliberately engineered to operate without cloud resources on developer-grade laptops. The reference environment and memory allocations are configured as follows:

| Component | Target Machine Specification | Allocation / Role |
|---|---|---|
| **Host Operating System** | Windows 11 / Windows 10 (64-bit) | Host OS, allocates background resources |
| **Virtualization Runtime** | **WSL2 (Ubuntu 22.04 LTS)** | Linux environment hosting Hadoop & Hive |
| **Physical Host CPU** | Intel Core i5 12th Gen P-series (or equivalent) | 4 vCPUs allocated to WSL2 |
| **Physical Host RAM** | **8.0 GB Total System RAM** | **5.0 GB given to WSL2**, ~3.0 GB reserved for Windows |
| **WSL2 Swap File** | 2.0 GB virtual memory swap | Emergency buffer against OOM kills |
| **Hadoop Mode** | **Pseudo-Distributed Mode** | Single node executing NameNode, DataNode, RM, NM |
| **Storage Location** | Native Linux virtual filesystem (`~/ai-trace-bigdata`) | **Never** run under `/mnt/c/` (avoids 9p I/O penalties) |
| **Cluster Topology** | 1 NodeManager, 1 DataNode | Replication factor = 1 (`dfs.replication=1`) |

### WSL2 Resource Limits (`%USERPROFILE%\.wslconfig`)
```ini
[wsl2]
memory=5GB
processors=4
swap=2GB
```

### Low-Memory Hadoop & YARN Configuration Tuning
To prevent YARN container kills while running MapReduce and Hive queries on 5 GB RAM, the following settings are enforced:

| Configuration File | Property Name | Production Value | Architectural Rationale |
|---|---|---|---|
| `hdfs-site.xml` | `dfs.replication` | `1` | Single DataNode pseudo-distributed operation |
| `yarn-site.xml` | `yarn.nodemanager.resource.memory-mb` | `3072` | 3 GB maximum container memory budget |
| `yarn-site.xml` | `yarn.scheduler.minimum-allocation-mb` | `256` | Enables lightweight containers |
| `yarn-site.xml` | `yarn.scheduler.maximum-allocation-mb` | `1536` | Prevents a single task from starving the node |
| `yarn-site.xml` | `yarn.nodemanager.resource.cpu-vcores` | `4` | Matches allocated WSL vCPUs |
| `yarn-site.xml` | `yarn.nodemanager.vmem-check-enabled` | `false` | Prevents premature JVM virtual memory container kills |
| `mapred-site.xml` | `mapreduce.map.memory.mb` | `768` | Container memory for Map tasks |
| `mapred-site.xml` | `mapreduce.reduce.memory.mb` | `1024` | Container memory for Reduce tasks |
| `mapred-site.xml` | `yarn.app.mapreduce.am.resource.mb` | `512` | Lightweight ApplicationMaster allocation |
| `mapred-site.xml` | `mapreduce.map.java.opts` | `-Xmx600m` | Heap cap for Mapper child JVM |
| `mapred-site.xml` | `mapreduce.reduce.java.opts` | `-Xmx800m` | Heap cap for Reducer child JVM |
| `mapred-site.xml` | `mapreduce.task.io.sort.mb` | `128` | Memory buffer for mapper sort/spill |
| `hadoop-env.sh` | `HADOOP_HEAPSIZE_MAX` | `512m` | Caps daemons (NameNode, DataNode, RM, NM) |

---

## 4. Prerequisites & Environment Setup

Ensure the following packages and services are configured in Ubuntu 22.04 LTS on WSL2:

1. **Java Development Kit**: OpenJDK 8 (`java-8-openjdk-amd64`)
   ```bash
   sudo apt-get update && sudo apt-get install -y openjdk-8-jdk maven ssh
   ```
2. **Passwordless SSH**: Required for Hadoop daemon lifecycle management
   ```bash
   ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
   cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
   chmod 0600 ~/.ssh/authorized_keys
   sudo service ssh start
   ssh localhost  # Confirm zero-prompt login
   ```
3. **Apache Hadoop 3.3.6**: Installed at `/opt/hadoop` (or `$HADOOP_HOME`), binaries exported in `$PATH`.
4. **Apache Hive 3.1.3**: Installed at `/opt/hive` (or `$HIVE_HOME`).
   - **Guava Incompatibility Fix**: Remove `guava-19.0.jar` from `$HIVE_HOME/lib/` and symlink Hadoop's `guava-27.0-jre.jar`:
     ```bash
     rm $HIVE_HOME/lib/guava-19.0.jar
     cp $HADOOP_HOME/share/hadoop/hdfs/lib/guava-27.0-jre.jar $HIVE_HOME/lib/
     ```
   - **Derby Metastore Initialization**:
     ```bash
     schematool -dbType derby -initSchema
     ```
5. **Python 3 Environment**: Standard scientific libraries (strictly no ML)
   ```bash
   sudo apt-get install -y python3-pip python3-pandas python3-matplotlib
   ```

---

## 5. Quick Start (3 Commands)

Execute the entire end-to-end telemetry pipeline from scratch on the **small dataset** (100,000 runs, ~956,000 steps) with three simple commands:

```bash
# 1. Start Hadoop HDFS and YARN daemons
start-dfs.sh && start-yarn.sh

# 2. Run the complete automated pipeline (data generation, MR jobs 1-5, Hive loading, queries)
./scripts/run_pipeline.sh small

# 3. Generate high-resolution figures (200 DPI) and analytical report
python3 analysis/make_charts.py
```

### Verification
Upon completion, the following artifacts are generated:
- MapReduce outputs in HDFS: `hdfs dfs -ls /data/processed/` and `/data/analytics/`
- 15 HiveQL result CSVs in `analysis/results/` (`q01_tool_failure_rate.csv` through `q15_data_quality_report.csv`)
- 8 publication-quality figures in `docs/figures/`
- Executive findings compiled into `docs/results_summary.md`

---

## 6. Repository Directory Tree

```
ai-trace-bigdata/
├── README.md                           # Master project guide, architecture & instructions
├── implementation_guide.md             # Complete step-by-step engineering roadmap
│
├── datagen/                            # Synthetic Trace Generation Subsystem
│   └── generate_traces.py              # Generator producing Format A (nested) & Format B (flat)
│
├── mapreduce/                          # Java MapReduce Maven Project
│   ├── pom.xml                         # Maven configuration (bundles Gson into shaded JAR)
│   └── src/
│       ├── main/java/com/aitrace/
│       │   ├── job/
│       │   │   ├── flatten/            # Job 1: TraceFlattenJob & TraceFlattenMapper (Map-Only)
│       │   │   ├── session/            # Job 2: SessionizationJob, Keys, Partitioner, Reducer
│       │   │   └── analytics/
│       │   │       ├── tool/           # Job 3: ToolStatsJob, Combiner, Reducer, Writable
│       │   │       ├── token/          # Job 4: TokenStatsJob, Combiner, Reducer, Writable
│       │   │       └── latency/        # Job 5: LatencyHistogramJob, Combiner, Reducer
│       │   ├── model/                  # POJO telemetry data structures
│       │   ├── parser/                 # Gson-based streaming trace parser
│       │   └── util/                   # Delimited formatters & string helpers
│       └── test/java/com/aitrace/      # 20 Unit and Integration Tests (Surefire)
│
├── hive/                               # Apache Hive Data Warehouse Tier
│   ├── ddl/
│   │   ├── 01_create_db.hql            # Creates ai_obs database
│   │   ├── 02_external_tables.hql      # External TSV tables over MapReduce HDFS outputs
│   │   ├── 03_curated_orc.hql          # Managed ORC tables (Snappy compressed, partitioned)
│   │   └── 04_views.hql                # Convenient analytical abstractions (v_tool_calls, v_llm_calls)
│   └── queries/                        # 15 Analytical HiveQL Production Queries
│       ├── q01_tool_failure_rate.hql   # Overall failure rates per tool
│       ├── q02_top_errors_per_tool.hql # Breakdown of specific error codes per tool
│       ├── q03_retry_loop_analysis.hql # Tool retry sequences and recovery rates
│       ├── q04_token_consumption.hql   # Prompt, completion, total tokens per model
│       ├── q05_token_bottlenecks.hql   # Top token-heavy steps and task types
│       ├── q06_context_growth.hql      # Context window expansion curve over turns
│       ├── q07_latency_percentiles.hql # p50, p90, p95, p99 latency by model
│       ├── q08_slowest_tools.hql       # Latency percentiles and total time per tool
│       ├── q09_daily_trend.hql         # 30-day volume and failure trends
│       ├── q10_hourly_failure_heatmap.hql # Failure distributions across 24h cycle
│       ├── q11_model_task_success_rate.hql # Success rate cross-tab: model x task_type
│       ├── q12_failing_tool_sequences.hql  # Consecutive failure transition chains
│       ├── q13_cost_estimation.hql     # Financial spend modeling based on token pricing
│       ├── q14_session_analysis.hql    # Session duration, idle gap, and retry stats
│       └── q15_data_quality_report.hql # Pipeline integrity & malformed record audit
│
├── scripts/                            # Automation & Lifecycle Management Scripts
│   ├── 03_gen_and_upload.sh            # Generates traces and uploads to HDFS
│   ├── 04_run_jobs.sh                  # Executes MapReduce Jobs 1-5 sequentially
│   ├── 05_load_hive.sh                 # Executes Hive DDL scripts and validates table rows
│   ├── 06_run_queries.sh              # Runs Q01-Q15 and exports CSVs to analysis/results/
│   ├── run_pipeline.sh                 # Unified end-to-end master pipeline driver
│   └── benchmark.sh                    # Automated benchmarking suite (scaling, combiner, ORC)
│
├── analysis/                           # Visualization & Analysis Engine
│   ├── make_charts.py                  # Generates 8 publication-grade charts from CSVs
│   └── results/                        # Exported Hive query CSVs & benchmark.csv
│
└── docs/                               # Formal Documentation & Deliverables
    ├── architecture.md                 # Detailed architectural specifications & data flows
    ├── schema.md                       # Complete schema definitions (Format A & Format B)
    ├── results_summary.md              # Empirical findings & benchmark metrics
    ├── report_outline.md               # Comprehensive university submission outline
    ├── viva_qa.md                      # 20 In-depth technical viva questions & answers
    └── figures/                        # 8 High-resolution PNG figures (200 DPI)
```

---

## 7. MapReduce Job Descriptions & Schemas

### Job 1: `TraceFlattenJob` (Map-Only)
- **Role**: High-throughput distributed ingestion, schema normalization, and quality validation.
- **Input**: Raw nested JSON Lines (`Format A`) from `/data/raw/nested/`.
- **Mechanism**: Map-only execution (`mapreduce.job.reduces = 0`). Parses each JSON object via streaming Gson. Emits normalized TSV step records while routing corrupted/unparseable lines to Hadoop custom counter `MalformedRecords.INVALID_JSON` and diverting to `/data/processed/_malformed/`.
- **Output Path**: `/data/processed/steps/`
- **Output Schema (Tab-Delimited TSV)**:
  ```
  run_id \t session_id \t step_index \t step_type \t model \t tool_name \t tool_status \t tool_error_type \t latency_ms \t prompt_tokens \t completion_tokens \t total_tokens \t start_time_ms \t dt
  ```

---

### Job 2: `SessionizationJob` (Secondary Sort MapReduce)
- **Role**: Reconstructs multi-run user sessions chronologically, calculates inter-run idle times, and tracks error propagation across consecutive agent runs.
- **Input**: Flat span JSON Lines (`Format B`) from `/data/raw/flat/`.
- **Secondary Sort Implementation**:
  - **Composite Key**: `RunTimeKey(Text sessionId, LongWritable startTimeMs)`.
  - **Partitioner (`RunIdPartitioner`)**: Hashes only `sessionId` modulo number of reducers (`sessionId.hashCode() & Integer.MAX_VALUE % numPartitions`). Ensures all runs belonging to a session arrive at the same physical reducer.
  - **Grouping Comparator (`RunIdGroupingComparator`)**: Compares solely `sessionId`. Ensures all runs of a session are presented to a single `reduce()` call.
  - **Key Comparator**: Compares `sessionId ASC`, then `startTimeMs ASC` to sort runs chronologically prior to reducer invocation.
- **Memory Safety**: Reducer streams across the sorted iterator directly, calculating idle times without buffering runs into Java Heap memory (zero OOM risk).
- **Output Path**: `/data/processed/runs/`
- **Output Schema (Tab-Delimited TSV)**:
  ```
  session_id \t run_id \t model \t task_type \t status \t start_time_ms \t end_time_ms \t duration_ms \t idle_time_ms \t step_count \t tool_call_count \t total_tokens \t final_error_type \t dt
  ```

---

### Job 3: `ToolStatsJob` (MapReduce with Combiner)
- **Role**: Aggregates execution volume, failure counts, timeouts, and cumulative latencies per tool and task type.
- **Input**: `/data/processed/steps/`
- **Map Output**: Key: `(tool_name \t task_type)`, Value: `ToolStatsWritable(total, ok, error, timeout, sum_latency_ms)`.
- **Combiner**: Executes associative partial sums on local mapper spill partitions, reducing network shuffle volume by **1,524.56x**.
- **Reducer**: Computes failure rate percentages and average latencies per tool across domains.
- **Output Path**: `/data/analytics/tool_stats/`
- **Output Schema (Tab-Delimited TSV)**:
  ```
  tool_name \t task_type \t total_calls \t ok_calls \t error_calls \t timeout_calls \t failure_rate_pct \t avg_latency_ms
  ```

---

### Job 4: `TokenStatsJob` (MapReduce with Combiner)
- **Role**: Evaluates token consumption dynamics (prompt vs completion tokens) and tracks context window expansion per model and task domain.
- **Input**: `/data/processed/steps/`
- **Map Output**: Key: `(model \t task_type)`, Value: `TokenStatsWritable(prompt_tokens, completion_tokens, total_tokens, min_prompt, max_prompt)`.
- **Combiner**: Performs local mapper-level partial aggregation, achieving a **198.50x** network shuffle reduction.
- **Reducer**: Aggregates total token consumption, calculates average token ratios, and extracts peak context window depths.
- **Output Path**: `/data/analytics/token_stats/`
- **Output Schema (Tab-Delimited TSV)**:
  ```
  model \t task_type \t total_prompt_tokens \t total_completion_tokens \t total_tokens \t min_prompt_tokens \t max_prompt_tokens \t avg_prompt_tokens
  ```

---

### Job 5: `LatencyHistogramJob` (MapReduce with Combiner)
- **Role**: Computes non-parametric latency distribution histograms across 8 logarithmic buckets for LLMs and external tools.
- **Input**: `/data/processed/steps/`
- **Map Output**: Key: `(model \t step_type)`, Value: `LatencyStatsWritable(b_lt100ms, b_100_250ms, b_250_500ms, b_500ms_1s, b_1s_2_5s, b_2_5s_5s, b_5s_10s, b_gt10s)`.
- **Combiner / Reducer**: Aggregates bucket counts across partitions to construct the empirical cumulative distribution function (ECDF).
- **Output Path**: `/data/analytics/latency_hist/`
- **Output Schema (Tab-Delimited TSV)**:
  ```
  model \t step_type \t lt_100ms \t 100_250ms \t 250_500ms \t 500ms_1s \t 1s_2_5s \t 2_5s_5s \t 5s_10s \t gt_10s \t total_calls
  ```

---

## 8. Apache Hive Data Warehouse & Analytics Suite

The analytics tier utilizes **Apache Hive 3.1.3** backed by a local Derby metastore. It employs a two-tier storage architecture to combine flexible ingestion with columnar query efficiency:

1. **External Layer**: TSV tables mapped directly over MapReduce HDFS outputs (`steps`, `runs`, `tool_stats`, `token_stats`, `latency_hist`) using `ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'`.
2. **Curated Managed Layer**: Columnar tables stored as **ORC (Optimized Row Columnar)** with **Snappy compression**, partitioned dynamically by date (`dt STRING`).
   - Achieves **2.04x disk compression** over raw text.
   - Enables columnar projection (skipping unneeded fields) and predicate pushdown (skipping stripes using built-in min/max indexes).

### Analytical Query Suite (Q01 – Q15)
The suite includes 15 production HiveQL queries located in [hive/queries/](file:///home/aksh_dhingra/ai-trace-bigdata/hive/queries/):

| Query File | Analysis Title | Key Analytical Insight / Output |
|---|---|---|
| `q01_tool_failure_rate.hql` | Tool Failure Rates | Identifies `code_exec` as the highest failing tool at 22.73% |
| `q02_top_errors_per_tool.hql` | Error Code Breakdown | Pinpoints `invalid_args` and `rate_limit` as primary failure causes |
| `q03_retry_loop_analysis.hql` | Retry Loop Recovery | Evaluates whether retrying failed tool calls leads to ultimate success |
| `q04_token_consumption.hql` | Model Token Volume | Shows Mistral-Large and Claude-Sonnet dominate token consumption |
| `q05_token_bottlenecks.hql` | Token Outliers | Identifies single steps consuming >10,000 prompt tokens |
| `q06_context_growth.hql` | Context Window Growth | Measures prompt inflation from 1,997 tokens (turn 0) to 5,021+ (turn 4+) |
| `q07_latency_percentiles.hql` | Latency Percentiles | Extracts p50, p90, p95, p99 latencies (Llama-3-70b slowest at 4,822 ms p95) |
| `q08_slowest_tools.hql` | Tool Latency Bottlenecks | Shows `code_exec` p95 is 28,414 ms (accounts for 22.4% of execution time) |
| `q09_daily_trend.hql` | 30-Day Operational Trend | Tracks daily run volume, tool failures, and token volume over time |
| `q10_hourly_failure_heatmap.hql` | Hourly Heatmap | Profiles agent failure frequencies across the 24-hour cycle |
| `q11_model_task_success_rate.hql` | Success Matrix | Cross-tabulates model performance across coding, research, and support |
| `q12_failing_tool_sequences.hql` | Failure Cascades | Identifies pairs of consecutive tool failures that cause agent aborts |
| `q13_cost_estimation.hql` | Financial Cost Modeling | Joins token volume with pricing to estimate real USD expenditure |
| `q14_session_analysis.hql` | Multi-Turn Sessions | Measures user session duration, inter-run idle times, and retry rates |
| `q15_data_quality_report.hql` | Pipeline Data Hygiene | Audits malformed lines (1.14%), null rates, and counter consistency |

---

## 9. How to Reproduce Benchmarks

The automated benchmarking framework ([scripts/benchmark.sh](file:///home/aksh_dhingra/ai-trace-bigdata/scripts/benchmark.sh)) systematically measures cluster scaling, combiner efficiency, compression savings, and reducer parallelism. All metrics are recorded directly from YARN job counters into `analysis/results/benchmark.csv`.

### Running the Full Benchmark Suite
```bash
./scripts/benchmark.sh
```

### Running Specific Benchmark Experiments
```bash
# Scaling experiment only (100K, 500K, 1M runs)
./scripts/benchmark.sh --experiment scaling

# Combiner impact evaluation (ToolStatsJob & TokenStatsJob at 500K)
./scripts/benchmark.sh --experiment combiner

# Reducer parallelism comparison (1 vs 2 reducers on SessionizationJob)
./scripts/benchmark.sh --experiment reducers

# Hive storage performance comparison (Text vs ORC Snappy)
./scripts/benchmark.sh --experiment hive

# HDFS output compression comparison (None vs GZIP on Job 1)
./scripts/benchmark.sh --experiment compression

# Rapid validation mode (10% scaled dataset for fast verification)
./scripts/benchmark.sh --quick
```

### Measured Benchmark Summary (Real Telemetry Data)

#### 1. Scaling Characteristics (100K -> 500K -> 1M Runs)
| Job Name | 100K Runs Runtime | 500K Runs Runtime | 1.0M Runs Runtime | Scaling Behavior |
|---|---|---|---|---|
| `TraceFlattenJob` (Map-Only) | **33 s** (100K recs) | **57 s** (500K recs) | **101 s** (1.0M recs) | Highly sub-linear (streaming parser, zero shuffle) |
| `SessionizationJob` (Secondary Sort) | **40 s** (956K recs) | **123 s** (4.77M recs) | — (Scaled down) | Near-linear (dominated by disk spill and shuffle sort) |
| `ToolStatsJob` (Combiner) | **27 s** (955K recs) | **45 s** (4.76M recs) | **54 s** (9.53M recs) | Sub-linear due to massive combiner pre-aggregation |
| `TokenStatsJob` (Combiner) | **27 s** (955K recs) | **46 s** (4.76M recs) | **59 s** (9.53M recs) | Sub-linear scaling across all data scales |
| `LatencyHistogramJob` | **25 s** (955K recs) | **39 s** (4.76M recs) | — (Scaled down) | Extremely fast single-pass bucketing |

#### 2. Combiner Network I/O Reduction (500K Runs)
| Job | Shuffle Bytes (Combiner OFF) | Shuffle Bytes (Combiner ON) | Network Reduction Ratio |
|---|---|---|---|
| `ToolStatsJob` | 131,121,258 bytes (~131.1 MB) | 86,006 bytes (~86.0 KB) | **1,524.56x reduction** |
| `TokenStatsJob` | 261,240,199 bytes (~261.2 MB) | 1,316,041 bytes (~1.3 MB) | **198.50x reduction** |

#### 3. Output Compression Efficiency (Job 1 at 500K Runs)
| Output Compression | Output Size in HDFS | Map Execution Runtime | Net Result |
|---|---|---|---|
| `None` (Uncompressed TSV) | 2.47 GB | 56 seconds | Baseline disk write bandwidth |
| `GZIP` (`.gz` TSV blocks) | 271 MB | **51 seconds** | **9.11x space savings & 8.9% faster runtime** |

#### 4. Reducer Parallelism on WSL2 (500K Runs)
- **1 Reducer**: 125 seconds (Shuffle bytes: 1,241,867,311 bytes)
- **2 Reducers**: 124 seconds (Shuffle bytes: 1,241,867,503 bytes)
- *Finding*: On a single-node virtualized WSL2 machine with 2 virtual CPU cores sharing a single physical NVMe/SATA bus, adding a second reducer yields marginal runtime benefits due to concurrent disk I/O contention. In a multi-node physical cluster, this split achieves near-linear speedups across independent machines.

---

## 10. Key Empirical Findings

All metrics reflect real execution results computed from cluster telemetry:

1. **Tool Reliability**: External tool failure rate is **8.31%** across 325 calls. `code_exec` is the most vulnerable tool with a **22.73% overall failure rate**, peaking at **37.50%** in customer support domains. `invalid_args` (malformed code parameters) and `timeout` account for 74% of all tool failures.
2. **Context Window Inflation**: Prompt tokens dominate completions by **37.37 : 1** (310.95M prompt tokens vs 8.32M completion tokens across the dataset). Average prompt tokens grow from **1,997.5** on turn 0 to **5,021.0+** by turn 4 (+151% growth), demonstrating that multi-turn history accumulation is the primary driver of agent operating costs.
3. **Tail Latency Bottlenecks**: `code_exec` exhibits a p95 latency of **28,414.4 ms** (average 9,477 ms), consuming 22.44% of total agent execution time. For LLMs, `llama-3-70b` exhibited the slowest p95 latency (**4,822.5 ms**), while `gpt-4o` was the fastest (**2,323.4 ms**).
4. **Data Quality**: The ingestion engine successfully caught and diverted **10 malformed records (1.14%)** without crashing batch jobs, validating the custom counter and quarantine architecture.

Visualizations of all findings are available in [`docs/figures/`](file:///home/aksh_dhingra/ai-trace-bigdata/docs/figures/) and summarized in [`docs/results_summary.md`](file:///home/aksh_dhingra/ai-trace-bigdata/docs/results_summary.md).

---

## 11. Technical Challenges & Limitations

### Challenges Encountered & Resolutions
1. **WSL2 Memory Pressures & YARN Container Kills**: With 5 GB allocated to WSL2, default YARN configurations frequently killed containers for exceeding memory thresholds. Resolved by disabling virtual memory checking (`yarn.nodemanager.vmem-check-enabled=false`) and capping mapper/reducer heaps to 600m/800m.
2. **Hive 3.1.3 Guava Incompatibility**: Hive 3.1.3 ships with Guava 19.0, whereas Hadoop 3.3.6 requires Guava 27.0, causing `NoSuchMethodError` during table creation. Resolved by removing the legacy JAR from `$HIVE_HOME/lib` and linking Hadoop's Guava 27.
3. **Secondary Sort Reducer Heap Overflow**: In naive implementations, grouping all steps of a long session into an in-memory `ArrayList` causes Java Heap OOM errors. Resolved by architecting a streaming reducer that iterates over the secondary-sorted stream in a single pass.
4. **Metastore Locking in Derby**: Embedded Apache Derby only supports a single active process connection. Scripts enforce sequential Hive invocations and release locks promptly.

### Project Limitations
- **Synthetic Data**: Traces are generated stochastically based on realistic parameter distributions (log-normal latency, exponential token growth, empirical failure rates) rather than harvested from a production customer-facing agent deployment.
- **Single-Node Virtualization**: Executing Hadoop on a single WSL2 node simulates distributed computing logically (YARN containers, HDFS block splitting), but physical storage and CPU resources share the same host hardware.
- **Scaled-Down Dataset Tiers**: Dataset sizes are capped between 100K and 1.0M runs (up to ~9.5 million steps) to operate safely within 5 GB RAM and 20 GB free disk space without host freezing.
- **Batch Processing Latency**: MapReduce and Hive are high-throughput batch processing engines with high job startup latency (20–40 seconds per job). Real-time telemetry monitoring in production would require streaming engines (e.g., Apache Flink or Kafka Streams).

---

## Formal Documentation Links
- [Detailed System Architecture & Data Flows](file:///home/aksh_dhingra/ai-trace-bigdata/docs/architecture.md)
- [Data Schema Specifications (Format A & B)](file:///home/aksh_dhingra/ai-trace-bigdata/docs/schema.md)
- [Empirical Results & Benchmark Analysis](file:///home/aksh_dhingra/ai-trace-bigdata/docs/results_summary.md)
- [Comprehensive University Report Outline](file:///home/aksh_dhingra/ai-trace-bigdata/docs/report_outline.md)
- [Viva Voce Technical Q&A (20 Questions)](file:///home/aksh_dhingra/ai-trace-bigdata/docs/viva_qa.md)
