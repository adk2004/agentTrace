# Large-Scale Distributed Analysis of AI Agent Execution Traces and LLM Telemetry Logs

**Stack:** Hadoop 3.3.x (HDFS + YARN, pseudo-distributed), Java MapReduce, Gson, Apache Hive 3.1.x
**Constraint:** Descriptive analytics only. NO ML training or inference anywhere.

## Target machine (design constraints for everything below)

| Item | Value |
|---|---|
| Host OS | Windows 11/10 |
| CPU | Intel i5 12th gen P-series (4 threads given to WSL) |
| RAM | 8 GB total, **about 5 GB given to WSL2**, about 3 GB left for Windows |
| Cloud | **None.** Everything is local and free |
| Runtime | **WSL2 + Ubuntu 22.04 only** (no native Windows Hadoop) |

This file has two parts:
- **Part A:** step-by-step implementation plan.
- **Part B:** copy-paste prompts for a coding agent (Claude Code / Cursor / Copilot), one per step.

---

# PART A: STEP-BY-STEP IMPLEMENTATION PLAN

## Phase 0: Decisions locked in

| Decision | Choice | Reason |
|---|---|---|
| Runtime | WSL2 + Ubuntu 22.04 | Hadoop is unreliable on native Windows |
| Project location | Inside WSL home (`~/ai-trace-bigdata`), **not** `/mnt/c/...` | The Windows-mounted drive is very slow for I/O |
| Java | OpenJDK 8 | Safest for Hadoop 3.3 + Hive 3.1 |
| Hadoop | 3.3.6 | Stable with Hive 3.1.3 |
| Hive | 3.1.3, Derby metastore, **MapReduce engine** | Light, no MySQL, no Tez/Spark |
| Build | Maven, shaded jar | Bundles Gson |
| Data | Synthetic OpenTelemetry-style agent traces (primary); small LMSYS sample (optional) | See note below |
| Dataset sizes | **small = 100K, medium = 500K, large = 1M runs** | Sized for 5 GB RAM and limited disk |

> **Dataset note:** LMSYS Chatbot Arena has conversations and model names but **no tool calls, latency or token counts**. For failure rates, latency and token analysis you need agent-style traces. So a **Python generator** (random sampling, not ML) produces realistic nested JSONL. LMSYS-Chat-1M (gated on Hugging Face, free login, not a cloud service) is optional: use only a **50K-row sample** to show the pipeline also handles real data. State this clearly in your report.

> **Size estimate:** about 8 steps per run, about 3 KB per run in JSON. 1M runs is roughly 3 to 4 GB per format. Keep **at least 20 GB free disk** in WSL.

## Phase 1: Environment (WSL2-first)
1. Enable WSL2, install Ubuntu 22.04, create `C:\Users\<you>\.wslconfig` (limits below), run `wsl --shutdown`, reopen.
2. Inside Ubuntu: install OpenJDK 8, ssh, Maven, Python 3; set up passwordless SSH to localhost and `sudo service ssh start`.
3. Install Hadoop with **low-memory configs** (below), format NameNode, start HDFS + YARN, verify with `jps` and WordCount.
4. Install Hive 3.1.3, fix guava conflict, init Derby, verify.
5. `scripts/00_verify_env.sh` checks everything and also prints free RAM and disk.

**`.wslconfig` (Windows side):**
```
[wsl2]
memory=5GB
processors=4
swap=2GB
```

**Low-memory Hadoop settings (the agent must apply these):**

| File | Property | Value |
|---|---|---|
| hdfs-site.xml | dfs.replication | 1 |
| yarn-site.xml | yarn.nodemanager.resource.memory-mb | 3072 |
| yarn-site.xml | yarn.scheduler.minimum-allocation-mb | 256 |
| yarn-site.xml | yarn.scheduler.maximum-allocation-mb | 1536 |
| yarn-site.xml | yarn.nodemanager.resource.cpu-vcores | 4 |
| yarn-site.xml | yarn.nodemanager.vmem-check-enabled | false |
| mapred-site.xml | mapreduce.map.memory.mb | 768 |
| mapred-site.xml | mapreduce.reduce.memory.mb | 1024 |
| mapred-site.xml | yarn.app.mapreduce.am.resource.mb | 512 |
| mapred-site.xml | mapreduce.map.java.opts | -Xmx600m |
| mapred-site.xml | mapreduce.reduce.java.opts | -Xmx800m |
| mapred-site.xml | mapreduce.task.io.sort.mb | 128 |
| hadoop-env.sh | HADOOP_HEAPSIZE_MAX | 512m (NameNode/DataNode/RM/NM) |

Rules of thumb on this machine: **one job at a time, no history server, 1 to 2 reducers, close browsers and heavy Windows apps during runs.**

## Phase 2: Data design and generation
1. Define the **nested run schema** (Format A) and a **flat span-event schema** (Format B) so sessionization is a real problem.
2. Generator knobs: runs, models, tools, per-tool failure rates, log-normal latency, token growth with step index, about 1% malformed lines, 30-day timestamp span.
3. Generate three sizes: 100K (development), 500K (main results), 1M (scaling test, nested only is fine if disk is tight).
4. Generator must **stream to disk** with low RAM, and split output into several files (for example 8 to 16 files) so map tasks run in parallel.

## Phase 3: Java MapReduce
1. Maven project, packages: `model`, `parser`, `jobs`, `util`.
2. **Job 1, TraceFlattenJob (map-only):** nested JSON → one TSV row per step, with quality counters.
3. **Job 2, SessionizationJob:** group span events by `run_id`, secondary sort by timestamp, build run summaries (custom key, partitioner, grouping comparator).
4. **Job 3, ToolStatsJob:** failure rates and error breakdown, combiner.
5. **Job 4, TokenStatsJob:** token usage per model, task type and day, combiner.
6. **Job 5, LatencyHistogramJob:** latency buckets per model and step type.
7. Pipeline script chains the jobs, one at a time.
8. Develop and test everything in **local runner mode on 100K first** (fast, low memory), then run on YARN.

## Phase 4: Hive
1. Database `ai_obs`; external tables over MR outputs; ORC partitioned copies.
2. 15 HiveQL queries (list in Part B, Step 8).
3. Export results to CSV.
4. Hive settings: `hive.execution.engine=mr`, `mapreduce.job.reduces=2`, `hive.auto.convert.join=true`, small-table map joins.

## Phase 5: Automation and benchmarks (laptop-safe)
- Full pipeline script with timing per stage.
- Benchmarks are limited so they finish in reasonable time:
  - Scaling: 100K, 500K, 1M (Jobs 1, 3, 4 only at 1M; the whole pipeline at 100K and 500K).
  - Combiner on vs off (Jobs 3 and 4 at 500K).
  - Reducers 1 vs 2 (Job 2 at 500K). **No 4-reducer test.**
  - Text vs ORC for 2 queries.
  - Compression none vs gzip on Job 1 output at 500K.
- Read counters from the **job console output**, not the history server.

## Phase 6: Visualization and report
Charts via pandas + matplotlib, report outline, README, viva Q&A.

## Timeline (about 4 weeks)
| Week | Work |
|---|---|
| 1 | Phases 1 and 2 (setup is where most time goes on Windows) |
| 2 | Phase 3 |
| 3 | Phases 4 and 5 |
| 4 | Phase 6 + report and viva prep |

## Definition of done
- [ ] Runs entirely in WSL2, offline after downloads, no cloud
- [ ] One command runs the whole pipeline on the small dataset
- [ ] 5 MapReduce jobs, tests passing
- [ ] About 4M+ step rows processed on the 500K-run dataset (about 8M on 1M runs)
- [ ] At least 12 Hive queries with saved outputs
- [ ] Benchmarks and charts in the report
- [ ] No ML libraries in the project

---

# PART B: AGENT PROMPTS

## How to use
1. In WSL, create `~/ai-trace-bigdata/` and put this file there as `AGENT_GUIDE.md`.
2. Paste **Prompt 0** first (or save it as `CLAUDE.md` / `.cursorrules`).
3. Feed **one step prompt at a time**. Don't continue until the acceptance check passes.

---

## PROMPT 0: Master Context (paste first, keep always)

```
You are a senior Big Data engineer helping me build a university project:
"Large-Scale Distributed Analysis of AI Agent Execution Traces and LLM Telemetry Logs".

MY MACHINE (design everything for this)
- Windows laptop, Intel i5 12th gen P-series, 8 GB RAM total.
- I work inside WSL2 Ubuntu 22.04, limited to 5 GB RAM, 4 processors, 2 GB swap. Project lives in ~/ai-trace-bigdata (never under /mnt/c).
- NO cloud services of any kind. Everything must run locally and free.
- Memory is the main constraint: never assume more than ~3 GB total for YARN containers; run one job at a time; prefer streaming over buffering; no history server; 1-2 reducers.

HARD RULES
- Stack: Hadoop 3.3.6 (HDFS+YARN, pseudo-distributed), Java MapReduce (Java 8 source/target), Gson for JSON, Apache Hive 3.1.3 (Derby metastore, execution engine = mr), Maven, bash, and Python 3 ONLY for data generation and charts.
- Descriptive analytics only. Do NOT use or suggest any machine learning (no training, no inference, no sklearn/torch).
- All nested JSON parsing happens inside Java MapReduce using Gson (not Hive JSON SerDe, not Spark).
- Hive is used only for tables over MapReduce output and analytical HiveQL.
- Dataset sizes: small=100K runs, medium=500K runs, large=1M runs. Never generate more.
- Code must be production-quality: clear packages, Javadoc on public classes, no hardcoded absolute paths, robust to malformed JSON (count with Hadoop counters, never crash).
- Test new jobs first with the local runner (-Dmapreduce.framework.name=local -fs file:///) on small data, then on YARN.
- Every step ends with: (1) files created, (2) exact commands to run, (3) an acceptance check I can verify.
- Work in small steps. After each step, stop and wait for me to say "next".
- Ask me before any assumption that changes the schema.

PROJECT LAYOUT (create progressively)
ai-trace-bigdata/
  README.md
  scripts/            (bash: setup, verify, pipeline, benchmark)
  datagen/            (python trace generator)
  mapreduce/          (maven project)
    pom.xml
    src/main/java/com/sauravpatel/aitrace/{model,parser,jobs,util}
    src/test/java/...
  hive/               (ddl/*.hql, queries/*.hql)
  analysis/           (python charts, exported csv)
  docs/               (architecture, report notes)

Confirm you understand, then wait for Step 1.
```

---

## PROMPT 1: Environment setup on WSL2

```
STEP 1: Environment (WSL2 Ubuntu 22.04, 5 GB RAM limit).
1. docs/wsl_setup.md: how to enable WSL2 on Windows, install Ubuntu 22.04, create C:\Users\<user>\.wslconfig with memory=5GB, processors=4, swap=2GB, run `wsl --shutdown`, and how to check with `free -h` and `nproc`. Also: keep the project in the WSL filesystem, and how to start ssh with `sudo service ssh start` (WSL has no auto-start by default).
2. scripts/01_install_hadoop.sh: installs OpenJDK 8, ssh, maven, python3-pip; downloads Hadoop 3.3.6; sets env vars in ~/.bashrc; configures pseudo-distributed mode: fs.defaultFS=hdfs://localhost:9000, dfs.replication=1, mapreduce.framework.name=yarn, and the following LOW-MEMORY settings:
   yarn.nodemanager.resource.memory-mb=3072, yarn.scheduler.minimum-allocation-mb=256, yarn.scheduler.maximum-allocation-mb=1536, yarn.nodemanager.resource.cpu-vcores=4, yarn.nodemanager.vmem-check-enabled=false,
   mapreduce.map.memory.mb=768, mapreduce.reduce.memory.mb=1024, yarn.app.mapreduce.am.resource.mb=512, mapreduce.map.java.opts=-Xmx600m, mapreduce.reduce.java.opts=-Xmx800m, mapreduce.task.io.sort.mb=128, HADOOP_HEAPSIZE_MAX=512m.
   Configure passwordless SSH to localhost. Format NameNode only if not already formatted.
3. scripts/02_install_hive.sh: downloads Hive 3.1.3, sets HIVE_HOME, fixes the guava jar conflict with Hadoop 3.3.x, creates /tmp and /user/hive/warehouse on HDFS with correct permissions, runs schematool -initSchema -dbType derby, and sets hive.execution.engine=mr and a low-memory Hive config in hive-site.xml.
4. scripts/start_cluster.sh and scripts/stop_cluster.sh: start/stop ssh, HDFS and YARN (no history server), and print `jps`.
5. scripts/00_verify_env.sh: checks java -version, free RAM and disk (warn if < 20 GB free), jps shows NameNode/DataNode/ResourceManager/NodeManager, hdfs put/ls, runs hadoop-mapreduce-examples wordcount on a small file, and runs `hive -e "show databases;"`. Prints PASS/FAIL for each check.
6. README troubleshooting: safe mode, port conflicts, Derby lock errors (only one Hive session), YARN containers killed (lower memory settings), WSL out-of-memory (check .wslconfig), slow I/O (project must not live in /mnt/c).
Do not write project code yet.
Acceptance check: 00_verify_env.sh prints PASS for all checks.
```

---

## PROMPT 2: Trace schema and data generator

```
STEP 2: Schema + synthetic data generator (random sampling only, no ML). Low-RAM, streaming.

A) Define two formats and document them in docs/schema.md with examples.

FORMAT A, nested run record (one JSON object per line, JSONL):
{
  "trace_id": "uuid",
  "session_id": "uuid",
  "run_id": "uuid",
  "agent_name": "string",
  "task_type": "coding|web_research|data_analysis|customer_support|summarization",
  "model": "gpt-4o|claude-sonnet|llama-3-70b|mistral-large|gemini-pro",
  "start_time": "ISO-8601 UTC",
  "end_time": "ISO-8601 UTC",
  "status": "success|failed|timeout",
  "metadata": {"user_tier":"free|pro", "region":"...", "agent_version":"..."},
  "steps": [
    {
      "step_index": 0,
      "type": "llm_call|tool_call|retrieval",
      "timestamp": "ISO-8601 UTC",
      "latency_ms": 0,
      "llm": {"prompt_tokens":0,"completion_tokens":0,"finish_reason":"stop|length|tool_calls"},
      "tool": {"name":"web_search|code_exec|sql_query|file_read|http_get|calculator",
               "status":"ok|error|timeout","error_type":null,"retry_count":0,"args_size_bytes":0}
    }
  ]
}
Rules: llm block only for llm_call/retrieval, tool block only for tool_call. About 3 percent of optional fields are null or missing on purpose.

FORMAT B, flat span events (one event per line; events of the same run_id are scattered across lines and files): trace_id, session_id, run_id, span_id, parent_span_id, event_type, timestamp, plus the same step fields flattened.

B) Write datagen/generate_traces.py (stdlib + numpy for random sampling only):
- CLI: --runs N --format nested|flat|both --out DIR --files K --seed S --malformed-pct 1.0
- Average about 8 steps per run (min 2, max about 40, heavy tail).
- Latency: log-normal, different params per model and tool; tool_call latency heavy-tailed.
- Tool failure probabilities differ per tool (web_search 8 percent, code_exec 15, http_get 12, calculator 1, sql_query 6, file_read 4) and per task type. Error types: rate_limit, timeout, invalid_args, network_error, parse_error.
- Failed tool calls retried up to 3 times, sometimes causing retry loops.
- Prompt tokens grow with step_index (context accumulation); completion tokens log-normal.
- Timestamps spread across 30 days.
- Inject malformed lines (truncated JSON, empty line, wrong types) at the given percent.
- MUST stream to disk line by line and use under 300 MB RAM even for 1M runs. Output split into K files (default 8).
- Print final file sizes and estimated step count.
Also scripts/03_gen_and_upload.sh <small|medium|large> (100K / 500K / 1M runs): checks free disk first and aborts with a clear message if less than 3x the estimated size is available, generates the data in ~/data/, uploads to /data/raw/nested and /data/raw/flat, and shows `hdfs dfs -du -h /data/raw`. For "large", generate the nested format only unless I pass --both.
Acceptance check: generate 1000 runs and show 3 sample lines of each format; upload the small dataset and show sizes.
```

---

## PROMPT 3: Maven project, model, parser

```
STEP 3: MapReduce project skeleton + robust parser.
1. mapreduce/pom.xml: Java 8, hadoop-client 3.3.6 (provided scope), gson 2.10.1, junit 4.13, mockito (test); maven-shade-plugin producing ai-trace-mr.jar that bundles Gson; exclude signature files.
2. Package model: POJOs RunRecord, StepRecord, LlmInfo, ToolInfo, Metadata (nullable fields, boxed types), SpanEvent.
3. Package parser: TraceParser with
   - Optional<RunRecord> parseRun(String jsonLine)
   - Optional<SpanEvent> parseSpanEvent(String jsonLine)
   Use Gson with try/catch for JsonSyntaxException, JsonParseException, NumberFormatException, ClassCastException; handle missing/null nested blocks safely; normalise timestamps to epoch millis; clamp negative latencies to 0 and flag them. Reuse a single Gson instance (memory-friendly).
4. util: TsvUtil (escape tabs/newlines, join fields, "\\N" for nulls), TimeUtil.
5. JUnit tests: valid, truncated, empty, wrong type, missing tool block, null steps, negative latency.
Do NOT write jobs yet.
Acceptance check: `mvn clean package` succeeds, tests pass, jar contains com/google/gson classes.
```

---

## PROMPT 4: Job 1, TraceFlattenJob (map-only)

```
STEP 4: Job 1 TraceFlattenJob (map-only, 0 reducers).
Input: /data/raw/nested. Output: /data/processed/steps (tab-separated text).
For each valid RunRecord emit ONE row per step with columns in this exact order:
run_id, session_id, trace_id, agent_name, task_type, model, run_status, user_tier, region, agent_version, step_index, step_type, step_ts_epoch_ms, dt (yyyy-MM-dd), latency_ms, prompt_tokens, completion_tokens, finish_reason, tool_name, tool_status, error_type, retry_count, args_size_bytes
Requirements:
- Mapper outputs NullWritable key + Text row (reuse Text objects, no per-record allocations where avoidable). Use \N for nulls.
- Counters in group "TRACE_QUALITY": TOTAL_LINES, VALID_RUNS, MALFORMED_LINES, EMPTY_STEPS, NEGATIVE_LATENCY_FIXED, STEPS_EMITTED.
- Use MultipleOutputs to write a capped sample (e.g. 100 per mapper) of malformed lines to /data/processed/_malformed.
- Driver via ToolRunner, args: <input> <output>. Support -Dmapreduce.output.fileoutputformat.compress=true with gzip.
- Unit-test the mapper (Mockito or local runner).
Acceptance check: run on the small dataset in local mode first (-Dmapreduce.framework.name=local -fs file:///), then on YARN; print counters; `hdfs dfs -cat /data/processed/steps/part-m-00000 | head -3`; confirm peak memory stays within limits (no container kills).
```

---

## PROMPT 5: Job 2, SessionizationJob

```
STEP 5: Job 2 SessionizationJob, group events into agent execution runs.
Input: /data/raw/flat. Output: /data/processed/runs.
Implement ALL of these (core MapReduce learning goals):
- Mapper: parse span event, emit composite key (run_id, timestamp_ms) -> compact step payload.
- Custom WritableComparable RunTimeKey (run_id, ts).
- Custom Partitioner: partition by run_id hash ONLY.
- Custom GroupingComparator: group by run_id ONLY.
- SortComparator: run_id, then ts ascending (SECONDARY SORT; the reducer sees steps already ordered, no in-memory sort).
- Reducer: STREAMS the steps of one run (never buffers all steps, because memory is tight) and emits one row:
  run_id, session_id, agent_name, task_type, model, start_ts, end_ts, duration_ms, total_steps, llm_calls, tool_calls, tool_failures, tool_timeouts, total_retries, prompt_tokens_total, completion_tokens_total, total_latency_ms, max_step_latency_ms, distinct_tools_used, first_error_type, final_status, is_retry_loop (same tool failed >= 3 times consecutively).
- Session-level rollup via MultipleOutputs or a small second job (document the choice): session_id, runs_in_session, total_tokens, total_duration_ms.
- Counters: RUNS_BUILT, ORPHAN_EVENTS, DUPLICATE_SPANS (dedupe by span_id using the previous span only, or a bounded set), LONG_RUNS (>500 steps).
- Default 2 reducers, configurable by -D.
Unit tests for the reducer logic with hand-made sequences (3 runs including a retry loop).
Acceptance check: row count in /data/processed/runs equals the number of distinct run_id in the flat input (verify on a 10K-run sample) and sample rows look correct.
```

---

## PROMPT 6: Jobs 3 to 5, aggregations

```
STEP 6: Three aggregation jobs reading /data/processed/steps. Each has Driver, Mapper, Reducer (+ Combiner where valid), counters, and a unit test. Use at most 2 reducers.

JOB 3, ToolStatsJob -> /data/analytics/tool_stats
 Key: tool_name|task_type. Output: calls, ok, errors, timeouts, failure_rate, avg_latency_ms, avg_retries, top error types as "type:count;type:count". Combiner with a custom Writable of partial counts. Only step_type=tool_call rows. Support -Duse.combiner=true/false.

JOB 4, TokenStatsJob -> /data/analytics/token_stats
 Key: model|task_type|dt. Output: llm_calls, sum_prompt, sum_completion, avg_prompt, avg_completion, max_prompt, max_completion, tokens by step_index bucket (0-1, 2-4, 5-9, 10+). Combiner required, with -Duse.combiner flag.

JOB 5, LatencyHistogramJob -> /data/analytics/latency_hist
 Key: model|step_type. Buckets (ms): <100, 100-250, 250-500, 500-1000, 1000-2500, 2500-5000, 5000-10000, >=10000. One row per key per bucket with count, plus min/max/avg/sum. Exact percentiles are computed in Hive.

Also scripts/04_run_jobs.sh chaining Job1 -> Job2 -> Jobs 3-5 sequentially (ONE job at a time) with exit-code checks, cleanup of previous outputs, and per-job timing.
Acceptance check: all jobs succeed on the medium dataset (500K runs) on YARN without container kills; show first 10 lines of each output and the key counters; show Combine input vs output records to prove the combiner reduced shuffle volume.
```

---

## PROMPT 7: Hive DDL

```
STEP 7: Hive warehouse in hive/ddl/. Always `set hive.execution.engine=mr; set mapreduce.job.reduces=2;`
1. 01_create_db.hql: CREATE DATABASE ai_obs.
2. 02_external_tables.hql: EXTERNAL tables with ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' NULL DEFINED AS '\\N' STORED AS TEXTFILE over: steps (/data/processed/steps), runs (/data/processed/runs), tool_stats, token_stats, latency_hist. Column order/types must match the MapReduce outputs exactly.
3. 03_curated_orc.hql: steps_orc PARTITIONED BY (dt STRING) STORED AS ORC (SNAPPY) loaded by INSERT OVERWRITE with dynamic partitioning (nonstrict, adequate max partitions: about 30 days); same for runs_orc. Keep memory low (hive.exec.max.created.files, set mapreduce.map.memory.mb=1024 if needed).
4. 04_views.hql: v_tool_calls, v_llm_calls.
5. scripts/05_load_hive.sh runs DDL in order and prints SELECT COUNT(*) for each table.
6. Comments per table (source job + schema).
Acceptance check: Hive counts match MapReduce counters (STEPS_EMITTED, RUNS_BUILT).
```

---

## PROMPT 8: HiveQL queries

```
STEP 8: hive/queries/ with one .hql per query, each with a header comment stating the question, and scripts/06_run_queries.sh executing all (sequentially, single Hive session at a time) and exporting results to analysis/results/<name>.csv. Use steps_orc/runs_orc.

Queries (descriptive only, no ML):
Q01 Tool failure rate by tool and task_type (with rank).
Q02 Top 5 error types per tool (ROW_NUMBER).
Q03 Retry-loop analysis: % of runs with is_retry_loop by model and tool.
Q04 Token consumption by model: total, avg per run, avg per llm_call.
Q05 Token bottlenecks: top 1 percent of runs by prompt tokens (NTILE or percentile_approx) and their task types.
Q06 Context growth: avg prompt_tokens by step_index.
Q07 Latency percentiles (p50, p90, p95, p99 via percentile_approx) by model and step_type.
Q08 Slowest tools: p95 latency by tool and share of total run time.
Q09 Daily trend: runs, failure rate, total tokens by dt (7-day moving average window).
Q10 Hour-of-day failure heatmap data.
Q11 Run success rate by model x task_type (conditional aggregation).
Q12 Most common failing tool sequences: LAG() to get (previous tool -> failing tool) top 10.
Q13 Cost estimate by model using a small pricing lookup table created in Hive (plain arithmetic).
Q14 Session analysis: sessions with most runs, avg runs per session, sessions where all runs failed.
Q15 Data-quality report: malformed ratio (from counters stored in a small table) and null rate per column.
Include EXPLAIN output and runtime comparison for Q07 on text vs ORC.
Acceptance check: every query runs and produces a CSV; Q01 agrees with ToolStatsJob output.
```

---

## PROMPT 9: Pipeline automation and laptop-safe benchmarks

```
STEP 9: Automation + benchmarks (must fit an 8 GB RAM laptop).
1. scripts/run_pipeline.sh <small|medium|large>: stages = generate -> upload -> build jar -> jobs 1-5 -> hive load -> queries -> charts. Each stage timed, logs in logs/<timestamp>/, stops on error, supports --skip-gen and --from-stage N. Checks free RAM/disk at start and warns.
2. scripts/benchmark.sh writes analysis/results/benchmark.csv with columns: experiment, dataset_runs, input_gb, job, runtime_sec, map_input_records, shuffle_bytes, reducers. Parse counters from the job console output (NO history server).
   Experiments (limited on purpose):
   a) Scaling: 100K and 500K full pipeline; 1M for Jobs 1, 3, 4 only.
   b) Combiner on vs off: Jobs 3 and 4 at 500K.
   c) Reducers 1 vs 2: Job 2 at 500K (no 4-reducer test).
   d) Hive: text vs ORC for Q01 and Q07.
   e) Compression none vs gzip on Job 1 output at 500K.
3. Between experiments, clean up HDFS outputs to save disk, and print elapsed time and estimated remaining time.
Acceptance check: benchmark.csv populated; the whole benchmark finishes without out-of-memory or container kills.
```

---

## PROMPT 10: Charts and report assets

```
STEP 10: analysis/make_charts.py (pandas + matplotlib only; no ML libs).
Read analysis/results/*.csv, save PNGs (dpi 200) to docs/figures/:
- Tool failure rate (bar, sorted)
- Failure rate by tool x task_type (heatmap)
- Token usage by model (stacked bar prompt vs completion)
- Context growth: avg prompt tokens vs step_index (line)
- Latency histogram per model + p50/p95/p99 table
- Daily trend (runs + failure rate, dual axis)
- Benchmark scaling: runtime vs dataset size per job
- Combiner effect: shuffle bytes bar
Also generate docs/results_summary.md with real numbers (top failing tool, most token-heavy model, p95 latency, scaling factor), and docs/architecture.md with a Mermaid diagram: Generator -> HDFS raw -> Job1 / Job2 -> HDFS processed -> Jobs 3-5 -> Hive external + ORC -> HiveQL -> CSV -> charts.
Acceptance check: all figures exist and the summary contains real numbers, no placeholders.
```

---

## PROMPT 11: README, report, final QA

```
STEP 11: Finalize.
1. README.md: overview, architecture diagram, hardware assumptions (Windows + WSL2, 8 GB RAM), prerequisites, quick start (3 commands), directory tree, job descriptions with input/output schemas, how to reproduce benchmarks, limitations.
2. docs/report_outline.md: Introduction, Problem Statement, Objectives, Tools, Architecture, Dataset and Schema, MapReduce Design (key/value diagrams per job), Hive Design, Results and Analysis (figures), Performance Evaluation, Challenges, Limitations (synthetic data, single node, scaled-down sizes due to 8 GB RAM), Future Work, Conclusion. Pre-fill bullets from the real project.
3. Final QA: grep the repo for ML libraries (must find none); shellcheck on scripts; `mvn test` passes; run `run_pipeline.sh small` from a clean clone.
4. docs/viva_qa.md with 20 likely questions and answers (why MapReduce over Spark here, secondary sort, combiner correctness conditions, partitioner vs grouping comparator, why ORC, schema-on-read, data skew, small files problem, YARN fault tolerance, why dataset sizes were scaled down, pseudo-distributed vs real cluster, etc.).
Acceptance check: a fresh clone follows the README and reproduces results.
```

---

## Debug prompt (use any time)

```
Debug mode. Error/log output: <PASTE>. Context: Step <N>. Machine: WSL2, 5 GB RAM limit.
Do NOT rewrite unrelated code. 1) Root cause in 2-3 sentences. 2) Minimal fix with exact file/line changes. 3) A command to verify. 4) If it is memory/ports/permissions, give the config change and say which value to lower or raise.
```

---

## Common pitfalls on this setup

| Pitfall | Fix |
|---|---|
| WSL2 uses too much RAM, Windows freezes | `.wslconfig` with memory=5GB, swap=2GB; run `wsl --shutdown` after editing |
| Everything is very slow | Project is under `/mnt/c`. Move it to `~/` inside WSL |
| `Connection refused` on ssh localhost | `sudo service ssh start` (WSL does not start services on boot) |
| YARN kills containers ("running beyond memory") | Lower map/reduce memory in the table above, keep vmem check off, run one job at a time |
| Job stuck in ACCEPTED state | Not enough YARN memory; check that AM (512 MB) + a container fit in 3072 MB, and no other job is still running |
| Hive 3.1.3 `NoSuchMethodError` (guava) | Replace Hive's `guava-19*.jar` with Hadoop's `guava-27*.jar` |
| Derby "database locked" | Only one Hive session at a time |
| Gson `ClassNotFound` on cluster | Use the shaded jar, or `-libjars` |
| Disk fills up | Delete old HDFS outputs between runs, generate only the sizes you need, keep 20 GB free |
| Many tiny output files | Use fewer reducers; consider `CombineTextInputFormat` for input |
| Hive shows all NULLs | Delimiter or `NULL DEFINED AS '\\N'` mismatch, or column order differs from the MR output |
| Reducer OOM in sessionization | Never buffer steps; stream with secondary sort |