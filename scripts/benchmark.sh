#!/bin/bash
# ==============================================================================
# AI-Trace BigData: Benchmark Automation (STEP 9)
# Usage: scripts/benchmark.sh [options]
#
# Generates: analysis/results/benchmark.csv
# Columns:
#   experiment,dataset_runs,input_gb,job,runtime_sec,map_input_records,shuffle_bytes,reducers
#
# Experiments:
#   a) Scaling: 100K and 500K full pipeline (Jobs 1-5); 1M for Jobs 1, 3, 4 only
#   b) Combiner on vs off: Jobs 3 and 4 at 500K
#   c) Reducers 1 vs 2: Job 2 at 500K (no 4-reducer test)
#   d) Hive: text vs ORC for Q01 and Q07
#   e) Compression none vs gzip on Job 1 output at 500K
# ==============================================================================
set -eo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${BASE_DIR}"

OUTPUT_CSV="${BASE_DIR}/analysis/results/benchmark.csv"
TMP_DIR="${BASE_DIR}/logs/benchmark_tmp_$(date +%Y%m%d_%H%M%S)"
mkdir -p "${BASE_DIR}/analysis/results" "${TMP_DIR}"

JAR="mapreduce/target/ai-trace-mr.jar"

# ------------------------------------------------------------------------------
# 1. Parse CLI Arguments
# ------------------------------------------------------------------------------
SCALE_FACTOR=1.0
RUNS_100K=100000
RUNS_500K=500000
RUNS_1M=1000000
EXPERIMENT_FILTER="all"
SKIP_GEN=false
QUICK_MODE=false

print_usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --quick                    Quick mode (uses scaled-down datasets 10K/50K/100K for rapid verification)"
    echo "  --scale <factor>           Scale all dataset run counts by float factor (e.g. 0.1 for 10% data)"
    echo "  --runs-100k <N>            Custom runs for 100K scaling tier (default: 100000)"
    echo "  --runs-500k <N>            Custom runs for 500K scaling tier (default: 500000)"
    echo "  --runs-1m <N>              Custom runs for 1M scaling tier (default: 1000000)"
    echo "  --experiment <name>        Filter experiments: all|scaling|combiner|reducers|hive|compression (default: all)"
    echo "  --skip-gen                 Reuse existing generated data without regenerating"
    echo "  --append                   Append to existing benchmark.csv instead of overwriting"
    echo "  --help, -h                 Show this help message"
}

APPEND_MODE=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --quick)
            QUICK_MODE=true
            SCALE_FACTOR=0.1
            shift
            ;;
        --scale)
            SCALE_FACTOR="$2"
            shift 2
            ;;
        --runs-100k)
            RUNS_100K="$2"
            shift 2
            ;;
        --runs-500k)
            RUNS_500K="$2"
            shift 2
            ;;
        --runs-1m)
            RUNS_1M="$2"
            shift 2
            ;;
        --experiment)
            EXPERIMENT_FILTER="$2"
            shift 2
            ;;
        --skip-gen)
            SKIP_GEN=true
            shift
            ;;
        --append)
            APPEND_MODE=true
            shift
            ;;
        --help|-h)
            print_usage
            exit 0
            ;;
        *)
            echo "ERROR: Unknown option '$1'"
            print_usage
            exit 1
            ;;
    esac
done

# Apply scaling factor if specified
if [ "${QUICK_MODE}" = true ] || [ "$(echo "${SCALE_FACTOR} != 1.0" | bc -l 2>/dev/null || echo 0)" -eq 1 ]; then
    RUNS_100K=$(awk "BEGIN {printf \"%d\", ${RUNS_100K} * ${SCALE_FACTOR}}")
    RUNS_500K=$(awk "BEGIN {printf \"%d\", ${RUNS_500K} * ${SCALE_FACTOR}}")
    RUNS_1M=$(awk "BEGIN {printf \"%d\", ${RUNS_1M} * ${SCALE_FACTOR}}")
fi

# Ensure minimum viable runs for MR jobs
[ "${RUNS_100K}" -lt 100 ] && RUNS_100K=100
[ "${RUNS_500K}" -lt 500 ] && RUNS_500K=500
[ "${RUNS_1M}" -lt 1000 ] && RUNS_1M=1000

# ------------------------------------------------------------------------------
# 2. Pre-flight Checks (JAR, RAM, Disk)
# ------------------------------------------------------------------------------
if [ ! -f "${JAR}" ]; then
    echo "Building MapReduce JAR..."
    mvn clean package -DskipTests -f mapreduce/pom.xml
fi

AVAIL_RAM_MB=$(free -m | awk '/Mem:/ {print $7}')
AVAIL_DISK_GB=$(df -k . | tail -1 | awk '{print int($4 / 1024 / 1024)}')

echo "======================================================================"
echo "AI-Trace BigData: Benchmark Automation Runner"
echo "Output CSV:         ${OUTPUT_CSV}"
echo "Datasets Target:    100K=${RUNS_100K}, 500K=${RUNS_500K}, 1M=${RUNS_1M}"
echo "Experiment Filter:  ${EXPERIMENT_FILTER}"
echo "Available RAM:      ${AVAIL_RAM_MB} MB"
echo "Available Disk:     ${AVAIL_DISK_GB} GB"
echo "======================================================================"

if [ "${AVAIL_RAM_MB}" -lt 1200 ]; then
    echo "[WARNING] System RAM is low (${AVAIL_RAM_MB} MB). Containers may encounter pressure."
fi

# Initialize benchmark.csv header if not appending
if [ "${APPEND_MODE}" = false ] || [ ! -f "${OUTPUT_CSV}" ]; then
    echo "experiment,dataset_runs,input_gb,job,runtime_sec,map_input_records,shuffle_bytes,reducers" > "${OUTPUT_CSV}"
fi

# ------------------------------------------------------------------------------
# 3. Helper Functions: Generation, MR Execution & Counter Parsing
# ------------------------------------------------------------------------------
TOTAL_EXPERIMENTS=0
COMPLETED_EXPERIMENTS=0
BENCHMARK_START=$(date +%s)

ensure_benchmark_dataset() {
    local SIZE_LABEL=$1
    local NUM_RUNS=$2
    local FORMAT=$3
    local HDFS_BASE="/data/benchmark/${SIZE_LABEL}"

    local NESTED_EXISTS=false
    local FLAT_EXISTS=false

    if hdfs dfs -test -e "${HDFS_BASE}/nested" 2>/dev/null; then
        local N_COUNT
        N_COUNT=$( (hdfs dfs -count "${HDFS_BASE}/nested" 2>/dev/null || echo "0 0 0") | awk '{print $2}')
        if [ "${N_COUNT:-0}" -gt 0 ]; then
            NESTED_EXISTS=true
        fi
    fi

    if hdfs dfs -test -e "${HDFS_BASE}/flat" 2>/dev/null; then
        local F_COUNT
        F_COUNT=$( (hdfs dfs -count "${HDFS_BASE}/flat" 2>/dev/null || echo "0 0 0") | awk '{print $2}')
        if [ "${F_COUNT:-0}" -gt 0 ]; then
            FLAT_EXISTS=true
        fi
    fi

    if [ "${SKIP_GEN}" = true ] && [ "${NESTED_EXISTS}" = true ]; then
        echo ">>> Reusing existing HDFS dataset at ${HDFS_BASE} (--skip-gen active)."
        return 0
    fi

    if [ "${FORMAT}" == "nested" ] && [ "${NESTED_EXISTS}" = true ]; then
        echo ">>> Dataset ${SIZE_LABEL} (nested) already exists in HDFS. Skipping generation."
        return 0
    fi

    if [ "${FORMAT}" == "both" ] && [ "${NESTED_EXISTS}" = true ] && [ "${FLAT_EXISTS}" = true ]; then
        echo ">>> Dataset ${SIZE_LABEL} (both) already exists in HDFS. Skipping generation."
        return 0
    fi

    echo "----------------------------------------------------------------------"
    echo "Generating dataset ${SIZE_LABEL}: ${NUM_RUNS} runs (${FORMAT})..."
    echo "----------------------------------------------------------------------"
    local LOCAL_OUT="${BASE_DIR}/data/benchmark_traces/${SIZE_LABEL}"
    rm -rf "${LOCAL_OUT}"
    mkdir -p "${LOCAL_OUT}"

    python3 datagen/generate_traces.py \
        --runs "${NUM_RUNS}" \
        --format "${FORMAT}" \
        --out "${LOCAL_OUT}" \
        --files 8 \
        --seed 42 \
        --malformed-pct 1.0

    hdfs dfs -mkdir -p "${HDFS_BASE}"

    if [ -d "${LOCAL_OUT}/nested" ]; then
        echo "Uploading ${SIZE_LABEL} nested to ${HDFS_BASE}/nested..."
        hdfs dfs -rm -r -f "${HDFS_BASE}/nested" >/dev/null 2>&1 || true
        hdfs dfs -mkdir -p "${HDFS_BASE}/nested"
        hdfs dfs -put -f "${LOCAL_OUT}/nested"/* "${HDFS_BASE}/nested/"
    fi

    if [ -d "${LOCAL_OUT}/flat" ]; then
        echo "Uploading ${SIZE_LABEL} flat to ${HDFS_BASE}/flat..."
        hdfs dfs -rm -r -f "${HDFS_BASE}/flat" >/dev/null 2>&1 || true
        hdfs dfs -mkdir -p "${HDFS_BASE}/flat"
        hdfs dfs -put -f "${LOCAL_OUT}/flat"/* "${HDFS_BASE}/flat/"
    fi

    # Clean local disk staging copy to save disk space
    rm -rf "${LOCAL_OUT}"
    echo ">>> Uploaded ${SIZE_LABEL} to HDFS."
    hdfs dfs -du -h "${HDFS_BASE}"
}

print_progress() {
    COMPLETED_EXPERIMENTS=$((COMPLETED_EXPERIMENTS + 1))
    local NOW=$(date +%s)
    local ELAPSED=$((NOW - BENCHMARK_START))
    local REMAINING="calculating..."
    if [ ${COMPLETED_EXPERIMENTS} -gt 0 ] && [ ${TOTAL_EXPERIMENTS} -gt 0 ]; then
        local AVG_PER_EXP=$((ELAPSED / COMPLETED_EXPERIMENTS))
        local REM_COUNT=$((TOTAL_EXPERIMENTS - COMPLETED_EXPERIMENTS))
        local EST_SEC=$((AVG_PER_EXP * REM_COUNT))
        REMAINING="${EST_SEC}s"
    fi
    echo ">>> Progress: [${COMPLETED_EXPERIMENTS}/${TOTAL_EXPERIMENTS}] | Elapsed: ${ELAPSED}s | Est. Remaining: ${REMAINING}"
    echo ""
}

# Run MapReduce job, parse counters from console output, record to CSV
record_mr_job() {
    local EXP_NAME=$1
    local RUNS_COUNT=$2
    local JOB_NAME=$3
    local MAIN_CLASS=$4
    local INPUT_PATH=$5
    local OUTPUT_PATH=$6
    local NUM_REDUCERS=$7
    local EXTRA_PROPS=$8

    echo "----------------------------------------------------------------------"
    echo "Benchmark Job: ${JOB_NAME} (${EXP_NAME})"
    echo "Input: ${INPUT_PATH} | Output: ${OUTPUT_PATH} | Reducers: ${NUM_REDUCERS}"
    echo "----------------------------------------------------------------------"

    # Compute input size in GB from HDFS (pipefail-safe)
    local INPUT_BYTES
    INPUT_BYTES=$( (hdfs dfs -du -s "${INPUT_PATH}" 2>/dev/null || echo 1) | awk '{print $1}' | tail -1)
    if [ -z "${INPUT_BYTES}" ] || [ "${INPUT_BYTES}" -eq 0 ]; then
        INPUT_BYTES=1
    fi
    local INPUT_GB
    INPUT_GB=$(awk "BEGIN {printf \"%.4f\", ${INPUT_BYTES} / (1024*1024*1024)}")

    # Clean prior output
    hdfs dfs -rm -r -f "${OUTPUT_PATH}" >/dev/null 2>&1 || true

    local LOG_FILE="${TMP_DIR}/${EXP_NAME}_${JOB_NAME}_$(date +%s).log"
    local T_START=$(date +%s)

    # Execute Job with direct console log capture
    hadoop jar "${JAR}" "${MAIN_CLASS}" ${EXTRA_PROPS} "${INPUT_PATH}" "${OUTPUT_PATH}" 2>&1 | tee "${LOG_FILE}"
    local STATUS=${PIPESTATUS[0]}

    local T_END=$(date +%s)
    local RUNTIME_SEC=$((T_END - T_START))

    if [ ${STATUS} -ne 0 ]; then
        echo "ERROR: Benchmark ${EXP_NAME} - ${JOB_NAME} failed with exit code ${STATUS}!"
        exit ${STATUS}
    fi

    # Parse console counters (NO history server, pipefail-safe)
    local MAP_INPUT_RECS
    MAP_INPUT_RECS=$( (grep -oE "Map input records[ =:]+[0-9]+" "${LOG_FILE}" || echo "0") | grep -oE "[0-9]+" | tail -1)
    [ -z "${MAP_INPUT_RECS}" ] && MAP_INPUT_RECS=0

    local SHUFFLE_BYTES
    SHUFFLE_BYTES=$( (grep -oE "Reduce shuffle bytes[ =:]+[0-9]+" "${LOG_FILE}" || echo "0") | grep -oE "[0-9]+" | tail -1)
    if [ -z "${SHUFFLE_BYTES}" ] || [ "${SHUFFLE_BYTES}" -eq 0 ]; then
        SHUFFLE_BYTES=$( (grep -oE "Map output materialized bytes[ =:]+[0-9]+" "${LOG_FILE}" || echo "0") | grep -oE "[0-9]+" | tail -1)
        [ -z "${SHUFFLE_BYTES}" ] && SHUFFLE_BYTES=0
    fi

    # Record row in benchmark.csv
    echo "${EXP_NAME},${RUNS_COUNT},${INPUT_GB},${JOB_NAME},${RUNTIME_SEC},${MAP_INPUT_RECS},${SHUFFLE_BYTES},${NUM_REDUCERS}" >> "${OUTPUT_CSV}"
    echo ">>> Recorded: ${EXP_NAME}, runs=${RUNS_COUNT}, input_gb=${INPUT_GB}, job=${JOB_NAME}, runtime=${RUNTIME_SEC}s, map_in=${MAP_INPUT_RECS}, shuffle=${SHUFFLE_BYTES}, reducers=${NUM_REDUCERS}"

    print_progress
}

# Run Hive query benchmark, parse timing and counters, record to CSV
record_hive_query() {
    local EXP_NAME=$1
    local RUNS_COUNT=$2
    local QUERY_NAME=$3
    local TABLE_NAME=$4
    local QUERY_SQL=$5
    local NUM_REDUCERS=$6

    echo "----------------------------------------------------------------------"
    echo "Benchmark Hive Query: ${QUERY_NAME} (${EXP_NAME})"
    echo "Table: ${TABLE_NAME} | Reducers: ${NUM_REDUCERS}"
    echo "----------------------------------------------------------------------"

    local HIVE_LOG="${TMP_DIR}/${EXP_NAME}_${QUERY_NAME}_$(date +%s).log"
    local T_START=$(date +%s)

    hive -e "
        USE ai_obs;
        SET hive.execution.engine=mr;
        SET mapreduce.job.reduces=${NUM_REDUCERS};
        ${QUERY_SQL}
    " 2>&1 | tee "${HIVE_LOG}"
    local STATUS=${PIPESTATUS[0]}

    local T_END=$(date +%s)
    local RUNTIME_SEC=$((T_END - T_START))

    if [ ${STATUS} -ne 0 ]; then
        echo "ERROR: Hive benchmark ${EXP_NAME} - ${QUERY_NAME} failed!"
        exit ${STATUS}
    fi

    # Input GB for table (pipefail-safe lookup across curated, processed, or warehouse paths)
    local INPUT_BYTES
    INPUT_BYTES=$( (hdfs dfs -du -s "/data/curated/${TABLE_NAME}" 2>/dev/null || \
                    hdfs dfs -du -s "/data/processed/${TABLE_NAME}" 2>/dev/null || \
                    hdfs dfs -du -s "/user/hive/warehouse/ai_obs.db/${TABLE_NAME}" 2>/dev/null || \
                    echo 1) | awk '{print $1}' | tail -1)
    if [ -z "${INPUT_BYTES}" ] || [ "${INPUT_BYTES}" -eq 0 ]; then
        INPUT_BYTES=1
    fi
    local INPUT_GB
    INPUT_GB=$(awk "BEGIN {printf \"%.4f\", ${INPUT_BYTES} / (1024*1024*1024)}")

    # Parse console counters (pipefail-safe)
    local MAP_INPUT_RECS
    MAP_INPUT_RECS=$( (grep -oE "Map input records[ =:]+[0-9]+" "${HIVE_LOG}" || echo "0") | grep -oE "[0-9]+" | tail -1)
    if [ -z "${MAP_INPUT_RECS}" ] || [ "${MAP_INPUT_RECS}" -eq 0 ]; then
        MAP_INPUT_RECS=$( (hive -S -e "SELECT count(*) FROM ai_obs.${TABLE_NAME};" 2>/dev/null || echo 0) | grep -E '^[0-9]+' | tail -1)
    fi
    [ -z "${MAP_INPUT_RECS}" ] && MAP_INPUT_RECS=0

    local SHUFFLE_BYTES
    SHUFFLE_BYTES=$( (grep -oE "Reduce shuffle bytes[ =:]+[0-9]+" "${HIVE_LOG}" || echo "0") | grep -oE "[0-9]+" | tail -1)
    [ -z "${SHUFFLE_BYTES}" ] && SHUFFLE_BYTES=0

    # Record row in benchmark.csv
    echo "${EXP_NAME},${RUNS_COUNT},${INPUT_GB},${QUERY_NAME},${RUNTIME_SEC},${MAP_INPUT_RECS},${SHUFFLE_BYTES},${NUM_REDUCERS}" >> "${OUTPUT_CSV}"
    echo ">>> Recorded: ${EXP_NAME}, runs=${RUNS_COUNT}, input_gb=${INPUT_GB}, job=${QUERY_NAME}, runtime=${RUNTIME_SEC}s, map_in=${MAP_INPUT_RECS}, shuffle=${SHUFFLE_BYTES}, reducers=${NUM_REDUCERS}"

    print_progress
}

# ------------------------------------------------------------------------------
# 4. Count Planned Experiments
# ------------------------------------------------------------------------------
TOTAL_EXPERIMENTS=0
if [ "${EXPERIMENT_FILTER}" == "all" ] || [ "${EXPERIMENT_FILTER}" == "scaling" ]; then
    TOTAL_EXPERIMENTS=$((TOTAL_EXPERIMENTS + 13)) # 5 (100k) + 5 (500k) + 3 (1m)
fi
if [ "${EXPERIMENT_FILTER}" == "all" ] || [ "${EXPERIMENT_FILTER}" == "combiner" ]; then
    TOTAL_EXPERIMENTS=$((TOTAL_EXPERIMENTS + 4))  # Job 3 on/off + Job 4 on/off
fi
if [ "${EXPERIMENT_FILTER}" == "all" ] || [ "${EXPERIMENT_FILTER}" == "reducers" ]; then
    TOTAL_EXPERIMENTS=$((TOTAL_EXPERIMENTS + 2))  # Job 2 reducers=1 vs 2
fi
if [ "${EXPERIMENT_FILTER}" == "all" ] || [ "${EXPERIMENT_FILTER}" == "hive" ]; then
    TOTAL_EXPERIMENTS=$((TOTAL_EXPERIMENTS + 4))  # Q01 text/orc + Q07 text/orc
fi
if [ "${EXPERIMENT_FILTER}" == "all" ] || [ "${EXPERIMENT_FILTER}" == "compression" ]; then
    TOTAL_EXPERIMENTS=$((TOTAL_EXPERIMENTS + 2))  # Job 1 none vs gzip
fi

echo "Total planned benchmark runs: ${TOTAL_EXPERIMENTS}"
echo ""

# ------------------------------------------------------------------------------
# 5. Execute Experiments
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# (a) Scaling Experiments: 100K & 500K full pipeline; 1M for Jobs 1, 3, 4 only
# ------------------------------------------------------------------------------
if [ "${EXPERIMENT_FILTER}" == "all" ] || [ "${EXPERIMENT_FILTER}" == "scaling" ]; then
    echo "======================================================================"
    echo "EXPERIMENT (a): SCALING BENCHMARK"
    echo "======================================================================"

    # --- Tier 1: 100K Full Pipeline ---
    ensure_benchmark_dataset "100k" "${RUNS_100K}" "both"

    record_mr_job "scaling_100k" "${RUNS_100K}" "TraceFlattenJob" \
        "com.aitrace.job.flatten.TraceFlattenJob" \
        "/data/benchmark/100k/nested" "/data/benchmark/100k/steps" 0 ""

    record_mr_job "scaling_100k" "${RUNS_100K}" "SessionizationJob" \
        "com.aitrace.job.session.SessionizationJob" \
        "/data/benchmark/100k/flat" "/data/benchmark/100k/runs" 2 "-Dmapreduce.job.reduces=2"

    record_mr_job "scaling_100k" "${RUNS_100K}" "ToolStatsJob" \
        "com.aitrace.job.analytics.tool.ToolStatsJob" \
        "/data/benchmark/100k/steps" "/data/benchmark/100k/tool_stats" 2 "-Duse.combiner=true -Dmapreduce.job.reduces=2"

    record_mr_job "scaling_100k" "${RUNS_100K}" "TokenStatsJob" \
        "com.aitrace.job.analytics.token.TokenStatsJob" \
        "/data/benchmark/100k/steps" "/data/benchmark/100k/token_stats" 2 "-Duse.combiner=true -Dmapreduce.job.reduces=2"

    record_mr_job "scaling_100k" "${RUNS_100K}" "LatencyHistogramJob" \
        "com.aitrace.job.analytics.latency.LatencyHistogramJob" \
        "/data/benchmark/100k/steps" "/data/benchmark/100k/latency_hist" 2 "-Duse.combiner=true -Dmapreduce.job.reduces=2"

    # Cleanup intermediate 100K outputs to preserve disk
    hdfs dfs -rm -r -f /data/benchmark/100k/runs /data/benchmark/100k/tool_stats /data/benchmark/100k/token_stats /data/benchmark/100k/latency_hist >/dev/null 2>&1 || true

    # --- Tier 2: 500K Full Pipeline ---
    ensure_benchmark_dataset "500k" "${RUNS_500K}" "both"

    record_mr_job "scaling_500k" "${RUNS_500K}" "TraceFlattenJob" \
        "com.aitrace.job.flatten.TraceFlattenJob" \
        "/data/benchmark/500k/nested" "/data/benchmark/500k/steps" 0 ""

    record_mr_job "scaling_500k" "${RUNS_500K}" "SessionizationJob" \
        "com.aitrace.job.session.SessionizationJob" \
        "/data/benchmark/500k/flat" "/data/benchmark/500k/runs" 2 "-Dmapreduce.job.reduces=2"

    record_mr_job "scaling_500k" "${RUNS_500K}" "ToolStatsJob" \
        "com.aitrace.job.analytics.tool.ToolStatsJob" \
        "/data/benchmark/500k/steps" "/data/benchmark/500k/tool_stats" 2 "-Duse.combiner=true -Dmapreduce.job.reduces=2"

    record_mr_job "scaling_500k" "${RUNS_500K}" "TokenStatsJob" \
        "com.aitrace.job.analytics.token.TokenStatsJob" \
        "/data/benchmark/500k/steps" "/data/benchmark/500k/token_stats" 2 "-Duse.combiner=true -Dmapreduce.job.reduces=2"

    record_mr_job "scaling_500k" "${RUNS_500K}" "LatencyHistogramJob" \
        "com.aitrace.job.analytics.latency.LatencyHistogramJob" \
        "/data/benchmark/500k/steps" "/data/benchmark/500k/latency_hist" 2 "-Duse.combiner=true -Dmapreduce.job.reduces=2"

    # Retain /data/benchmark/500k/steps and flat for subsequent combiner/reducer/compression experiments

    # --- Tier 3: 1M Scaling (Jobs 1, 3, 4 only) ---
    ensure_benchmark_dataset "1m" "${RUNS_1M}" "nested"

    record_mr_job "scaling_1m" "${RUNS_1M}" "TraceFlattenJob" \
        "com.aitrace.job.flatten.TraceFlattenJob" \
        "/data/benchmark/1m/nested" "/data/benchmark/1m/steps" 0 ""

    record_mr_job "scaling_1m" "${RUNS_1M}" "ToolStatsJob" \
        "com.aitrace.job.analytics.tool.ToolStatsJob" \
        "/data/benchmark/1m/steps" "/data/benchmark/1m/tool_stats" 2 "-Duse.combiner=true -Dmapreduce.job.reduces=2"

    record_mr_job "scaling_1m" "${RUNS_1M}" "TokenStatsJob" \
        "com.aitrace.job.analytics.token.TokenStatsJob" \
        "/data/benchmark/1m/steps" "/data/benchmark/1m/token_stats" 2 "-Duse.combiner=true -Dmapreduce.job.reduces=2"

    # Cleanup 1M outputs to preserve disk
    hdfs dfs -rm -r -f /data/benchmark/1m/tool_stats /data/benchmark/1m/token_stats /data/benchmark/1m/steps >/dev/null 2>&1 || true
fi

# ------------------------------------------------------------------------------
# (b) Combiner on vs off: Jobs 3 and 4 at 500K
# ------------------------------------------------------------------------------
if [ "${EXPERIMENT_FILTER}" == "all" ] || [ "${EXPERIMENT_FILTER}" == "combiner" ]; then
    echo "======================================================================"
    echo "EXPERIMENT (b): COMBINER ON VS OFF (Jobs 3 & 4 at 500K)"
    echo "======================================================================"
    ensure_benchmark_dataset "500k" "${RUNS_500K}" "both"
    if ! hdfs dfs -test -e "/data/benchmark/500k/steps" 2>/dev/null; then
        echo "Generating 500k steps input for combiner test..."
        record_mr_job "prep_500k_steps" "${RUNS_500K}" "TraceFlattenJob" \
            "com.aitrace.job.flatten.TraceFlattenJob" \
            "/data/benchmark/500k/nested" "/data/benchmark/500k/steps" 0 ""
    fi

    # Job 3 Combiner ON
    record_mr_job "combiner_on" "${RUNS_500K}" "ToolStatsJob" \
        "com.aitrace.job.analytics.tool.ToolStatsJob" \
        "/data/benchmark/500k/steps" "/data/benchmark/combiner_on/tool_stats" 2 "-Duse.combiner=true -Dmapreduce.job.reduces=2"

    # Job 3 Combiner OFF
    record_mr_job "combiner_off" "${RUNS_500K}" "ToolStatsJob" \
        "com.aitrace.job.analytics.tool.ToolStatsJob" \
        "/data/benchmark/500k/steps" "/data/benchmark/combiner_off/tool_stats" 2 "-Duse.combiner=false -Dmapreduce.job.reduces=2"

    # Job 4 Combiner ON
    record_mr_job "combiner_on" "${RUNS_500K}" "TokenStatsJob" \
        "com.aitrace.job.analytics.token.TokenStatsJob" \
        "/data/benchmark/500k/steps" "/data/benchmark/combiner_on/token_stats" 2 "-Duse.combiner=true -Dmapreduce.job.reduces=2"

    # Job 4 Combiner OFF
    record_mr_job "combiner_off" "${RUNS_500K}" "TokenStatsJob" \
        "com.aitrace.job.analytics.token.TokenStatsJob" \
        "/data/benchmark/500k/steps" "/data/benchmark/combiner_off/token_stats" 2 "-Duse.combiner=false -Dmapreduce.job.reduces=2"

    # Clean intermediate combiner experiment outputs
    hdfs dfs -rm -r -f /data/benchmark/combiner_on /data/benchmark/combiner_off >/dev/null 2>&1 || true
fi

# ------------------------------------------------------------------------------
# (c) Reducers 1 vs 2: Job 2 at 500K (no 4-reducer test)
# ------------------------------------------------------------------------------
if [ "${EXPERIMENT_FILTER}" == "all" ] || [ "${EXPERIMENT_FILTER}" == "reducers" ]; then
    echo "======================================================================"
    echo "EXPERIMENT (c): REDUCERS 1 VS 2 (Job 2 at 500K)"
    echo "======================================================================"
    ensure_benchmark_dataset "500k" "${RUNS_500K}" "both"

    # Job 2 with 1 Reducer
    record_mr_job "reducers_1" "${RUNS_500K}" "SessionizationJob" \
        "com.aitrace.job.session.SessionizationJob" \
        "/data/benchmark/500k/flat" "/data/benchmark/reducers_1/runs" 1 "-Dmapreduce.job.reduces=1"

    # Job 2 with 2 Reducers
    record_mr_job "reducers_2" "${RUNS_500K}" "SessionizationJob" \
        "com.aitrace.job.session.SessionizationJob" \
        "/data/benchmark/500k/flat" "/data/benchmark/reducers_2/runs" 2 "-Dmapreduce.job.reduces=2"

    # Clean intermediate reducer experiment outputs
    hdfs dfs -rm -r -f /data/benchmark/reducers_1 /data/benchmark/reducers_2 >/dev/null 2>&1 || true
fi

# ------------------------------------------------------------------------------
# (d) Hive: Text vs ORC for Q01 and Q07
# ------------------------------------------------------------------------------
if [ "${EXPERIMENT_FILTER}" == "all" ] || [ "${EXPERIMENT_FILTER}" == "hive" ]; then
    echo "======================================================================"
    echo "EXPERIMENT (d): HIVE TEXT VS ORC (Q01 and Q07)"
    echo "======================================================================"

    Q01_TEXT_SQL="
        SELECT tool_name, task_type, count(*) AS total,
               sum(case when lower(tool_status) in ('error','timeout') then 1 else 0 end) AS failures,
               round(cast(sum(case when lower(tool_status) in ('error','timeout') then 1 else 0 end) as double) / count(*), 4) AS fail_rate
        FROM steps
        WHERE tool_name is not null
        GROUP BY tool_name, task_type;
    "

    Q01_ORC_SQL="
        SELECT tool_name, task_type, count(*) AS total,
               sum(case when lower(tool_status) in ('error','timeout') then 1 else 0 end) AS failures,
               round(cast(sum(case when lower(tool_status) in ('error','timeout') then 1 else 0 end) as double) / count(*), 4) AS fail_rate
        FROM steps_orc
        WHERE tool_name is not null
        GROUP BY tool_name, task_type;
    "

    Q07_TEXT_SQL="
        SELECT model, step_type, count(*),
               round(percentile_approx(latency_ms, 0.50), 2) AS p50,
               round(percentile_approx(latency_ms, 0.95), 2) AS p95,
               round(avg(latency_ms), 2) AS avg_lat
        FROM steps
        WHERE latency_ms is not null and latency_ms >= 0
        GROUP BY model, step_type;
    "

    Q07_ORC_SQL="
        SELECT model, step_type, count(*),
               round(percentile_approx(latency_ms, 0.50), 2) AS p50,
               round(percentile_approx(latency_ms, 0.95), 2) AS p95,
               round(avg(latency_ms), 2) AS avg_lat
        FROM steps_orc
        WHERE latency_ms is not null and latency_ms >= 0
        GROUP BY model, step_type;
    "

    record_hive_query "hive_text" "${RUNS_500K}" "Hive_Q01" "steps" "${Q01_TEXT_SQL}" 2
    record_hive_query "hive_orc"  "${RUNS_500K}" "Hive_Q01" "steps_orc" "${Q01_ORC_SQL}" 2
    record_hive_query "hive_text" "${RUNS_500K}" "Hive_Q07" "steps" "${Q07_TEXT_SQL}" 2
    record_hive_query "hive_orc"  "${RUNS_500K}" "Hive_Q07" "steps_orc" "${Q07_ORC_SQL}" 2
fi

# ------------------------------------------------------------------------------
# (e) Compression none vs gzip on Job 1 output at 500K
# ------------------------------------------------------------------------------
if [ "${EXPERIMENT_FILTER}" == "all" ] || [ "${EXPERIMENT_FILTER}" == "compression" ]; then
    echo "======================================================================"
    echo "EXPERIMENT (e): COMPRESSION NONE VS GZIP (Job 1 at 500K)"
    echo "======================================================================"
    ensure_benchmark_dataset "500k" "${RUNS_500K}" "nested"

    # Compression: NONE
    record_mr_job "compression_none" "${RUNS_500K}" "TraceFlattenJob" \
        "com.aitrace.job.flatten.TraceFlattenJob" \
        "/data/benchmark/500k/nested" "/data/benchmark/compression/none" 0 \
        "-Dmapreduce.output.fileoutputformat.compress=false"

    # Compression: GZIP
    record_mr_job "compression_gzip" "${RUNS_500K}" "TraceFlattenJob" \
        "com.aitrace.job.flatten.TraceFlattenJob" \
        "/data/benchmark/500k/nested" "/data/benchmark/compression/gzip" 0 \
        "-Dmapreduce.output.fileoutputformat.compress=true -Dmapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec"

    # Clean intermediate compression experiment outputs
    hdfs dfs -rm -r -f /data/benchmark/compression >/dev/null 2>&1 || true
fi

# Clean temporary benchmark staging outputs
hdfs dfs -rm -r -f /data/benchmark/500k/steps /data/benchmark/500k/runs >/dev/null 2>&1 || true
rm -rf "${TMP_DIR}"

# ------------------------------------------------------------------------------
# 6. Final Reporting & Verification
# ------------------------------------------------------------------------------
BENCHMARK_END=$(date +%s)
TOTAL_RUN_TIME=$((BENCHMARK_END - BENCHMARK_START))

echo "======================================================================"
echo "AI-Trace BigData Benchmark Completed in ${TOTAL_RUN_TIME}s!"
echo "Results written to: ${OUTPUT_CSV}"
echo "======================================================================"
echo ""
cat "${OUTPUT_CSV}"
echo ""

# Acceptance check: benchmark.csv must exist and have at least 1 header + 1 row
ROW_COUNT=$(wc -l < "${OUTPUT_CSV}")
if [ "${ROW_COUNT}" -ge 2 ]; then
    echo ">>> ACCEPTANCE CHECK PASSED: ${OUTPUT_CSV} populated with ${ROW_COUNT} rows."
else
    echo ">>> ACCEPTANCE CHECK FAILED: ${OUTPUT_CSV} has insufficient rows (${ROW_COUNT})."
    exit 1
fi
