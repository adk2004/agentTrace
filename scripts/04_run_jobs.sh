#!/bin/bash
set -eo pipefail

JAR="mapreduce/target/ai-trace-mr.jar"
if [ ! -f "$JAR" ]; then
    echo "ERROR: JAR not found at $JAR. Run 'mvn clean package -DskipTests' first."
    exit 1
fi

echo "======================================================================"
echo "AI-Trace BigData MapReduce Pipeline: Sequential Execution"
echo "======================================================================"
TOTAL_START=$(date +%s)

run_mr_job() {
    local JOB_NUM=$1
    local JOB_NAME=$2
    local MAIN_CLASS=$3
    local INPUT_PATH=$4
    local OUTPUT_PATH=$5
    local EXTRA_ARGS=$6

    echo "----------------------------------------------------------------------"
    echo "Starting Job ${JOB_NUM}: ${JOB_NAME}"
    echo "Input:  ${INPUT_PATH}"
    echo "Output: ${OUTPUT_PATH}"
    echo "----------------------------------------------------------------------"

    echo "Cleaning up prior output at ${OUTPUT_PATH}..."
    hdfs dfs -rm -r -f "${OUTPUT_PATH}" >/dev/null 2>&1 || true

    local LOG_FILE="/tmp/job_${JOB_NUM}_$(date +%s).log"
    local JOB_START=$(date +%s)

    # Execute Job
    hadoop jar "${JAR}" "${MAIN_CLASS}" ${EXTRA_ARGS} "${INPUT_PATH}" "${OUTPUT_PATH}" 2>&1 | tee "${LOG_FILE}"
    local STATUS=${PIPESTATUS[0]}

    local JOB_END=$(date +%s)
    local JOB_DURATION=$((JOB_END - JOB_START))

    if [ ${STATUS} -ne 0 ]; then
        echo "ERROR: Job ${JOB_NUM} (${JOB_NAME}) failed with exit code ${STATUS} after ${JOB_DURATION}s!"
        echo "Check log at: ${LOG_FILE}"
        exit ${STATUS}
    fi

    echo ">>> Job ${JOB_NUM} (${JOB_NAME}) SUCCEEDED in ${JOB_DURATION}s."
    echo ""
    echo "Key Counters for Job ${JOB_NUM}:"
    grep -E "Map input records|Map output records|Combine input records|Combine output records|Reduce input records|Reduce output records|STEPS_EMITTED|RUNS_BUILT|TOOL_CALLS|LLM_CALLS|RECORDS_PROCESSED" "${LOG_FILE}" | sed 's/^[ \t]*/  /' || true
    echo ""
}

# Cleanup malformed output path for Job 1 if any
hdfs dfs -rm -r -f /data/processed/_malformed >/dev/null 2>&1 || true

# JOB 1: TraceFlattenJob
run_mr_job 1 "TraceFlattenJob" "com.aitrace.job.flatten.TraceFlattenJob" "/data/raw/nested" "/data/processed/steps" ""

# JOB 2: SessionizationJob
run_mr_job 2 "SessionizationJob" "com.aitrace.job.session.SessionizationJob" "/data/raw/flat" "/data/processed/runs" "-Dmapreduce.job.reduces=2"

# JOB 3: ToolStatsJob
run_mr_job 3 "ToolStatsJob" "com.aitrace.job.analytics.tool.ToolStatsJob" "/data/processed/steps" "/data/analytics/tool_stats" "-Duse.combiner=true -Dmapreduce.job.reduces=2"

# JOB 4: TokenStatsJob
run_mr_job 4 "TokenStatsJob" "com.aitrace.job.analytics.token.TokenStatsJob" "/data/processed/steps" "/data/analytics/token_stats" "-Duse.combiner=true -Dmapreduce.job.reduces=2"

# JOB 5: LatencyHistogramJob
run_mr_job 5 "LatencyHistogramJob" "com.aitrace.job.analytics.latency.LatencyHistogramJob" "/data/processed/steps" "/data/analytics/latency_hist" "-Duse.combiner=true -Dmapreduce.job.reduces=2"

TOTAL_END=$(date +%s)
TOTAL_DURATION=$((TOTAL_END - TOTAL_START))

echo "======================================================================"
echo "Pipeline Execution Completed Successfully in ${TOTAL_DURATION}s!"
echo "======================================================================"

echo ""
echo "=== Sample Output: /data/processed/steps (first 10 lines) ==="
hdfs dfs -cat "/data/processed/steps/part-*" | head -n 10 || true

echo ""
echo "=== Sample Output: /data/processed/runs (first 10 lines) ==="
hdfs dfs -cat "/data/processed/runs/part-r-*" | head -n 10 || true

echo ""
echo "=== Sample Output: /data/analytics/tool_stats (first 10 lines) ==="
hdfs dfs -cat "/data/analytics/tool_stats/part-r-*" | head -n 10 || true

echo ""
echo "=== Sample Output: /data/analytics/token_stats (first 10 lines) ==="
hdfs dfs -cat "/data/analytics/token_stats/part-r-*" | head -n 10 || true

echo ""
echo "=== Sample Output: /data/analytics/latency_hist (first 10 lines) ==="
hdfs dfs -cat "/data/analytics/latency_hist/part-r-*" | head -n 10 || true
