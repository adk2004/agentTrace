#!/bin/bash
set -eo pipefail

echo "======================================================================"
echo "AI-Trace BigData: Hive DDL & Table Loading Pipeline"
echo "======================================================================"

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${BASE_DIR}"

TOTAL_START=$(date +%s)

# Helper function to execute Hive script
run_hive_script() {
    local STAGE_NAME=$1
    local HQL_FILE=$2
    echo "----------------------------------------------------------------------"
    echo "Executing ${STAGE_NAME}: ${HQL_FILE}"
    echo "----------------------------------------------------------------------"
    local START_T=$(date +%s)
    hive -f "${HQL_FILE}"
    local END_T=$(date +%s)
    echo ">>> ${STAGE_NAME} finished in $((END_T - START_T))s."
    echo ""
}

# 1. Create Database
run_hive_script "Stage 1: Create Database" "hive/ddl/01_create_db.hql"

# 2. External Tables over MR outputs
run_hive_script "Stage 2: External Tables" "hive/ddl/02_external_tables.hql"

# 3. Curated ORC Tables with Dynamic Partitioning
run_hive_script "Stage 3: Curated ORC Tables" "hive/ddl/03_curated_orc.hql"

# 4. Analytical Views
run_hive_script "Stage 4: Analytical Views" "hive/ddl/04_views.hql"

TOTAL_END=$(date +%s)
echo "======================================================================"
echo "Hive Warehouse Loading Completed in $((TOTAL_END - TOTAL_START))s!"
echo "======================================================================"

echo ""
echo "======================================================================"
echo "Verifying Table & View Row Counts in ai_obs:"
echo "======================================================================"

COUNT_LOG=$(mktemp)
hive -e "
USE ai_obs;
SELECT 'steps' AS table_name, count(*) AS row_count FROM steps
UNION ALL
SELECT 'runs' AS table_name, count(*) AS row_count FROM runs
UNION ALL
SELECT 'tool_stats' AS table_name, count(*) AS row_count FROM tool_stats
UNION ALL
SELECT 'token_stats' AS table_name, count(*) AS row_count FROM token_stats
UNION ALL
SELECT 'latency_hist' AS table_name, count(*) AS row_count FROM latency_hist
UNION ALL
SELECT 'steps_orc' AS table_name, count(*) AS row_count FROM steps_orc
UNION ALL
SELECT 'runs_orc' AS table_name, count(*) AS row_count FROM runs_orc
UNION ALL
SELECT 'v_tool_calls' AS table_name, count(*) AS row_count FROM v_tool_calls
UNION ALL
SELECT 'v_llm_calls' AS table_name, count(*) AS row_count FROM v_llm_calls;
" 2>&1 | tee "${COUNT_LOG}"

# Extract counts
HIVE_STEPS=$(awk '$1 == "steps" {print $2}' "${COUNT_LOG}" | tail -n 1)
HIVE_RUNS=$(awk '$1 == "runs" {print $2}' "${COUNT_LOG}" | tail -n 1)
HIVE_STEPS_ORC=$(awk '$1 == "steps_orc" {print $2}' "${COUNT_LOG}" | tail -n 1)
HIVE_RUNS_ORC=$(awk '$1 == "runs_orc" {print $2}' "${COUNT_LOG}" | tail -n 1)
rm -f "${COUNT_LOG}"

# MapReduce Counters from HDFS outputs
MR_STEPS_EMITTED=$(hdfs dfs -cat "/data/processed/steps/part-m-*" | wc -l)
MR_RUNS_BUILT=$(hdfs dfs -cat "/data/processed/runs/part-r-*" | wc -l)

echo ""
echo "======================================================================"
echo "ACCEPTANCE CHECK VERIFICATION:"
echo "----------------------------------------------------------------------"
printf "  %-24s: %s\n" "MapReduce STEPS_EMITTED" "${MR_STEPS_EMITTED}"
printf "  %-24s: %s\n" "Hive steps count" "${HIVE_STEPS}"
printf "  %-24s: %s\n" "Hive steps_orc count" "${HIVE_STEPS_ORC}"
echo "----------------------------------------------------------------------"
printf "  %-24s: %s\n" "MapReduce RUNS_BUILT" "${MR_RUNS_BUILT}"
printf "  %-24s: %s\n" "Hive runs count" "${HIVE_RUNS}"
printf "  %-24s: %s\n" "Hive runs_orc count" "${HIVE_RUNS_ORC}"
echo "======================================================================"

CHECK_FAILED=0
if [ "${HIVE_STEPS}" != "${MR_STEPS_EMITTED}" ]; then
    echo "ERROR: steps count (${HIVE_STEPS}) does not match MR STEPS_EMITTED (${MR_STEPS_EMITTED})!"
    CHECK_FAILED=1
fi
if [ "${HIVE_STEPS_ORC}" != "${MR_STEPS_EMITTED}" ]; then
    echo "ERROR: steps_orc count (${HIVE_STEPS_ORC}) does not match MR STEPS_EMITTED (${MR_STEPS_EMITTED})!"
    CHECK_FAILED=1
fi
if [ "${HIVE_RUNS}" != "${MR_RUNS_BUILT}" ]; then
    echo "ERROR: runs count (${HIVE_RUNS}) does not match MR RUNS_BUILT (${MR_RUNS_BUILT})!"
    CHECK_FAILED=1
fi
if [ "${HIVE_RUNS_ORC}" != "${MR_RUNS_BUILT}" ]; then
    echo "ERROR: runs_orc count (${HIVE_RUNS_ORC}) does not match MR RUNS_BUILT (${MR_RUNS_BUILT})!"
    CHECK_FAILED=1
fi

if [ ${CHECK_FAILED} -eq 0 ]; then
    echo ">>> SUCCESS: All Hive table counts match MapReduce counters exactly!"
else
    echo ">>> FAILURE: Acceptance check failed."
    exit 1
fi

