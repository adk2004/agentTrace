#!/bin/bash
# ==============================================================================
# AI-Trace BigData: End-to-End Automated Pipeline (STEP 9)
# Usage: scripts/run_pipeline.sh <small|medium|large> [--skip-gen] [--from-stage N] [--both]
#
# Stages:
#   1. generate  - Generate synthetic trace dataset
#   2. upload    - Ingest raw dataset into HDFS (/data/raw/)
#   3. build_jar - Compile MapReduce Java codebase (mvn clean package)
#   4. jobs      - Execute MapReduce Jobs 1 to 5 sequentially
#   5. hive_load - Load Hive external/ORC tables and analytical views
#   6. queries   - Run 15 analytical HiveQL queries and acceptance checks
#   7. charts    - Generate visualization charts and markdown report
# ==============================================================================
set -eo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${BASE_DIR}"

# ------------------------------------------------------------------------------
# 1. Parse Arguments & Configuration
# ------------------------------------------------------------------------------
SIZE_CLASS=""
SKIP_GEN=false
FROM_STAGE=1
BOTH_FLAG=""

print_usage() {
    echo "Usage: $0 <small|medium|large> [options]"
    echo ""
    echo "Arguments:"
    echo "  <small|medium|large>   Dataset size class (small=100K, medium=500K, large=1M runs)"
    echo ""
    echo "Options:"
    echo "  --skip-gen             Skip stage 1 (data generation), reuse existing traces"
    echo "  --from-stage N         Resume execution starting from stage N (1 to 7)"
    echo "  --both                 Generate both nested and flat traces for 'large' size"
    echo "  --help, -h             Show this help message"
    echo ""
    echo "Stages:"
    echo "  1: generate  - Generate synthetic traces (datagen/generate_traces.py)"
    echo "  2: upload    - Upload raw traces to HDFS (/data/raw/)"
    echo "  3: build_jar - Build MapReduce shaded JAR with Maven"
    echo "  4: jobs      - Execute MapReduce Jobs 1-5"
    echo "  5: hive_load - Ingest into Hive warehouse & build curated ORC tables"
    echo "  6: queries   - Execute 15 HiveQL analytical queries"
    echo "  7: charts    - Render figures & summary report (analysis/make_charts.py)"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        small|medium|large)
            SIZE_CLASS="$1"
            shift
            ;;
        --skip-gen)
            SKIP_GEN=true
            shift
            ;;
        --from-stage)
            FROM_STAGE="$2"
            shift 2
            ;;
        --both)
            BOTH_FLAG="--both"
            shift
            ;;
        --help|-h)
            print_usage
            exit 0
            ;;
        *)
            echo "ERROR: Unknown option or size class '$1'"
            print_usage
            exit 1
            ;;
    esac
done

if [ -z "${SIZE_CLASS}" ]; then
    echo "ERROR: Missing required size class: <small|medium|large>"
    print_usage
    exit 1
fi

if ! [[ "${FROM_STAGE}" =~ ^[1-7]$ ]]; then
    echo "ERROR: --from-stage must be an integer between 1 and 7 (got '${FROM_STAGE}')"
    exit 1
fi

# Size class mapping
RUNS=100000
FMT="both"
EST_DISK_GB=10

if [ "${SIZE_CLASS}" == "small" ]; then
    RUNS=100000
    FMT="both"
    EST_DISK_GB=10
elif [ "${SIZE_CLASS}" == "medium" ]; then
    RUNS=500000
    FMT="both"
    EST_DISK_GB=35
elif [ "${SIZE_CLASS}" == "large" ]; then
    RUNS=1000000
    FMT="nested"
    if [ "${BOTH_FLAG}" == "--both" ]; then
        FMT="both"
    fi
    EST_DISK_GB=70
fi

# ------------------------------------------------------------------------------
# 2. System Resource Checks (RAM & Disk)
# ------------------------------------------------------------------------------
echo "======================================================================"
echo "AI-Trace BigData: Automated Pipeline Runner"
echo "Size Class: ${SIZE_CLASS} (${RUNS} runs, format: ${FMT})"
echo "From Stage: ${FROM_STAGE} | Skip Generation: ${SKIP_GEN}"
echo "======================================================================"

# Memory Check
AVAIL_RAM_MB=$(free -m | awk '/Mem:/ {print $7}')
TOTAL_RAM_MB=$(free -m | awk '/Mem:/ {print $2}')
echo "System RAM: ${AVAIL_RAM_MB} MB available / ${TOTAL_RAM_MB} MB total"

if [ "${AVAIL_RAM_MB}" -lt 1536 ]; then
    echo ">>> [WARNING] Available memory is low (${AVAIL_RAM_MB} MB). Recommended >= 2048 MB."
    echo ">>> Ensure background processes are minimized before proceeding."
fi

# Disk Space Check
AVAIL_DISK_GB=$(df -k . | tail -1 | awk '{print int($4 / 1024 / 1024)}')
echo "Disk Space: ${AVAIL_DISK_GB} GB available on project filesystem"

if [ "${AVAIL_DISK_GB}" -lt "${EST_DISK_GB}" ]; then
    echo ">>> [WARNING] Low disk space (${AVAIL_DISK_GB} GB available). Recommended >= ${EST_DISK_GB} GB for ${SIZE_CLASS} dataset."
    echo ">>> Please monitor disk usage during execution."
fi

# ------------------------------------------------------------------------------
# 3. Setup Logging & Timings
# ------------------------------------------------------------------------------
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_DIR="${BASE_DIR}/logs/pipeline_${TIMESTAMP}"
mkdir -p "${LOG_DIR}"

echo "Pipeline Logs Directory: ${LOG_DIR}"
echo ""

declare -A STAGE_STATUS
declare -A STAGE_DURATION
PIPELINE_START=$(date +%s)

run_stage() {
    local STAGE_NUM=$1
    local STAGE_ID=$2
    local STAGE_TITLE=$3
    local STAGE_CMD=$4
    local LOG_FILE="${LOG_DIR}/stage${STAGE_NUM}_${STAGE_ID}.log"

    echo "======================================================================"
    echo "Stage ${STAGE_NUM}/7: ${STAGE_TITLE}"
    echo "Log: ${LOG_FILE}"
    echo "Started at: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "======================================================================"

    local S_START=$(date +%s)
    
    # Execute stage command with stdout and stderr captured into log and console
    if eval "${STAGE_CMD}" 2>&1 | tee "${LOG_FILE}"; then
        local S_END=$(date +%s)
        local DURATION=$((S_END - S_START))
        STAGE_STATUS[${STAGE_NUM}]="SUCCESS"
        STAGE_DURATION[${STAGE_NUM}]=${DURATION}
        echo ""
        echo ">>> Stage ${STAGE_NUM} (${STAGE_TITLE}) COMPLETED in ${DURATION}s."
        echo ""
    else
        local S_END=$(date +%s)
        local DURATION=$((S_END - S_START))
        STAGE_STATUS[${STAGE_NUM}]="FAILED"
        STAGE_DURATION[${STAGE_NUM}]=${DURATION}
        echo ""
        echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        echo "ERROR: Stage ${STAGE_NUM} (${STAGE_TITLE}) FAILED after ${DURATION}s!"
        echo "Check error log at: ${LOG_FILE}"
        echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        exit 1
    fi
}

# ------------------------------------------------------------------------------
# 4. Pipeline Execution
# ------------------------------------------------------------------------------

# STAGE 1: Data Generation
if [ ${FROM_STAGE} -le 1 ]; then
    if [ "${SKIP_GEN}" = true ]; then
        echo ">>> Stage 1: Skipping data generation as requested (--skip-gen)."
        STAGE_STATUS[1]="SKIPPED"
        STAGE_DURATION[1]=0
    else
        GEN_CMD="python3 datagen/generate_traces.py --runs ${RUNS} --format ${FMT} --out data/traces --files 8 --seed 42 --malformed-pct 1.0"
        run_stage 1 "generate" "Generate Synthetic Traces" "${GEN_CMD}"
    fi
else
    STAGE_STATUS[1]="SKIPPED"
    STAGE_DURATION[1]=0
fi

# STAGE 2: HDFS Upload
if [ ${FROM_STAGE} -le 2 ]; then
    UPLOAD_CMD="
        hdfs dfs -mkdir -p /data/raw/nested /data/raw/flat &&
        hdfs dfs -rm -r -f /data/raw/nested/* /data/raw/flat/* || true &&
        if [ -d data/traces/nested ] && [ \"\$(ls -A data/traces/nested 2>/dev/null)\" ]; then
            echo 'Uploading nested traces to /data/raw/nested/...'
            hdfs dfs -put -f data/traces/nested/* /data/raw/nested/
        fi &&
        if [ -d data/traces/flat ] && [ \"\$(ls -A data/traces/flat 2>/dev/null)\" ]; then
            echo 'Uploading flat traces to /data/raw/flat/...'
            hdfs dfs -put -f data/traces/flat/* /data/raw/flat/
        fi &&
        echo 'Current raw datasets in HDFS:' &&
        hdfs dfs -du -h /data/raw
    "
    run_stage 2 "upload" "Upload Raw Traces to HDFS" "${UPLOAD_CMD}"
else
    STAGE_STATUS[2]="SKIPPED"
    STAGE_DURATION[2]=0
fi

# STAGE 3: Build JAR
if [ ${FROM_STAGE} -le 3 ]; then
    BUILD_CMD="mvn clean package -DskipTests -f mapreduce/pom.xml"
    run_stage 3 "build_jar" "Compile MapReduce Java Codebase" "${BUILD_CMD}"
else
    STAGE_STATUS[3]="SKIPPED"
    STAGE_DURATION[3]=0
fi

# STAGE 4: MapReduce Jobs 1 to 5
if [ ${FROM_STAGE} -le 4 ]; then
    run_stage 4 "jobs" "Execute MapReduce Jobs 1 to 5" "bash scripts/04_run_jobs.sh"
else
    STAGE_STATUS[4]="SKIPPED"
    STAGE_DURATION[4]=0
fi

# STAGE 5: Hive Loading
if [ ${FROM_STAGE} -le 5 ]; then
    run_stage 5 "hive_load" "Load Hive Tables & Curated ORC" "bash scripts/05_load_hive.sh"
else
    STAGE_STATUS[5]="SKIPPED"
    STAGE_DURATION[5]=0
fi

# STAGE 6: Analytical Queries
if [ ${FROM_STAGE} -le 6 ]; then
    run_stage 6 "queries" "Run 15 Analytical HiveQL Queries" "bash scripts/06_run_queries.sh"
else
    STAGE_STATUS[6]="SKIPPED"
    STAGE_DURATION[6]=0
fi

# STAGE 7: Charts & Reporting
if [ ${FROM_STAGE} -le 7 ]; then
    CHARTS_CMD="
        if [ -f analysis/make_charts.py ]; then
            python3 analysis/make_charts.py
        else
            echo '[INFO] analysis/make_charts.py not found (scheduled for Step 10). Skipping charts.'
        fi
    "
    run_stage 7 "charts" "Generate Visualizations & Report Assets" "${CHARTS_CMD}"
else
    STAGE_STATUS[7]="SKIPPED"
    STAGE_DURATION[7]=0
fi

# ------------------------------------------------------------------------------
# 5. Pipeline Summary Report
# ------------------------------------------------------------------------------
PIPELINE_END=$(date +%s)
TOTAL_ELAPSED=$((PIPELINE_END - PIPELINE_START))

echo "======================================================================"
echo "AI-Trace BigData Pipeline Execution Completed Successfully!"
echo "Total Elapsed Time: ${TOTAL_ELAPSED}s"
echo "======================================================================"
printf "%-10s %-35s %-12s %-12s\n" "Stage" "Description" "Status" "Duration"
printf "%-10s %-35s %-12s %-12s\n" "----------" "-----------------------------------" "------------" "------------"
printf "%-10s %-35s %-12s %-12s\n" "Stage 1" "Generate Synthetic Traces" "${STAGE_STATUS[1]:-N/A}" "${STAGE_DURATION[1]:-0}s"
printf "%-10s %-35s %-12s %-12s\n" "Stage 2" "Upload Raw Traces to HDFS" "${STAGE_STATUS[2]:-N/A}" "${STAGE_DURATION[2]:-0}s"
printf "%-10s %-35s %-12s %-12s\n" "Stage 3" "Build MapReduce JAR" "${STAGE_STATUS[3]:-N/A}" "${STAGE_DURATION[3]:-0}s"
printf "%-10s %-35s %-12s %-12s\n" "Stage 4" "Execute MapReduce Jobs 1-5" "${STAGE_STATUS[4]:-N/A}" "${STAGE_DURATION[4]:-0}s"
printf "%-10s %-35s %-12s %-12s\n" "Stage 5" "Load Hive Tables & ORC" "${STAGE_STATUS[5]:-N/A}" "${STAGE_DURATION[5]:-0}s"
printf "%-10s %-35s %-12s %-12s\n" "Stage 6" "Execute Analytical Queries" "${STAGE_STATUS[6]:-N/A}" "${STAGE_DURATION[6]:-0}s"
printf "%-10s %-35s %-12s %-12s\n" "Stage 7" "Charts & Visualizations" "${STAGE_STATUS[7]:-N/A}" "${STAGE_DURATION[7]:-0}s"
echo "======================================================================"
echo "Logs saved in: ${LOG_DIR}/"
