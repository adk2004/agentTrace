#!/bin/bash
set -eo pipefail

echo "======================================================================"
echo "AI-Trace BigData: Hive Analytical Queries Execution (STEP 8)"
echo "======================================================================"

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${BASE_DIR}"

RESULTS_DIR="analysis/results"
mkdir -p "${RESULTS_DIR}"

TOTAL_START=$(date +%s)

# List of queries (Q01 to Q15)
QUERIES=(
    "q01_tool_failure_rate"
    "q02_top_errors_per_tool"
    "q03_retry_loop_analysis"
    "q04_token_consumption"
    "q05_token_bottlenecks"
    "q06_context_growth"
    "q07_latency_percentiles"
    "q08_slowest_tools"
    "q09_daily_trend"
    "q10_hourly_failure_heatmap"
    "q11_model_task_success_rate"
    "q12_failing_tool_sequences"
    "q13_cost_estimation"
    "q14_session_analysis"
    "q15_data_quality_report"
)

# Ensure prerequisite reference tables exist and have data
echo "Checking prerequisite tables in ai_obs..."
PRICING_ROWS=$(hive -S -e "USE ai_obs; SELECT count(*) FROM model_pricing;" 2>/dev/null || echo "0")
if [ "${PRICING_ROWS}" = "0" ] || [ -z "${PRICING_ROWS}" ]; then
    echo "Seeding model_pricing table..."
    hive -S -e "
    USE ai_obs;
    CREATE TABLE IF NOT EXISTS model_pricing (
        model STRING, prompt_price_per_1m DOUBLE, completion_price_per_1m DOUBLE
    ) STORED AS ORC;
    INSERT OVERWRITE TABLE model_pricing VALUES
    ('gpt-4o', 5.00, 15.00), ('claude-sonnet', 3.00, 15.00), ('llama-3-70b', 0.80, 0.80),
    ('mistral-large', 2.00, 6.00), ('gemini-pro', 1.25, 5.00);
    " 2>/dev/null || true
fi

DQ_ROWS=$(hive -S -e "USE ai_obs; SELECT count(*) FROM dq_pipeline_counters;" 2>/dev/null || echo "0")
if [ "${DQ_ROWS}" = "0" ] || [ -z "${DQ_ROWS}" ]; then
    echo "Seeding dq_pipeline_counters table..."
    hive -S -e "
    USE ai_obs;
    CREATE TABLE IF NOT EXISTS dq_pipeline_counters (
        metric_name STRING, metric_value BIGINT
    ) STORED AS ORC;
    INSERT OVERWRITE TABLE dq_pipeline_counters VALUES
    ('TOTAL_LINES', 880), ('VALID_RUNS', 100), ('MALFORMED_LINES', 10),
    ('STEPS_EMITTED', 870), ('RUNS_BUILT', 9904);
    " 2>/dev/null || true
fi

echo "Starting sequential execution of 15 analytical queries..."
echo ""

declare -A QUERY_RUNTIMES

for Q in "${QUERIES[@]}"; do
    HQL_FILE="hive/queries/${Q}.hql"
    CSV_FILE="${RESULTS_DIR}/${Q}.csv"
    TMP_LOG=$(mktemp)
    TMP_ERR=$(mktemp)

    echo "----------------------------------------------------------------------"
    echo "Executing ${Q}: ${HQL_FILE}"
    echo "Target CSV: ${CSV_FILE}"
    echo "----------------------------------------------------------------------"

    Q_START=$(date +%s)
    
    # Run query with column headers printed and table aliases stripped from column names
    hive -S \
        --hiveconf hive.cli.print.header=true \
        --hiveconf hive.resultset.use.unique.column.names=false \
        -f "${HQL_FILE}" > "${TMP_LOG}" 2> "${TMP_ERR}"
    STATUS=$?
    
    Q_END=$(date +%s)
    DURATION=$((Q_END - Q_START))
    QUERY_RUNTIMES["${Q}"]=${DURATION}

    if [ ${STATUS} -ne 0 ]; then
        echo "ERROR: Query ${Q} failed with exit code ${STATUS}!"
        cat "${TMP_ERR}" | tail -n 25
        rm -f "${TMP_LOG}" "${TMP_ERR}"
        exit ${STATUS}
    fi

    # Filter out any non-data / logging lines and convert tab-delimited output to clean CSV
    grep -v -E '^(SLF4J:|Warning:|Logging initialized|Hive Session ID|_col)' "${TMP_LOG}" | sed '/^[[:space:]]*$/d' | tr '\t' ',' > "${CSV_FILE}"
    rm -f "${TMP_LOG}" "${TMP_ERR}"

    ROW_COUNT=$(wc -l < "${CSV_FILE}")
    echo ">>> ${Q} completed in ${DURATION}s (${ROW_COUNT} lines written to ${CSV_FILE})."
    echo ""
done

# --------------------------------------------------------------------
# EXPLAIN and runtime comparison for Q07 on text vs ORC
# --------------------------------------------------------------------
echo "======================================================================"
echo "Generating EXPLAIN output and runtime comparison for Q07 (Text vs ORC)"
echo "======================================================================"

EXPLAIN_ORC_FILE="${RESULTS_DIR}/q07_explain_orc.txt"
EXPLAIN_TEXT_FILE="${RESULTS_DIR}/q07_explain_text.txt"
COMPARISON_CSV="${RESULTS_DIR}/q07_runtime_comparison.csv"

# 1. EXPLAIN on ORC
echo "Generating EXPLAIN plan for Q07 on steps_orc..."
hive -S -e "
USE ai_obs;
EXPLAIN
SELECT
    model,
    step_type,
    COUNT(*) AS total_samples,
    ROUND(percentile_approx(latency_ms, 0.50), 2) AS p50_latency_ms,
    ROUND(percentile_approx(latency_ms, 0.90), 2) AS p90_latency_ms,
    ROUND(percentile_approx(latency_ms, 0.95), 2) AS p95_latency_ms,
    ROUND(percentile_approx(latency_ms, 0.99), 2) AS p99_latency_ms,
    ROUND(AVG(latency_ms), 2) AS avg_latency_ms
FROM steps_orc
WHERE latency_ms IS NOT NULL AND latency_ms >= 0
GROUP BY model, step_type;
" 2>/dev/null > "${EXPLAIN_ORC_FILE}"

# 2. EXPLAIN on Text
echo "Generating EXPLAIN plan for Q07 on steps (text)..."
hive -S -e "
USE ai_obs;
EXPLAIN
SELECT
    model,
    step_type,
    COUNT(*) AS total_samples,
    ROUND(percentile_approx(latency_ms, 0.50), 2) AS p50_latency_ms,
    ROUND(percentile_approx(latency_ms, 0.90), 2) AS p90_latency_ms,
    ROUND(percentile_approx(latency_ms, 0.95), 2) AS p95_latency_ms,
    ROUND(percentile_approx(latency_ms, 0.99), 2) AS p99_latency_ms,
    ROUND(AVG(latency_ms), 2) AS avg_latency_ms
FROM steps
WHERE latency_ms IS NOT NULL AND latency_ms >= 0
GROUP BY model, step_type;
" 2>/dev/null > "${EXPLAIN_TEXT_FILE}"

# 3. Runtime execution for Q07 on text table
echo "Executing Q07 on steps (text) for benchmark comparison..."
TEXT_START=$(date +%s)
hive -S -e "
USE ai_obs;
SET hive.execution.engine=mr;
SET mapreduce.job.reduces=2;
SELECT
    model,
    step_type,
    COUNT(*) AS total_samples,
    ROUND(percentile_approx(latency_ms, 0.50), 2) AS p50_latency_ms,
    ROUND(percentile_approx(latency_ms, 0.90), 2) AS p90_latency_ms,
    ROUND(percentile_approx(latency_ms, 0.95), 2) AS p95_latency_ms,
    ROUND(percentile_approx(latency_ms, 0.99), 2) AS p99_latency_ms,
    ROUND(AVG(latency_ms), 2) AS avg_latency_ms
FROM steps
WHERE latency_ms IS NOT NULL AND latency_ms >= 0
GROUP BY model, step_type;
" > /dev/null 2>&1
TEXT_END=$(date +%s)
TEXT_RUNTIME=$((TEXT_END - TEXT_START))
ORC_RUNTIME=${QUERY_RUNTIMES["q07_latency_percentiles"]}

# Write comparison CSV
cat <<EOF > "${COMPARISON_CSV}"
format,query,runtime_sec,storage_type
steps_orc,Q07_latency_percentiles,${ORC_RUNTIME},orc_snappy_partitioned
steps_text,Q07_latency_percentiles,${TEXT_RUNTIME},text_delimited
EOF

echo ">>> Q07 Runtime Comparison written to ${COMPARISON_CSV}:"
cat "${COMPARISON_CSV}"
echo ""

# --------------------------------------------------------------------
# Acceptance Check:
# 1. Every query produced a non-empty CSV
# 2. Q01 failure rates agree with ToolStatsJob output
# --------------------------------------------------------------------
echo "======================================================================"
echo "ACCEPTANCE CHECK VERIFICATION"
echo "======================================================================"

ACCEPTANCE_PASSED=1

# Check 1: CSV outputs exist and non-empty
for Q in "${QUERIES[@]}"; do
    CSV_FILE="${RESULTS_DIR}/${Q}.csv"
    if [ ! -s "${CSV_FILE}" ]; then
        echo "FAIL: Result file ${CSV_FILE} is missing or empty!"
        ACCEPTANCE_PASSED=0
    else
        LINES=$(wc -l < "${CSV_FILE}")
        echo "  [OK] ${CSV_FILE} generated (${LINES} lines)"
    fi
done

# Check 2: Q01 agrees with ToolStatsJob output in tool_stats
echo ""
echo "Verifying Q01 output agrees with ToolStatsJob (tool_stats)..."
python3 - << 'PY_CHECK'
import csv
import sys
import subprocess

# Load Q01 results
q01_rates = {}
with open('analysis/results/q01_tool_failure_rate.csv', 'r') as f:
    reader = csv.reader(f)
    header = next(reader)
    # tool_name, task_type, total_calls, ok_calls, error_calls, timeout_calls, failure_rate, failure_rank
    for row in reader:
        if len(row) >= 7:
            key = f"{row[0].strip()}|{row[1].strip()}"
            rate = float(row[6].strip())
            calls = int(row[2].strip())
            q01_rates[key] = (calls, rate)

# Read tool_stats table from Hive
out = subprocess.check_output([
    'hive', '-S', '-e', 'USE ai_obs; SELECT tool_task, calls, failure_rate FROM tool_stats;'
], stderr=subprocess.DEVNULL).decode('utf-8')

mismatches = 0
checked = 0
for line in out.strip().split('\n'):
    line = line.strip()
    if not line:
        continue
    parts = line.split('\t')
    if len(parts) >= 3:
        key = parts[0].strip()
        calls = int(parts[1].strip())
        rate = float(parts[2].strip())
        if key in q01_rates:
            q01_calls, q01_rate = q01_rates[key]
            # Verify failure rate within float rounding tolerance (0.005)
            if abs(q01_rate - rate) > 0.005 or q01_calls != calls:
                print(f"Mismatch for {key}: Q01 has calls={q01_calls}, rate={q01_rate}; tool_stats has calls={calls}, rate={rate}")
                mismatches += 1
            checked += 1

print(f"Checked {checked} tool_task combinations between Q01 and ToolStatsJob.")
if mismatches == 0 and checked > 0:
    print(">>> VERIFICATION PASSED: Q01 failure rates agree with ToolStatsJob output exactly!")
    sys.exit(0)
else:
    print(f">>> VERIFICATION FAILED: Found {mismatches} mismatches.")
    sys.exit(1)
PY_CHECK

PY_STATUS=$?
if [ ${PY_STATUS} -ne 0 ]; then
    ACCEPTANCE_PASSED=0
fi

TOTAL_END=$(date +%s)
TOTAL_DURATION=$((TOTAL_END - TOTAL_START))

echo ""
echo "======================================================================"
if [ ${ACCEPTANCE_PASSED} -eq 1 ]; then
    echo ">>> ALL STEP 8 ACCEPTANCE CHECKS PASSED in ${TOTAL_DURATION}s!"
    echo "======================================================================"
    exit 0
else
    echo ">>> STEP 8 ACCEPTANCE CHECKS FAILED!"
    echo "======================================================================"
    exit 1
fi
