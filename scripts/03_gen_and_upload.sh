#!/bin/bash
set -e

if [ $# -lt 1 ]; then
    echo "Usage: $0 <small|medium|large> [--both]"
    exit 1
fi

SIZE_CLASS=$1
BOTH_FLAG=$2

RUNS=0
FMT="both"
EST_NESTED_GB=0
EST_FLAT_GB=0

if [ "$SIZE_CLASS" == "small" ]; then
    RUNS=100000
    EST_NESTED_GB=1.5
    EST_FLAT_GB=2.2
elif [ "$SIZE_CLASS" == "medium" ]; then
    RUNS=500000
    EST_NESTED_GB=7.5
    EST_FLAT_GB=11.0
elif [ "$SIZE_CLASS" == "large" ]; then
    RUNS=1000000
    EST_NESTED_GB=15.0
    EST_FLAT_GB=22.0
    FMT="nested"
    if [ "$BOTH_FLAG" == "--both" ]; then
        FMT="both"
    fi
else
    echo "Error: Unknown size class '$SIZE_CLASS'. Use small, medium, or large."
    exit 1
fi

if [ "$SIZE_CLASS" != "large" ] && [ "$BOTH_FLAG" == "--both" ]; then
    FMT="both"
fi

EST_TOTAL_GB=$EST_NESTED_GB
if [ "$FMT" == "both" ]; then
    EST_TOTAL_GB=$(awk "BEGIN {print $EST_NESTED_GB + $EST_FLAT_GB}")
fi

REQ_SPACE_GB=$(awk "BEGIN {print $EST_TOTAL_GB * 3}")

FREE_SPACE_KB=$(df -k ~/ai-trace-bigdata/ | tail -1 | awk '{print $4}')
FREE_SPACE_GB=$(awk "BEGIN {print $FREE_SPACE_KB / 1024 / 1024}")

echo "Estimated dataset size: ~${EST_TOTAL_GB} GB"
echo "Required free space (3x): ~${REQ_SPACE_GB} GB"
echo "Available free space: ~${FREE_SPACE_GB} GB"

if $(awk "BEGIN {exit ($FREE_SPACE_GB < $REQ_SPACE_GB) ? 0 : 1}"); then
    echo "Error: Not enough free disk space. Aborting."
    exit 1
fi

echo "Disk space check passed. Generating data..."
mkdir -p ~/ai-trace-bigdata/data/traces

rm -rf ~/ai-trace-bigdata/data/traces/*

python3 ~/ai-trace-bigdata/datagen/generate_traces.py \
    --runs $RUNS \
    --format $FMT \
    --out ~/ai-trace-bigdata/data/traces \
    --files 8 \
    --seed 42 \
    --malformed-pct 1.0

echo "Uploading to HDFS..."
hdfs dfs -mkdir -p /data/raw/nested
hdfs dfs -mkdir -p /data/raw/flat

hdfs dfs -rm -r -f /data/raw/nested/* || true
hdfs dfs -rm -r -f /data/raw/flat/* || true

if [ -d ~/ai-trace-bigdata/data/traces/nested ]; then
    echo "Uploading nested format..."
    hdfs dfs -put ~/ai-trace-bigdata/data/traces/nested/* /data/raw/nested/
fi

if [ -d ~/ai-trace-bigdata/data/traces/flat ]; then
    echo "Uploading flat format..."
    hdfs dfs -put ~/ai-trace-bigdata/data/traces/flat/* /data/raw/flat/
fi

echo "HDFS upload complete. Current size in HDFS:"
hdfs dfs -du -h /data/raw
