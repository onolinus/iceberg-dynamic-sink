#!/usr/bin/env bash
# =============================================================================
# run-job.sh  —  Submit the Dynamic Iceberg Sink jar to your Flink cluster
#
# Prerequisites:
#   1. mvn clean package  (builds target/lakehouse-dynamic-sink-1.0-SNAPSHOT.jar)
#   2. OSS_ACCESS_KEY_ID and OSS_ACCESS_KEY_SECRET exported in your shell
#   3. `flink` CLI on PATH  (or set FLINK_HOME below)
#
# Usage:
#   export OSS_ACCESS_KEY_ID=xxxxx
#   export OSS_ACCESS_KEY_SECRET=xxxxx
#   bash scripts/run-job.sh
# =============================================================================

set -euo pipefail

# ---------- paths & names ---------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
JAR="${SCRIPT_DIR}/../target/lakehouse-dynamic-sink-1.0-SNAPSHOT.jar"
MAIN_CLASS="com.bfi.lakehouse.sink.DynamicIcebergSinkJob"

# Set FLINK_HOME if `flink` is not on PATH (e.g. /opt/flink or $EMR_FLINK_HOME)
# export FLINK_HOME=/opt/flink
FLINK_BIN="${FLINK_HOME:+$FLINK_HOME/bin/}flink"

# ---------- validate --------------------------------------------------------
if [[ ! -f "$JAR" ]]; then
    echo "ERROR: Fat jar not found: $JAR"
    echo "       Run: mvn clean package"
    exit 1
fi

if [[ -z "${OSS_ACCESS_KEY_ID:-}" || -z "${OSS_ACCESS_KEY_SECRET:-}" ]]; then
    echo "ERROR: OSS credentials not set."
    echo "       export OSS_ACCESS_KEY_ID=<your-key-id>"
    echo "       export OSS_ACCESS_KEY_SECRET=<your-key-secret>"
    exit 1
fi

echo "Submitting job to Flink cluster..."
echo "  JAR   : $JAR"
echo "  Class : $MAIN_CLASS"
echo ""

# ---------- submit ----------------------------------------------------------
# -D env.* passes environment variables into the JobManager and TaskManagers.
# Credentials are never written to logs because we do NOT echo them here.
"$FLINK_BIN" run \
    --class "$MAIN_CLASS" \
    --detached \
    -D "env.java.opts.taskmanager=-DOSS_ACCESS_KEY_ID=${OSS_ACCESS_KEY_ID} -DOSS_ACCESS_KEY_SECRET=${OSS_ACCESS_KEY_SECRET}" \
    -D "containerized.taskmanager.env.OSS_ACCESS_KEY_ID=${OSS_ACCESS_KEY_ID}" \
    -D "containerized.taskmanager.env.OSS_ACCESS_KEY_SECRET=${OSS_ACCESS_KEY_SECRET}" \
    -D "containerized.master.env.OSS_ACCESS_KEY_ID=${OSS_ACCESS_KEY_ID}" \
    -D "containerized.master.env.OSS_ACCESS_KEY_SECRET=${OSS_ACCESS_KEY_SECRET}" \
    "$JAR"

echo ""
echo "Job submitted (detached mode). Check the Flink Web UI for status."
