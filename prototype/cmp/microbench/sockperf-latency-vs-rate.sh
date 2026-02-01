#!/usr/bin/env sh
set -euo pipefail

# Usage: ./sockperf-latency-vs-rate.sh <min_mps> <max_mps> <step_mps> [output_dir] [prefix]
# Example: ./sockperf-latency-vs-rate.sh 100000 500000 50000 results/microbench c6gd

if [ $# -lt 3 ]; then
    echo "Usage: $0 <min_mps> <max_mps> <step_mps> [output_dir] [prefix]" >&2
    echo "" >&2
    echo "Arguments:" >&2
    echo "  min_mps     - Minimum messages-per-second rate limit" >&2
    echo "  max_mps     - Maximum messages-per-second rate limit" >&2
    echo "  step_mps    - MPS increment step" >&2
    echo "  output_dir  - Output directory (default: ./results)" >&2
    echo "  prefix      - Filename prefix, e.g., instance type (default: sockperf)" >&2
    echo "" >&2
    echo "Example:" >&2
    echo "  $0 100000 500000 50000 results/microbench c6gd" >&2
    echo "" >&2
    echo "Environment variables:" >&2
    echo "  SERVER_IP       - Server IP address (default: 127.0.0.1)" >&2
    echo "  SERVER_PORT     - Server port (default: 11111)" >&2
    echo "  MSG_SIZE        - Message size in bytes (default: 14)" >&2
    echo "  RUNTIME         - Test duration in seconds (default: 10)" >&2
    echo "  BURST           - Messages per burst (default: 1)" >&2
    echo "  REPLY_EVERY     - Reply every N messages (default: 1)" >&2
    exit 1
fi

MIN_MPS="$1"
MAX_MPS="$2"
STEP_MPS="$3"
OUTPUT_DIR="${4:-./results}"
PREFIX="${5:-sockperf}"

# Validate numeric inputs
if ! echo "$MIN_MPS" | grep -qE '^[0-9]+$' || \
   ! echo "$MAX_MPS" | grep -qE '^[0-9]+$' || \
   ! echo "$STEP_MPS" | grep -qE '^[0-9]+$'; then
    echo "Error: MPS values must be positive integers" >&2
    exit 1
fi

if [ "$MIN_MPS" -gt "$MAX_MPS" ]; then
    echo "Error: min_mps ($MIN_MPS) must be less than or equal to max_mps ($MAX_MPS)" >&2
    exit 1
fi

if [ "$STEP_MPS" -le 0 ]; then
    echo "Error: step_mps must be greater than 0" >&2
    exit 1
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LATENCY_SCRIPT="$SCRIPT_DIR/sockperf-latency-at-rate.sh"

# Verify the latency script exists
if [ ! -x "$LATENCY_SCRIPT" ]; then
    echo "Error: Cannot find or execute $LATENCY_SCRIPT" >&2
    exit 1
fi

# Calculate total number of runs
TOTAL_RUNS=$(( (MAX_MPS - MIN_MPS) / STEP_MPS + 1 ))
CURRENT_RUN=0

echo "Starting sockperf latency vs rate sweep" >&2
echo "Configuration:" >&2
echo "  MPS range: $MIN_MPS to $MAX_MPS (step: $STEP_MPS)" >&2
echo "  Total runs: $TOTAL_RUNS" >&2
echo "  Output directory: $OUTPUT_DIR" >&2
echo "  Filename prefix: $PREFIX" >&2
echo "  Server: ${SERVER_IP:-127.0.0.1}:${SERVER_PORT:-11111}" >&2
echo "  Message size: ${MSG_SIZE:-14} bytes" >&2
echo "  Runtime per test: ${RUNTIME:-10}s" >&2
echo "  Burst: ${BURST:-1}, Reply every: ${REPLY_EVERY:-1}" >&2
echo "" >&2

# Create a summary file
SUMMARY_FILE="$OUTPUT_DIR/${PREFIX}--sockperf-latency-vs-rate-summary.csv"
echo "# Sockperf Latency vs Rate Summary" > "$SUMMARY_FILE"
echo "# Generated: $(date -Iseconds)" >> "$SUMMARY_FILE"
echo "# Server: ${SERVER_IP:-127.0.0.1}:${SERVER_PORT:-11111}, Message size: ${MSG_SIZE:-14} bytes, Runtime: ${RUNTIME:-10}s" >> "$SUMMARY_FILE"
echo "mps_limit,avg_rtt_usec,stddev_usec,min_usec,max_usec,p25_usec,p50_usec,p75_usec,p90_usec,p99_usec,p999_usec,p9999_usec,p99999_usec,sent_msgs,recv_msgs" >> "$SUMMARY_FILE"

# Iterate through MPS range
CURRENT_MPS="$MIN_MPS"
while [ "$CURRENT_MPS" -le "$MAX_MPS" ]; do
    CURRENT_RUN=$((CURRENT_RUN + 1))

    echo "======================================" >&2
    echo "Run $CURRENT_RUN/$TOTAL_RUNS: Testing at $CURRENT_MPS MPS" >&2
    echo "======================================" >&2

    # Generate output filename
    # e.g., c6gd--sockperf-100000mps.csv, c6gd--sockperf-150000mps.csv
    OUTPUT_FILE="$OUTPUT_DIR/${PREFIX}--sockperf-${CURRENT_MPS}mps.csv"

    # Run the latency test with the current MPS limit
    # Export MPS_LIMIT for the child script
    if MPS_LIMIT="$CURRENT_MPS" "$LATENCY_SCRIPT" > "$OUTPUT_FILE" 2>&1; then
        echo "Completed: $OUTPUT_FILE" >&2

        # Extract summary statistics from the output file
        # Parse the header comments to extract statistics
        # sockperf format:
        # # sockperf: ====> avg-rtt=XX.XXX (std-dev=X.XXX)
        # # sockperf: ---> percentile XX.XXX = YYY.YYY
        # # sockperf: ---> <MAX> observation = XXX.XXX
        # # sockperf: ---> <MIN> observation = XXX.XXX
        # # sockperf: [Total Run] RunTime=X.XXX sec; ... SentMessages=XXXX; ReceivedMessages=XXXX

        AVG=$(grep "avg-rtt=" "$OUTPUT_FILE" | sed -n 's/.*avg-rtt=\([0-9.]*\).*/\1/p' | head -1)
        STDDEV=$(grep "avg-rtt=" "$OUTPUT_FILE" | sed -n 's/.*std-dev=\([0-9.]*\).*/\1/p' | head -1)
        MIN=$(grep "<MIN> observation" "$OUTPUT_FILE" | sed -n 's/.*= *\([0-9.]*\)/\1/p' | head -1)
        MAX=$(grep "<MAX> observation" "$OUTPUT_FILE" | sed -n 's/.*= *\([0-9.]*\)/\1/p' | head -1)

        # Extract percentiles - sockperf outputs them in descending order
        P25=$(grep "percentile 25.0" "$OUTPUT_FILE" | sed -n 's/.*= *\([0-9.]*\)/\1/p' | head -1)
        P50=$(grep "percentile 50.0" "$OUTPUT_FILE" | sed -n 's/.*= *\([0-9.]*\)/\1/p' | head -1)
        P75=$(grep "percentile 75.0" "$OUTPUT_FILE" | sed -n 's/.*= *\([0-9.]*\)/\1/p' | head -1)
        P90=$(grep "percentile 90.0" "$OUTPUT_FILE" | sed -n 's/.*= *\([0-9.]*\)/\1/p' | head -1)
        P99=$(grep "percentile 99.0" "$OUTPUT_FILE" | sed -n 's/.*= *\([0-9.]*\)/\1/p' | head -1)
        P999=$(grep "percentile 99.9" "$OUTPUT_FILE" | sed -n 's/.*= *\([0-9.]*\)/\1/p' | head -1)
        P9999=$(grep "percentile 99.99" "$OUTPUT_FILE" | sed -n 's/.*= *\([0-9.]*\)/\1/p' | head -1)
        P99999=$(grep "percentile 99.999" "$OUTPUT_FILE" | sed -n 's/.*= *\([0-9.]*\)/\1/p' | head -1)

        # Extract message counts
        SENT_MSGS=$(grep "SentMessages=" "$OUTPUT_FILE" | sed -n 's/.*SentMessages=\([0-9]*\).*/\1/p' | head -1)
        RECV_MSGS=$(grep "ReceivedMessages=" "$OUTPUT_FILE" | sed -n 's/.*ReceivedMessages=\([0-9]*\).*/\1/p' | head -1)

        # Append to summary file
        echo "$CURRENT_MPS,$AVG,$STDDEV,$MIN,$MAX,$P25,$P50,$P75,$P90,$P99,$P999,$P9999,$P99999,$SENT_MSGS,$RECV_MSGS" >> "$SUMMARY_FILE"

        echo "Summary: avg=${AVG}us, p50=${P50}us, p99=${P99}us, p999=${P999}us (sent=$SENT_MSGS, recv=$RECV_MSGS)" >&2

        # Check for dropped messages
        if [ -n "$SENT_MSGS" ] && [ -n "$RECV_MSGS" ]; then
            DROPPED=$((SENT_MSGS - RECV_MSGS))
            if [ "$DROPPED" -gt 0 ]; then
                echo "WARNING: $DROPPED messages dropped!" >&2
            fi
        fi
    else
        echo "Error: Failed to run test at $CURRENT_MPS MPS" >&2
        echo "Check $OUTPUT_FILE for details" >&2
        # Continue with next iteration instead of failing completely
    fi

    echo "" >&2

    # Increment MPS
    CURRENT_MPS=$((CURRENT_MPS + STEP_MPS))
done

echo "======================================" >&2
echo "All tests completed!" >&2
echo "Summary file: $SUMMARY_FILE" >&2
echo "Individual results: $OUTPUT_DIR/${PREFIX}--sockperf-*mps.csv" >&2
echo "======================================" >&2

# Display quick summary
echo "" >&2
echo "Quick Summary:" >&2
column -t -s',' "$SUMMARY_FILE" | head -20 >&2
