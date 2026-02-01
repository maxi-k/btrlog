#!/usr/bin/env bash
set -euo pipefail

# Usage: ./sockperf-latency-vs-rate-mt.sh <min_mps> <max_mps> <step_mps> [output_dir] [prefix] [num_clients]
# Example: ./sockperf-latency-vs-rate-mt.sh 100000 500000 50000 results/microbench c6gd 8

if [ $# -lt 3 ]; then
    echo "Usage: $0 <min_mps> <max_mps> <step_mps> [output_dir] [prefix] [num_clients]" >&2
    echo "" >&2
    echo "Arguments:" >&2
    echo "  min_mps      - Minimum messages-per-second rate limit" >&2
    echo "  max_mps      - Maximum messages-per-second rate limit" >&2
    echo "  step_mps     - MPS increment step" >&2
    echo "  output_dir   - Output directory (default: ./results)" >&2
    echo "  prefix       - Filename prefix, e.g., instance type (default: sockperf)" >&2
    echo "  num_clients  - Number of parallel client processes (default: nproc)" >&2
    echo "" >&2
    echo "Example:" >&2
    echo "  $0 100000 500000 50000 results/microbench c6gd 8" >&2
    echo "" >&2
    echo "Environment variables:" >&2
    echo "  SERVER_IP       - Server IP address (default: 127.0.0.1)" >&2
    echo "  BASE_PORT       - Base server port (default: 11111)" >&2
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
NUM_CLIENTS="${6:-$(nproc)}"

# Configuration
SERVER_IP="${SERVER_IP:-127.0.0.1}"
BASE_PORT="${BASE_PORT:-11111}"
MSG_SIZE="${MSG_SIZE:-14}"
RUNTIME="${RUNTIME:-10}"
BURST="${BURST:-1}"
REPLY_EVERY="${REPLY_EVERY:-1}"

# Validate numeric inputs
if ! [[ "$MIN_MPS" =~ ^[0-9]+$ ]] || \
   ! [[ "$MAX_MPS" =~ ^[0-9]+$ ]] || \
   ! [[ "$STEP_MPS" =~ ^[0-9]+$ ]] || \
   ! [[ "$NUM_CLIENTS" =~ ^[0-9]+$ ]]; then
    echo "Error: Numeric values must be positive integers" >&2
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

if [ "$NUM_CLIENTS" -le 0 ]; then
    echo "Error: num_clients must be greater than 0" >&2
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

echo "Starting multi-threaded sockperf latency vs rate sweep" >&2
echo "Configuration:" >&2
echo "  MPS range: $MIN_MPS to $MAX_MPS (step: $STEP_MPS)" >&2
echo "  Total runs: $TOTAL_RUNS" >&2
echo "  Parallel clients: $NUM_CLIENTS" >&2
echo "  Output directory: $OUTPUT_DIR" >&2
echo "  Filename prefix: $PREFIX" >&2
echo "  Server: $SERVER_IP:$BASE_PORT-$((BASE_PORT + NUM_CLIENTS - 1))" >&2
echo "  Message size: $MSG_SIZE bytes" >&2
echo "  Runtime per test: ${RUNTIME}s" >&2
echo "  Burst: $BURST, Reply every: $REPLY_EVERY" >&2
echo "" >&2

# Create a summary file
SUMMARY_FILE="$OUTPUT_DIR/${PREFIX}--sockperf-latency-vs-rate-mt-summary.csv"
echo "# Sockperf Multi-Threaded Latency vs Rate Summary" > "$SUMMARY_FILE"
echo "# Generated: $(date -Iseconds)" >> "$SUMMARY_FILE"
echo "# Clients: $NUM_CLIENTS, Server: $SERVER_IP:$BASE_PORT-$((BASE_PORT + NUM_CLIENTS - 1)), Message size: $MSG_SIZE bytes, Runtime: ${RUNTIME}s" >> "$SUMMARY_FILE"
echo "mps_limit,avg_rtt_usec,stddev_usec,min_usec,max_usec,p25_usec,p50_usec,p75_usec,p90_usec,p99_usec,p999_usec,p9999_usec,p99999_usec,sent_msgs,recv_msgs" >> "$SUMMARY_FILE"

# Function to calculate statistics from merged CSV data
calculate_stats() {
    local csv_file="$1"

    # Use awk to calculate statistics from the RTT column (5th column, after adding client_id)
    # Skip header and comment lines
    awk -F',' '
    BEGIN {
        count = 0
        sum = 0
        sum_sq = 0
        min = 999999999
        max = 0
    }
    /^[0-9]/ {
        rtt = $5
        values[count++] = rtt
        sum += rtt
        sum_sq += rtt * rtt
        if (rtt < min) min = rtt
        if (rtt > max) max = rtt
    }
    END {
        if (count == 0) {
            print "0,0,0,0,0,0,0,0,0,0,0,0"
            exit
        }

        # Calculate average and standard deviation
        avg = sum / count
        variance = (sum_sq / count) - (avg * avg)
        stddev = sqrt(variance > 0 ? variance : 0)

        # Sort values for percentile calculation
        asort(values, sorted)

        # Calculate percentiles
        p25_idx = int(count * 0.25)
        p50_idx = int(count * 0.50)
        p75_idx = int(count * 0.75)
        p90_idx = int(count * 0.90)
        p99_idx = int(count * 0.99)
        p999_idx = int(count * 0.999)
        p9999_idx = int(count * 0.9999)
        p99999_idx = int(count * 0.99999)

        # Ensure indices are at least 1
        if (p25_idx < 1) p25_idx = 1
        if (p50_idx < 1) p50_idx = 1
        if (p75_idx < 1) p75_idx = 1
        if (p90_idx < 1) p90_idx = 1
        if (p99_idx < 1) p99_idx = 1
        if (p999_idx < 1) p999_idx = 1
        if (p9999_idx < 1) p9999_idx = 1
        if (p99999_idx < 1) p99999_idx = 1

        printf "%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%d\n",
            avg, stddev, min, max,
            sorted[p25_idx], sorted[p50_idx], sorted[p75_idx],
            sorted[p90_idx], sorted[p99_idx], sorted[p999_idx],
            sorted[p9999_idx], sorted[p99999_idx], count
    }
    ' "$csv_file"
}

# Iterate through MPS range
CURRENT_MPS="$MIN_MPS"
while [ "$CURRENT_MPS" -le "$MAX_MPS" ]; do
    CURRENT_RUN=$((CURRENT_RUN + 1))

    echo "======================================" >&2
    echo "Run $CURRENT_RUN/$TOTAL_RUNS: Testing at $CURRENT_MPS MPS ($NUM_CLIENTS clients)" >&2
    echo "======================================" >&2

    # Calculate MPS per client
    MPS_PER_CLIENT=$((CURRENT_MPS / NUM_CLIENTS))
    MPS_REMAINDER=$((CURRENT_MPS % NUM_CLIENTS))

    echo "MPS per client: $MPS_PER_CLIENT (remainder: $MPS_REMAINDER will be added to first clients)" >&2

    # Create temporary directory for this run
    RUN_TMPDIR=$(mktemp -d)
    trap "rm -rf $RUN_TMPDIR" EXIT

    # Start all client processes in parallel
    declare -a CLIENT_PIDS=()
    for i in $(seq 0 $((NUM_CLIENTS - 1))); do
        PORT=$((BASE_PORT + i))
        CLIENT_OUTPUT="$RUN_TMPDIR/client-$i.csv"

        # Calculate MPS for this client (distribute remainder among first clients)
        if [ "$i" -lt "$MPS_REMAINDER" ]; then
            CLIENT_MPS=$((MPS_PER_CLIENT + 1))
        else
            CLIENT_MPS=$MPS_PER_CLIENT
        fi

        echo "  Starting client $((i + 1))/$NUM_CLIENTS: $CLIENT_MPS MPS -> $SERVER_IP:$PORT" >&2

        # Run sockperf client in background
        (
            SERVER_IP="$SERVER_IP" \
            SERVER_PORT="$PORT" \
            MPS_LIMIT="$CLIENT_MPS" \
            MSG_SIZE="$MSG_SIZE" \
            RUNTIME="$RUNTIME" \
            BURST="$BURST" \
            REPLY_EVERY="$REPLY_EVERY" \
            "$LATENCY_SCRIPT" > "$CLIENT_OUTPUT" 2>&1
        ) &

        CLIENT_PIDS+=($!)
    done

    # Wait for all clients to complete
    echo "  Waiting for all clients to complete..." >&2
    FAILED=0
    for i in $(seq 0 $((NUM_CLIENTS - 1))); do
        if ! wait "${CLIENT_PIDS[$i]}"; then
            echo "  WARNING: Client $((i + 1)) failed" >&2
            FAILED=1
        fi
    done

    if [ "$FAILED" -eq 1 ]; then
        echo "Error: One or more clients failed at $CURRENT_MPS MPS" >&2
        echo "Check temporary files in $RUN_TMPDIR for details" >&2
        # Continue with next iteration instead of failing completely
        CURRENT_MPS=$((CURRENT_MPS + STEP_MPS))
        continue
    fi

    echo "  All clients completed. Merging results..." >&2

    # Generate output filename
    OUTPUT_FILE="$OUTPUT_DIR/${PREFIX}--sockperf-mt-${CURRENT_MPS}mps.csv"

    # Merge CSV files
    # Extract header from first file and write it
    {
        # Write header comments from first client
        grep "^#" "$RUN_TMPDIR/client-0.csv" | head -10
        echo "# Merged output from $NUM_CLIENTS parallel clients"
        echo "# Total MPS limit: $CURRENT_MPS"
        echo "# ------------------------------"

        # Write CSV header
        echo "client_id, packet, tx_timestamp_sec, rx_timestamp_sec, rtt_usec"

        # Merge all packet data from all clients
        # Add client_id prefix and sort by timestamp for chronological order
        for i in $(seq 0 $((NUM_CLIENTS - 1))); do
            sed -n '/^[0-9]/p' "$RUN_TMPDIR/client-$i.csv" | awk -v client="$i" '{print client "," $0}'
        done | sort -t',' -k3 -n

    } > "$OUTPUT_FILE"

    echo "Completed: $OUTPUT_FILE" >&2

    # Calculate statistics from merged data
    STATS=$(calculate_stats "$OUTPUT_FILE")
    IFS=',' read -r AVG STDDEV MIN MAX P25 P50 P75 P90 P99 P999 P9999 P99999 RECV_MSGS <<< "$STATS"

    # Calculate total sent messages
    SENT_MSGS=0
    for i in $(seq 0 $((NUM_CLIENTS - 1))); do
        CLIENT_SENT=$(grep "SentMessages=" "$RUN_TMPDIR/client-$i.csv" | sed -n 's/.*SentMessages=\([0-9]*\).*/\1/p' | head -1)
        if [ -n "$CLIENT_SENT" ]; then
            SENT_MSGS=$((SENT_MSGS + CLIENT_SENT))
        fi
    done

    # Append to summary file
    echo "$CURRENT_MPS,$AVG,$STDDEV,$MIN,$MAX,$P25,$P50,$P75,$P90,$P99,$P999,$P9999,$P99999,$SENT_MSGS,$RECV_MSGS" >> "$SUMMARY_FILE"

    echo "Summary: avg=${AVG}us, p50=${P50}us, p99=${P99}us, p999=${P999}us (sent=$SENT_MSGS, recv=$RECV_MSGS)" >&2

    # Check for dropped messages
    if [ -n "$SENT_MSGS" ] && [ -n "$RECV_MSGS" ]; then
        DROPPED=$((SENT_MSGS - RECV_MSGS))
        if [ "$DROPPED" -gt 0 ]; then
            LOSS_PCT=$(awk "BEGIN {printf \"%.2f\", ($DROPPED / $SENT_MSGS) * 100}")
            echo "WARNING: $DROPPED messages dropped ($LOSS_PCT%)!" >&2
        fi
    fi

    echo "" >&2

    # Clean up temporary directory for this run
    rm -rf "$RUN_TMPDIR"

    # Increment MPS
    CURRENT_MPS=$((CURRENT_MPS + STEP_MPS))
done

echo "======================================" >&2
echo "All tests completed!" >&2
echo "Summary file: $SUMMARY_FILE" >&2
echo "Individual results: $OUTPUT_DIR/${PREFIX}--sockperf-mt-*mps.csv" >&2
echo "======================================" >&2

# Display quick summary
echo "" >&2
echo "Quick Summary:" >&2
column -t -s',' "$SUMMARY_FILE" | head -20 >&2
