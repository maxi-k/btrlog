#!/usr/bin/env sh
set -euo pipefail

# Configuration
SERVER_IP="${SERVER_IP:-127.0.0.1}"
SERVER_PORT="${SERVER_PORT:-11111}"
MPS_LIMIT="${MPS_LIMIT:-10000}"  # Messages per second (rate limit)
MSG_SIZE="${MSG_SIZE:-14}"
RUNTIME="${RUNTIME:-10}"         # Runtime in seconds
BURST="${BURST:-1}"              # Messages per burst
REPLY_EVERY="${REPLY_EVERY:-1}"  # Reply every N messages
WARMUP="${WARMUP:-0.4}"          # Warmup time in seconds (default 400ms)

echo "# Running sockperf latency benchmark to $SERVER_IP:$SERVER_PORT" >&2
echo "# MPS limit: $MPS_LIMIT, Message size: $MSG_SIZE bytes, Runtime: ${RUNTIME}s, Burst: $BURST" >&2

# Create temp file for full log output
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

FULL_LOG="$TMPDIR/sockperf-full.csv"

# Run sockperf under-load test
# Key parameters:
# - under-load: latency under load test (rate controlled)
# - --mps: messages per second (rate limit)
# - --msg-size: size of each message in bytes
# - --time: duration of test in seconds
# - --burst: number of messages to send in each burst
# - --reply-every: server replies every N messages
# - --full-log: output detailed per-packet CSV
# - --full-rtt: report round-trip-time instead of one-way latency
#
# Note: sockperf outputs statistics to stderr and we redirect stdout to capture
# the full log CSV output

sockperf under-load \
    --ip "$SERVER_IP" \
    --port "$SERVER_PORT" \
    --mps="$MPS_LIMIT" \
    --msg-size="$MSG_SIZE" \
    --time="$RUNTIME" \
    --burst="$BURST" \
    --reply-every="$REPLY_EVERY" \
    --full-log="$FULL_LOG" \
    --full-rtt \
    2>&1 | tee "$TMPDIR/sockperf-stats.txt" >/dev/null

# Check if the full log was created
if [ ! -f "$FULL_LOG" ]; then
    echo "Error: Full log not found at $FULL_LOG" >&2
    echo "Sockperf may have failed. Check output above." >&2
    exit 1
fi

# Extract test parameters from the stats output
STATS_FILE="$TMPDIR/sockperf-stats.txt"

# Parse key statistics from sockperf output
# The output format includes lines like:
# sockperf: [Total Run] RunTime=10.001 sec; Warm up time=400 msec; SentMessages=99999; ReceivedMessages=99998
# sockperf: ====> avg-rtt=XX.XXX (std-dev=X.XXX)
# sockperf: Total XXXXX observations
# sockperf: ---> <MAX> observation = XXX.XXX
# sockperf: ---> percentile XX.XXX = XXX.XXX

# Build header similar to sockperf's native output
echo "# ------------------------------"
echo "# test was performed using the following parameters: --mps=$MPS_LIMIT --burst=$BURST --reply-every=$REPLY_EVERY --msg-size=$MSG_SIZE --time=$RUNTIME --full-rtt"
echo "# ------------------------------"

# Extract and print statistics from sockperf output
grep "sockperf:" "$STATS_FILE" | while IFS= read -r line; do
    echo "# $line"
done

echo "# ------------------------------"
echo "# Note: Timestamps may have limited precision depending on clock resolution"
echo "# ------------------------------"

# Output the CSV data
# sockperf's --full-log outputs in format: packet, tx_timestamp_sec, rx_timestamp_sec, rtt_usec
# But it includes separator lines, "test was performed", and header without # prefix
# We need to filter these out and only output the numeric data rows
echo "packet, tx_timestamp_sec, rx_timestamp_sec, rtt_usec"
sed -n '/^[0-9]/p' "$FULL_LOG"

echo "# Benchmark completed successfully" >&2
