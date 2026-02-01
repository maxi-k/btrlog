#!/usr/bin/env bash
set -euo pipefail

# Usage: ./sockperf-mt-server.sh [num_servers] [base_port]
# Example: ./sockperf-mt-server.sh 8 11111

# Default to number of CPU cores
NUM_SERVERS="${1:-$(nproc)}"
BASE_PORT="${2:-11111}"

echo "Starting $NUM_SERVERS sockperf server processes on ports $BASE_PORT-$((BASE_PORT + NUM_SERVERS - 1))"

# Array to store background PIDs
declare -a PIDS=()

# Cleanup function to kill all servers on exit
cleanup() {
    echo ""
    echo "Shutting down sockperf servers..."
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    wait 2>/dev/null || true
    echo "All servers stopped."
}

trap cleanup EXIT INT TERM

# Start sockperf servers on sequential ports
for i in $(seq 0 $((NUM_SERVERS - 1))); do
    PORT=$((BASE_PORT + i))
    echo "Starting sockperf server on port $PORT (process $((i + 1))/$NUM_SERVERS)..."

    # Start sockperf in server mode
    # Redirect output to avoid clutter
    sockperf server --ip 0.0.0.0 --port "$PORT" > /dev/null 2>&1 &

    pid=$!
    PIDS+=("$pid")
    echo "  Server $((i + 1)) started with PID $pid on port $PORT"

    # Brief sleep to avoid startup race conditions
    sleep 0.1
done

echo ""
echo "All $NUM_SERVERS sockperf servers running."
echo "Press Ctrl+C to stop all servers."
echo ""

# Wait for all background processes
wait
