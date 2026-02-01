#!/usr/bin/env sh
set -euo pipefail

# Usage: ./fio-latency-vs-rate.sh <min_iops> <max_iops> <step_iops> [output_dir] [prefix]
# Example: ./fio-latency-vs-rate.sh 10000 30000 1000 results/microbench c6gd

if [ $# -lt 3 ]; then
    echo "Usage: $0 <min_iops> <max_iops> <step_iops> [output_dir] [prefix]" >&2
    echo "" >&2
    echo "Arguments:" >&2
    echo "  min_iops    - Minimum IOPS rate limit" >&2
    echo "  max_iops    - Maximum IOPS rate limit" >&2
    echo "  step_iops   - IOPS increment step" >&2
    echo "  output_dir  - Output directory (default: ./results)" >&2
    echo "  prefix      - Filename prefix, e.g., instance type (default: fio)" >&2
    echo "" >&2
    echo "Example:" >&2
    echo "  $0 10000 30000 1000 results/microbench c6gd" >&2
    exit 1
fi

MIN_IOPS="$1"
MAX_IOPS="$2"
STEP_IOPS="$3"
OUTPUT_DIR="${4:-./results}"
PREFIX="${5:-fio}"

# Validate numeric inputs
if ! echo "$MIN_IOPS" | grep -qE '^[0-9]+$' || \
   ! echo "$MAX_IOPS" | grep -qE '^[0-9]+$' || \
   ! echo "$STEP_IOPS" | grep -qE '^[0-9]+$'; then
    echo "Error: IOPS values must be positive integers" >&2
    exit 1
fi

if [ "$MIN_IOPS" -gt "$MAX_IOPS" ]; then
    echo "Error: min_iops ($MIN_IOPS) must be less than or equal to max_iops ($MAX_IOPS)" >&2
    exit 1
fi

if [ "$STEP_IOPS" -le 0 ]; then
    echo "Error: step_iops must be greater than 0" >&2
    exit 1
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LATENCY_SCRIPT="$SCRIPT_DIR/fio-latency-at-rate.sh"

# Verify the latency script exists
if [ ! -x "$LATENCY_SCRIPT" ]; then
    echo "Error: Cannot find or execute $LATENCY_SCRIPT" >&2
    exit 1
fi

# Calculate total number of runs
TOTAL_RUNS=$(( (MAX_IOPS - MIN_IOPS) / STEP_IOPS + 1 ))
CURRENT_RUN=0

echo "Starting fio latency vs rate sweep" >&2
echo "Configuration:" >&2
echo "  IOPS range: $MIN_IOPS to $MAX_IOPS (step: $STEP_IOPS)" >&2
echo "  Total runs: $TOTAL_RUNS" >&2
echo "  Output directory: $OUTPUT_DIR" >&2
echo "  Filename prefix: $PREFIX" >&2
echo "  Device: ${DEVICE:-/blk/ssd-0}" >&2
echo "  Block size: ${BLOCK_SIZE:-4k}" >&2
echo "  IO depth: ${IO_DEPTH:-1}" >&2
echo "  Runtime per test: ${RUNTIME:-10}s" >&2
echo "" >&2

# Create a summary file
SUMMARY_FILE="$OUTPUT_DIR/${PREFIX}--latency-vs-rate-summary.csv"
echo "# Latency vs Rate Summary" > "$SUMMARY_FILE"
echo "# Generated: $(date -Iseconds)" >> "$SUMMARY_FILE"
echo "# Device: ${DEVICE:-/blk/ssd-0}, Block size: ${BLOCK_SIZE:-4k}, IO depth: ${IO_DEPTH:-1}, Runtime: ${RUNTIME:-10}s" >> "$SUMMARY_FILE"
echo "iops_limit,avg_latency_usec,stddev_usec,min_usec,max_usec,p50_usec,p90_usec,p99_usec,p999_usec,p9999_usec,total_ios" >> "$SUMMARY_FILE"

# Iterate through IOPS range
CURRENT_IOPS="$MIN_IOPS"
while [ "$CURRENT_IOPS" -le "$MAX_IOPS" ]; do
    CURRENT_RUN=$((CURRENT_RUN + 1))

    echo "======================================" >&2
    echo "Run $CURRENT_RUN/$TOTAL_RUNS: Testing at $CURRENT_IOPS IOPS" >&2
    echo "======================================" >&2

    # Generate output filename with zero-padded IOPS for better sorting
    # e.g., c6gd--fio-10000iops.csv, c6gd--fio-15000iops.csv
    OUTPUT_FILE="$OUTPUT_DIR/${PREFIX}--fio-${CURRENT_IOPS}iops.csv"

    # Run the latency test with the current IOPS limit
    # Export IOPS_LIMIT for the child script
    if IOPS_LIMIT="$CURRENT_IOPS" "$LATENCY_SCRIPT" > "$OUTPUT_FILE" 2>&1; then
        echo "Completed: $OUTPUT_FILE" >&2

        # Extract summary statistics from the output file for the summary CSV
        # Parse the header comments to extract statistics
        AVG=$(grep "# fio: ====> avg-lat=" "$OUTPUT_FILE" | sed -n 's/.*avg-lat=\([0-9.]*\).*/\1/p')
        STDDEV=$(grep "# fio: ====> avg-lat=" "$OUTPUT_FILE" | sed -n 's/.*std-dev=\([0-9.]*\).*/\1/p')
        MIN=$(grep "# fio: ---> <MIN> observation" "$OUTPUT_FILE" | sed -n 's/.*= \([0-9.]*\)/\1/p')
        MAX=$(grep "# fio: ---> <MAX> observation" "$OUTPUT_FILE" | sed -n 's/.*= \([0-9.]*\)/\1/p')
        P50=$(grep "# fio: ---> percentile 50.00" "$OUTPUT_FILE" | sed -n 's/.*= \([0-9.]*\)/\1/p')
        P90=$(grep "# fio: ---> percentile 90.00" "$OUTPUT_FILE" | sed -n 's/.*= \([0-9.]*\)/\1/p')
        P99=$(grep "# fio: ---> percentile 99.00" "$OUTPUT_FILE" | sed -n 's/.*= \([0-9.]*\)/\1/p')
        P999=$(grep "# fio: ---> percentile 99.90" "$OUTPUT_FILE" | sed -n 's/.*= \([0-9.]*\)/\1/p')
        P9999=$(grep "# fio: ---> percentile 99.99" "$OUTPUT_FILE" | sed -n 's/.*= \([0-9.]*\)/\1/p')
        TOTAL_IOS=$(grep "# fio: Total .* observations" "$OUTPUT_FILE" | sed -n 's/.*Total \([0-9]*\) observations/\1/p')

        # Append to summary file
        echo "$CURRENT_IOPS,$AVG,$STDDEV,$MIN,$MAX,$P50,$P90,$P99,$P999,$P9999,$TOTAL_IOS" >> "$SUMMARY_FILE"

        echo "Summary: avg=${AVG}us, p50=${P50}us, p99=${P99}us, p999=${P999}us" >&2
    else
        echo "Error: Failed to run test at $CURRENT_IOPS IOPS" >&2
        echo "Check $OUTPUT_FILE for details" >&2
        # Continue with next iteration instead of failing completely
    fi

    echo "" >&2

    # Increment IOPS
    CURRENT_IOPS=$((CURRENT_IOPS + STEP_IOPS))
done

echo "======================================" >&2
echo "All tests completed!" >&2
echo "Summary file: $SUMMARY_FILE" >&2
echo "Individual results: $OUTPUT_DIR/${PREFIX}--fio-*iops.csv" >&2
echo "======================================" >&2

# Display quick summary
echo "" >&2
echo "Quick Summary:" >&2
column -t -s',' "$SUMMARY_FILE" | head -20 >&2
