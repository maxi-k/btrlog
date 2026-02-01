#!/usr/bin/env sh
set -euo pipefail

# Configuration
DEVICE="${DEVICE:-/blk/ssd-0}"
IOPS_LIMIT="${IOPS_LIMIT:-1000}"  # Fixed throughput in IOPS
BLOCK_SIZE="${BLOCK_SIZE:-4k}"
IO_DEPTH="${IO_DEPTH:-1}"          # Queue depth
RUNTIME="${RUNTIME:-10}"           # Runtime in seconds
OUTPUT_PREFIX="${OUTPUT_PREFIX:-fio-latency}"
WARMUP="${WARMUP:-2}"              # Warmup time in seconds

# Temp directory for fio logs
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

echo "# Running fio latency benchmark on $DEVICE" >&2
echo "# IOPS limit: $IOPS_LIMIT, Block size: $BLOCK_SIZE, Runtime: ${RUNTIME}s, Queue depth: $IO_DEPTH" >&2

# Run fio with latency logging
# Key parameters:
# - ioengine=libaio: async I/O for better performance
# - direct=1: O_DIRECT to bypass page cache
# - rw=randwrite: random writes (can be changed to write, randread, read, etc.)
# - bs=$BLOCK_SIZE: block size for each I/O
# - iodepth=$IO_DEPTH: number of I/O units to keep in flight
# - rate_iops=$IOPS_LIMIT: limit IOPS to fixed rate
# - write_lat_log: enable per-IO latency logging
# - log_avg_msec=0: log every single I/O (no averaging)
fio --name=latency_test \
    --ioengine=libaio \
    --direct=1 \
    --rw=randwrite \
    --bs="$BLOCK_SIZE" \
    --iodepth="$IO_DEPTH" \
    --rate_iops="$IOPS_LIMIT" \
    --numjobs=1 \
    --runtime="$RUNTIME" \
    --time_based \
    --group_reporting=1 \
    --filename="$DEVICE" \
    --write_lat_log="$TMPDIR/$OUTPUT_PREFIX" \
    --log_avg_msec=0 \
    --output="$TMPDIR/fio-output.txt" \
    >&2

# Parse the fio output to extract statistics
LAT_LOG="$TMPDIR/${OUTPUT_PREFIX}_lat.1.log"

if [ ! -f "$LAT_LOG" ]; then
    echo "Error: Latency log not found at $LAT_LOG" >&2
    exit 1
fi

# Count total I/Os
TOTAL_IOS=$(wc -l < "$LAT_LOG")

# Calculate statistics from the latency log
# Format: time(msec), latency(nsec), direction, block_size, offset, priority, issue_time
# We'll convert latency from nsec to usec for consistency with sockperf
echo "# Processing $TOTAL_IOS I/O operations..." >&2

# Use awk to calculate statistics and convert to microseconds
STATS=$(awk -F',' '
BEGIN {
    min = 999999999;
    max = 0;
    sum = 0;
    sum_sq = 0;
    count = 0;
}
{
    # Latency is in column 2, convert from nsec to usec
    lat = $2 / 1000.0;
    latencies[count] = lat;
    count++;
    sum += lat;
    sum_sq += lat * lat;
    if (lat < min) min = lat;
    if (lat > max) max = lat;
}
END {
    if (count > 0) {
        avg = sum / count;
        stddev = sqrt(sum_sq / count - avg * avg);

        # Sort latencies for percentile calculation
        n = asort(latencies, sorted);

        # Calculate percentiles
        p25 = sorted[int(n * 0.25)];
        p50 = sorted[int(n * 0.50)];
        p75 = sorted[int(n * 0.75)];
        p90 = sorted[int(n * 0.90)];
        p99 = sorted[int(n * 0.99)];
        p999 = sorted[int(n * 0.999)];
        p9999 = sorted[int(n * 0.9999)];

        printf "avg=%.3f stddev=%.3f min=%.3f max=%.3f\n", avg, stddev, min, max;
        printf "p25=%.3f p50=%.3f p75=%.3f p90=%.3f p99=%.3f p999=%.3f p9999=%.3f\n",
               p25, p50, p75, p90, p99, p999, p9999;
    }
}
' "$LAT_LOG")

AVG_STATS=$(echo "$STATS" | head -1)
PCT_STATS=$(echo "$STATS" | tail -1)

# Extract individual values for header
AVG_RTT=$(echo "$AVG_STATS" | grep -o 'avg=[0-9.]*' | cut -d= -f2)
STDDEV=$(echo "$AVG_STATS" | grep -o 'stddev=[0-9.]*' | cut -d= -f2)
MIN_LAT=$(echo "$AVG_STATS" | grep -o 'min=[0-9.]*' | cut -d= -f2)
MAX_LAT=$(echo "$AVG_STATS" | grep -o 'max=[0-9.]*' | cut -d= -f2)

# Print header in sockperf-like format
cat <<EOF
# ------------------------------
# test was performed using the following parameters: --iops=$IOPS_LIMIT --bs=$BLOCK_SIZE --iodepth=$IO_DEPTH --runtime=${RUNTIME}s --device=$DEVICE
# ------------------------------
# fio: [Total Run] RunTime=${RUNTIME}.000 sec; Warm up time=${WARMUP} sec; TotalIOs=$TOTAL_IOS
# fio: ========= Latency Statistics
# fio: ====> avg-lat=${AVG_RTT} (std-dev=${STDDEV})
# fio: Total $TOTAL_IOS observations
# fio: ---> <MAX> observation = $MAX_LAT
# NOTE: latency is accurate to nanoseconds, submission and completion timestamp are only accurate to milliseconds
# and inferred from the latency 
EOF

# Print percentiles
echo "$PCT_STATS" | awk -F'[= ]' '{
    printf "# fio: ---> percentile 99.99 = %s\n", $14;
    printf "# fio: ---> percentile 99.90 = %s\n", $12;
    printf "# fio: ---> percentile 99.00 = %s\n", $10;
    printf "# fio: ---> percentile 90.00 = %s\n", $8;
    printf "# fio: ---> percentile 75.00 = %s\n", $6;
    printf "# fio: ---> percentile 50.00 = %s\n", $4;
    printf "# fio: ---> percentile 25.00 = %s\n", $2;
}'

echo "# fio: ---> <MIN> observation = $MIN_LAT"
echo "# ------------------------------"

# Print CSV header (matching sockperf format)
echo "io_num, issue_timestamp_sec, completion_timestamp_sec, latency_usec"

# Convert fio log to CSV format
# fio format: time(msec), latency(nsec), direction, block_size, offset, priority, issue_time(nsec)
# target format: io_num, issue_timestamp_sec, completion_timestamp_sec, latency_usec
awk -F',' '
{
    # time is in msec (column 1), issue_time is in nsec (column 7)
    completion_time_sec = $1 / 1000.0;
    latency_usec = $2 / 1000.0;
    issue_time_nsec = $7;

    # If issue_time is available and non-zero, calculate issue timestamp
    if (issue_time_nsec > 0) {
        issue_time_sec = issue_time_nsec / 1000000000.0;
    } else {
        # Fallback: approximate issue time from completion time - latency
        issue_time_sec = completion_time_sec - (latency_usec / 1000000.0);
    }

    printf "%d, %.9f, %.9f, %.3f\n", NR-1, issue_time_sec, completion_time_sec, latency_usec;
}
' "$LAT_LOG"

echo "# Benchmark completed successfully" >&2
