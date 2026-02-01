#!/usr/bin/env sh
set -euo pipefail

# Usage: ./import-to-duckdb.sh <results_dir> <db_file> <tag>
# Example: ./import-to-duckdb.sh results/microbench results.duckdb c6gd

if [ $# -lt 3 ]; then
    echo "Usage: $0 <results_dir> <db_file> <tag>" >&2
    echo "" >&2
    echo "Arguments:" >&2
    echo "  results_dir - Directory containing CSV result files" >&2
    echo "  db_file     - Path to DuckDB database file (created if doesn't exist)" >&2
    echo "  tag         - Tag/label for this import (e.g., instance type like 'c6gd')" >&2
    echo "" >&2
    echo "Example:" >&2
    echo "  $0 results/microbench results.duckdb c6gd" >&2
    echo "" >&2
    echo "This will import all fio and sockperf CSV files from results_dir into the database." >&2
    exit 1
fi

RESULTS_DIR="$1"
DB_FILE="$2"
TAG="$3"

if [ ! -d "$RESULTS_DIR" ]; then
    echo "Error: Results directory '$RESULTS_DIR' does not exist" >&2
    exit 1
fi

echo "Importing results from $RESULTS_DIR into $DB_FILE with tag '$TAG'" >&2
echo "" >&2

# Create a temporary SQL script
TEMP_SQL=$(mktemp)
trap "rm -f $TEMP_SQL" EXIT

# Build SQL script for schema and imports
cat > "$TEMP_SQL" <<'EOF'
-- Create tables if they don't exist

CREATE TABLE IF NOT EXISTS runs (
    runid INTEGER PRIMARY KEY,
    rate_limit INTEGER NOT NULL,
    tag VARCHAR NOT NULL,
    type VARCHAR NOT NULL,  -- 'fio' or 'sockperf'
    avg_latency_us DOUBLE,
    stddev_us DOUBLE,
    min_us DOUBLE,
    max_us DOUBLE,
    p25_us DOUBLE,
    p50_us DOUBLE,
    p75_us DOUBLE,
    p90_us DOUBLE,
    p99_us DOUBLE,
    p999_us DOUBLE,
    p9999_us DOUBLE,
    p99999_us DOUBLE,
    total_ios INTEGER,
    sent_msgs INTEGER,
    recv_msgs INTEGER,
    imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS trace (
    runid INTEGER NOT NULL,
    rate_limit INTEGER NOT NULL,
    id INTEGER NOT NULL,           -- packet/io id
    start_time DOUBLE NOT NULL,    -- start timestamp in seconds
    end_time DOUBLE NOT NULL,      -- end timestamp in seconds
    latency_us DOUBLE NOT NULL,
    FOREIGN KEY (runid) REFERENCES runs(runid)
);

-- Create indexes on trace for faster queries
-- CREATE INDEX IF NOT EXISTS idx_trace_runid ON trace(runid);
-- CREATE INDEX IF NOT EXISTS idx_trace_runid_rate ON trace(runid, rate_limit);

EOF

echo "Creating/checking database schema..." >&2
duckdb "$DB_FILE" < "$TEMP_SQL" >&2

# Get the next available runid
NEXT_RUNID=$(duckdb "$DB_FILE" -csv -noheader -c 'SELECT COALESCE(MAX(runid), 0) + 1 FROM runs;')

echo "Starting import with runid $NEXT_RUNID" >&2
echo "" >&2

# Find all CSV files matching our patterns
FIO_FILES=$(find "$RESULTS_DIR" -maxdepth 1 -name "${TAG}--fio-*iops.csv" -type f | sort || true)
SOCKPERF_FILES=$(find "$RESULTS_DIR" -maxdepth 1 -name "${TAG}--sockperf-*mps.csv" -type f | sort || true)

TOTAL_FILES=$(echo "$FIO_FILES $SOCKPERF_FILES" | wc -w)

if [ "$TOTAL_FILES" -eq 0 ]; then
    echo "Warning: No matching CSV files found for tag '$TAG' in $RESULTS_DIR" >&2
    echo "Looking for patterns: ${TAG}--fio-*iops.csv or ${TAG}--sockperf-*mps.csv" >&2
    exit 1
fi

echo "Found $TOTAL_FILES files to import" >&2
echo "" >&2

# Function to extract rate from filename
extract_fio_rate() {
    basename "$1" | sed -n 's/.*--fio-\([0-9]*\)iops\.csv/\1/p'
}

extract_sockperf_rate() {
    basename "$1" | sed -n 's/.*--sockperf-\([0-9]*\)mps\.csv/\1/p'
}

CURRENT_RUNID=$NEXT_RUNID
CURRENT_FILE=0

# Process fio files
for csv_file in $FIO_FILES; do
    CURRENT_FILE=$((CURRENT_FILE + 1))
    RATE=$(extract_fio_rate "$csv_file")

    if [ -z "$RATE" ]; then
        echo "  Warning: Could not extract rate from filename: $csv_file" >&2
        continue
    fi

    echo "[$CURRENT_FILE/$TOTAL_FILES] Importing: $(basename "$csv_file") (fio, rate=${RATE} IOPS, runid=$CURRENT_RUNID)" >&2

    # Extract statistics from CSV header comments
    AVG=$(grep "avg-lat=" "$csv_file" | sed -n 's/.*avg-lat=\([0-9.]*\).*/\1/p' | head -1)
    STDDEV=$(grep "std-dev=" "$csv_file" | sed -n 's/.*std-dev=\([0-9.]*\).*/\1/p' | head -1)
    MIN=$(grep "<MIN> observation" "$csv_file" | sed -n 's/.*= \([0-9.]*\)/\1/p' | head -1)
    MAX=$(grep "<MAX> observation" "$csv_file" | sed -n 's/.*= \([0-9.]*\)/\1/p' | head -1)
    P25=$(grep "percentile 25.00" "$csv_file" | sed -n 's/.*= \([0-9.]*\)/\1/p' | head -1)
    P50=$(grep "percentile 50.00" "$csv_file" | sed -n 's/.*= \([0-9.]*\)/\1/p' | head -1)
    P75=$(grep "percentile 75.00" "$csv_file" | sed -n 's/.*= \([0-9.]*\)/\1/p' | head -1)
    P90=$(grep "percentile 90.00" "$csv_file" | sed -n 's/.*= \([0-9.]*\)/\1/p' | head -1)
    P99=$(grep "percentile 99.00" "$csv_file" | sed -n 's/.*= \([0-9.]*\)/\1/p' | head -1)
    P999=$(grep "percentile 99.90" "$csv_file" | sed -n 's/.*= \([0-9.]*\)/\1/p' | head -1)
    P9999=$(grep "percentile 99.99" "$csv_file" | sed -n 's/.*= \([0-9.]*\)/\1/p' | head -1)
    TOTAL_IOS=$(grep "Total .* observations" "$csv_file" | sed -n 's/.*Total \([0-9]*\) observations/\1/p' | head -1)

    # Create import SQL
    IMPORT_SQL=$(mktemp)
    cat > "$IMPORT_SQL" <<SQLEND
-- Import fio run $CURRENT_RUNID
INSERT INTO runs (runid, rate_limit, tag, type, avg_latency_us, stddev_us, min_us, max_us,
                  p25_us, p50_us, p75_us, p90_us, p99_us, p999_us, p9999_us, p99999_us,
                  total_ios, sent_msgs, recv_msgs)
VALUES ($CURRENT_RUNID, $RATE, '$TAG', 'fio',
        ${AVG:-NULL}, ${STDDEV:-NULL}, ${MIN:-NULL}, ${MAX:-NULL},
        ${P25:-NULL}, ${P50:-NULL}, ${P75:-NULL}, ${P90:-NULL},
        ${P99:-NULL}, ${P999:-NULL}, ${P9999:-NULL}, NULL,
        ${TOTAL_IOS:-NULL}, NULL, NULL);

-- Import trace data directly from CSV
INSERT INTO trace (runid, rate_limit, id, start_time, end_time, latency_us)
SELECT $CURRENT_RUNID, $RATE,
       io_num, issue_timestamp_sec, completion_timestamp_sec, latency_usec
FROM read_csv('$csv_file', comment='#', header=true, auto_detect=true);
SQLEND

    duckdb "$DB_FILE" < "$IMPORT_SQL" 2>&1 | grep -v "^$" >&2 || true
    rm "$IMPORT_SQL"

    CURRENT_RUNID=$((CURRENT_RUNID + 1))
done

# Process sockperf files
for csv_file in $SOCKPERF_FILES; do
    CURRENT_FILE=$((CURRENT_FILE + 1))
    RATE=$(extract_sockperf_rate "$csv_file")

    if [ -z "$RATE" ]; then
        echo "  Warning: Could not extract rate from filename: $csv_file" >&2
        continue
    fi

    echo "[$CURRENT_FILE/$TOTAL_FILES] Importing: $(basename "$csv_file") (sockperf, rate=${RATE} MPS, runid=$CURRENT_RUNID)" >&2

    # Extract statistics (use head -1 to get only first match)
    #AVG=$(grep "avg-rtt=" "$csv_file" | sed -n 's/.*avg-rtt=\([0-9.]*\).*/\1/p' | head -1)
    #STDDEV=$(grep "std-dev=" "$csv_file" | sed -n 's/.*std-dev=\([0-9.]*\).*/\1/p' | head -1)
    #MIN=$(grep "<MIN> observation" "$csv_file" | sed -n 's/.*= *\([0-9.]*\)/\1/p' | head -1)
    #MAX=$(grep "<MAX> observation" "$csv_file" | sed -n 's/.*= *\([0-9.]*\)/\1/p' | head -1)
    #P25=$(grep "percentile 25.0" "$csv_file" | sed -n 's/.*= *\([0-9.]*\)/\1/p' | head -1)
    #P50=$(grep "percentile 50.0" "$csv_file" | sed -n 's/.*= *\([0-9.]*\)/\1/p' | head -1)
    #P75=$(grep "percentile 75.0" "$csv_file" | sed -n 's/.*= *\([0-9.]*\)/\1/p' | head -1)
    #P90=$(grep "percentile 90.0" "$csv_file" | sed -n 's/.*= *\([0-9.]*\)/\1/p' | head -1)
    #P99=$(grep "percentile 99.0" "$csv_file" | sed -n 's/.*= *\([0-9.]*\)/\1/p' | head -1)
    #P999=$(grep "percentile 99.9" "$csv_file" | sed -n 's/.*= *\([0-9.]*\)/\1/p' | head -1)
    #P9999=$(grep "percentile 99.99" "$csv_file" | sed -n 's/.*= *\([0-9.]*\)/\1/p' | head -1)
    #P99999=$(grep "percentile 99.999" "$csv_file" | sed -n 's/.*= *\([0-9.]*\)/\1/p' | head -1)
    #SENT_MSGS=$(grep "SentMessages=" "$csv_file" | sed -n 's/.*SentMessages=\([0-9]*\).*/\1/p' | head -1)
    #RECV_MSGS=$(grep "ReceivedMessages=" "$csv_file" | sed -n 's/.*ReceivedMessages=\([0-9]*\).*/\1/p' | head -1)

    # Create import SQL
    IMPORT_SQL=$(mktemp)
    cat > "$IMPORT_SQL" <<SQLEND
-- Import sockperf run $CURRENT_RUNID
INSERT INTO runs (runid, rate_limit, tag, type, avg_latency_us, stddev_us, min_us, max_us,
                  p25_us, p50_us, p75_us, p90_us, p99_us, p999_us, p9999_us, p99999_us,
                  total_ios, sent_msgs, recv_msgs)
VALUES ($CURRENT_RUNID, $RATE, '$TAG', 'sockperf',
        ${AVG:-NULL}, ${STDDEV:-NULL}, ${MIN:-NULL}, ${MAX:-NULL},
        ${P25:-NULL}, ${P50:-NULL}, ${P75:-NULL}, ${P90:-NULL},
        ${P99:-NULL}, ${P999:-NULL}, ${P9999:-NULL}, ${P99999:-NULL},
        NULL, ${SENT_MSGS:-NULL}, ${RECV_MSGS:-NULL});

-- Import trace data directly from CSV
INSERT INTO trace (runid, rate_limit, id, start_time, end_time, latency_us)
SELECT $CURRENT_RUNID, $RATE,
       packet, tx_timestamp_sec, rx_timestamp_sec, rtt_usec
FROM read_csv('$csv_file', comment='#', header=true, auto_detect=true);
SQLEND

    echo "executing $IMPORT_SQL" >&2

    duckdb "$DB_FILE" < "$IMPORT_SQL" 2>&1 | grep -v "^$" >&2 || true
    rm "$IMPORT_SQL"

    CURRENT_RUNID=$((CURRENT_RUNID + 1))
done

echo "" >&2
echo "Import completed successfully!" >&2
echo "Database: $DB_FILE" >&2
echo "Imported runs: $NEXT_RUNID to $((CURRENT_RUNID - 1))" >&2
echo "" >&2

# Show summary
echo "Summary:" >&2
duckdb "$DB_FILE" -c "
SELECT
    tag, type, COUNT(*) as num_runs,
    MIN(rate_limit) as min_rate, MAX(rate_limit) as max_rate,
    MIN(p50_us) as min_p50_us, MAX(p50_us) as max_p50_us
FROM runs
WHERE tag = '$TAG'
GROUP BY tag, type
ORDER BY tag, type;
" >&2

echo "" >&2
echo "Total rows in trace table:" >&2
duckdb "$DB_FILE" -c "SELECT COUNT(*) as total_trace_rows FROM trace;" >&2
