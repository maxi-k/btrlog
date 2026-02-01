#!/usr/bin/env sh

start_node() {
    nodeId=$1; shift
    size=$1; shift
    bin=$1; shift
    (   # subshell
        export JOURNAL_PLATFORM=local
        export JOURNAL_NODE_ID=$nodeId
        export JOURNAL_CLUSTER_SIZE=$size
        for i in $(seq 0 $size); do
            ipvar=JOURNAL_NODE_IP_$i
            export $ipvar=127.0.$i.1
        done
        # start binary with proper argument passing
        # exec binary with all CLI args, redirecting stdout/stderr
        savedEnv=/tmp/journal.$nodeId.env
        args="$@"
        if [ "$DEBUG_NODE" = "$nodeId" ] || [ "$DEBUG_NODE" = "all" ]; then
            export -p > $savedEnv
            echo "debug-attaching node $nodeId with '$bin $@' | log level $RUST_LOG"
            exec "$TERMINAL" -e $SHELL -c "source $savedEnv; cd $(pwd); ( rust-gdb --ex='break rust_panic' --args $bin $args )"
        elif [ "$PERF_NODE" = "$nodeId" ] || [ "$PERF_NODE" = "all" ]; then
            export -p > $savedEnv
            echo "perfing node $nodeId with '$bin $@' | log level $RUST_LOG"
            exec "$TERMINAL" -e $SHELL -c "source $savedEnv; cd $(pwd); echo 'perfing node $nodeId'; ( perf record -o perf.$nodeId.out $PERF_ARGS $bin $args ); perf report -i perf.$nodeId.out"
        elif [ ! -z "$LAUNCH_TERMINALS" ]; then
            export -p > $savedEnv
            echo "launching $nodeId in new $TERMINAL with '$bin $@' | log level $RUST_LOG"
            # > '/tmp/journal.$nodeId.log' 2> '/tmp/journal.$nodeId.err'
            exec "$TERMINAL" -e $SHELL -c "source $savedEnv; cd $(pwd); echo 'node $nodeId'; $bin $args"
        else
            echo "starting node $nodeId with '$bin $@' | log level $RUST_LOG"
            exec "$bin" $args > "/tmp/journal.$nodeId.log" 2> "/tmp/journal.$nodeId.err"
        fi
    )
}

onerr() {
    msg=$1
    echo "ERR: $msg; exiting"
    exit 1
}

usage() {
    echo "usage: $0 <cluster-size> <binary> <...args>"
    echo "  respected environment variables: "
    echo "  - DEBUG_NODE=[<nodeId>|'all']: which node to start inside rust-gdb"
    echo "  - PERF_NODE=[<nodeId>|'all']: which node to start inside perf record"
    echo "  - LAUNCH_TERMINALS=[nonempty]: whether to launch a terminal for each node"
    exit 1
}

test -z "$1" && usage
size=$1; shift
test -z "$1" && usage
binary=$1; shift
echo "[ctrl] will pass these args to each node: $@"

test -f "$binary" || onerr "binary $binary does not exist"

logfiles=""
errfiles=""
pids=""
trap 'kill 0' EXIT # kill all children when 'C-c'-ing or similar
for n in $(seq 0 $((size - 1))); do
    echo "" > /tmp/journal.$n.log; echo "" > /tmp/journal.$n.err # clean up prev logs
    logfiles="$logfiles /tmp/journal.$n.log"
    errfiles="$errfiles /tmp/journal.$n.err"
    start_node "$n" "$size" "$binary" "$@" &
    pids="$pids $!"
done
tail -f $logfiles $errfiles &
logpid=$!

wait $pids

echo "nodes finished, stopping log process..."
kill $logpid
