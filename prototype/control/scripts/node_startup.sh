#!/usr/bin/env sh

log() {
    echo "[node][startup][$(date --utc)] $@\n" >> /tmp/node_startup.log
}

## --------------------------------------------------------------------------------
log "installing required packages"
# Install required packages
# no sudo required, as the script is run as root (and it would get stuck waiting for a password)
apt-get update
DEBIAN_FRONTEND=noninteractive \
    apt-get install -yq \
    curl unzip gdb rust-gdb rustc \
    iperf iperf3 iotop sockperf \
    tmux htop nethogs glances jq hwloc \
    liburing-dev linux-libc-dev patchelf \
    libtbb-dev openjdk-17-jdk libsqlite3-dev libmysqlcppconn-dev libpq-dev libfuse-dev libgflags-dev
# ^ leanstore 
(
    cd /home/ubuntu
    git clone https://github.com/brendangregg/FlameGraph /home/ubuntu/FlameGraph
    echo "perf script > out.perf && ./FlameGraph/stackcollapse-perf.pl out.perf > out.folded && ./FlameGraph/flamegraph.pl out.folded > perf.svg" > perf-to-flamegraph.sh
    chmod +x ./perf-to-flamegraph.sh
    chown -R ubuntu:ubuntu FlameGraph perf-to-flamegraph.sh
)

## --------------------------------------------------------------------------------
log "configuring system for perfevent & vm overcommit"
sysctl -w vm.overcommit_memory=1
sysctl -w kernel.kptr_restrict=0
sysctl -w kernel.perf_event_paranoid=-1

## --------------------------------------------------------------------------------
log "setting open file limit for ubuntu user to 65536"
cat >> /etc/security/limits.conf <<EOF
ubuntu soft nofile 65536
ubuntu hard nofile 65536
EOF

## --------------------------------------------------------------------------------
log "linking local ssds to /blk/ssd-* and setting permissions"
mkdir -p /blk
id=0
for disk in $(ls /dev/disk/by-id/ | grep Instance_Storage | grep -v _1); do
    ln -s /dev/disk/by-id/$disk /blk/ssd-$id
    id=$((id+1))
done

# Create disk group if it doesn't exist, add ubuntu user to it, and set block device permissions
groupadd -f disk
usermod -a -G disk ubuntu
chmod g+rw /blk/ssd-*

## --------------------------------------------------------------------------------
log "fetching instance metadata"
## requires that token auth for IMDSv2 is set to be optional
name=$(curl http://169.254.169.254/latest/meta-data/tags/instance/Name)
ip=$(curl http://169.254.169.254/latest/meta-data/local-ipv4)
clusterSize=$(curl http://169.254.169.254/latest/meta-data/tags/instance/ClusterSize)
nodeId=$(curl http://169.254.169.254/latest/meta-data/tags/instance/NodeId)
region=$(curl http://169.254.169.254/latest/meta-data/placement/region)
loggerInstance=$(curl http://169.254.169.254/latest/meta-data/tags/instance/LoggerInstance)
primaryInstance=$(curl http://169.254.169.254/latest/meta-data/tags/instance/PrimaryInstance)
log "metadata service provided name $name, cluster size $clusterSize, node id $nodeId"

## --------------------------------------------------------------------------------
log "fetching IAM credentials from IMDS"
# Get the role name
role_name=$(curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/)
if [ -z "$role_name" ]; then
    log "ERROR: Failed to retrieve IAM role name from IMDS"
    exit 1
fi
log "retrieved IAM role name: $role_name"

# Get the credentials JSON
creds_json=$(curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/$role_name)
if [ -z "$creds_json" ]; then
    log "ERROR: Failed to retrieve credentials for role $role_name from IMDS"
    exit 1
fi

# Parse credentials using jq
access_key=$(echo "$creds_json" | jq -r '.AccessKeyId')
secret_key=$(echo "$creds_json" | jq -r '.SecretAccessKey')
session_token=$(echo "$creds_json" | jq -r '.Token')

if [ "$access_key" = "null" ] || [ "$secret_key" = "null" ] || [ "$session_token" = "null" ]; then
    log "ERROR: Failed to parse credentials from IMDS response"
    log "IMDS response: $creds_json"
    exit 1
fi

log "successfully retrieved AWS credentials from IMDS"

## --------------------------------------------------------------------------------
log "setting global environment variables"
assigns="
export JOURNAL_PLATFORM=aws
export JOURNAL_NODE_NAME=$name
export JOURNAL_NODE_ID=$nodeId
export JOURNAL_NODE_IP_$nodeId=$ip
export JOURNAL_CLUSTER_SIZE=$clusterSize
export JOURNAL_LOGGER_INSTANCE=$loggerInstance
export JOURNAL_PRIMARY_INSTANCE=$primaryInstance
export JOURNAL_SSD_COUNT=$((id))
export AWS_REGION=$region
export AWS_ACCESS_KEY_ID=$access_key
export AWS_SECRET_ACCESS_KEY=$secret_key
export AWS_SESSION_TOKEN=$session_token
# leanstore
export LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:/usr/local/lib/:/usr/lib/x86_64-linux-gnu/:/usr/lib/jvm/java-17-openjdk-amd64/lib/server/:/usr/lib/jvm/java-17-openjdk-amd64/lib/
"

# really make sure, /etc/environment is flaky sometimes for some reason
# should find out why...
echo "$assigns" >> /etc/environment
echo "$assigns" > /etc/profile.d/99-journal-node-env.sh
echo "echo startup script successful. node id is $nodeId" > /etc/profile.d/99-journal-node-env.sh

## --------------------------------------------------------------------------------
log "generating sockperf scripts"

sockperf_cmd=""
for id in $( seq 0 $(($clusterSize-2)) ); do
    sockperf_cmd="$sockperf_cmd\necho '---------------------------------------- node $id ----------------------------------------';\nsockperf pp --burst 1 --full-rtt --ip \$JOURNAL_NODE_IP_$id;"
done

echo "
#!/usr/bin/env bash
if [ $nodeId -eq $(($clusterSize-1)) ]; then
  $sockperf_cmd
  echo 'done'
else
  sockperf server
fi
" > /home/ubuntu/check-latency
chmod +x /home/ubuntu/check-latency

## --------------------------------------------------------------------------------
log "startup script finished. node id $nodeId"
