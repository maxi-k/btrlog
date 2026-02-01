#!/usr/bin/env bash

log() {
    echo "[bookkeeper][startup][$(date --utc)] $@" >> /tmp/node_startup.log
}

## --------------------------------------------------------------------------------
log "installing required packages for bookkeeper"
# Install required packages (including Java)
apt-get update
DEBIAN_FRONTEND=noninteractive \
    apt-get install -yq \
    iperf iperf3 curl unzip gdb awscli sockperf \
    tmux htop nethogs glances jq netcat-openbsd \
    openjdk-11-jdk-headless \
    liburing-dev linux-libc-dev patchelf

# Set JAVA_HOME
echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> /etc/environment
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

(
    cd /home/ubuntu
    git clone https://github.com/brendangregg/FlameGraph /home/ubuntu/FlameGraph
    echo "perf script > out.perf && ./FlameGraph/stackcollapse-perf.pl out.perf > out.folded && ./FlameGraph/flamegraph.pl out.folded > perf.svg" > perf-to-flamegraph.sh
    chmod +x ./perf-to-flamegraph.sh
    chown -R ubuntu:ubuntu FlameGraph perf-to-flamegraph.sh
)

## --------------------------------------------------------------------------------
log "configuring system for perfevent"
sysctl -w vm.overcommit_memory=1
sysctl -w kernel.kptr_restrict=0
sysctl -w kernel.perf_event_paranoid=-1

## --------------------------------------------------------------------------------
log "setting up RAID0 array from local SSDs"
mkdir -p /blk

# Install mdadm for software RAID
apt-get install -y mdadm

# Collect all SSD devices
SSD_DEVICES=""
id=0
for disk in $(ls /dev/disk/by-id/ | grep Instance_Storage | grep -v _1); do
    device_path="/dev/disk/by-id/$disk"
    # Link for reference
    ln -s "$device_path" "/blk/ssd-$id"
    # Add to RAID array
    SSD_DEVICES="$SSD_DEVICES $device_path"
    id=$((id+1))
done

log "found $id SSDs: $SSD_DEVICES"

if [ $id -gt 0 ]; then
    # Create RAID0 array
    log "creating RAID0 array /dev/md0 with devices: $SSD_DEVICES"
    mdadm --create /dev/md0 --level=0 --raid-devices=$id $SSD_DEVICES --force
    
    # Format with XFS
    log "formatting RAID array with XFS"
    mkfs.xfs -f /dev/md0
    
    # Mount the RAID array
    log "mounting RAID array to /blk/raid"
    mkdir -p /blk/raid
    mount /dev/md0 /blk/raid
    
    # Add to fstab for persistence
    echo "/dev/md0 /blk/raid xfs defaults,noatime 0 2" >> /etc/fstab
    
    # Create disk group if it doesn't exist, add ubuntu user to it, and set permissions
    groupadd -f disk
    usermod -a -G disk ubuntu
    chmod g+rw /dev/md0
    
    # Create directories for BookKeeper and ZooKeeper on RAID
    mkdir -p /blk/raid/{journal,ledgers,zk-data,zk-logs}
    chown -R ubuntu:ubuntu /blk/raid/
    
    log "RAID setup complete, mounted at /blk/raid"
else
    log "ERROR: No SSDs found for RAID setup"
    # exit 1
fi

## --------------------------------------------------------------------------------
log "fetching instance metadata"
## requires that token auth for IMDSv2 is set to be optional
name=$(curl -s http://169.254.169.254/latest/meta-data/tags/instance/Name)
ip=$(curl -s http://169.254.169.254/latest/meta-data/local-ipv4)
clusterSize=$(curl -s http://169.254.169.254/latest/meta-data/tags/instance/ClusterSize)
nodeId=$(curl -s http://169.254.169.254/latest/meta-data/tags/instance/NodeId)
region=$(curl -s http://169.254.169.254/latest/meta-data/placement/region)
loggerInstance=$(curl -s http://169.254.169.254/latest/meta-data/tags/instance/LoggerInstance)
primaryInstance=$(curl -s http://169.254.169.254/latest/meta-data/tags/instance/PrimaryInstance)
log "metadata service provided name $name, cluster size $clusterSize, node id $nodeId"

## --------------------------------------------------------------------------------
log "fetching IAM credentials from IMDS"
# Get the role name
role_name=$(curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/)
if [ -z "$role_name" ]; then
    log "ERROR: Failed to retrieve IAM role name from IMDS"
    # exit 1
fi
log "retrieved IAM role name: $role_name"

# Get the credentials JSON
creds_json=$(curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/$role_name)
if [ -z "$creds_json" ]; then
    log "ERROR: Failed to retrieve credentials for role $role_name from IMDS"
    # exit 1
fi

# Parse credentials using jq
access_key=$(echo "$creds_json" | jq -r '.AccessKeyId')
secret_key=$(echo "$creds_json" | jq -r '.SecretAccessKey')
session_token=$(echo "$creds_json" | jq -r '.Token')

if [ "$access_key" = "null" ] || [ "$secret_key" = "null" ] || [ "$session_token" = "null" ]; then
    log "ERROR: Failed to parse credentials from IMDS response"
    log "IMDS response: $creds_json"
    # exit 1
fi

log "successfully retrieved AWS credentials from IMDS"

## --------------------------------------------------------------------------------
log "downloading and installing ZooKeeper and BookKeeper"

# Download ZooKeeper
ZOOKEEPER_VERSION="3.8.5"
cd /opt
(
    if [ ! -d "apache-zookeeper-${ZOOKEEPER_VERSION}-bin" ]; then
        log "downloading ZooKeeper ${ZOOKEEPER_VERSION}"
        # Try S3 first (faster), fallback to archive.apache.org if unavailable
        if ! aws s3 cp "s3://journal-service-bookkeeper/apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz" .; then
            log "S3 download failed, falling back to archive.apache.org"
            wget -q "https://archive.apache.org/dist/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz"
        fi
        tar -xzf "apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz"
        ln -sf "apache-zookeeper-${ZOOKEEPER_VERSION}-bin" zookeeper
        rm -f "apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz"
    fi
) &

# Download BookKeeper
BOOKKEEPER_VERSION="4.16.7"
(
    if [ ! -d "bookkeeper-server-${BOOKKEEPER_VERSION}" ]; then
        log "downloading BookKeeper ${BOOKKEEPER_VERSION}"
        # Try S3 first (faster), fallback to archive.apache.org if unavailable
        if ! aws s3 cp "s3://journal-service-bookkeeper/bookkeeper-server-${BOOKKEEPER_VERSION}-bin.tar.gz" .; then
            log "S3 download failed, falling back to archive.apache.org"
            wget -q "https://archive.apache.org/dist/bookkeeper/bookkeeper-${BOOKKEEPER_VERSION}/bookkeeper-server-${BOOKKEEPER_VERSION}-bin.tar.gz"
        fi
        tar -xzf "bookkeeper-server-${BOOKKEEPER_VERSION}-bin.tar.gz"
        ln -sf "bookkeeper-server-${BOOKKEEPER_VERSION}" bookkeeper
        rm -f "bookkeeper-server-${BOOKKEEPER_VERSION}-bin.tar.gz"
    fi
) &

wait
chown -R ubuntu:ubuntu zookeeper* bookkeeper*

## --------------------------------------------------------------------------------
log "determining node role and configuring services"

# Determine role based on node ID
# Nodes 0,1,2: ZooKeeper + BookKeeper bookie
# Node N-1: Benchmark client only
if [ "$nodeId" -lt "$((clusterSize - 1))" ]; then
    BOOKKEEPER_ROLE="bookie"
    log "configuring node as ZooKeeper + BookKeeper BOOKIE"
else
    BOOKKEEPER_ROLE="client"
    log "configuring node as benchmark CLIENT"
fi

## --------------------------------------------------------------------------------
if [ "$BOOKKEEPER_ROLE" = "bookie" ]; then
    log "configuring ZooKeeper cluster"
    
    # Create ZooKeeper configuration
    cat > /opt/zookeeper/conf/zoo.cfg << EOF
tickTime=2000
dataDir=/blk/raid/zk-data
dataLogDir=/blk/raid/zk-logs
clientPort=2181
initLimit=10
syncLimit=5
maxClientCnxns=0

# ZooKeeper cluster configuration
EOF

    # Add ZK cluster members (nodes 0 to clusterSize-2)
    for i in $(seq 0 $((clusterSize - 2))); do
        # hostname set in /etc/hosts by terraform  
        echo "server.$((i + 1))=journal-node-$i:2888:3888" >> /opt/zookeeper/conf/zoo.cfg
    done

    # Create ZK myid file
    echo "$((nodeId + 1))" > /blk/raid/zk-data/myid
    chown ubuntu:ubuntu /blk/raid/zk-data/myid

    log "creating BookKeeper bookie configuration"
    
    # Create bookie configuration
    cat > /opt/bookkeeper/conf/bk_server.conf << EOF
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Bookie settings
bookiePort=3181
httpServerPort=8080

# Storage directories on RAID
journalDirectories=/blk/raid/journal
ledgerDirectories=/blk/raid/ledgers
indexDirectories=/blk/raid/ledgers

# ZooKeeper settings - will be replaced with actual IPs by environment substitution
zkServers=\${ZK_SERVERS}
zkTimeout=30000

# Performance optimizations to match journal-service behavior
journalSyncData=true
journalAdaptiveGroupWrites=false
journalMaxGroupWaitMSec=0
journalAlignmentSize=4096
journalBufferedWritesThreshold=524288
journalBufferedEntriesThreshold=5000

# Use direct I/O for better performance
journalWriteData=true
journalRemoveFromPageCache=true

# Ledger storage settings
ledgerStorageClass=org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage

# Garbage collection
gcWaitTime=300000
minorCompactionInterval=3600
majorCompactionInterval=86400

# Netty settings
serverTcpNoDelay=true
clientTcpNoDelay=true

# Ledger cache
openFileLimit=20000
readBufferSizeBytes=4096
writeBufferSizeBytes=65536

# Enable statistics
enableStatistics=true

# Allow loopback for local testing
allowLoopback=false

# Set advertised address to this node's IP
advertisedAddress=journal-node-$nodeId
EOF

fi

## --------------------------------------------------------------------------------
log "setting global environment variables"

# Build ZooKeeper connection string
ZK_SERVERS=""
for i in $(seq 0 $((clusterSize - 2))); do
    if [ -z "$ZK_SERVERS" ]; then
        ZK_SERVERS="journal-node-$i:2181"
    else
        ZK_SERVERS="$ZK_SERVERS,journal-node-$i:2181"
    fi
done

assigns="
export BOOKKEEPER_ROLE=$BOOKKEEPER_ROLE
export BOOKKEEPER_VERSION=$BOOKKEEPER_VERSION
export ZOOKEEPER_VERSION=$ZOOKEEPER_VERSION
export ZK_SERVERS='$ZK_SERVERS'
export JOURNAL_PLATFORM=aws
export JOURNAL_NODE_NAME=$name
export JOURNAL_NODE_ID=$nodeId
export JOURNAL_NODE_IP_$nodeId=$ip
export JOURNAL_CLUSTER_SIZE=$clusterSize
export JOURNAL_LOGGER_INSTANCE=$loggerInstance
export JOURNAL_PRIMARY_INSTANCE=$primaryInstance
export JOURNAL_SSD_COUNT=$id
export AWS_REGION=$region
export AWS_ACCESS_KEY_ID=$access_key
export AWS_SECRET_ACCESS_KEY=$secret_key
export AWS_SESSION_TOKEN=$session_token
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
# export PATH=\$PATH:/opt/zookeeper/bin:/opt/bookkeeper/bin
"

# really make sure, /etc/environment is flaky sometimes for some reason
echo "$assigns" >> /etc/environment
echo "$assigns" > /etc/profile.d/99-bookkeeper-node-env.sh

## --------------------------------------------------------------------------------
log "creating service startup scripts"

# Create ZooKeeper startup script
cat > /home/ubuntu/start-zookeeper.sh << 'EOF'
#!/bin/bash
source /etc/profile.d/99-bookkeeper-node-env.sh

# Substitute environment variables in zoo.cfg
envsubst < /opt/zookeeper/conf/zoo.cfg > /tmp/zoo.cfg.resolved
mv -f /tmp/zoo.cfg.resolved /opt/zookeeper/conf/zoo.cfg

echo "Starting ZooKeeper..."
cd /opt/zookeeper
./bin/zkServer.sh start-foreground
EOF

# Create BookKeeper startup script  
cat > /home/ubuntu/start-bookie.sh << 'EOF'
#!/bin/bash
source /etc/profile.d/99-bookkeeper-node-env.sh

# Wait for ZooKeeper to be ready
echo "Waiting for ZooKeeper cluster to be ready..."
for i in {1..60}; do
    if nc -z $(echo $ZK_SERVERS | cut -d',' -f1 | cut -d':' -f1) 2181 2>/dev/null; then
        echo "ZooKeeper is ready"
        break
    fi
    if [ $i -eq 60 ]; then
        echo "ERROR: ZooKeeper not ready after 60 seconds"
        exit 1
    fi
    sleep 1
done

# Substitute environment variables in bookie config
envsubst < /opt/bookkeeper/conf/bk_server.conf > /tmp/bk_server.conf.resolved
mv -f /tmp/bk_server.conf.resolved /opt/bookkeeper/conf/bk_server.conf

# Initialize BookKeeper metadata in ZooKeeper (only needs to be done once per cluster)
echo "Initializing BookKeeper metadata..."
cd /opt/bookkeeper
./bin/bookkeeper shell metaformat -nonInteractive || echo "Metadata already exists or initialization failed"

echo "Starting BookKeeper bookie..."
./bin/bookkeeper bookie
EOF

# Create benchmark client startup script
cat > /home/ubuntu/start-benchmark.sh << 'EOF'
#!/bin/bash
source /etc/profile.d/99-bookkeeper-node-env.sh

# Wait for BookKeeper cluster to be ready
echo "Waiting for BookKeeper cluster to be ready..."
EXPECTED_BOOKIES=$((JOURNAL_CLUSTER_SIZE - 1))

for i in {1..120}; do
    # Check if we can connect to ZooKeeper
    if ! nc -z $(echo $ZK_SERVERS | cut -d',' -f1 | cut -d':' -f1) 2181 2>/dev/null; then
        echo "Waiting for ZooKeeper... ($i/120)"
        sleep 2
        continue
    fi
    
    # Check if expected number of bookies are registered
    # This is a simplified check - in production you might query ZK directly
    READY_BOOKIES=0
    for j in $(seq 0 $((JOURNAL_CLUSTER_SIZE - 2))); do
        bookie_ip="journal-node-$j"
        if nc -z $bookie_ip 3181 2>/dev/null; then
            READY_BOOKIES=$((READY_BOOKIES + 1))
        fi
    done
    
    if [ $READY_BOOKIES -eq $EXPECTED_BOOKIES ]; then
        echo "All $EXPECTED_BOOKIES bookies are ready"
        break
    fi
    
    echo "Waiting for bookies... ($READY_BOOKIES/$EXPECTED_BOOKIES ready) ($i/120)"
    
    if [ $i -eq 120 ]; then
        echo "WARNING: Not all bookies ready, starting benchmark anyway"
        break
    fi
    sleep 2
done

echo "Starting benchmark client..."
cd /home/ubuntu

# Run benchmark with realistic quorum settings for cluster
java -jar bookkeeper-benchmark-1.0-SNAPSHOT.jar open \
    --zk-servers "$ZK_SERVERS" \
    --ensemble-size $((JOURNAL_CLUSTER_SIZE - 1)) \
    --write-quorum-size $((JOURNAL_CLUSTER_SIZE - 1)) \
    --ack-quorum-size $(((JOURNAL_CLUSTER_SIZE - 1) / 2 + 1)) \
    "$@"
EOF

chmod +x /home/ubuntu/start-*.sh
chown ubuntu:ubuntu /home/ubuntu/start-*.sh

## --------------------------------------------------------------------------------
log "startup script finished. Services will be started after cluster IP configuration."
log "Node role: $BOOKKEEPER_ROLE"
log "Next steps:"
if [ "$BOOKKEEPER_ROLE" = "bookie" ]; then
    log "  1. Wait for all nodes to be configured"
    log "  2. Start ZooKeeper: sudo -u ubuntu /home/ubuntu/start-zookeeper.sh"
    log "  3. Start BookKeeper: sudo -u ubuntu /home/ubuntu/start-bookie.sh"
else
    log "  1. Wait for all bookie nodes to be ready"
    log "  2. Run benchmark: sudo -u ubuntu /home/ubuntu/start-benchmark.sh [args...]"
fi
