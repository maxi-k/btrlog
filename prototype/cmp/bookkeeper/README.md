# BookKeeper Benchmark Setup

## Local Testing

```bash
# Enter nix environment
nix-shell

# Build the benchmark client
cd cmp/bookkeeper
mvn clean package
```

## Terraform Deployment

To deploy BookKeeper instead of journal-service, edit `control/node.tf`:

**1. Comment out the journal-service variables:**
```terraform
# variable "engine_binary" {
#   description = "Name of the binary to deploy"
#   default = "bench"
# }
```

**2. Uncomment the BookKeeper variables:**
```terraform
variable "engine_binary" {
  description = "Name of the binary to deploy"
  default = "bookkeeper-benchmark-1.0-SNAPSHOT-shaded.jar"
}

variable "local_binary_directory" {
  description = "Path to the binary to deploy"
  default = {
    "x86" = "../cmp/bookkeeper/target"
    "arm" = "../cmp/bookkeeper/target"
  }
}
```

**3. Switch to BookKeeper startup script:**
```terraform
# node_startup_script_file = "./scripts/node_startup.sh"
node_startup_script_file = "./scripts/node_startup_bookkeeper.sh"
```

**4. Update archive command:**
```terraform
# command = "cd ${var.local_binary_directory[var.node_arch]} && zip ${var.engine_binary}.zip ${var.engine_binary} *.so *.rlib ${local.local_sourcedirs}"
command = "cd ${var.local_binary_directory[var.node_arch]} && zip ${var.engine_binary}.zip ${var.engine_binary}"
```

**5. Update deployment commands:**
```terraform
# "cd ${local.node_homedir} && ( while [ ! -f /usr/bin/patchelf ]; do sleep 1; done ) && patchelf --set-interpreter /lib64/$(ls /lib64) ${var.engine_binary} || true",
"chmod +x ${local.node_homedir}/${var.engine_binary}",
"echo '#!/bin/bash' > ${local.node_homedir}/benchmark && echo 'java -jar ${local.node_homedir}/${var.engine_binary} \"$@\"' >> ${local.node_homedir}/benchmark && chmod +x ${local.node_homedir}/benchmark",
```

## Build and Deploy

```bash
# Build the JAR
cd cmp/bookkeeper && mvn clean package

# Deploy with terraform
cd control
terraform apply
```

## Architecture

- **Nodes 0-(N-2)**: Run BookKeeper bookies + embedded ZooKeeper
- **Node N-1**: Runs benchmark client against the bookie cluster
- **Storage**: Uses same SSD configuration as journal-service (`/blk/ssd-*`)
- **Output**: Same JSON format as journal-service for comparison

## Local Testing

**Quick local test without AWS:**
```bash
# Test setup
./test_local_cluster.sh

# Run basic benchmark (2 bookies + 1 client)
./local_cluster_bookkeeper.sh 3 --op-count 1000 --thread-count 1

# Debug the benchmark client
DEBUG_NODE=2 ./local_cluster_bookkeeper.sh 3 --op-count 100

# Launch components in separate terminals  
LAUNCH_TERMINALS=1 ./local_cluster_bookkeeper.sh 3
```
