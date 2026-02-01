# BtrLog Protocol Specifications

This folders contains an abstract TLA+ model of the BtrLog protocol:
a single-writer, quorum-replicated append protocol with writer fencing (epochs/write tokens), client failover repair, and asynchronous flushing to durable object storage (blob store).

The model is intentionally abstract: it focuses on safety properties around committed data and fencing under message loss/reordering and crash-stop node failures, rather than on segment layout, payload bytes, or concrete object-store APIs.

## Model Overview

**Entities**
- **Writers**: clients that can acquire a monotonically increasing write token (epoch) from a reliable metadata store.
- **Servers**: staging nodes that store append records in a local storage and may flush them to durable storage.
- **Readers**: clients that read committed records using committed-watermark metadata.

**Core mechanics**
- **Fencing via epochs**: a writer must acquire a new epoch and install it on a write quorum. Servers adopt higher epochs and can cause older writers to become fenced.
- **Quorum replication**: appends are sent to a nondeterministically chosen write quorum to model message loss and partial delivery.
- **Out-of-order delivery stress**: appends are modeled in batches with reverse-order sends to exercise reordering and gaps.
- **Durable blob store**: servers may flush their staged ledger to an abstract durable store; durable data may include uncommitted items.

**Failure model**
- Non-Byzantine crash-stop failures and asynchronous networks: message loss/reordering, partitions, node failures, client failures are modeled via nondeterminism and fault injection actions.


What is *not* modeled (intentional abstractions)

The model does **not** attempt to capture:
- Segment sizes, byte offsets, prefix sums, or object naming/content hashing.
- Object-store HEAD/PUT-if-absent behavior or cost optimizations (flush leaders, etc.).
- Timing/heartbeats; failure detection is abstracted as nondeterministic actions.
- Full read-path complexity (merging overlaps, filtering duplicates) beyond the checked safety invariants.

These are deliberate to keep the state space tractable and focus on core safety properties.

## Contents

 - `BtrLogSpec.tla` (main spec module)
 - `BtrLogSpec.cfg` (TLC model configuration)

## Setup and Model Checking

1) Download and install java:
(The following will use sdkman to install and manage java - local installations in home folder)
```
curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk install java
```

2) Download TLA+ tools and modules:
(The following will download the assets to a `bin` folder in the current directory)
```
wget -P bin https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar
wget -P bin https://github.com/tlaplus/CommunityModules/releases/download/202412190202/CommunityModules.jar
```
3) Run TLC model checker on a spec
```
java -XX:+UseParallelGC -cp "bin/*" tlc2.TLC BtrLogSpec -config BtrLogSpec.cfg -workers auto
```
When stats are required use:
```
java -Dtlc2.tool.Simulator.extendedStatistics=true -XX:+UseParallelGC -cp "bin/*" tlc2.TLC BtrLogSpec -config BtrLogSpec.cfg -workers auto -coverage 1 -simulate
```

## Useful TLC options
### Generating state graphs
Pass the following flag to TLC (See https://docs.tlapl.us/using:generating_state_graph and https://docs.tlapl.us/using:vscode:visualizing_states):
```
-dump dot ${modelName}.dot
```

### Output coverage information
Tell TLC to print coverage information every num minutes. Without the option, TLC prints no coverage information (See https://lamport.azurewebsites.net/tla/current-tools.pdf).
The printed coverage information can be interpreted as explained here: https://docs.tlapl.us/using:coverage?s[]=coverage
```
-coverage 1
```

### Visualize error traces in VSCode extension
Use the `-tool` option and redirect the ouptu to a `.out` file.
Open the output-file in VSCode and run the `Visualize TLC ouput` command.

### Debugging using statistics
See https://github.com/microsoft/CCF/issues/6537#issuecomment-2652300191

### Checking statistical properties
See https://muratbuffalo.blogspot.com/2022/10/checking-statistical-properties-of.html

### Trace validation
See https://docs.tlapl.us/using:tlc:trace_validation

### Simulation mode
Use TLC's simulation mode to randomly explore the state graph instead of exhaustive model checking.
```
-simulate
```
This can be faster for large specs and huge state spaces.
For small specs, model checking can actually be faster

## Troubleshooting

OutOfMemoryError: increase heap size, e.g. add -Xmx16g to the java command.

Huge state space: reduce APPEND_COUNT_MAX, FAULTS_COUNT, or sizes of *_ids, or run -simulate.

Slow runs: try -workers auto, ensure you are on a recent JVM, and consider running on a machine with more RAM.
