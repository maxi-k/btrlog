# Prototype

Rust prototype for BtrLog, with client/server runtimes and benchmarking utilities.

## Build

Prereqs:
- Rust nightly toolchain (see `rust-toolchain.toml`)
- Linux with `io_uring` support for the default IO backend

Common commands (from this directory):

```bash
# Build all default features (client + server + bench)
cargo build

# Release build
cargo build --release

# Build specific binaries
cargo build --bin v2_bench
cargo build --bin roofline_uring
```

## Source Outline (High Level)

- `src/lib.rs`: crate root and feature-gated modules.
- `src/bin/`: CLI binaries (`logger`, `v2_bench`, `roofline_uring`).
- `src/config.rs`: configuration structs and CLI flags.
- `src/client/`: client runtime, quorum logic, stats, journal state.
- `src/server/`: server runtime, journal, WAL, blob store, message server.
- `src/io/`: io_uring plumbing, buffers, watermarks, send paths.
- `src/runtime/`: custom runtime primitives and scheduling utilities.
- `src/node/`: cluster membership and health checks.
- `src/types/`: shared message, packet, id, and error types.
- `src/bench.rs`: benchmark support and logger routines.
- `src/trace.rs`: latency/trace hooks and feature flags.
- `src/util.rs`: shared helpers.
- `dep/`: local Rust dependencies (`reqtrace`, `perfevent`).
- `cmp/`: external benchmarking and comparison tooling.
- `control/`: infra scripts and cluster orchestration.
