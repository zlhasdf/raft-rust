# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build / Test Commands

```bash
# Build the library
cargo build

# Run all tests
cargo test

# Run a specific test
cargo test <test_name>

# Run tests in a specific module (e.g., kv store tests)
cargo test kv::store::
```

This is a library crate (no binary target), so there is no `cargo run`.

## Architecture

This is an in-progress Rust implementation of the Raft consensus protocol with a pluggable KV store as its state machine. The project uses `tokio` for async I/O, `prost` for protobuf serialization, and `bincode` for snapshot encoding.

### Module Layout

```
src/
├── lib.rs          # Crate root: re-exports error::Result and error::KvraftError
├── error.rs        # Top-level error enum (KvraftError) and Result type alias
├── kv/             # State machine — the data that Raft replicates
│   ├── command.rs  # Op enum (Get/Put/Append), OpResult, bincode encode/decode
│   └── store.rs    # StorageEngine trait + KvStore (BTreeMap or SkipList backend)
└── rpc/            # Network transport layer
    ├── codec.rs    # Frame-based wire protocol: read_frame / write_frame over tokio AsyncRead/AsyncWrite
    ├── error.rs    # RpcError enum and Result alias
    └── client.rs   # RpcClient (in progress — missing Message trait and rand/time deps)
```

### Key Design Decisions

- **`StorageEngine` trait** (`kv/store.rs`): The abstraction over the state machine backend. `KvStore` wraps either a `BTreeMap` or a `SkipListStore` behind the same trait. KVServer code depends only on the trait, not the concrete backend.
- **`KvStore` is an enum wrapper** (not a trait object): The backend is a `KvStoreBackend` enum so that `serde + bincode` can serialize it directly for Raft snapshots. A `Box<dyn StorageEngine>` would be harder to snapshot.
- **SkipListStore** (`kv/store.rs`): Port of a C++ skip list. Uses array indices instead of raw pointers for Rust safety and serde compatibility. Each node stores its forward pointers as `Vec<Option<usize>>`. The RNG state is saved in the struct so snapshots are deterministic.
- **Wire protocol** (`rpc/codec.rs`): Length-prefixed frames: `[u32 total_len][u8 msg_type][u64 request_id (BE)][body bytes]`. Max frame size is 32 MiB. Uses tokio's `AsyncRead`/`AsyncWrite` traits (not `TcpStream` directly), so it works with any byte transport.
- **Append = Upsert**: Both `KvStore` and `SkipListStore` implement `append` as `put` (upsert semantics), matching the original C++ implementation where Append routes through `insert_set_element`.

### Error Hierarchy

- `KvraftError` (top-level in `src/error.rs`) — the application's error type, used by KV server and raft logic.
- `RpcError` (`src/rpc/error.rs`) — transport-layer errors (IO, protobuf encode/decode, frame format, timeout).
- `CommandError` (`src/kv/command.rs`) — bincode encode/decode errors for Op serialization.

Each module defines its own `Result<T>` type alias scoped to its error enum.

### Current State / Known Gaps

- `src/rpc/client.rs` does not compile — it needs `rand`, a `time` crate with `timeout`, and a `Message` trait (likely from prost) added to `Cargo.toml`.
- The raft consensus logic itself (leader election, log replication, etc.) is not yet implemented. Current modules are the building blocks: KV state machine + RPC transport.
- No `.proto` files or `build.rs` exist yet; the `prost` dependency is declared but not wired up with generated code.
