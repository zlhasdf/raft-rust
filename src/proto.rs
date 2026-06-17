//! protobuf 生成代码的统一导出。
//!
//! `build.rs` 会在编译时调用 `prost-build`，把 `proto/*.proto` 生成 Rust 代码。
//! 生成文件位于 Cargo 的 `OUT_DIR`，业务代码通过本模块访问它们。

/// Raft 节点之间使用的 protobuf message。
pub mod raft {
    include!(concat!(env!("OUT_DIR"), "/kvraft.raft.rs"));
}

/// Clerk 和 KVServer 之间使用的 protobuf message。
pub mod kv {
    include!(concat!(env!("OUT_DIR"), "/kvraft.kv.rs"));
}
