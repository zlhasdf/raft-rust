//! Raft 模块。
//!
//! `state.rs` 只定义 Raft 状态数据结构；`node.rs` 包含真正的算法逻辑。

/// Raft 节点行为：选举、复制、apply、snapshot。
pub mod node;
/// Raft 状态结构：角色、日志、索引等。
pub mod state;

pub use node::{ApplyMsg, RaftNode, StartResult};
pub use state::{LogEntry, RaftState, Role};
