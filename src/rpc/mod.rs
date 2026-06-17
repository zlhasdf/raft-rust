//! 自定义 TCP RPC 框架。
//!
//! 本项目没有使用 tonic/gRPC，而是自己定义一个简单帧协议：
//! `total_len + msg_type + request_id + protobuf_body`。
//! protobuf 只负责 body 的结构化编码，TCP 拆包/粘包由 `codec` 处理。
//!
//! 新手须知：
//! - 以 `//!` 开头的是「模块文档注释」，描述它所在的整个文件/模块；
//!   以 `///` 开头的是「条目文档注释」，描述紧跟其下的那一项（模块/函数/常量等）。
//! - mod.rs 是当前文件夹（rpc 模块）的入口，负责声明并公开下面的各个子模块。

// `pub mod 名字;` = 「存在一个名为『名字』的子模块（对应同名 .rs 文件），并对外公开」。
// 没有 `pub` 则只能在本模块内部访问。
/// RPC 客户端。
pub mod client;
/// TCP 帧读写。
pub mod codec;
/// RPC 专用错误类型。
pub mod error;
/// RPC 服务端分发器。
pub mod server;

// 下面是一组「消息类型编号」：写进每个帧头部的那 1 字节 `msg_type`，
// 收发双方靠它判断「这是哪种 RPC 请求/响应」。
// `pub const 名字: 类型 = 值;` 定义公开的编译期常量：编译时直接内联，零运行时开销，名字惯例全大写。
/// Raft RequestVote 请求。
pub const MSG_REQUEST_VOTE: u8 = 1;
/// Raft RequestVote 响应。
pub const MSG_REQUEST_VOTE_RESPONSE: u8 = 2;
/// Raft AppendEntries 请求。
pub const MSG_APPEND_ENTRIES: u8 = 3;
/// Raft AppendEntries 响应。
pub const MSG_APPEND_ENTRIES_RESPONSE: u8 = 4;
/// Raft InstallSnapshot 请求。
pub const MSG_INSTALL_SNAPSHOT: u8 = 5;
/// Raft InstallSnapshot 响应。
pub const MSG_INSTALL_SNAPSHOT_RESPONSE: u8 = 6;
/// KV Get 请求。
pub const MSG_GET: u8 = 10;
/// KV Get 响应。
pub const MSG_GET_RESPONSE: u8 = 11;
/// KV Put/Append 请求。
pub const MSG_PUT_APPEND: u8 = 12;
/// KV Put/Append 响应。
pub const MSG_PUT_APPEND_RESPONSE: u8 = 13;
