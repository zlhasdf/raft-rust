//! RPC 服务端。
//!
//! 服务端监听 TCP 端口，收到 frame 后根据 `msg_type` 分发到 RaftNode 或 KVServer。
//! 这对应原 C++ 项目里 `RpcProvider` 同时注册 Raft RPC 和 KV RPC 的做法。

// `Arc` = Atomically Reference Counted（原子引用计数）：一种「智能指针」。
// 它让同一份数据被多处「共享只读」，内部用计数记录有多少持有者，最后一个释放时才真正回收。
// 在多线程/多任务场景里共享数据时几乎必用它。
use std::sync::Arc;

use prost::Message;
use tokio::net::{TcpListener, TcpStream}; // TcpListener 负责监听端口、接受连接；TcpStream 是单条连接。
use tracing::{debug, warn};               // 结构化日志宏：debug! 调试级、warn! 警告级。

use crate::kv::server::KvServer;
use crate::proto;
use crate::raft::RaftNode;

use super::codec::{read_frame, write_frame, RpcFrame};
use super::error::{Result, RpcError};
use super::{
    MSG_APPEND_ENTRIES, MSG_APPEND_ENTRIES_RESPONSE, MSG_GET, MSG_GET_RESPONSE,
    MSG_INSTALL_SNAPSHOT, MSG_INSTALL_SNAPSHOT_RESPONSE, MSG_PUT_APPEND, MSG_PUT_APPEND_RESPONSE,
    MSG_REQUEST_VOTE, MSG_REQUEST_VOTE_RESPONSE,
};

/// RPC 服务端。
#[derive(Debug)]
pub struct RpcServer {
    addr: String,
    raft: Arc<RaftNode>,         // 共享的 Raft 节点（多个连接处理任务会同时用到它）。
    kv: Option<Arc<KvServer>>,   // `Option<T>` 表示「可能有、也可能没有」：Some(值) 或 None。这里 None 代表纯 Raft 节点、不提供 KV 服务。
}

impl RpcServer {
    /// 创建服务端。`kv` 为 None 时只暴露 Raft RPC，正常 KV 节点会同时传入 Raft 和 KVServer。
    pub fn new(addr: impl Into<String>, raft: Arc<RaftNode>, kv: Option<Arc<KvServer>>) -> Self {
        Self {
            addr: addr.into(),
            raft,
            kv,
        }
    }

    /// 开始监听并循环接收连接。
    // 注意这里是 `self`（按值），run 会一直占有这个 server 并进入死循环，不再归还。
    pub async fn run(self) -> Result<()> {
        // bind：在指定地址上开始监听。`?` 在失败时直接把错误抛出去。
        let listener = TcpListener::bind(&self.addr).await?;
        // 把 self 包进 Arc，方便后面每个连接任务共享同一个服务端实例。
        let shared = Arc::new(self);
        // `loop {}` 是无限循环（要靠 break/return 退出），这里持续接受新连接。
        loop {
            // accept 等待并接受一条新连接，返回 (连接, 对端地址)。这是「解构」：把元组拆成两个变量。
            let (stream, peer) = listener.accept().await?;
            debug!(%peer, "accepted rpc connection"); // `%peer` 表示用 Display 方式记录 peer。
            // Arc::clone 只是把引用计数 +1，并不复制底层数据，开销很小。
            let server = Arc::clone(&shared);
            // tokio::spawn 把这段 async 块作为一个独立任务丢到后台并发执行，
            // 这样主循环能马上回去 accept 下一条连接，实现「一连接一任务」的并发处理。
            tokio::spawn(async move {
                // `if let Err(err) = 表达式`：只在结果是 Err 时进入分支。这里把连接出错的情况记成警告日志。
                if let Err(err) = server.handle_connection(stream).await {
                    warn!(%err, "rpc connection closed with error");
                }
            });
        }
    }

    /// 处理单个 TCP 连接。
    // `self: Arc<Self>`：接收者本身是一个 Arc，表示这个方法要在「被 Arc 共享的实例」上调用。
    async fn handle_connection(self: Arc<Self>, mut stream: TcpStream) -> Result<()> {
        // 一条连接上可能连续发来多个请求，这里循环处理直到对端关闭。
        loop {
            // `match` 模式匹配：根据 read_frame 的结果分情况处理。
            let frame = match read_frame(&mut stream).await {
                Ok(frame) => frame, // 正常读到一帧。
                // 这条分支带「守卫」：当错误是 IO 错误且类型为 UnexpectedEof（对端正常关闭）时，
                // 视为连接结束，返回 Ok(()) 优雅退出，而不是当成异常报错。
                Err(RpcError::Io(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Ok(());
                }
                Err(err) => return Err(err), // 其它错误则向上抛出。
            };
            let response = self.dispatch(frame).await?; // 按消息类型分发并得到响应。
            write_frame(&mut stream, &response).await?; // 把响应写回这条连接。
        }
    }

    /// 根据消息类型分发请求。
    // 这是整个服务端的「路由」：看帧头的 msg_type 是哪种，就解码成对应请求、调用对应处理逻辑，再编码回响应。
    async fn dispatch(&self, frame: RpcFrame) -> Result<RpcFrame> {
        // `match 值 { 分支 => 结果, ... }`：把 msg_type 和各个常量逐一比对，命中哪个走哪个分支。
        match frame.msg_type {
            MSG_REQUEST_VOTE => {
                // 先把 body 字节按对应 protobuf 类型解码成请求结构体。
                let request = proto::raft::RequestVoteRequest::decode(frame.body.as_slice())?;
                let response = self.raft.request_vote(request); // 交给 Raft 业务逻辑处理。
                // 把响应编码成帧；沿用同一个 request_id，方便客户端对号入座。
                encode_response(MSG_REQUEST_VOTE_RESPONSE, frame.request_id, &response)
            }
            MSG_APPEND_ENTRIES => {
                let request = proto::raft::AppendEntriesRequest::decode(frame.body.as_slice())?;
                let response = self.raft.append_entries(request);
                encode_response(MSG_APPEND_ENTRIES_RESPONSE, frame.request_id, &response)
            }
            MSG_INSTALL_SNAPSHOT => {
                let request = proto::raft::InstallSnapshotRequest::decode(frame.body.as_slice())?;
                let response = self.raft.install_snapshot(request);
                encode_response(MSG_INSTALL_SNAPSHOT_RESPONSE, frame.request_id, &response)
            }
            MSG_GET => {
                // `let Some(kv) = ... else { ... }` 是「let-else」：能取出 Some 里的值就继续，
                // 否则（是 None，即没注册 KV 服务）走 else 分支提前返回错误。else 必须中断流程。
                let Some(kv) = &self.kv else {
                    return Err(RpcError::Handler("kv service is not registered".to_owned()));
                };
                let request = proto::kv::GetRequest::decode(frame.body.as_slice())?;
                let response = kv.handle_get(request).await;
                encode_response(MSG_GET_RESPONSE, frame.request_id, &response)
            }
            MSG_PUT_APPEND => {
                let Some(kv) = &self.kv else {
                    return Err(RpcError::Handler("kv service is not registered".to_owned()));
                };
                let request = proto::kv::PutAppendRequest::decode(frame.body.as_slice())?;
                let response = kv.handle_put_append(request).await;
                encode_response(MSG_PUT_APPEND_RESPONSE, frame.request_id, &response)
            }
            // `other` 是「兜底分支」：前面都没匹配上时，把那个值绑定到 other。这里表示收到未知消息类型。
            other => Err(RpcError::InvalidFrame(format!("unknown msg_type {other}"))),
        }
    }
}

/// 把 protobuf response 编码成 RPC frame。
// 这是一个普通自由函数（不在 impl 里，所以没有 self）。泛型 `<M: Message>` 让它能编码任意一种响应类型。
fn encode_response<M>(msg_type: u8, request_id: u64, message: &M) -> Result<RpcFrame>
where
    M: Message,
{
    let mut body = Vec::with_capacity(message.encoded_len()); // 预分配刚好够用的容量。
    message.encode(&mut body)?;                               // 把响应写进字节缓冲。
    Ok(RpcFrame::new(msg_type, request_id, body))             // 组装成帧返回。
}