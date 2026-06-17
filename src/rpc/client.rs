//! RPC 客户端。
//!
//! 每次调用会建立一个 TCP 连接、发送一个 frame、读取一个响应 frame。
//! 这种短连接实现简单，适合教学项目；如果以后追求性能，可以扩展成长连接连接池。

use std::time::Duration; // 标准库：表示「一段时长」，用于设置超时。

use prost::Message;        // protobuf 的核心 trait：实现它的类型才能 encode/decode 成字节。
use tokio::net::TcpStream; // 异步 TCP 连接（不阻塞线程）。
use tokio::time;           // 提供 time::timeout，给操作加一个时间上限。

// `crate::` 从「整个 crate 的根」开始找路径；这里取生成好的 protobuf 模块 kv 和 raft。
use crate::proto::{kv, raft};

// `super::` 指「上一级模块」（即 rpc 模块）。下面从同级文件 codec/error 以及 mod.rs 里取东西。
use super::codec::{read_frame, write_frame, RpcFrame};
use super::error::{Result, RpcError};
// 引入 mod.rs 里定义的那组消息类型编号常量。
use super::{
    MSG_APPEND_ENTRIES, MSG_APPEND_ENTRIES_RESPONSE, MSG_GET, MSG_GET_RESPONSE,
    MSG_INSTALL_SNAPSHOT, MSG_INSTALL_SNAPSHOT_RESPONSE, MSG_PUT_APPEND, MSG_PUT_APPEND_RESPONSE,
    MSG_REQUEST_VOTE, MSG_REQUEST_VOTE_RESPONSE,
};

/// 指向某个服务端节点的 RPC 客户端。
// Clone 让它能被 .clone() 复制一份（多个任务想各自持有一个客户端时很方便）。
#[derive(Debug, Clone)]
pub struct RpcClient {
    addr: String,       // 目标服务端地址，如 "127.0.0.1:8080"。
    timeout: Duration,  // 单次调用的最长等待时间。
}

// `impl RpcClient { ... }`：为 RpcClient 实现方法。第一个参数是 self/&self 的是「实例方法」，
// 否则（如 new/with_timeout）是「关联函数」，用 RpcClient::函数名(...) 调用。
impl RpcClient {
    /// 创建默认 450ms 超时的客户端。
    // `impl Into<String>`：参数只要「能转成 String」即可（传 &str 或 String 都行）。
    pub fn new(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),                   // .into() 真正执行转换。
            timeout: Duration::from_millis(450),
        }
    }

    /// 创建可配置超时时间的客户端，测试时很有用。
    pub fn with_timeout(addr: impl Into<String>, timeout: Duration) -> Self {
        Self {
            addr: addr.into(),
            timeout, // 字段名与变量名相同时的简写。
        }
    }

    /// 返回目标地址。
    // `&self`：只读借用当前对象；返回 `&str` 是借用内部字符串，避免复制。
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// 通用 RPC 调用模板。
    ///
    /// `Req` 和 `Resp` 都是 prost 生成的 protobuf message。
    // `async`：异步方法，返回 Future，需 .await 驱动。
    // `<Req, Resp>`：泛型，让同一套逻辑适配各种请求/响应类型，具体类型在编译期确定。
    pub async fn call<Req, Resp>(
        &self,             // 只读借用当前客户端（用到它的 addr 和 timeout）。
        msg_type: u8,      // 本次请求的消息类型编号。
        response_type: u8, // 期望收到的响应类型编号，用于校验。
        request: &Req,     // 以借用方式接收请求对象，不夺走所有权。
    ) -> Result<Resp>
    // `where` 约束泛型必须具备的能力：
    where
        Req: Message,            // 请求要能被编码成字节。
        Resp: Message + Default, // 响应要能被解码，且可创建默认值。
    {
        // 随机生成请求 ID。`::<u64>` 是 turbofish 写法，显式指定生成 u64。
        let request_id = rand::random::<u64>();
        // `mut` 表示可变；with_capacity 预分配空间，减少写入时的内存重分配。
        let mut body = Vec::with_capacity(request.encoded_len());
        // `?`：若 encode 返回 Err 就立即把错误从本函数返回；成功则继续。
        request.encode(&mut body)?;

        // .clone() 复制一份地址；下面的 async move 会拿走捕获变量的所有权，复制后 self 仍完好。
        let addr = self.addr.clone();
        let timeout = self.timeout; // Duration 是 Copy 类型，直接按位复制，无需 clone。
        let frame = RpcFrame::new(msg_type, request_id, body);
        // `async move { ... }`：异步代码块，`move` 把用到的变量（addr、frame…）的所有权移进块内，
        // 保证它在后台运行期间始终有效。time::timeout 给整段操作设上限。
        time::timeout(timeout, async move {
            // 每次调用都新建一条连接（短连接）。`.await` 期间线程可去做别的事。
            let mut stream = TcpStream::connect(&addr).await?;
            write_frame(&mut stream, &frame).await?;          // 发送请求帧。
            let response_frame = read_frame(&mut stream).await?; // 读取响应帧。
            // 校验响应类型是否符合预期，不符就提前返回错误。
            if response_frame.msg_type != response_type {
                return Err(RpcError::UnexpectedMessage {
                    expected: response_type,
                    actual: response_frame.msg_type,
                });
            }
            // 校验响应 ID 与请求一致，避免拿到串号的响应。
            if response_frame.request_id != request_id {
                return Err(RpcError::InvalidFrame("mismatched request id".to_owned()));
            }
            // 块末尾不带分号的表达式即为返回值：把响应体解码成 Resp 并用 Ok 包好。
            Ok(Resp::decode(response_frame.body.as_slice())?)
        })
        .await                                  // 等带超时的任务结束。
        .map_err(|_| RpcError::Timeout)?        // 超时(外层 Err)→转成 Timeout；`?` 再解开一层，得到内层 Result<Resp>。
    }

    // 下面这些都是「便捷方法」：把通用的 call 针对每种具体 RPC 各包一层，
    // 调用方就不必自己去记每种请求对应哪个消息编号、响应又是哪个类型。
    /// 调用 Raft RequestVote。
    pub async fn request_vote(
        &self,
        request: &raft::RequestVoteRequest,
    ) -> Result<raft::RequestVoteResponse> {
        self.call(MSG_REQUEST_VOTE, MSG_REQUEST_VOTE_RESPONSE, request)
            .await
    }

    /// 调用 Raft AppendEntries。
    pub async fn append_entries(
        &self,
        request: &raft::AppendEntriesRequest,
    ) -> Result<raft::AppendEntriesResponse> {
        self.call(MSG_APPEND_ENTRIES, MSG_APPEND_ENTRIES_RESPONSE, request)
            .await
    }

    /// 调用 Raft InstallSnapshot。
    pub async fn install_snapshot(
        &self,
        request: &raft::InstallSnapshotRequest,
    ) -> Result<raft::InstallSnapshotResponse> {
        self.call(MSG_INSTALL_SNAPSHOT, MSG_INSTALL_SNAPSHOT_RESPONSE, request)
            .await
    }

    /// 调用 KV Get。
    pub async fn get(&self, request: &kv::GetRequest) -> Result<kv::GetResponse> {
        self.call(MSG_GET, MSG_GET_RESPONSE, request).await
    }

    /// 调用 KV Put/Append。
    pub async fn put_append(
        &self,
        request: &kv::PutAppendRequest,
    ) -> Result<kv::PutAppendResponse> {
        self.call(MSG_PUT_APPEND, MSG_PUT_APPEND_RESPONSE, request)
            .await
    }
}