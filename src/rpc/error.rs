//! RPC 层错误类型。
//!
//! 把 IO、protobuf 编解码、帧格式错误和超时统一起来，方便 `RpcClient`
//! 和 `RpcServer` 都返回同一个 `Result`。

// `thiserror::Error` 是个派生宏，能帮我们自动为枚举实现标准库的 `std::error::Error` trait，
// 省去大量样板代码。下面的 `#[derive(Error)]` 和 `#[error("...")]` 都是它提供的。
use thiserror::Error;

/// RPC 过程中可能出现的错误。
// `enum`（枚举）表示「这个类型的值，只能是下列变体之一」，非常适合用来罗列各种错误情况。
// `#[derive(Debug, Error)]`：Debug 让它能用 {:?} 打印；Error 由 thiserror 提供，自动实现错误接口。
#[derive(Debug, Error)]
pub enum RpcError {
    // `#[error("...")]` 定义这个变体转成字符串时的展示文案，`{0}` 指代该变体里第 0 个字段。
    // `#[from]` 表示「允许用 `?` 把 std::io::Error 自动转换成 RpcError::Io」，写起来很省事。
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("protobuf encode error: {0}")]
    Encode(#[from] prost::EncodeError),

    #[error("protobuf decode error: {0}")]
    Decode(#[from] prost::DecodeError),

    // 圆括号变体 `FrameTooLarge(usize)`：携带一个匿名字段（这里是字节数）。
    #[error("frame too large: {0} bytes")]
    FrameTooLarge(usize),

    #[error("invalid frame: {0}")]
    InvalidFrame(String),

    // 花括号变体 `{ expected, actual }`：携带「具名字段」，可读性更好，文案里用 {字段名} 引用。
    #[error("unexpected message type: expected {expected}, got {actual}")]
    UnexpectedMessage { expected: u8, actual: u8 },

    // 没有字段的变体，单纯表示一种状态（超时）。
    #[error("rpc timeout")]
    Timeout,

    #[error("rpc handler error: {0}")]
    Handler(String),
}

// `type 别名 = 原类型;` 给类型起个短名字。
// 这里把「错误固定为 RpcError 的 Result」起名叫本模块的 `Result<T>`，
// 之后函数只需写 `Result<Resp>`，不必每次重复写 `std::result::Result<Resp, RpcError>`。
pub type Result<T> = std::result::Result<T, RpcError>;
