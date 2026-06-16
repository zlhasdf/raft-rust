use thiserror::Error;

pub type Result<T> = std::result::Result<T, KvraftError>;

/// kvraft-rs 可能向上传递的错误。
#[derive(Debug, Error)]
pub enum KvraftError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("bincode error: {0}")]
    Bincode(#[from] bincode::Error),

    // Temporarily disabled until `prost` is added to dependencies.
    // #[error("protobuf encode error: {0}")]
    // ProstEncode(#[from] prost::EncodeError),

    // Temporarily disabled until `prost` is added to dependencies.
    // #[error("protobuf decode error: {0}")]
    // ProstDecode(#[from] prost::DecodeError),

    // Temporarily disabled until the `rpc` module exists in the crate root.
    // #[error("rpc error: {0}")]
    // Rpc(#[from] crate::rpc::error::RpcError),

    #[error("invalid operation: {0}")]
    InvalidOperation(String),

    #[error("request timed out")]
    Timeout,
}
