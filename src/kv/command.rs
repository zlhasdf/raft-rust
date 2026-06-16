use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CommandError {
    #[error("encode/decode error: {0}")]
    Codec(#[from] bincode::Error),
}

pub type CommandResult<T> = std::result::Result<T, CommandError>;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum Op {
    Get {
        key: String,
        client_id: String,
        request_id: u64,
    },

    Put {
        key: String,
        value: String,
        client_id: String,
        request_id: u64,
    },

    Append {
        key: String,
        value: String,
        client_id: String,
        request_id: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpResult {
    pub value: String,
    pub err: String,
}

impl Op {
    pub fn client_id(&self) -> &str {
        match self {
            Op::Get { client_id, .. }
            | Op::Put { client_id, .. }
            | Op::Append { client_id, .. } => client_id,
        }
    }

    pub fn request_id(&self) -> u64 {
        match self {
            Op::Get { request_id, .. }
            | Op::Put { request_id, .. }
            | Op::Append { request_id, .. } => *request_id,
        }
    }

    pub fn key(&self) -> &str {
        match self {
            Op::Get { key, .. } | Op::Put { key, .. } | Op::Append { key, .. } => key.as_str(),
        }
    }

    pub fn value(&self) -> &str {
        match self {
            Op::Put { value, .. } | Op::Append { value, .. } => value.as_str(),
            Op::Get { .. } => "",
        }
    }

    pub fn op_name(&self) -> &str {
        match self {
            Op::Get { .. } => "get",
            Op::Put { .. } => "put",
            Op::Append { .. } => "append",
        }
    }

    pub fn is_write(&self) -> bool {
        matches!(self, Op::Put { .. } | Op::Append { .. })
    }
    pub fn encode(&self) -> CommandResult<Vec<u8>> {
        Ok(bincode::serialize(self)?)
    }
    pub fn decode(data: &[u8]) -> CommandResult<Op> {
        Ok(bincode::deserialize(data)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn op_round_trip() {
        let op = Op::Put {
            key: "x".to_owned(),
            value: "1".to_owned(),
            client_id: "client".to_owned(),
            request_id: 7,
        };
        assert_eq!(Op::decode(&op.encode().unwrap()).unwrap(), op);
    }

    #[test]
    fn op_accessors_work() {
        let op = Op::Append {
            key: "x".to_owned(),
            value: "y".to_owned(),
            client_id: "c1".to_owned(),
            request_id: 42,
        };
        assert_eq!(op.client_id(), "c1");
        assert_eq!(op.request_id(), 42);
        assert_eq!(op.key(), "x");
        assert_eq!(op.value(), "y");
        assert_eq!(op.op_name(), "append");
    }
}
