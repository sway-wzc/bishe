pub mod protocol;
pub mod manager;
pub mod types;
pub mod chunked;

// 重新导出常用类型
pub use protocol::RbcInstance;
pub use manager::RbcManager;
pub use types::{RbcConfig, RbcMessage, RbcOutput, RbcInstanceState, ChunkedBroadcastMeta, ChunkedBroadcastOutput, DEFAULT_CHUNK_SIZE};
pub use chunked::ChunkedBroadcastManager;

#[cfg(test)]
mod test_rbc;
