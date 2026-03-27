pub mod codec;
pub mod shard;

// 重新导出常用类型，方便外部直接使用
pub use codec::ErasureCodec;
pub use shard::{DataShard, ShardGroup, ShardId};

#[cfg(test)]
mod test_erasure;
