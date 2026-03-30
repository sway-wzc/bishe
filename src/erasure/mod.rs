//! # 纠删码模块
//!
//! 基于 Reed-Solomon 编码（GF(2^8) 有限域）实现数据分片与纠错恢复。
//!
//! ## 核心能力
//!
//! - **擦除恢复**：任意 `k` 个正确分片即可恢复原始数据
//! - **错误纠正**：通过 Berlekamp-Welch 算法自动检测并纠正恶意/损坏分片，
//!   最多可纠正 `⌊(n-k)/2⌋` 个错误分片
//!
//! ## 子模块
//!
//! - [`codec`] — 编解码器核心逻辑（编码、解码、纠错）
//! - [`shard`] — 分片数据结构定义（`DataShard`、`ShardGroup`）

pub mod codec;
pub mod shard;

// 重新导出常用类型，方便外部直接使用
pub use codec::ErasureCodec;
pub use shard::{DataShard, ShardGroup, ShardId};

#[cfg(test)]
mod test_erasure;
