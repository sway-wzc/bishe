use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;

/// 分片唯一标识
#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct ShardId {
    /// 所属数据组的ID（通常是原始数据的哈希）
    pub group_id: String,
    /// 分片在组内的索引
    pub index: usize,
    /// 是否为校验分片
    pub is_parity: bool,
}

impl fmt::Display for ShardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let shard_type = if self.is_parity { "parity" } else { "data" };
        write!(f, "{}:{}({})", self.group_id, self.index, shard_type)
    }
}

/// 单个数据分片
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataShard {
    /// 分片标识
    pub id: ShardId,
    /// 分片数据
    pub data: Vec<u8>,
    /// 分片数据的SHA-256哈希（用于完整性校验）
    pub hash: String,
    /// 原始数据总大小（字节）
    pub original_size: usize,
    /// 数据分片数量
    pub data_shard_count: usize,
    /// 校验分片数量
    pub parity_shard_count: usize,
}

impl DataShard {
    /// 创建新的数据分片
    pub fn new(
        group_id: String,
        index: usize,
        is_parity: bool,
        data: Vec<u8>,
        original_size: usize,
        data_shard_count: usize,
        parity_shard_count: usize,
    ) -> Self {
        let hash = Self::compute_hash(&data);
        Self {
            id: ShardId {
                group_id,
                index,
                is_parity,
            },
            data,
            hash,
            original_size,
            data_shard_count,
            parity_shard_count,
        }
    }

    /// 计算数据的SHA-256哈希
    fn compute_hash(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        let result = hasher.finalize();
        hex::encode(result)
    }

    /// 验证分片数据完整性
    pub fn verify(&self) -> bool {
        let computed = Self::compute_hash(&self.data);
        computed == self.hash
    }

    /// 获取分片大小（字节）
    pub fn size(&self) -> usize {
        self.data.len()
    }
}

/// 分片组：包含同一份原始数据的所有分片
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardGroup {
    /// 组ID（原始数据的哈希）
    pub group_id: String,
    /// 原始数据大小
    pub original_size: usize,
    /// 数据分片数量
    pub data_shard_count: usize,
    /// 校验分片数量
    pub parity_shard_count: usize,
    /// 所有分片（数据分片在前，校验分片在后）
    pub shards: Vec<DataShard>,
}

impl ShardGroup {
    /// 获取总分片数
    pub fn total_shard_count(&self) -> usize {
        self.data_shard_count + self.parity_shard_count
    }

    /// 获取当前可用的分片数量
    pub fn available_shard_count(&self) -> usize {
        self.shards.len()
    }

    /// 检查是否有足够的分片进行数据恢复
    pub fn can_reconstruct(&self) -> bool {
        self.available_shard_count() >= self.data_shard_count
    }

    /// 验证所有分片的完整性
    pub fn verify_all(&self) -> Vec<(ShardId, bool)> {
        self.shards
            .iter()
            .map(|s| (s.id.clone(), s.verify()))
            .collect()
    }
}