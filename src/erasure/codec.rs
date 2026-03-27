use anyhow::{anyhow, Context, Result};
use log::{debug, info, warn};
use reed_solomon_erasure::galois_8::ReedSolomon;
use sha2::{Digest, Sha256};

use super::shard::{DataShard, ShardGroup};

/// 纠删码编解码器
///
/// 基于 Reed-Solomon 编码，将原始数据分成 `data_shards` 个数据分片，
/// 并生成 `parity_shards` 个校验分片。只要收集到任意 `data_shards` 个
/// 分片（数据或校验均可），即可恢复原始数据。
#[derive(Debug, Clone)]
pub struct ErasureCodec {
    /// 数据分片数量
    data_shards: usize,
    /// 校验分片数量
    parity_shards: usize,
}

impl ErasureCodec {
    /// 创建新的纠删码编解码器
    ///
    /// # 参数
    /// - `data_shards`: 数据分片数量，必须 >= 1
    /// - `parity_shards`: 校验分片数量，必须 >= 1
    ///
    /// # 示例
    /// ```
    /// use bishe1::erasure::ErasureCodec;
    /// // 4个数据分片 + 2个校验分片，可容忍任意2个分片丢失
    /// let codec = ErasureCodec::new(4, 2).unwrap();
    /// ```
    pub fn new(data_shards: usize, parity_shards: usize) -> Result<Self> {
        if data_shards == 0 {
            return Err(anyhow!("数据分片数量必须大于0"));
        }
        if parity_shards == 0 {
            return Err(anyhow!("校验分片数量必须大于0"));
        }
        if data_shards + parity_shards > 256 {
            return Err(anyhow!(
                "总分片数量不能超过256（GF(2^8)的限制），当前: {}",
                data_shards + parity_shards
            ));
        }

        info!(
            "创建纠删码编解码器: data_shards={}, parity_shards={}, 容错能力={}个分片",
            data_shards, parity_shards, parity_shards
        );

        Ok(Self {
            data_shards,
            parity_shards,
        })
    }

    /// 获取数据分片数量
    pub fn data_shards(&self) -> usize {
        self.data_shards
    }

    /// 获取校验分片数量
    pub fn parity_shards(&self) -> usize {
        self.parity_shards
    }

    /// 获取总分片数量
    pub fn total_shards(&self) -> usize {
        self.data_shards + self.parity_shards
    }

    /// 计算数据的SHA-256哈希（用作group_id）
    fn compute_group_id(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        let result = hasher.finalize();
        hex::encode(result)
    }

    /// 编码：将原始数据分片并生成校验分片
    ///
    /// # 参数
    /// - `data`: 原始数据字节切片
    ///
    /// # 返回
    /// 返回 `ShardGroup`，包含所有数据分片和校验分片
    ///
    /// # 示例
    /// ```
    /// use bishe1::erasure::ErasureCodec;
    /// let codec = ErasureCodec::new(4, 2).unwrap();
    /// let data = b"Hello, Erasure Coding! This is a test.";
    /// let shard_group = codec.encode(data).unwrap();
    /// assert_eq!(shard_group.shards.len(), 6); // 4 + 2
    /// ```
    pub fn encode(&self, data: &[u8]) -> Result<ShardGroup> {
        if data.is_empty() {
            return Err(anyhow!("输入数据不能为空"));
        }

        let original_size = data.len();
        let group_id = Self::compute_group_id(data);

        debug!(
            "开始编码: 数据大小={}字节, group_id={}",
            original_size, &group_id[..16]
        );

        // 创建 Reed-Solomon 编码器
        let rs = ReedSolomon::new(self.data_shards, self.parity_shards)
            .map_err(|e| anyhow!("创建Reed-Solomon编码器失败: {}", e))?;

        // 计算每个分片的大小（向上取整）
        let shard_size = (original_size + self.data_shards - 1) / self.data_shards;

        // 构建分片数据：数据分片 + 校验分片（初始化为0）
        let mut shard_data: Vec<Vec<u8>> = Vec::with_capacity(self.total_shards());

        // 填充数据分片
        for i in 0..self.data_shards {
            let start = i * shard_size;
            let end = std::cmp::min(start + shard_size, original_size);
            let mut shard = vec![0u8; shard_size];
            if start < original_size {
                let copy_len = end - start;
                shard[..copy_len].copy_from_slice(&data[start..end]);
            }
            shard_data.push(shard);
        }

        // 添加空的校验分片
        for _ in 0..self.parity_shards {
            shard_data.push(vec![0u8; shard_size]);
        }

        // 执行 Reed-Solomon 编码，生成校验数据
        rs.encode(&mut shard_data)
            .map_err(|e| anyhow!("Reed-Solomon编码失败: {}", e))?;

        // 构建 DataShard 对象
        let shards: Vec<DataShard> = shard_data
            .into_iter()
            .enumerate()
            .map(|(i, data_vec)| {
                let is_parity = i >= self.data_shards;
                DataShard::new(
                    group_id.clone(),
                    i,
                    is_parity,
                    data_vec,
                    original_size,
                    self.data_shards,
                    self.parity_shards,
                )
            })
            .collect();

        info!(
            "编码完成: 生成{}个分片（{}数据 + {}校验），每个分片{}字节",
            self.total_shards(),
            self.data_shards,
            self.parity_shards,
            shard_size
        );

        Ok(ShardGroup {
            group_id,
            original_size,
            data_shard_count: self.data_shards,
            parity_shard_count: self.parity_shards,
            shards,
        })
    }

    /// 解码：从分片中恢复原始数据
    ///
    /// 支持部分分片丢失的情况，只要可用分片数 >= data_shards 即可恢复。
    /// 传入的分片可以是任意顺序，函数会根据分片索引自动排列。
    ///
    /// # 参数
    /// - `available_shards`: 可用的分片列表（可以少于总分片数）
    ///
    /// # 返回
    /// 恢复后的原始数据
    ///
    /// # 示例
    /// ```
    /// use bishe1::erasure::ErasureCodec;
    /// let codec = ErasureCodec::new(4, 2).unwrap();
    /// let data = b"Hello, Erasure Coding! This is a test.";
    /// let shard_group = codec.encode(data).unwrap();
    ///
    /// // 模拟丢失2个分片，只保留4个
    /// let partial: Vec<_> = shard_group.shards[..4].to_vec();
    /// let recovered = codec.decode(&partial).unwrap();
    /// assert_eq!(recovered, data);
    /// ```
    pub fn decode(&self, available_shards: &[DataShard]) -> Result<Vec<u8>> {
        if available_shards.is_empty() {
            return Err(anyhow!("没有可用的分片"));
        }

        // 从第一个分片获取元信息
        let first = &available_shards[0];
        let original_size = first.original_size;
        let data_shard_count = first.data_shard_count;
        let parity_shard_count = first.parity_shard_count;
        let total_shards = data_shard_count + parity_shard_count;

        // 验证编解码器参数匹配
        if data_shard_count != self.data_shards || parity_shard_count != self.parity_shards {
            return Err(anyhow!(
                "编解码器参数不匹配: 期望({},{}), 分片记录({},{})",
                self.data_shards,
                self.parity_shards,
                data_shard_count,
                parity_shard_count
            ));
        }

        if available_shards.len() < self.data_shards {
            return Err(anyhow!(
                "可用分片不足: 需要至少{}个，当前只有{}个",
                self.data_shards,
                available_shards.len()
            ));
        }

        // 验证所有分片的完整性
        for shard in available_shards {
            if !shard.verify() {
                warn!("分片 {} 完整性校验失败", shard.id);
                return Err(anyhow!("分片 {} 完整性校验失败", shard.id));
            }
        }

        let shard_size = available_shards[0].data.len();

        debug!(
            "开始解码: 可用分片={}/{}, 分片大小={}字节",
            available_shards.len(),
            total_shards,
            shard_size
        );

        // 创建 Reed-Solomon 解码器
        let rs = ReedSolomon::new(self.data_shards, self.parity_shards)
            .map_err(|e| anyhow!("创建Reed-Solomon解码器失败: {}", e))?;

        // 构建分片数组，缺失的分片用 None 表示
        let mut shard_data: Vec<Option<Vec<u8>>> = vec![None; total_shards];

        for shard in available_shards {
            if shard.id.index >= total_shards {
                return Err(anyhow!(
                    "分片索引越界: index={}, total={}",
                    shard.id.index,
                    total_shards
                ));
            }
            shard_data[shard.id.index] = Some(shard.data.clone());
        }

        // 执行 Reed-Solomon 重建
        rs.reconstruct(&mut shard_data)
            .map_err(|e| anyhow!("Reed-Solomon重建失败: {}", e))?;

        // 从数据分片中提取原始数据
        let mut recovered = Vec::with_capacity(original_size);
        for i in 0..self.data_shards {
            if let Some(ref shard_bytes) = shard_data[i] {
                recovered.extend_from_slice(shard_bytes);
            } else {
                return Err(anyhow!("重建后数据分片 {} 仍然缺失", i));
            }
        }

        // 截断到原始大小（去除填充的零字节）
        recovered.truncate(original_size);

        info!("解码完成: 恢复数据大小={}字节", recovered.len());

        Ok(recovered)
    }

    /// 从 ShardGroup 中解码（便捷方法）
    pub fn decode_from_group(&self, group: &ShardGroup) -> Result<Vec<u8>> {
        self.decode(&group.shards)
    }

    /// 模拟分片丢失场景并尝试恢复
    ///
    /// # 参数
    /// - `group`: 完整的分片组
    /// - `lost_indices`: 要模拟丢失的分片索引列表
    ///
    /// # 返回
    /// 恢复后的原始数据
    pub fn simulate_loss_and_recover(
        &self,
        group: &ShardGroup,
        lost_indices: &[usize],
    ) -> Result<Vec<u8>> {
        let total = group.total_shard_count();
        let remaining: Vec<DataShard> = group
            .shards
            .iter()
            .filter(|s| !lost_indices.contains(&s.id.index))
            .cloned()
            .collect();

        info!(
            "模拟丢失{}个分片 {:?}，剩余{}个分片，尝试恢复...",
            lost_indices.len(),
            lost_indices,
            remaining.len()
        );

        if remaining.len() < self.data_shards {
            return Err(anyhow!(
                "丢失过多分片无法恢复: 丢失{}个，剩余{}个，需要至少{}个",
                total - remaining.len(),
                remaining.len(),
                self.data_shards
            ));
        }

        self.decode(&remaining)
    }

    /// 验证编码-解码的正确性（自测方法）
    pub fn verify_roundtrip(&self, data: &[u8]) -> Result<bool> {
        let group = self.encode(data).context("编码阶段失败")?;
        let recovered = self.decode_from_group(&group).context("解码阶段失败")?;
        Ok(recovered == data)
    }

    /// 获取编码后的存储开销比
    /// 返回值 = 总分片大小 / 原始数据大小
    pub fn storage_overhead_ratio(&self) -> f64 {
        (self.data_shards + self.parity_shards) as f64 / self.data_shards as f64
    }

    /// 获取容错描述信息
    pub fn fault_tolerance_info(&self) -> String {
        format!(
            "纠删码参数: ({},{}) - 总{}个分片，可容忍任意{}个分片丢失，存储开销{:.1}x",
            self.data_shards,
            self.parity_shards,
            self.total_shards(),
            self.parity_shards,
            self.storage_overhead_ratio()
        )
    }
}