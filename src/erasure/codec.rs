use anyhow::{anyhow, Context, Result};
use log::{debug, info, warn};
use reed_solomon_rs::fec::fec::{Share, FEC};
use sha2::{Digest, Sha256};

use super::shard::{DataShard, ShardGroup};

/// 纠删码编解码器
///
/// 基于 Reed-Solomon 编码（Berlekamp-Welch 算法），将原始数据分成 `data_shards` 个数据分片，
/// 并生成 `parity_shards` 个校验分片。
///
/// ## 纠错能力
/// - **擦除恢复**：只要收集到任意 `data_shards` 个正确分片即可恢复原始数据
/// - **错误纠正**：在收集到 n 个分片（含错误分片）的情况下，
///   可自动纠正最多 `⌊parity_shards / 2⌋` 个错误分片（Berlekamp-Welch 算法）
///
/// 这对于 RBC 协议中的拜占庭容错至关重要：恶意节点发送的假分片
/// 可以被自动识别和纠正，无需事先知道哪些分片是错误的。
#[derive(Debug, Clone)]
pub struct ErasureCodec {
    /// 数据分片数量（k）
    data_shards: usize,
    /// 校验分片数量（n - k）
    parity_shards: usize,
}

impl ErasureCodec {
    /// 创建新的纠删码编解码器
    ///
    /// # 参数
    /// - `data_shards`: 数据分片数量（k），必须 >= 1
    /// - `parity_shards`: 校验分片数量，必须 >= 1
    ///
    /// # 纠错能力
    /// - 可容忍 `parity_shards` 个分片丢失（擦除恢复）
    /// - 可自动纠正 `⌊parity_shards / 2⌋` 个错误分片（纠错）
    ///
    /// # 示例
    /// ```
    /// use bishe1::erasure::ErasureCodec;
    /// // 4个数据分片 + 4个校验分片，可容忍4个丢失或纠正2个错误
    /// let codec = ErasureCodec::new(4, 4).unwrap();
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

        // 验证 FEC 能否正确创建
        FEC::new(data_shards, data_shards + parity_shards)
            .map_err(|e| anyhow!("创建FEC编解码器失败: {}", e))?;

        info!(
            "创建纠删码编解码器: data_shards={}, parity_shards={}, 擦除容错={}个分片, 纠错能力={}个错误分片 (Berlekamp-Welch)",
            data_shards, parity_shards, parity_shards, parity_shards / 2
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

    /// 获取纠错能力（可纠正的最大错误分片数）
    pub fn error_correction_capacity(&self) -> usize {
        self.parity_shards / 2
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
    /// # 编码过程
    /// 1. 将数据填充到 k 的倍数长度
    /// 2. 使用 RS 编码生成 n 个分片（k 个数据分片 + (n-k) 个校验分片）
    /// 3. 每个分片包含索引、数据、哈希等元信息
    pub fn encode(&self, data: &[u8]) -> Result<ShardGroup> {
        if data.is_empty() {
            return Err(anyhow!("输入数据不能为空"));
        }

        let original_size = data.len();
        let group_id = Self::compute_group_id(data);
        let k = self.data_shards;
        let n = self.total_shards();

        debug!(
            "开始编码: 数据大小={}字节, group_id={}",
            original_size, &group_id[..16]
        );

        // 创建 FEC 编码器
        let fec = FEC::new(k, n)
            .map_err(|e| anyhow!("创建FEC编码器失败: {}", e))?;

        // reed_solomon_rs 要求输入长度是 k 的倍数
        // 库内部会用 '_' 填充，但我们用 0 填充以保持一致性
        let padded_size = if original_size % k == 0 {
            original_size
        } else {
            original_size + (k - (original_size % k))
        };
        let mut padded_data = vec![0u8; padded_size];
        padded_data[..original_size].copy_from_slice(data);

        // 编码生成 n 个 Share
        let mut shares: Vec<Share> = vec![
            Share {
                number: 0,
                data: vec![],
            };
            n
        ];

        fec.encode(&padded_data, |s: Share| {
            let idx = s.number;
            shares[idx] = s;
        })
        .map_err(|e| anyhow!("Reed-Solomon编码失败: {}", e))?;

        // 将 Share 转换为 DataShard
        let shards: Vec<DataShard> = shares
            .into_iter()
            .enumerate()
            .map(|(i, share)| {
                let is_parity = i >= k;
                DataShard::new(
                    group_id.clone(),
                    i,
                    is_parity,
                    share.data,
                    original_size,
                    self.data_shards,
                    self.parity_shards,
                )
            })
            .collect();

        let shard_size = shards[0].data.len();
        info!(
            "编码完成: 生成{}个分片（{}数据 + {}校验），每个分片{}字节, 纠错能力={}个错误分片",
            n,
            self.data_shards,
            self.parity_shards,
            shard_size,
            self.error_correction_capacity()
        );

        Ok(ShardGroup {
            group_id,
            original_size,
            data_shard_count: self.data_shards,
            parity_shard_count: self.parity_shards,
            shards,
        })
    }

    /// 解码：从分片中恢复原始数据（带纠错能力）
    ///
    /// 使用 Berlekamp-Welch 算法，不仅能从部分分片中恢复数据，
    /// 还能自动检测和纠正错误的分片（无需知道哪些分片是错误的）。
    ///
    /// ## 纠错能力
    /// 设 n = 总分片数，k = 数据分片数，提供的分片数为 m：
    /// - 可纠正的最大错误分片数 e = ⌊(m - k) / 2⌋
    /// - 需要至少 k 个正确分片才能恢复
    ///
    /// ## 对 RBC 协议的意义
    /// 在 READY 阶段收集到的分片中，可能包含恶意节点发送的假分片。
    /// Berlekamp-Welch 算法能自动识别并纠正这些假分片，
    /// 配合 `hash(M') == h` 的整体哈希验证，确保最终输出正确。
    ///
    /// # 参数
    /// - `available_shards`: 可用的分片列表（可能包含错误分片）
    ///
    /// # 返回
    /// 恢复后的原始数据
    pub fn decode(&self, available_shards: &[DataShard]) -> Result<Vec<u8>> {
        if available_shards.is_empty() {
            return Err(anyhow!("没有可用的分片"));
        }

        // 从第一个分片获取元信息
        let first = &available_shards[0];
        let original_size = first.original_size;
        let data_shard_count = first.data_shard_count;
        let parity_shard_count = first.parity_shard_count;
        let n = data_shard_count + parity_shard_count;

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

        let error_capacity = (available_shards.len() - self.data_shards) / 2;

        debug!(
            "开始解码: 可用分片={}/{}, 纠错能力={}个错误分片 (Berlekamp-Welch)",
            available_shards.len(),
            n,
            error_capacity
        );

        // 创建 FEC 解码器
        let fec = FEC::new(self.data_shards, n)
            .map_err(|e| anyhow!("创建FEC解码器失败: {}", e))?;

        // 将 DataShard 转换为 Share
        let shares: Vec<Share> = available_shards
            .iter()
            .map(|shard| Share {
                number: shard.id.index,
                data: shard.data.clone(),
            })
            .collect();

        // 根据分片数量选择解码策略：
        // - 分片数 > k：使用 decode()（Berlekamp-Welch 纠错 + 重建）
        // - 分片数 == k：使用 rebuild()（仅重建，无纠错余量）
        let recovered = if available_shards.len() > self.data_shards {
            // 有冗余分片，使用 Berlekamp-Welch 纠错解码
            // decode() 内部会先调用 correct() 纠正错误分片，再调用 rebuild() 重建数据
            fec.decode(Vec::new(), shares)
                .map_err(|e| anyhow!("Reed-Solomon纠错解码失败: {}", e))?
        } else {
            // 分片数刚好等于 k，没有纠错余量，直接重建
            // 此时假设所有分片都是正确的（无法纠错）
            debug!(
                "分片数=k={}，无纠错余量，直接重建",
                self.data_shards
            );

            // 检查是否所有 k 个数据分片都在（number 0..k-1）
            let has_all_data_shards = (0..self.data_shards)
                .all(|i| shares.iter().any(|s| s.number == i));

            if has_all_data_shards {
                // 所有数据分片都在，直接按顺序拼接
                let piece_len = shares[0].data.len();
                let result_len = piece_len * self.data_shards;
                let mut dst = vec![0u8; result_len];
                for share in &shares {
                    if share.number < self.data_shards {
                        let start = share.number * piece_len;
                        dst[start..start + piece_len].copy_from_slice(&share.data);
                    }
                }
                dst
            } else {
                // 包含校验分片，需要矩阵求逆重建
                // 使用 encode_single 反向计算：先用 rebuild 重建
                // 为避免 reed_solomon_rs rebuild 的边界 bug，
                // 我们给 shares 补充一个虚拟分片使其 > k，然后用 decode
                // 但这里 shares.len() == k，无法纠错，只能尝试 rebuild
                let piece_len = shares[0].data.len();
                let result_len = piece_len * self.data_shards;
                let mut dst = vec![0u8; result_len];
                // 使用 catch_unwind 捕获 rebuild 可能的 panic
                let shares_clone = shares.clone();
                let rebuild_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let mut inner_dst = vec![0u8; result_len];
                    fec.rebuild(shares_clone, |s: Share| {
                        if s.number < self.data_shards {
                            let start = s.number * piece_len;
                            let end = start + piece_len;
                            if end <= inner_dst.len() {
                                inner_dst[start..end].copy_from_slice(&s.data);
                            }
                        }
                    }).map(|_| inner_dst)
                }));

                match rebuild_result {
                    Ok(Ok(data)) => data,
                    Ok(Err(e)) => return Err(anyhow!("Reed-Solomon重建失败: {}", e)),
                    Err(_) => return Err(anyhow!(
                        "Reed-Solomon重建失败: 分片数量刚好等于k={}且包含校验分片，无法安全重建",
                        self.data_shards
                    )),
                }
            }
        };

        // 截断到原始大小（去除填充字节）
        let mut result = recovered;
        result.truncate(original_size);

        info!("解码完成: 恢复数据大小={}字节", result.len());

        Ok(result)
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

    /// 模拟分片损坏场景并尝试纠错恢复
    ///
    /// # 参数
    /// - `group`: 完整的分片组
    /// - `corrupt_indices`: 要模拟损坏的分片索引列表
    /// - `corrupt_byte`: 用于替换分片数据的字节值
    ///
    /// # 返回
    /// 纠错恢复后的原始数据
    pub fn simulate_corruption_and_recover(
        &self,
        group: &ShardGroup,
        corrupt_indices: &[usize],
        corrupt_byte: u8,
    ) -> Result<Vec<u8>> {
        let mut corrupted_shards = group.shards.clone();

        for shard in corrupted_shards.iter_mut() {
            if corrupt_indices.contains(&shard.id.index) {
                // 损坏分片数据（用指定字节填充）
                for byte in shard.data.iter_mut() {
                    *byte = corrupt_byte;
                }
                // 注意：不更新哈希，模拟恶意篡改
            }
        }

        info!(
            "模拟损坏{}个分片 {:?}，纠错能力={}，尝试纠错恢复...",
            corrupt_indices.len(),
            corrupt_indices,
            self.error_correction_capacity()
        );

        self.decode(&corrupted_shards)
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
            "纠删码参数: ({},{}) - 总{}个分片，可容忍任意{}个分片丢失，可纠正{}个错误分片，存储开销{:.1}x (Berlekamp-Welch)",
            self.data_shards,
            self.parity_shards,
            self.total_shards(),
            self.parity_shards,
            self.error_correction_capacity(),
            self.storage_overhead_ratio()
        )
    }
}