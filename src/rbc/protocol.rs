use anyhow::{anyhow, Result};
use log::{debug, info, warn};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

use crate::erasure::codec::ErasureCodec;
use crate::erasure::shard::DataShard;

use super::types::{RbcConfig, RbcInstanceState, RbcMessage, RbcOutput};

/// 单个RBC广播实例的状态机
///
/// 严格对应论文算法4：Four-round RBC protocol for long messages
///
/// ## 算法概述
/// 1. 广播者发送 ⟨PROPOSE, M⟩ 给所有节点
/// 2. 节点收到 M 后，计算 h=hash(M)，RS编码为 n 个分片，
///    向节点 j 发送 ⟨ECHO, m_j, h⟩
/// 3. 收到 2t+1 个匹配的 ECHO 后，广播 ⟨READY, m_i, h⟩
/// 4. 收集 READY 中的分片到 T_h，当 |T_h| ≥ 2t+r+1 时，
///    用 RSDec(t+1, r, T) 尝试纠错解码，若 hash(M')=h 则输出
///
/// ## 纠错能力（Berlekamp-Welch）
/// 底层RS码具有纠错能力，可自动检测和纠正恶意分片，
/// 无需在分片级别做哈希校验。最终通过 hash(M')=h 验证正确性。
#[derive(Debug)]
pub struct RbcInstance {
    /// 广播实例唯一标识
    instance_id: String,
    /// 本节点ID
    local_node_id: String,
    /// 本节点在网络中的索引（0..n-1，用于确定自己的分片）
    local_index: usize,
    /// RBC协议配置
    config: RbcConfig,
    /// 当前状态
    state: RbcInstanceState,
    /// 纠删码编解码器
    codec: ErasureCodec,
    /// 所有节点ID列表（有序，索引即为分片索引）
    node_ids: Vec<String>,

    // === ECHO阶段状态（算法第11行）===
    /// 收到的ECHO消息：sender_node_id -> (shard_data, data_hash)
    /// 每个sender只接受一次ECHO（去重）
    /// 注意：这里只存储发给自己（shard_index == local_index）的ECHO
    echo_received: HashMap<String, (Vec<u8>, String)>,
    /// 按(shard_data, data_hash)组合分组的ECHO计数
    /// 论文第11行 "matching" 要求 (m_i, h) 完全相同
    /// key = (shard_data的SHA-256摘要, data_hash), value = 计数
    echo_match_count: HashMap<(String, String), usize>,
    /// 按data_hash分组的ECHO计数（用于READY放大路径第14行的t+1判断）
    echo_hash_count: HashMap<String, usize>,
    /// 本节点自己的分片 m_i（从ECHO中确认的）
    my_shard: Option<Vec<u8>>,
    /// 已确认的数据哈希 h
    confirmed_hash: Option<String>,
    /// 是否已发送READY消息
    ready_sent: bool,

    // === READY阶段状态（算法第16行）===
    /// T_h: 按data_hash分组的READY分片集合
    /// 对应算法第16行：For the first ⟨READY, m_j*, h⟩ received from node j,
    ///                  add (j, m_j*) to T_h
    /// 外层key = data_hash, 内层key = sender的索引 j, value = 分片数据 m_j*
    /// 按data_hash分组管理，防止不同哈希的READY互相抢占分片Slot
    ready_shards: HashMap<String, HashMap<usize, Vec<u8>>>,
    /// 按data_hash分组的sender去重集合
    /// 外层key = data_hash, 内层key = sender_node_id, value = 该sender声称的shard_index
    ready_senders: HashMap<String, HashMap<String, usize>>,
    /// 按data_hash分组的READY计数
    ready_hash_count: HashMap<String, usize>,

    // === 渐进式纠错解码状态（算法第17-21行优化）===
    /// 上次解码失败时的r值，下次直接从此值开始尝试
    last_failed_r: usize,
    /// 上次尝试解码时的分片数量（用于判断分片集合是否有变化）
    last_decode_shard_count: usize,

    // === 元信息（从第一个ECHO/READY中获取）===
    original_size: Option<usize>,
    data_shard_count: Option<usize>,
    parity_shard_count: Option<usize>,

    /// 广播者节点ID（从第一个PROPOSE或ECHO中获取）
    broadcaster_id: Option<String>,

    /// 最终输出
    output: Option<RbcOutput>,

    /// 跟踪的不同data_hash数量上限（防止内存膨胀攻击）
    /// 恶意节点可能发送大量不同哈希的READY消息，导致ready_shards无限增长
    max_tracked_hashes: usize,
}

impl RbcInstance {
    /// 创建新的RBC广播实例
    ///
    /// # 参数
    /// - `instance_id`: 广播实例唯一标识
    /// - `local_node_id`: 本节点ID
    /// - `config`: RBC协议配置
    /// - `node_ids`: 所有节点ID列表（有序）
    pub fn new(
        instance_id: String,
        local_node_id: String,
        config: RbcConfig,
        node_ids: Vec<String>,
    ) -> Result<Self> {
        let local_index = node_ids
            .iter()
            .position(|id| id == &local_node_id)
            .ok_or_else(|| anyhow!("本节点ID {} 不在节点列表中", local_node_id))?;

        let codec = ErasureCodec::new(config.data_shards, config.parity_shards)?;
        // 最多跟踪 t+2 个不同的哈希（1个正确 + t个恶意 + 1个余量）
        let max_tracked_hashes = config.fault_tolerance + 2;

        info!(
            "[RBC-{}] 创建实例: 本节点={} (索引={}), {}",
            &instance_id[..8.min(instance_id.len())],
            local_node_id,
            local_index,
            config.info()
        );

        Ok(Self {
            instance_id,
            local_node_id,
            local_index,
            config,
            state: RbcInstanceState::Init,
            codec,
            node_ids,
            echo_received: HashMap::new(),
            echo_match_count: HashMap::new(),
            echo_hash_count: HashMap::new(),
            my_shard: None,
            confirmed_hash: None,
            ready_sent: false,
            ready_shards: HashMap::new(),  // HashMap<data_hash, HashMap<shard_index, data>>
            ready_senders: HashMap::new(), // HashMap<data_hash, HashMap<sender_id, shard_index>>
            ready_hash_count: HashMap::new(),
            last_failed_r: 0,
            last_decode_shard_count: 0,
            original_size: None,
            data_shard_count: None,
            parity_shard_count: None,
            broadcaster_id: None,
            output: None,
            max_tracked_hashes,
        })
    }

    /// 获取当前状态
    pub fn state(&self) -> &RbcInstanceState {
        &self.state
    }

    /// 获取输出结果
    pub fn output(&self) -> Option<&RbcOutput> {
        self.output.as_ref()
    }

    /// 是否已完成
    pub fn is_completed(&self) -> bool {
        self.state == RbcInstanceState::Completed
    }

    /// 计算数据的SHA-256哈希
    fn compute_hash(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        hex::encode(hasher.finalize())
    }

    // ========================================================================
    // 算法第1-3行：PROPOSE — 广播者发起
    // ========================================================================

    /// 作为广播者发起广播
    ///
    /// ```text
    /// 1: // only broadcaster node
    /// 2: input M
    /// 3: send ⟨PROPOSE, M⟩ to all
    /// ```
    ///
    /// # 返回
    /// 需要发送的 (目标节点ID, RBC消息) 列表
    pub fn broadcast(&mut self, data: Vec<u8>) -> Result<Vec<(String, RbcMessage)>> {
        if self.state != RbcInstanceState::Init {
            return Err(anyhow!("只能在Init状态发起广播，当前状态: {:?}", self.state));
        }

        info!(
            "[RBC-{}] 广播者发起PROPOSE，数据大小={}字节",
            &self.instance_id[..8.min(self.instance_id.len())],
            data.len()
        );

        // 算法第3行：send ⟨PROPOSE, M⟩ to all
        let mut messages = Vec::new();
        for node_id in &self.node_ids {
            messages.push((
                node_id.clone(),
                RbcMessage::Propose {
                    instance_id: self.instance_id.clone(),
                    broadcaster: self.local_node_id.clone(),
                    data: data.clone(),
                },
            ));
        }

        Ok(messages)
    }

    // ========================================================================
    // 算法第6-10行：收到PROPOSE后编码并分发ECHO
    // ========================================================================

    /// 处理收到的 PROPOSE 消息
    ///
    /// ```text
    /// 6: upon receiving ⟨PROPOSE, M⟩ from the broadcaster do
    /// 7:   if P(M) then
    /// 8:     Let h := hash(M)
    /// 9:     Let M' := [m_1, m_2, ..., m_n] := RSEnc(M_i, n, t+1)
    /// 10:    send ⟨ECHO, m_j, h⟩ to node j for j = 1, 2, ..., n
    /// ```
    ///
    /// # 返回
    /// 需要发送的 (目标节点ID, RBC消息) 列表
    pub fn handle_propose(
        &mut self,
        broadcaster: &str,
        data: &[u8],
    ) -> Result<Vec<(String, RbcMessage)>> {
        // 算法第7行：P(M) 验证（predicate，默认返回true，这里验证非空）
        if data.is_empty() {
            warn!(
                "[RBC-{}] 收到空的PROPOSE消息，P(M)=false，忽略",
                &self.instance_id[..8.min(self.instance_id.len())]
            );
            return Ok(Vec::new());
        }

        // 如果已经处理过PROPOSE，忽略重复的
        if self.state != RbcInstanceState::Init {
            debug!(
                "[RBC-{}] 已处理过PROPOSE，忽略重复消息",
                &self.instance_id[..8.min(self.instance_id.len())]
            );
            return Ok(Vec::new());
        }

        // 算法第8行：h := hash(M)
        let data_hash = Self::compute_hash(data);

        // 记录广播者ID
        self.broadcaster_id = Some(broadcaster.to_string());

        info!(
            "[RBC-{}] 收到PROPOSE: 广播者={}, 数据大小={}, h={}...",
            &self.instance_id[..8.min(self.instance_id.len())],
            broadcaster,
            data.len(),
            &data_hash[..16]
        );

        // 算法第9行：M' := [m_1, ..., m_n] := RSEnc(M_i, n, t+1)
        let shard_group = self.codec.encode(data)?;

        // 保存元信息
        self.original_size = Some(shard_group.original_size);
        self.data_shard_count = Some(shard_group.data_shard_count);
        self.parity_shard_count = Some(shard_group.parity_shard_count);
        self.confirmed_hash = Some(data_hash.clone());

        // 进入ECHO阶段
        self.state = RbcInstanceState::EchoPhase;

        // 算法第10行：send ⟨ECHO, m_j, h⟩ to node j for j = 1, 2, ..., n
        // 注意：不包含shard_hash，算法4依赖Berlekamp-Welch纠错
        let mut messages = Vec::new();
        for (j, node_id) in self.node_ids.iter().enumerate() {
            if j < shard_group.shards.len() {
                let shard = &shard_group.shards[j];
                messages.push((
                    node_id.clone(),
                    RbcMessage::Echo {
                        instance_id: self.instance_id.clone(),
                        sender: self.local_node_id.clone(),
                        data_hash: data_hash.clone(),
                        shard_index: j,
                        shard_data: shard.data.clone(),
                        original_size: shard_group.original_size,
                        data_shard_count: shard_group.data_shard_count,
                        parity_shard_count: shard_group.parity_shard_count,
                    },
                ));
            }
        }

        info!(
            "[RBC-{}] 生成{}个ECHO消息分发给各节点",
            &self.instance_id[..8.min(self.instance_id.len())],
            messages.len()
        );

        Ok(messages)
    }

    // ========================================================================
    // 算法第11-12行：处理ECHO消息
    // ========================================================================

    /// 处理收到的 ECHO 消息
    ///
    /// ```text
    /// 11: upon receiving 2t+1 ⟨ECHO, m_i, h⟩ matching messages
    ///     and not having sent a READY message do
    /// 12:   send ⟨READY, m_i, h⟩ to all
    /// ```
    ///
    /// 注意：不做shard_hash校验。论文算法4依赖Berlekamp-Welch纠错能力，
    /// 恶意分片在最终解码阶段被自动纠正，通过 hash(M')=h 验证正确性。
    ///
    /// # 返回
    /// 需要发送的 (目标节点ID, RBC消息) 列表
    pub fn handle_echo(
        &mut self,
        sender: &str,
        data_hash: &str,
        shard_index: usize,
        shard_data: &[u8],
        original_size: usize,
        data_shard_count: usize,
        parity_shard_count: usize,
    ) -> Result<Vec<(String, RbcMessage)>> {
        if self.is_completed() {
            return Ok(Vec::new());
        }

        // 验证这个ECHO是发给我的（分片索引应该等于我的索引）
        // 算法第10行：send ⟨ECHO, m_j, h⟩ to node j
        // 所以节点 i 只应该收到 shard_index == i 的ECHO
        if shard_index != self.local_index {
            debug!(
                "[RBC-{}] 收到非本节点的ECHO: shard_index={}, local_index={}，忽略",
                &self.instance_id[..8.min(self.instance_id.len())],
                shard_index,
                self.local_index
            );
            return Ok(Vec::new());
        }

        // 去重：同一个sender只接受一次ECHO
        if self.echo_received.contains_key(sender) {
            return Ok(Vec::new());
        }

        // 保存元信息（如果还没有的话）
        if self.original_size.is_none() {
            self.original_size = Some(original_size);
            self.data_shard_count = Some(data_shard_count);
            self.parity_shard_count = Some(parity_shard_count);
        }

        // 记录ECHO（不做shard_hash校验，依赖Berlekamp-Welch纠错）
        self.echo_received.insert(
            sender.to_string(),
            (shard_data.to_vec(), data_hash.to_string()),
        );

        // 按(m_i, h)组合计数（论文第11行的"matching"条件）
        // 论文要求2t+1个ECHO的(m_i, h)完全相同，而非仅h相同
        // 这样corrupt_shard节点（篡改分片但保持hash不变）的ECHO不会被计入匹配
        let shard_digest = Self::compute_hash(shard_data);
        let match_key = (shard_digest, data_hash.to_string());
        let match_count = self
            .echo_match_count
            .entry(match_key)
            .or_insert(0);
        *match_count += 1;
        let current_match_count = *match_count;

        // 同时按data_hash计数（用于READY放大路径第14行的t+1判断）
        let hash_count = self
            .echo_hash_count
            .entry(data_hash.to_string())
            .or_insert(0);
        *hash_count += 1;

        debug!(
            "[RBC-{}] 收到ECHO: sender={}, h={}..., (m_i,h)匹配计数={}/{}",
            &self.instance_id[..8.min(self.instance_id.len())],
            sender,
            &data_hash[..16.min(data_hash.len())],
            current_match_count,
            self.config.echo_threshold()
        );

        // 算法第11行：upon receiving 2t+1 ⟨ECHO, m_i, h⟩ matching messages
        //             and not having sent a READY message do
        // 严格按论文语义：\"matching\" 要求 (m_i, h) 完全相同
        if current_match_count >= self.config.echo_threshold() && !self.ready_sent {
            info!(
                "[RBC-{}] ECHO阈值达到 ({}/{}), 确立分片m_i并发送READY",
                &self.instance_id[..8.min(self.instance_id.len())],
                current_match_count,
                self.config.echo_threshold()
            );

            // 确立自己的分片 m_i
            // 由于2t+1个ECHO的(m_i, h)完全相同，当前shard_data就是正确的分片
            // （corrupt_shard节点的篡改分片不会被计入匹配，因为m_i不同）
            self.my_shard = Some(shard_data.to_vec());
            self.confirmed_hash = Some(data_hash.to_string());
            self.state = RbcInstanceState::ReadyPhase;

            // 算法第12行：send ⟨READY, m_i, h⟩ to all
            return self.send_ready(data_hash, shard_data);
        } else if current_match_count >= self.config.ready_amplify_threshold() && !self.ready_sent {
            // 补充实现算法第14行的异步唤醒逻辑：
            // 如果已经收到了 t+1 个 READY，并且现在 ECHO 也达到了 t+1 个，则触发 READY 放大
            let ready_count = self.ready_hash_count.get(data_hash).copied().unwrap_or(0);
            if ready_count >= self.config.ready_amplify_threshold() {
                info!(
                    "[RBC-{}] ECHO达到放大阈值 ({}/{}) 且已收到足够的READY ({}/{}), 触发READY放大",
                    &self.instance_id[..8.min(self.instance_id.len())],
                    current_match_count,
                    self.config.ready_amplify_threshold(),
                    ready_count,
                    self.config.ready_amplify_threshold()
                );
                
                self.my_shard = Some(shard_data.to_vec());
                self.confirmed_hash = Some(data_hash.to_string());
                self.state = RbcInstanceState::ReadyPhase;
                
                return self.send_ready(data_hash, shard_data);
            }
        }

        Ok(Vec::new())
    }

    // ========================================================================
    // 算法第13-21行：处理READY消息
    // ========================================================================

    /// 处理收到的 READY 消息
    ///
    /// ```text
    /// 13: upon receiving t+1 ⟨READY, *, h⟩ messages
    ///     and not having sent a READY message do
    /// 14:   Wait for t+1 matching ⟨ECHO, m_i', h⟩
    /// 15:   send ⟨READY, m_i', h⟩ to all
    ///
    /// 16: For the first ⟨READY, m_j*, h⟩ received from node j,
    ///     add (j, m_j*) to T_h    // T_h initialized as {}
    ///
    /// 17: for 0 ≤ r ≤ t do                    // Error Correction
    /// 18:   upon |T_h| ≥ 2t + r + 1 do
    /// 19:     Let M' be coefficients of RSDec(t+1, r, T)
    /// 20:     if hash(M') = h then
    /// 21:       output M' and return
    /// ```
    ///
    /// # 返回
    /// 需要发送的消息列表
    pub fn handle_ready(
        &mut self,
        sender: &str,
        data_hash: &str,
        shard_index: usize,
        shard_data: &[u8],
        original_size: usize,
        data_shard_count: usize,
        parity_shard_count: usize,
    ) -> Result<Vec<(String, RbcMessage)>> {
        if self.is_completed() {
            return Ok(Vec::new());
        }

        // 保存元信息
        if self.original_size.is_none() {
            self.original_size = Some(original_size);
            self.data_shard_count = Some(data_shard_count);
            self.parity_shard_count = Some(parity_shard_count);
        }

        // ====================================================================
        // 算法第16行：For the first ⟨READY, m_j*, h⟩ received from node j,
        //             add (j, m_j*) to T_h
        // ====================================================================
        // 按(data_hash, sender)去重：每个sender在每个hash组中只取第一条READY消息
        // sender的索引 j 就是 shard_index（因为每个诚实节点发送自己索引的分片）
        //
        // 关键安全设计1：通过sender在node_ids中的真实索引确定分片位置
        // 不信任消息中的shard_index（恶意节点可能伪造索引污染其他位置）
        let real_shard_index = match self.node_ids.iter().position(|id| id == sender) {
            Some(idx) => idx,
            None => {
                warn!(
                    "[RBC-{}] READY消息来自未知sender: {}, 忽略",
                    &self.instance_id[..8.min(self.instance_id.len())],
                    sender
                );
                return Ok(Vec::new());
            }
        };

        // 关键安全设计2：ready_shards 按 data_hash 分组管理
        // 这样即使 confirmed_hash 尚未确立，不同哈希的 READY 也不会互相抢占分片Slot
        // 当 confirmed_hash 确立后，只使用对应哈希组的 T_h 进行解码
        //
        // 关键安全设计3：限制跟踪的不同哈希数量，防止内存膨胀攻击
        // 恶意节点可能发送大量不同哈希的READY消息，导致ready_shards无限增长
        {
            // 检查是否超过哈希跟踪上限（已存在的哈希不受限制）
            if !self.ready_shards.contains_key(data_hash)
                && self.ready_shards.len() >= self.max_tracked_hashes
            {
                debug!(
                    "[RBC-{}] 已跟踪{}个不同哈希，拒绝新哈希: {}...",
                    &self.instance_id[..8.min(self.instance_id.len())],
                    self.ready_shards.len(),
                    &data_hash[..16.min(data_hash.len())]
                );
                return Ok(Vec::new());
            }

            let senders_for_hash = self.ready_senders
                .entry(data_hash.to_string())
                .or_insert_with(HashMap::new);
            if !senders_for_hash.contains_key(sender) {
                senders_for_hash.insert(sender.to_string(), real_shard_index);
                // 将 (j, m_j*) 加入 T_h
                // 使用 real_shard_index（基于sender在node_ids中的真实位置）
                // 而非消息中的 shard_index（可能被恶意伪造）
                let shards_for_hash = self.ready_shards
                    .entry(data_hash.to_string())
                    .or_insert_with(HashMap::new);
                shards_for_hash
                    .entry(real_shard_index)
                    .or_insert_with(|| shard_data.to_vec());
            }
        }

        // 按data_hash计数READY
        let count = self
            .ready_hash_count
            .entry(data_hash.to_string())
            .or_insert(0);
        *count += 1;
        let current_count = *count;

        // 计算当前哈希组的T_h大小
        let current_th_size = self.ready_shards
            .get(data_hash)
            .map(|s| s.len())
            .unwrap_or(0);

        debug!(
            "[RBC-{}] 收到READY: sender={}, claimed_index={}, real_index={}, h={}..., 计数={}, |T_h|={}",
            &self.instance_id[..8.min(self.instance_id.len())],
            sender,
            shard_index,
            real_shard_index,
            &data_hash[..16.min(data_hash.len())],
            current_count,
            current_th_size
        );

        let mut outgoing = Vec::new();

        // ====================================================================
        // 算法第13-15行：READY放大
        // upon receiving t+1 ⟨READY, *, h⟩ messages
        // and not having sent a READY message do
        //   Wait for t+1 matching ⟨ECHO, m_i', h⟩
        //   send ⟨READY, m_i', h⟩ to all
        // ====================================================================
        if current_count >= self.config.ready_amplify_threshold() && !self.ready_sent {
            info!(
                "[RBC-{}] READY放大阈值达到 ({}/{}), 检查ECHO确认...",
                &self.instance_id[..8.min(self.instance_id.len())],
                current_count,
                self.config.ready_amplify_threshold()
            );

            // 算法第14行：Wait for t+1 matching ⟨ECHO, m_i', h⟩
            // 论文要求 (m_i', h) 完全匹配，与第11行的matching语义一致
            // 查找echo_match_count中是否有某个(shard_digest, h)组合达到t+1
            let amplify_threshold = self.config.ready_amplify_threshold();
            let matching_echo = self.echo_match_count.iter()
                .find(|((_, h), count)| h == data_hash && **count >= amplify_threshold);

            if matching_echo.is_some() {
                // 有足够的(m_i', h)完全匹配的ECHO
                if let Some(ref shard) = self.my_shard {
                    // 已有确认的分片（来自第11行路径），直接使用
                    let shard_clone = shard.clone();
                    self.confirmed_hash = Some(data_hash.to_string());
                    self.state = RbcInstanceState::ReadyPhase;
                    // 算法第15行：send ⟨READY, m_i', h⟩ to all
                    let msgs = self.send_ready(data_hash, &shard_clone)?;
                    outgoing.extend(msgs);
                } else {
                    // 从匹配的ECHO中选择出现次数最多的分片（多数投票）
                    // 因为t+1个(m_i', h)匹配的ECHO中，分片数据都相同
                    let majority_shard = self.select_majority_shard_for_hash(data_hash);

                    if let Some(shard) = majority_shard {
                        self.my_shard = Some(shard.clone());
                        self.confirmed_hash = Some(data_hash.to_string());
                        self.state = RbcInstanceState::ReadyPhase;
                        let msgs = self.send_ready(data_hash, &shard)?;
                        outgoing.extend(msgs);
                    } else {
                        debug!(
                            "[RBC-{}] READY放大触发但无匹配的ECHO分片，等待...",
                            &self.instance_id[..8.min(self.instance_id.len())]
                        );
                    }
                }
            } else {
                // 没有足够的(m_i', h)完全匹配的ECHO
                let max_echo_match = self.echo_match_count.iter()
                    .filter(|((_, h), _)| h == data_hash)
                    .map(|(_, count)| *count)
                    .max()
                    .unwrap_or(0);
                debug!(
                    "[RBC-{}] READY放大触发但ECHO(m_i,h)匹配不足 ({}/{}), 等待更多ECHO",
                    &self.instance_id[..8.min(self.instance_id.len())],
                    max_echo_match,
                    self.config.ready_amplify_threshold()
                );
            }
        }

        // ====================================================================
        // 算法第17-21行：Error Correction — 渐进式纠错解码
        //
        // for 0 ≤ r ≤ t do
        //   upon |T_h| ≥ 2t + r + 1 do
        //     Let M' be coefficients of RSDec(t+1, r, T)
        //     if hash(M') = h then
        //       output M' and return
        //
        // 优化策略：
        //   1. 用 last_failed_r 记录上次失败的r值，下次直接从该值开始
        //   2. 用 last_decode_shard_count 记录上次分片数，若无变化则跳过
        //   3. 收到新分片时重置 last_failed_r（新分片可能改变解码结果）
        // ====================================================================

        // 使用confirmed_hash（ECHO阶段确认的正确哈希）进行验证，
        // 而非当前READY消息的data_hash（可能来自wrong_hash恶意节点）
        let verified_hash = match &self.confirmed_hash {
            Some(h) => h.clone(),
            None => {
                // 如果还没有confirmed_hash，暂时使用当前消息的data_hash
                data_hash.to_string()
            }
        };

        // 关键安全设计4：只对满足"放大阈值"(t+1)的哈希进行解码尝试
        // 防止攻击者伪造大量不同data_hash触发昂贵的纠错运算（CPU DoS）
        // 在算法中，只有当某个哈希收到了至少 t+1 个 READY 时，它才具有"合法性"倾向
        let verified_hash_count = self.ready_hash_count
            .get(&verified_hash)
            .copied()
            .unwrap_or(0);
        if verified_hash_count < self.config.ready_amplify_threshold() {
            return Ok(outgoing); // 票数不足，没必要浪费CPU去尝试解码
        }

        // 从对应哈希组的 T_h 中获取分片数量
        let shard_count = self.ready_shards
            .get(&verified_hash)
            .map(|s| s.len())
            .unwrap_or(0);

        // 优化1：分片集合无变化时，跳过本轮解码尝试
        if shard_count == self.last_decode_shard_count && shard_count > 0 {
            return Ok(outgoing);
        }

        // 分片集合有变化，重置缓存的r值
        if shard_count > self.last_decode_shard_count {
            self.last_failed_r = 0;
        }
        self.last_decode_shard_count = shard_count;

        // 从上次失败的r值开始尝试
        let start_r = self.last_failed_r;

        for r in start_r..=self.config.fault_tolerance {
            // 算法第18行：upon |T_h| ≥ 2t + r + 1 do
            let threshold = self.config.reconstruct_threshold(r);
            if shard_count >= threshold {
                // 算法第19行：Let M' be coefficients of RSDec(t+1, r, T)
                match self.try_reconstruct_from_hash(&verified_hash) {
                    Ok(Some(output)) => {
                        // 算法第20-21行：if hash(M') = h then output M' and return
                        info!(
                            "[RBC-{}] 纠错解码成功! r={}, |T_h|={}, 数据大小={}字节",
                            &self.instance_id[..8.min(self.instance_id.len())],
                            r,
                            shard_count,
                            output.data.len()
                        );
                        self.output = Some(output);
                        self.state = RbcInstanceState::Completed;
                        return Ok(outgoing);
                    }
                    Ok(None) => {
                        // hash(M') ≠ h，记录失败的r值，继续尝试更大的r
                        self.last_failed_r = r + 1;
                        debug!(
                            "[RBC-{}] r={}: RSDec成功但hash(M')≠h，尝试r={}",
                            &self.instance_id[..8.min(self.instance_id.len())],
                            r,
                            r + 1
                        );
                    }
                    Err(e) => {
                        // RSDec失败（分片不足等），记录失败的r值
                        self.last_failed_r = r + 1;
                        debug!(
                            "[RBC-{}] r={}: RSDec失败: {}, 尝试r={}",
                            &self.instance_id[..8.min(self.instance_id.len())],
                            r,
                            e,
                            r + 1
                        );
                    }
                }
            }
        }

        Ok(outgoing)
    }

    // ========================================================================
    // 内部辅助方法
    // ========================================================================

    /// 从data_hash匹配的ECHO中选择出现次数最多的分片数据（多数投票）
    ///
    /// 用于READY放大路径（算法第14行），当my_shard尚未确立时，
    /// 从t+1个匹配的ECHO中选择正确的分片。
    /// 由于t+1个匹配ECHO中至少1个来自诚实节点，正确分片一定存在。
    fn select_majority_shard_for_hash(&self, target_hash: &str) -> Option<Vec<u8>> {
        let mut shard_counts: HashMap<Vec<u8>, usize> = HashMap::new();
        for (shard_data, hash) in self.echo_received.values() {
            if hash == target_hash {
                let count = shard_counts.entry(shard_data.clone()).or_insert(0);
                *count += 1;
            }
        }

        shard_counts
            .into_iter()
            .max_by_key(|(_, count)| *count)
            .map(|(data, count)| {
                debug!(
                    "[RBC-{}] READY放大分片选择: 选择出现{}次的分片数据",
                    &self.instance_id[..8.min(self.instance_id.len())],
                    count
                );
                data
            })
    }

    /// 发送READY消息到所有节点（算法第12行/第15行）
    ///
    /// send ⟨READY, m_i, h⟩ to all
    fn send_ready(
        &mut self,
        data_hash: &str,
        shard_data: &[u8],
    ) -> Result<Vec<(String, RbcMessage)>> {
        if self.ready_sent {
            return Ok(Vec::new());
        }

        self.ready_sent = true;

        let original_size = self.original_size.unwrap_or(0);
        let data_shard_count = self.data_shard_count.unwrap_or(self.config.data_shards);
        let parity_shard_count = self.parity_shard_count.unwrap_or(self.config.parity_shards);

        info!(
            "[RBC-{}] 广播READY: shard_index={}, h={}...",
            &self.instance_id[..8.min(self.instance_id.len())],
            self.local_index,
            &data_hash[..16.min(data_hash.len())]
        );

        // 注意：READY消息中不包含shard_hash（论文算法4不需要）
        let mut messages = Vec::new();
        for node_id in &self.node_ids {
            messages.push((
                node_id.clone(),
                RbcMessage::Ready {
                    instance_id: self.instance_id.clone(),
                    sender: self.local_node_id.clone(),
                    data_hash: data_hash.to_string(),
                    shard_index: self.local_index,
                    shard_data: shard_data.to_vec(),
                    original_size,
                    data_shard_count,
                    parity_shard_count,
                },
            ));
        }

        Ok(messages)
    }

    /// 尝试从指定哈希组的 T_h 中纠错解码恢复原始数据（算法第19-21行）
    ///
    /// ```text
    /// 19: Let M' be coefficients of RSDec(t+1, r, T)
    /// 20: if hash(M') = h then
    /// 21:   output M' and return
    /// ```
    ///
    /// 使用 Berlekamp-Welch 算法进行纠错解码：
    /// - 自动检测和纠正 T_h 中的错误分片（恶意节点发送的假分片）
    /// - 无需知道哪些分片是错误的
    fn try_reconstruct_from_hash(&self, expected_hash: &str) -> Result<Option<RbcOutput>> {
        let original_size = self
            .original_size
            .ok_or_else(|| anyhow!("缺少original_size元信息"))?;
        let data_shard_count = self
            .data_shard_count
            .ok_or_else(|| anyhow!("缺少data_shard_count元信息"))?;
        let parity_shard_count = self
            .parity_shard_count
            .ok_or_else(|| anyhow!("缺少parity_shard_count元信息"))?;

        // 从对应哈希组的 T_h 中获取分片
        let hash_shards = match self.ready_shards.get(expected_hash) {
            Some(shards) => shards,
            None => return Ok(None), // 该哈希组没有分片
        };

        // 将 T_h 中的分片构造为 DataShard 对象
        let shards: Vec<DataShard> = hash_shards
            .iter()
            .map(|(&index, data)| {
                let is_parity = index >= data_shard_count;
                DataShard::new(
                    self.instance_id.clone(),
                    index,
                    is_parity,
                    data.clone(),
                    original_size,
                    data_shard_count,
                    parity_shard_count,
                )
            })
            .collect();

        debug!(
            "[RBC-{}] RSDec: |T_h|={}, k={}, 纠错能力=⌊({}-{})/2⌋={}",
            &self.instance_id[..8.min(self.instance_id.len())],
            shards.len(),
            data_shard_count,
            shards.len(),
            data_shard_count,
            (shards.len().saturating_sub(data_shard_count)) / 2
        );

        // 算法第19行：RSDec(t+1, r, T)
        // 使用Berlekamp-Welch纠错解码
        let recovered = self.codec.decode(&shards)?;

        // 算法第20行：if hash(M') = h then
        let recovered_hash = Self::compute_hash(&recovered);
        if recovered_hash == expected_hash {
            // 算法第21行：output M' and return
            Ok(Some(RbcOutput {
                instance_id: self.instance_id.clone(),
                broadcaster: self.broadcaster_id.clone().unwrap_or_default(),
                data: recovered,
                data_hash: recovered_hash,
            }))
        } else {
            debug!(
                "[RBC-{}] hash(M')≠h: expected={}..., got={}...",
                &self.instance_id[..8.min(self.instance_id.len())],
                &expected_hash[..16.min(expected_hash.len())],
                &recovered_hash[..16.min(recovered_hash.len())]
            );
            Ok(None)
        }
    }
}

/// RBC协议管理器
///
/// 管理多个并发的RBC广播实例，提供统一的消息处理接口
pub struct RbcManager {
    /// 本节点ID
    local_node_id: String,
    /// RBC协议配置
    config: RbcConfig,
    /// 所有节点ID列表（有序）
    node_ids: Vec<String>,
    /// 活跃的RBC实例：instance_id -> RbcInstance
    instances: HashMap<String, RbcInstance>,
    /// 已完成的输出队列
    completed_outputs: Vec<RbcOutput>,
}

impl RbcManager {
    /// 创建RBC协议管理器
    pub fn new(
        local_node_id: String,
        config: RbcConfig,
        mut node_ids: Vec<String>,
    ) -> Self {
        // 对节点ID排序，确保所有节点的顺序一致
        node_ids.sort();

        info!(
            "[RBC管理器] 初始化: 本节点={}, 节点数={}, {}",
            local_node_id,
            node_ids.len(),
            config.info()
        );

        Self {
            local_node_id,
            config,
            node_ids,
            instances: HashMap::new(),
            completed_outputs: Vec::new(),
        }
    }

    /// 获取或创建RBC实例
    fn get_or_create_instance(&mut self, instance_id: &str) -> Result<&mut RbcInstance> {
        if !self.instances.contains_key(instance_id) {
            let instance = RbcInstance::new(
                instance_id.to_string(),
                self.local_node_id.clone(),
                self.config.clone(),
                self.node_ids.clone(),
            )?;
            self.instances.insert(instance_id.to_string(), instance);
        }
        Ok(self.instances.get_mut(instance_id).unwrap())
    }

    /// 作为广播者发起一次RBC广播
    pub fn broadcast(
        &mut self,
        instance_id: String,
        data: Vec<u8>,
    ) -> Result<Vec<(String, RbcMessage)>> {
        let instance = RbcInstance::new(
            instance_id.clone(),
            self.local_node_id.clone(),
            self.config.clone(),
            self.node_ids.clone(),
        )?;
        self.instances.insert(instance_id.clone(), instance);
        let instance = self.instances.get_mut(&instance_id).unwrap();
        instance.broadcast(data)
    }

    /// 处理收到的RBC消息
    pub fn handle_message(
        &mut self,
        message: RbcMessage,
    ) -> Result<Vec<(String, RbcMessage)>> {
        match message {
            RbcMessage::Propose {
                instance_id,
                broadcaster,
                data,
            } => {
                let instance = self.get_or_create_instance(&instance_id)?;
                let msgs = instance.handle_propose(&broadcaster, &data)?;
                self.check_completion(&instance_id);
                Ok(msgs)
            }
            RbcMessage::Echo {
                instance_id,
                sender,
                data_hash,
                shard_index,
                shard_data,
                original_size,
                data_shard_count,
                parity_shard_count,
            } => {
                let instance = self.get_or_create_instance(&instance_id)?;
                let msgs = instance.handle_echo(
                    &sender,
                    &data_hash,
                    shard_index,
                    &shard_data,
                    original_size,
                    data_shard_count,
                    parity_shard_count,
                )?;
                self.check_completion(&instance_id);
                Ok(msgs)
            }
            RbcMessage::Ready {
                instance_id,
                sender,
                data_hash,
                shard_index,
                shard_data,
                original_size,
                data_shard_count,
                parity_shard_count,
            } => {
                let instance = self.get_or_create_instance(&instance_id)?;
                let msgs = instance.handle_ready(
                    &sender,
                    &data_hash,
                    shard_index,
                    &shard_data,
                    original_size,
                    data_shard_count,
                    parity_shard_count,
                )?;
                self.check_completion(&instance_id);
                Ok(msgs)
            }
        }
    }

    /// 检查实例是否完成，如果完成则收集输出
    fn check_completion(&mut self, instance_id: &str) {
        if let Some(instance) = self.instances.get(instance_id) {
            if instance.is_completed() {
                if let Some(output) = instance.output().cloned() {
                    info!(
                        "[RBC管理器] 实例 {} 已完成，数据大小={}字节",
                        &instance_id[..8.min(instance_id.len())],
                        output.data.len()
                    );
                    self.completed_outputs.push(output);
                }
            }
        }
    }

    /// 取出所有已完成的输出
    pub fn drain_outputs(&mut self) -> Vec<RbcOutput> {
        std::mem::take(&mut self.completed_outputs)
    }

    /// 获取活跃实例数量
    pub fn active_instance_count(&self) -> usize {
        self.instances
            .values()
            .filter(|i| !i.is_completed())
            .count()
    }

    /// 清理已完成的实例
    pub fn cleanup_completed(&mut self) {
        self.instances.retain(|_, i| !i.is_completed());
    }
}
