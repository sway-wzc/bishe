use anyhow::{anyhow, Result};
use log::{debug, error, info, warn};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};

use crate::erasure::codec::ErasureCodec;
use crate::erasure::shard::DataShard;

use super::types::{RbcConfig, RbcInstanceState, RbcMessage, RbcOutput};

/// 单个RBC广播实例的状态机
///
/// 对应算法4：四轮长消息RBC协议
/// 每个广播实例独立维护自己的状态，支持多个并发广播
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

    // === ECHO阶段状态 ===
    /// 收到的ECHO消息：(sender_node_id) -> (shard_index, shard_data, data_hash)
    echo_received: HashMap<String, (usize, Vec<u8>, String)>,
    /// 按data_hash分组的ECHO计数，用于判断是否达到 2t+1 阈值
    echo_hash_count: HashMap<String, usize>,
    /// 本节点自己的分片（从ECHO中确认的）
    my_shard: Option<(usize, Vec<u8>)>,
    /// 已确认的数据哈希
    confirmed_hash: Option<String>,
    /// 是否已发送READY消息
    ready_sent: bool,

    // === READY阶段状态 ===
    /// 收到的READY消息中的分片：(shard_index) -> shard_data
    /// 对应算法第16行：T_h = {(j, m_j*)}
    ready_shards: HashMap<usize, Vec<u8>>,
    /// 按data_hash分组的READY计数
    ready_hash_count: HashMap<String, usize>,
    /// 是否通过READY放大触发（算法第13-15行）
    ready_amplified: bool,

    // === 元信息（从第一个ECHO/READY中获取） ===
    original_size: Option<usize>,
    data_shard_count: Option<usize>,
    parity_shard_count: Option<usize>,

    /// 最终输出
    output: Option<RbcOutput>,
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
            echo_hash_count: HashMap::new(),
            my_shard: None,
            confirmed_hash: None,
            ready_sent: false,
            ready_shards: HashMap::new(),
            ready_hash_count: HashMap::new(),
            ready_amplified: false,
            original_size: None,
            data_shard_count: None,
            parity_shard_count: None,
            output: None,
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
    // 第1轮：PROPOSE — 广播者发起
    // ========================================================================

    /// 作为广播者发起广播（算法第1-3行）
    ///
    /// 广播者调用此方法，生成 PROPOSE 消息发送给所有节点
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

        let mut messages = Vec::new();

        // 向所有节点（包括自己）发送 PROPOSE
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
    // 第2轮：ECHO — 收到PROPOSE后编码并分发
    // ========================================================================

    /// 处理收到的 PROPOSE 消息（算法第6-10行）
    ///
    /// 节点收到 M 后：
    /// 1. 计算 h = hash(M)
    /// 2. 用RS码将 M 编码为 n 个分片 [m_1, ..., m_n]
    /// 3. 向每个节点 j 发送 ⟨ECHO, m_j, h⟩
    ///
    /// # 返回
    /// 需要发送的 (目标节点ID, RBC消息) 列表
    pub fn handle_propose(
        &mut self,
        broadcaster: &str,
        data: &[u8],
    ) -> Result<Vec<(String, RbcMessage)>> {
        // 算法第7行：P(M) 验证（这里简单验证数据非空）
        if data.is_empty() {
            warn!("[RBC-{}] 收到空的PROPOSE消息，忽略", &self.instance_id[..8.min(self.instance_id.len())]);
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

        info!(
            "[RBC-{}] 收到PROPOSE: 广播者={}, 数据大小={}, hash={}...",
            &self.instance_id[..8.min(self.instance_id.len())],
            broadcaster,
            data.len(),
            &data_hash[..16]
        );

        // 算法第9行：M' := [m_1, ..., m_n] := RSEnc(M_i, n, t+1)
        // 使用纠删码编码器将数据编码为 n 个分片
        let shard_group = self.codec.encode(data)?;

        // 保存元信息
        self.original_size = Some(shard_group.original_size);
        self.data_shard_count = Some(shard_group.data_shard_count);
        self.parity_shard_count = Some(shard_group.parity_shard_count);
        self.confirmed_hash = Some(data_hash.clone());

        // 进入ECHO阶段
        self.state = RbcInstanceState::EchoPhase;

        // 算法第10行：send ⟨ECHO, m_j, h⟩ to node j for j = 1,2,...,n
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
                        shard_hash: shard.hash.clone(),
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
    // 第2-3轮：处理ECHO消息
    // ========================================================================

    /// 处理收到的 ECHO 消息（算法第11-12行）
    ///
    /// 当节点 i 收到 2t+1 个哈希和分片均匹配的专属 ECHO 消息时，
    /// 确立自己的分片 m_i，并向全网广播 ⟨READY, m_i, h⟩
    ///
    /// # 返回
    /// 需要发送的 (目标节点ID, RBC消息) 列表 + 可能的输出
    pub fn handle_echo(
        &mut self,
        sender: &str,
        data_hash: &str,
        shard_index: usize,
        shard_data: &[u8],
        shard_hash: &str,
        original_size: usize,
        data_shard_count: usize,
        parity_shard_count: usize,
    ) -> Result<Vec<(String, RbcMessage)>> {
        if self.is_completed() {
            return Ok(Vec::new());
        }

        // 验证分片完整性
        let computed_hash = Self::compute_hash(shard_data);
        if computed_hash != shard_hash {
            warn!(
                "[RBC-{}] ECHO分片完整性校验失败: sender={}, index={}",
                &self.instance_id[..8.min(self.instance_id.len())],
                sender,
                shard_index
            );
            return Ok(Vec::new());
        }

        // 验证这个ECHO是发给我的（分片索引应该等于我的索引）
        if shard_index != self.local_index {
            debug!(
                "[RBC-{}] 收到非本节点的ECHO: shard_index={}, local_index={}",
                &self.instance_id[..8.min(self.instance_id.len())],
                shard_index,
                self.local_index
            );
            // 根据协议，每个节点只接收自己索引的ECHO
            return Ok(Vec::new());
        }

        // 去重：同一个sender只接受一次
        if self.echo_received.contains_key(sender) {
            return Ok(Vec::new());
        }

        // 保存元信息（如果还没有的话）
        if self.original_size.is_none() {
            self.original_size = Some(original_size);
            self.data_shard_count = Some(data_shard_count);
            self.parity_shard_count = Some(parity_shard_count);
        }

        // 记录ECHO
        self.echo_received.insert(
            sender.to_string(),
            (shard_index, shard_data.to_vec(), data_hash.to_string()),
        );

        // 按data_hash计数
        let count = self
            .echo_hash_count
            .entry(data_hash.to_string())
            .or_insert(0);
        *count += 1;
        let current_count = *count;

        debug!(
            "[RBC-{}] 收到ECHO: sender={}, hash={}..., 当前计数={}/{}",
            &self.instance_id[..8.min(self.instance_id.len())],
            sender,
            &data_hash[..16.min(data_hash.len())],
            current_count,
            self.config.echo_threshold()
        );

        // 算法第11行：upon receiving 2t+1 ⟨ECHO, m_i, h⟩ matching messages
        // and not having sent a READY message do
        if current_count >= self.config.echo_threshold() && !self.ready_sent {
            info!(
                "[RBC-{}] ECHO阈值达到 ({}/{}), 确立分片并发送READY",
                &self.instance_id[..8.min(self.instance_id.len())],
                current_count,
                self.config.echo_threshold()
            );

            // 确立自己的分片
            self.my_shard = Some((self.local_index, shard_data.to_vec()));
            self.confirmed_hash = Some(data_hash.to_string());
            self.state = RbcInstanceState::ReadyPhase;

            // 算法第12行：send ⟨READY, m_i, h⟩ to all
            return self.send_ready(data_hash, self.local_index, shard_data);
        }

        Ok(Vec::new())
    }

    // ========================================================================
    // 第3轮：处理READY消息
    // ========================================================================

    /// 处理收到的 READY 消息（算法第13-15行 + 第16-21行）
    ///
    /// 两个触发条件：
    /// 1. 收到 t+1 个 READY 且未发送过 READY → 等待 ECHO 确认后发送 READY（放大）
    /// 2. 收集 READY 中的分片，尝试重建
    ///
    /// # 返回
    /// 需要发送的消息列表
    pub fn handle_ready(
        &mut self,
        sender: &str,
        data_hash: &str,
        shard_index: usize,
        shard_data: &[u8],
        shard_hash: &str,
        original_size: usize,
        data_shard_count: usize,
        parity_shard_count: usize,
    ) -> Result<Vec<(String, RbcMessage)>> {
        if self.is_completed() {
            return Ok(Vec::new());
        }

        // 验证分片完整性
        let computed_hash = Self::compute_hash(shard_data);
        if computed_hash != shard_hash {
            warn!(
                "[RBC-{}] READY分片完整性校验失败: sender={}, index={}",
                &self.instance_id[..8.min(self.instance_id.len())],
                sender,
                shard_index
            );
            return Ok(Vec::new());
        }

        // 保存元信息
        if self.original_size.is_none() {
            self.original_size = Some(original_size);
            self.data_shard_count = Some(data_shard_count);
            self.parity_shard_count = Some(parity_shard_count);
        }

        // 算法第16行：For the first ⟨READY, m_j*, h⟩ received from node j,
        // add (j, m_j*) to T_h
        // 每个分片索引只保留第一个收到的
        self.ready_shards
            .entry(shard_index)
            .or_insert_with(|| shard_data.to_vec());

        // 按data_hash计数READY
        let count = self
            .ready_hash_count
            .entry(data_hash.to_string())
            .or_insert(0);
        *count += 1;
        let current_count = *count;

        debug!(
            "[RBC-{}] 收到READY: sender={}, shard_index={}, hash={}..., 计数={}, 分片集合大小={}",
            &self.instance_id[..8.min(self.instance_id.len())],
            sender,
            shard_index,
            &data_hash[..16.min(data_hash.len())],
            current_count,
            self.ready_shards.len()
        );

        let mut outgoing = Vec::new();

        // 算法第13-15行：upon receiving t+1 ⟨READY, *, h⟩ messages
        // and not having sent a READY message do
        if current_count >= self.config.ready_amplify_threshold()
            && !self.ready_sent
            && !self.ready_amplified
        {
            info!(
                "[RBC-{}] READY放大阈值达到 ({}/{}), 触发READY放大",
                &self.instance_id[..8.min(self.instance_id.len())],
                current_count,
                self.config.ready_amplify_threshold()
            );

            self.ready_amplified = true;

            // 算法第14行：Wait for t+1 matching ⟨ECHO, m_i', h⟩
            // 如果已经有自己的分片（从ECHO阶段确认的），直接发送READY
            let shard_clone = self.my_shard.clone();
            if let Some((idx, data)) = shard_clone {
                self.confirmed_hash = Some(data_hash.to_string());
                self.state = RbcInstanceState::ReadyPhase;
                let msgs = self.send_ready(data_hash, idx, &data)?;
                outgoing.extend(msgs);
            } else {
                // 还没有确认自己的分片，检查是否已经收到足够的ECHO
                // 使用已收到的ECHO中的分片数据
                // 先收集匹配的ECHO数据（克隆以避免借用冲突）
                let matching_echos: Vec<(usize, Vec<u8>)> = self
                    .echo_received
                    .values()
                    .filter(|(_, _, h)| h == data_hash)
                    .map(|(idx, data, _)| (*idx, data.clone()))
                    .collect();

                if matching_echos.len() >= self.config.ready_amplify_threshold() {
                    // 使用第一个匹配的ECHO中的分片作为自己的分片
                    if let Some((idx, data)) = matching_echos.first() {
                        self.my_shard = Some((*idx, data.clone()));
                        self.confirmed_hash = Some(data_hash.to_string());
                        self.state = RbcInstanceState::ReadyPhase;
                        let msgs = self.send_ready(data_hash, *idx, data)?;
                        outgoing.extend(msgs);
                    }
                } else {
                    debug!(
                        "[RBC-{}] READY放大触发但ECHO不足，等待更多ECHO",
                        &self.instance_id[..8.min(self.instance_id.len())]
                    );
                }
            }
        }

        // 算法第17-21行：Error Correction — 尝试重建
        // for 0 <= r <= t do
        //   upon |T_h| >= 2t + r + 1 do
        //     尝试 RSDec(t+1, r, T) 解码
        let shard_count = self.ready_shards.len();
        for r in 0..=self.config.fault_tolerance {
            let threshold = self.config.reconstruct_threshold(r);
            if shard_count >= threshold {
                match self.try_reconstruct(data_hash) {
                    Ok(Some(output)) => {
                        info!(
                            "[RBC-{}] 重建成功! r={}, 使用{}个分片, 数据大小={}字节",
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
                        // 哈希不匹配，继续尝试更大的r
                        debug!(
                            "[RBC-{}] r={} 重建后哈希不匹配，继续尝试",
                            &self.instance_id[..8.min(self.instance_id.len())],
                            r
                        );
                    }
                    Err(e) => {
                        debug!(
                            "[RBC-{}] r={} 重建失败: {}, 继续尝试",
                            &self.instance_id[..8.min(self.instance_id.len())],
                            r,
                            e
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

    /// 发送READY消息到所有节点
    fn send_ready(
        &mut self,
        data_hash: &str,
        shard_index: usize,
        shard_data: &[u8],
    ) -> Result<Vec<(String, RbcMessage)>> {
        if self.ready_sent {
            return Ok(Vec::new());
        }

        self.ready_sent = true;
        let shard_hash = Self::compute_hash(shard_data);

        let original_size = self.original_size.unwrap_or(0);
        let data_shard_count = self.data_shard_count.unwrap_or(self.config.data_shards);
        let parity_shard_count = self.parity_shard_count.unwrap_or(self.config.parity_shards);

        info!(
            "[RBC-{}] 广播READY: shard_index={}, hash={}...",
            &self.instance_id[..8.min(self.instance_id.len())],
            shard_index,
            &data_hash[..16.min(data_hash.len())]
        );

        let mut messages = Vec::new();
        for node_id in &self.node_ids {
            messages.push((
                node_id.clone(),
                RbcMessage::Ready {
                    instance_id: self.instance_id.clone(),
                    sender: self.local_node_id.clone(),
                    data_hash: data_hash.to_string(),
                    shard_index,
                    shard_data: shard_data.to_vec(),
                    shard_hash: shard_hash.clone(),
                    original_size,
                    data_shard_count,
                    parity_shard_count,
                },
            ));
        }

        Ok(messages)
    }

    /// 尝试从收集到的READY分片中重建原始数据（算法第19-21行）
    ///
    /// 使用RS码解码算法尝试还原消息 M'，
    /// 如果 hash(M') = h 则输出 M'
    fn try_reconstruct(&self, expected_hash: &str) -> Result<Option<RbcOutput>> {
        let original_size = self
            .original_size
            .ok_or_else(|| anyhow!("缺少original_size元信息"))?;
        let data_shard_count = self
            .data_shard_count
            .ok_or_else(|| anyhow!("缺少data_shard_count元信息"))?;
        let parity_shard_count = self
            .parity_shard_count
            .ok_or_else(|| anyhow!("缺少parity_shard_count元信息"))?;

        // 将收集到的分片构造为 DataShard 对象
        let shards: Vec<DataShard> = self
            .ready_shards
            .iter()
            .map(|(&index, data)| {
                let is_parity = index >= data_shard_count;
                DataShard::new(
                    self.instance_id.clone(), // 使用instance_id作为group_id
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
            "[RBC-{}] 尝试重建: 可用分片={}, 需要至少{}个",
            &self.instance_id[..8.min(self.instance_id.len())],
            shards.len(),
            data_shard_count
        );

        // 使用纠删码解码器重建
        let recovered = self.codec.decode(&shards)?;

        // 算法第20行：if hash(M') = h then
        let recovered_hash = Self::compute_hash(&recovered);
        if recovered_hash == expected_hash {
            // 算法第21行：output M' and return
            Ok(Some(RbcOutput {
                instance_id: self.instance_id.clone(),
                broadcaster: String::new(), // 广播者信息在外层维护
                data: recovered,
                data_hash: recovered_hash,
            }))
        } else {
            debug!(
                "[RBC-{}] 重建数据哈希不匹配: expected={}..., got={}...",
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
    ///
    /// # 参数
    /// - `instance_id`: 广播实例唯一标识
    /// - `data`: 要广播的数据
    ///
    /// # 返回
    /// 需要发送的 (目标节点ID, RBC消息) 列表
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
    ///
    /// # 返回
    /// 需要发送的 (目标节点ID, RBC消息) 列表
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
                shard_hash,
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
                    &shard_hash,
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
                shard_hash,
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
                    &shard_hash,
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
