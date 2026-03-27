use anyhow::{anyhow, Result};
use log::{debug, error, info, warn};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

use super::protocol::RbcManager;
use super::types::{
    ChunkedBroadcastMeta, ChunkedBroadcastOutput, RbcMessage, RbcOutput,
    DEFAULT_CHUNK_SIZE,
};

/// 分块广播会话状态
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkedSessionState {
    /// 等待元信息
    WaitingMeta,
    /// 正在接收分块
    Receiving {
        /// 已接收的分块数
        received_count: usize,
        /// 总分块数
        total_chunks: usize,
    },
    /// 所有分块已接收，重组完成
    Completed,
    /// 会话失败
    Failed(String),
}

/// 分块广播接收会话
/// 管理单个超大文件的分块接收和重组
#[derive(Debug)]
struct ChunkedReceiveSession {
    /// 会话ID
    session_id: String,
    /// 元信息（收到后填充）
    meta: Option<ChunkedBroadcastMeta>,
    /// 已接收的分块数据：chunk_index -> data
    received_chunks: HashMap<usize, Vec<u8>>,
    /// 会话状态
    state: ChunkedSessionState,
    /// 最终输出
    output: Option<ChunkedBroadcastOutput>,
}

impl ChunkedReceiveSession {
    fn new(session_id: String) -> Self {
        Self {
            session_id,
            meta: None,
            received_chunks: HashMap::new(),
            state: ChunkedSessionState::WaitingMeta,
            output: None,
        }
    }

    /// 设置元信息
    fn set_meta(&mut self, meta: ChunkedBroadcastMeta) {
        info!(
            "[分块会话-{}] 收到元信息: 文件大小={}字节, 分块数={}, 分块大小={}字节",
            &self.session_id[..8.min(self.session_id.len())],
            meta.total_size,
            meta.total_chunks,
            meta.chunk_size
        );
        let total_chunks = meta.total_chunks;
        self.meta = Some(meta);
        self.state = ChunkedSessionState::Receiving {
            received_count: self.received_chunks.len(),
            total_chunks,
        };
        // 检查是否已经收到了所有分块（可能元信息后到）
        self.try_complete();
    }

    /// 接收一个分块
    fn receive_chunk(&mut self, chunk_index: usize, data: Vec<u8>) -> Result<()> {
        // 如果有元信息，验证分块哈希
        if let Some(ref meta) = self.meta {
            if chunk_index >= meta.total_chunks {
                return Err(anyhow!(
                    "分块索引越界: index={}, total={}",
                    chunk_index,
                    meta.total_chunks
                ));
            }

            // 验证分块哈希
            let chunk_hash = Self::compute_hash(&data);
            if chunk_hash != meta.chunk_hashes[chunk_index] {
                return Err(anyhow!(
                    "分块{}哈希不匹配: 期望={}, 实际={}",
                    chunk_index,
                    &meta.chunk_hashes[chunk_index][..16],
                    &chunk_hash[..16]
                ));
            }
        }

        self.received_chunks.insert(chunk_index, data);

        if let Some(ref meta) = self.meta {
            self.state = ChunkedSessionState::Receiving {
                received_count: self.received_chunks.len(),
                total_chunks: meta.total_chunks,
            };

            debug!(
                "[分块会话-{}] 收到分块 {}/{}, 已接收 {}/{}",
                &self.session_id[..8.min(self.session_id.len())],
                chunk_index,
                meta.total_chunks,
                self.received_chunks.len(),
                meta.total_chunks
            );
        }

        self.try_complete();
        Ok(())
    }

    /// 尝试重组完整文件
    fn try_complete(&mut self) {
        let meta = match &self.meta {
            Some(m) => m,
            None => return,
        };

        if self.received_chunks.len() < meta.total_chunks {
            return;
        }

        // 所有分块已收到，按顺序重组
        let mut full_data = Vec::with_capacity(meta.total_size);
        for i in 0..meta.total_chunks {
            if let Some(chunk) = self.received_chunks.get(&i) {
                full_data.extend_from_slice(chunk);
            } else {
                self.state = ChunkedSessionState::Failed(
                    format!("缺少分块 {}", i),
                );
                return;
            }
        }

        // 截断到原始大小（最后一个分块可能有填充）
        full_data.truncate(meta.total_size);

        // 验证完整文件哈希
        let file_hash = Self::compute_hash(&full_data);
        if file_hash != meta.file_hash {
            self.state = ChunkedSessionState::Failed(format!(
                "文件哈希不匹配: 期望={}, 实际={}",
                &meta.file_hash[..16],
                &file_hash[..16]
            ));
            error!(
                "[分块会话-{}] 文件重组后哈希不匹配!",
                &self.session_id[..8.min(self.session_id.len())]
            );
            return;
        }

        info!(
            "[分块会话-{}] 文件重组成功! 大小={}字节, hash={}...",
            &self.session_id[..8.min(self.session_id.len())],
            full_data.len(),
            &file_hash[..16]
        );

        self.output = Some(ChunkedBroadcastOutput {
            session_id: self.session_id.clone(),
            total_size: full_data.len(),
            total_chunks: meta.total_chunks,
            file_hash: file_hash.clone(),
            data: full_data,
        });
        self.state = ChunkedSessionState::Completed;
    }

    fn compute_hash(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        hex::encode(hasher.finalize())
    }

    fn is_completed(&self) -> bool {
        self.state == ChunkedSessionState::Completed
    }
}

/// 分块广播管理器
///
/// 在RbcManager之上提供超大文件分块广播能力。
/// 将大文件拆分为多个块，每个块通过独立的RBC实例广播，
/// 接收端自动重组还原完整文件。
pub struct ChunkedBroadcastManager {
    /// 底层RBC管理器引用（通过外部传入进行操作）
    /// 注意：ChunkedBroadcastManager不持有RbcManager，
    /// 而是在每次操作时由调用者传入，避免双重借用问题
    
    /// 分块大小（字节）
    chunk_size: usize,
    /// 活跃的接收会话：session_id -> session
    receive_sessions: HashMap<String, ChunkedReceiveSession>,
    /// 已完成的输出队列
    completed_outputs: Vec<ChunkedBroadcastOutput>,
    /// 元信息实例ID到会话ID的映射
    meta_instance_to_session: HashMap<String, String>,
    /// 分块实例ID到(会话ID, 分块索引)的映射
    chunk_instance_to_session: HashMap<String, (String, usize)>,
}

impl ChunkedBroadcastManager {
    /// 创建分块广播管理器
    pub fn new() -> Self {
        Self::with_chunk_size(DEFAULT_CHUNK_SIZE)
    }

    /// 创建指定分块大小的分块广播管理器
    pub fn with_chunk_size(chunk_size: usize) -> Self {
        info!("创建分块广播管理器: 分块大小={}字节 ({}MB)",
            chunk_size, chunk_size / 1024 / 1024);
        Self {
            chunk_size,
            receive_sessions: HashMap::new(),
            completed_outputs: Vec::new(),
            meta_instance_to_session: HashMap::new(),
            chunk_instance_to_session: HashMap::new(),
        }
    }

    /// 发起超大文件分块广播
    ///
    /// 将文件数据拆分为多个块，生成元信息和各块的RBC广播消息。
    ///
    /// # 参数
    /// - `session_id`: 分块广播会话唯一标识
    /// - `data`: 完整文件数据
    /// - `rbc_manager`: RBC管理器（用于发起各块的RBC广播）
    ///
    /// # 返回
    /// 所有需要发送的 (目标节点ID, RBC消息) 列表
    pub fn broadcast(
        &mut self,
        session_id: String,
        data: Vec<u8>,
        rbc_manager: &mut RbcManager,
    ) -> Result<Vec<(String, RbcMessage)>> {
        let total_size = data.len();
        if total_size == 0 {
            return Err(anyhow!("数据不能为空"));
        }

        // 计算分块
        let total_chunks = (total_size + self.chunk_size - 1) / self.chunk_size;
        let file_hash = Self::compute_hash(&data);

        info!(
            "[分块广播-{}] 发起广播: 文件大小={}字节 ({:.2}MB), 分块数={}, 分块大小={}字节",
            &session_id[..8.min(session_id.len())],
            total_size,
            total_size as f64 / 1024.0 / 1024.0,
            total_chunks,
            self.chunk_size
        );

        // 计算每个分块的哈希
        let mut chunk_hashes = Vec::with_capacity(total_chunks);
        for i in 0..total_chunks {
            let start = i * self.chunk_size;
            let end = std::cmp::min(start + self.chunk_size, total_size);
            let chunk_hash = Self::compute_hash(&data[start..end]);
            chunk_hashes.push(chunk_hash);
        }

        // 构建元信息
        let meta = ChunkedBroadcastMeta {
            session_id: session_id.clone(),
            total_size,
            chunk_size: self.chunk_size,
            total_chunks,
            file_hash: file_hash.clone(),
            chunk_hashes: chunk_hashes.clone(),
        };

        let mut all_messages = Vec::new();

        // 1. 先广播元信息（通过一个特殊的RBC实例）
        let meta_instance_id = format!("{}_meta", session_id);
        let meta_bytes = bincode::serialize(&meta)
            .map_err(|e| anyhow!("序列化元信息失败: {}", e))?;

        info!(
            "[分块广播-{}] 广播元信息: instance={}, 大小={}字节",
            &session_id[..8.min(session_id.len())],
            &meta_instance_id[..16.min(meta_instance_id.len())],
            meta_bytes.len()
        );

        let meta_msgs = rbc_manager.broadcast(meta_instance_id.clone(), meta_bytes)?;
        self.meta_instance_to_session
            .insert(meta_instance_id, session_id.clone());
        all_messages.extend(meta_msgs);

        // 2. 逐块广播数据
        for i in 0..total_chunks {
            let start = i * self.chunk_size;
            let end = std::cmp::min(start + self.chunk_size, total_size);
            let chunk_data = data[start..end].to_vec();

            let chunk_instance_id = format!("{}_chunk_{}", session_id, i);

            info!(
                "[分块广播-{}] 广播分块 {}/{}: instance={}, 大小={}字节",
                &session_id[..8.min(session_id.len())],
                i + 1,
                total_chunks,
                &chunk_instance_id[..16.min(chunk_instance_id.len())],
                chunk_data.len()
            );

            let chunk_msgs = rbc_manager.broadcast(chunk_instance_id.clone(), chunk_data)?;
            self.chunk_instance_to_session
                .insert(chunk_instance_id, (session_id.clone(), i));
            all_messages.extend(chunk_msgs);
        }

        // 自己也创建一个接收会话（广播者也需要输出）
        let mut session = ChunkedReceiveSession::new(session_id.clone());
        session.set_meta(meta);
        self.receive_sessions.insert(session_id, session);

        info!(
            "[分块广播] 共生成 {} 条RBC消息（1个元信息 + {}个分块）",
            all_messages.len(),
            total_chunks
        );

        Ok(all_messages)
    }

    /// 处理RBC输出，检查是否属于某个分块广播会话
    ///
    /// 当RBC管理器产生输出时，调用此方法检查该输出是否属于分块广播。
    /// 如果是，则自动收集分块并在所有分块到齐后重组文件。
    ///
    /// # 返回
    /// - `true`: 该输出已被分块广播管理器处理
    /// - `false`: 该输出不属于任何分块广播会话（应作为普通RBC输出处理）
    pub fn handle_rbc_output(&mut self, output: &RbcOutput) -> bool {
        let instance_id = &output.instance_id;

        // 检查是否是元信息
        if let Some(session_id) = self.meta_instance_to_session.get(instance_id).cloned() {
            match bincode::deserialize::<ChunkedBroadcastMeta>(&output.data) {
                Ok(meta) => {
                    let session = self.receive_sessions
                        .entry(session_id.clone())
                        .or_insert_with(|| ChunkedReceiveSession::new(session_id.clone()));
                    session.set_meta(meta);
                    self.check_session_completion(&session_id);
                    return true;
                }
                Err(e) => {
                    error!("反序列化分块广播元信息失败: {}", e);
                    return true; // 仍然标记为已处理
                }
            }
        }

        // 检查是否是分块数据
        if let Some((session_id, chunk_index)) = self.chunk_instance_to_session.get(instance_id).cloned() {
            let session = self.receive_sessions
                .entry(session_id.clone())
                .or_insert_with(|| ChunkedReceiveSession::new(session_id.clone()));

            if let Err(e) = session.receive_chunk(chunk_index, output.data.clone()) {
                warn!("接收分块失败: {}", e);
            }
            self.check_session_completion(&session_id);
            return true;
        }

        // 尝试通过实例ID模式匹配（用于接收端没有预注册映射的情况）
        if instance_id.ends_with("_meta") {
            let session_id = instance_id.trim_end_matches("_meta").to_string();
            match bincode::deserialize::<ChunkedBroadcastMeta>(&output.data) {
                Ok(meta) => {
                    // 注册映射关系
                    self.meta_instance_to_session
                        .insert(instance_id.clone(), session_id.clone());
                    // 注册所有分块的映射
                    for i in 0..meta.total_chunks {
                        let chunk_instance_id = format!("{}_chunk_{}", session_id, i);
                        self.chunk_instance_to_session
                            .insert(chunk_instance_id, (session_id.clone(), i));
                    }

                    let session = self.receive_sessions
                        .entry(session_id.clone())
                        .or_insert_with(|| ChunkedReceiveSession::new(session_id.clone()));
                    session.set_meta(meta);
                    self.check_session_completion(&session_id);
                    return true;
                }
                Err(_) => {
                    // 不是有效的元信息，不处理
                    return false;
                }
            }
        }

        if let Some(pos) = instance_id.rfind("_chunk_") {
            let session_id = instance_id[..pos].to_string();
            if let Ok(chunk_index) = instance_id[pos + 7..].parse::<usize>() {
                // 注册映射
                self.chunk_instance_to_session
                    .insert(instance_id.clone(), (session_id.clone(), chunk_index));

                let session = self.receive_sessions
                    .entry(session_id.clone())
                    .or_insert_with(|| ChunkedReceiveSession::new(session_id.clone()));

                if let Err(e) = session.receive_chunk(chunk_index, output.data.clone()) {
                    warn!("接收分块失败: {}", e);
                }
                self.check_session_completion(&session_id);
                return true;
            }
        }

        false
    }

    /// 检查会话是否完成
    fn check_session_completion(&mut self, session_id: &str) {
        if let Some(session) = self.receive_sessions.get(session_id) {
            if session.is_completed() {
                if let Some(output) = session.output.clone() {
                    info!(
                        "[分块广播管理器] 会话 {} 完成! 文件大小={}字节, 分块数={}",
                        &session_id[..8.min(session_id.len())],
                        output.total_size,
                        output.total_chunks
                    );
                    self.completed_outputs.push(output);
                }
            }
        }
    }

    /// 取出所有已完成的分块广播输出
    pub fn drain_outputs(&mut self) -> Vec<ChunkedBroadcastOutput> {
        std::mem::take(&mut self.completed_outputs)
    }

    /// 获取活跃会话数量
    pub fn active_session_count(&self) -> usize {
        self.receive_sessions
            .values()
            .filter(|s| !s.is_completed())
            .count()
    }

    /// 获取指定会话的状态
    pub fn session_state(&self, session_id: &str) -> Option<ChunkedSessionState> {
        self.receive_sessions.get(session_id).map(|s| s.state.clone())
    }

    /// 清理已完成的会话
    pub fn cleanup_completed(&mut self) {
        self.receive_sessions.retain(|_, s| !s.is_completed());
    }

    fn compute_hash(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        hex::encode(hasher.finalize())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rbc::protocol::RbcManager;
    use crate::rbc::types::RbcConfig;

    /// 辅助函数：创建n个RBC管理器
    fn create_managers(n: usize) -> Vec<RbcManager> {
        let node_ids: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();
        let config = RbcConfig::new(n).unwrap();
        node_ids
            .iter()
            .map(|id| RbcManager::new(id.clone(), config.clone(), node_ids.clone()))
            .collect()
    }

    /// 辅助函数：将消息路由到目标节点
    fn route_messages(
        managers: &mut Vec<RbcManager>,
        messages: Vec<(String, RbcMessage)>,
        node_ids: &[String],
    ) -> Vec<(String, RbcMessage)> {
        let mut new_messages = Vec::new();
        for (target_id, msg) in messages {
            if let Some(idx) = node_ids.iter().position(|id| id == &target_id) {
                match managers[idx].handle_message(msg) {
                    Ok(msgs) => new_messages.extend(msgs),
                    Err(e) => eprintln!("节点 {} 处理消息失败: {}", target_id, e),
                }
            }
        }
        new_messages
    }

    /// 辅助函数：运行消息传递直到收敛
    fn run_until_quiescent(
        managers: &mut Vec<RbcManager>,
        initial_messages: Vec<(String, RbcMessage)>,
        node_ids: &[String],
    ) {
        let mut pending = initial_messages;
        let mut rounds = 0;
        let max_rounds = 500; // 分块广播可能需要更多轮次

        while !pending.is_empty() && rounds < max_rounds {
            rounds += 1;
            let msg_count = pending.len();
            pending = route_messages(managers, pending, node_ids);
            if rounds <= 5 || rounds % 20 == 0 {
                println!(
                    "第{}轮: 处理{}条消息, 产生{}条新消息",
                    rounds, msg_count, pending.len()
                );
            }
        }
        println!("协议在{}轮后收敛", rounds);
    }

    #[test]
    fn test_chunked_broadcast_small_file() {
        // 测试小文件（小于一个分块）的分块广播
        println!("\n=== 分块广播测试: 小文件（100字节）===");

        let n = 4;
        let node_ids: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();
        let mut managers = create_managers(n);

        // 使用小分块大小便于测试
        let mut chunked_mgr = ChunkedBroadcastManager::with_chunk_size(1024);

        let data: Vec<u8> = (0..100).map(|i| (i % 256) as u8).collect();
        let original_hash = ChunkedBroadcastManager::compute_hash(&data);

        let initial_msgs = chunked_mgr
            .broadcast("small_test".to_string(), data.clone(), &mut managers[0])
            .unwrap();

        println!("生成 {} 条初始消息", initial_msgs.len());

        run_until_quiescent(&mut managers, initial_msgs, &node_ids);

        // 收集所有RBC输出并交给分块管理器处理
        let mut all_chunked_mgrs: Vec<ChunkedBroadcastManager> = (0..n)
            .map(|_| ChunkedBroadcastManager::with_chunk_size(1024))
            .collect();
        // 第一个节点使用已有的管理器
        all_chunked_mgrs[0] = chunked_mgr;

        let mut success_count = 0;
        for (i, manager) in managers.iter_mut().enumerate() {
            let outputs = manager.drain_outputs();
            for output in &outputs {
                all_chunked_mgrs[i].handle_rbc_output(output);
            }
            let chunked_outputs = all_chunked_mgrs[i].drain_outputs();
            for co in &chunked_outputs {
                assert_eq!(co.data, data, "节点 {} 数据不一致", i);
                assert_eq!(co.file_hash, original_hash);
                println!(
                    "节点 {} 成功重组文件: 大小={}字节, 分块数={}",
                    i, co.total_size, co.total_chunks
                );
                success_count += 1;
            }
        }

        println!("小文件分块广播: {}/{} 个节点成功", success_count, n);
        assert!(success_count >= n - 1, "至少n-1个节点应成功");
    }

    #[test]
    fn test_chunked_broadcast_multi_chunk() {
        // 测试需要多个分块的文件
        println!("\n=== 分块广播测试: 多分块文件（5KB，分块大小1KB）===");

        let n = 4;
        let node_ids: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();
        let mut managers = create_managers(n);

        let chunk_size = 1024; // 1KB分块
        let mut chunked_mgr = ChunkedBroadcastManager::with_chunk_size(chunk_size);

        // 5KB数据 → 5个分块
        let data: Vec<u8> = (0..5120).map(|i| ((i * 7 + 13) % 256) as u8).collect();
        let original_hash = ChunkedBroadcastManager::compute_hash(&data);

        let initial_msgs = chunked_mgr
            .broadcast("multi_chunk_test".to_string(), data.clone(), &mut managers[0])
            .unwrap();

        println!("5KB文件分为5个分块，生成 {} 条初始消息", initial_msgs.len());

        run_until_quiescent(&mut managers, initial_msgs, &node_ids);

        let mut all_chunked_mgrs: Vec<ChunkedBroadcastManager> = (0..n)
            .map(|_| ChunkedBroadcastManager::with_chunk_size(chunk_size))
            .collect();
        all_chunked_mgrs[0] = chunked_mgr;

        let mut success_count = 0;
        for (i, manager) in managers.iter_mut().enumerate() {
            let outputs = manager.drain_outputs();
            for output in &outputs {
                all_chunked_mgrs[i].handle_rbc_output(output);
            }
            let chunked_outputs = all_chunked_mgrs[i].drain_outputs();
            for co in &chunked_outputs {
                assert_eq!(co.data, data, "节点 {} 数据不一致", i);
                assert_eq!(co.file_hash, original_hash);
                assert_eq!(co.total_chunks, 5);
                println!(
                    "节点 {} 成功重组: 大小={}字节, 分块数={}, hash={}...",
                    i,
                    co.total_size,
                    co.total_chunks,
                    &co.file_hash[..16]
                );
                success_count += 1;
            }
        }

        println!("多分块广播: {}/{} 个节点成功", success_count, n);
        assert!(success_count >= n - 1);
    }

    #[test]
    fn test_chunked_broadcast_large_data() {
        // 测试较大数据的分块广播（模拟超大文件场景）
        println!("\n=== 分块广播测试: 大数据（50KB，分块大小10KB）===");

        let n = 7;
        let node_ids: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();
        let mut managers = create_managers(n);

        let chunk_size = 10 * 1024; // 10KB分块
        let mut chunked_mgr = ChunkedBroadcastManager::with_chunk_size(chunk_size);

        // 50KB数据 → 5个分块
        let data: Vec<u8> = (0..50 * 1024).map(|i| ((i * 31 + 17) % 256) as u8).collect();
        let original_hash = ChunkedBroadcastManager::compute_hash(&data);

        let initial_msgs = chunked_mgr
            .broadcast("large_data_test".to_string(), data.clone(), &mut managers[0])
            .unwrap();

        println!(
            "50KB文件分为{}个分块，生成 {} 条初始消息",
            (50 * 1024 + chunk_size - 1) / chunk_size,
            initial_msgs.len()
        );

        run_until_quiescent(&mut managers, initial_msgs, &node_ids);

        let mut all_chunked_mgrs: Vec<ChunkedBroadcastManager> = (0..n)
            .map(|_| ChunkedBroadcastManager::with_chunk_size(chunk_size))
            .collect();
        all_chunked_mgrs[0] = chunked_mgr;

        let mut success_count = 0;
        for (i, manager) in managers.iter_mut().enumerate() {
            let outputs = manager.drain_outputs();
            for output in &outputs {
                all_chunked_mgrs[i].handle_rbc_output(output);
            }
            let chunked_outputs = all_chunked_mgrs[i].drain_outputs();
            for co in &chunked_outputs {
                assert_eq!(co.data, data, "节点 {} 数据不一致", i);
                assert_eq!(co.file_hash, original_hash);
                success_count += 1;
            }
        }

        println!("大数据分块广播: {}/{} 个节点成功", success_count, n);
        assert!(success_count >= n - 2);
    }

    #[test]
    fn test_chunked_broadcast_with_node_failure() {
        // 测试分块广播在节点故障下的容错能力
        println!("\n=== 分块广播测试: 节点故障容错（7节点，1节点宕机）===");

        let n = 7;
        let node_ids: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();
        let mut managers = create_managers(n);

        let chunk_size = 1024;
        let mut chunked_mgr = ChunkedBroadcastManager::with_chunk_size(chunk_size);

        let data: Vec<u8> = (0..3000).map(|i| ((i * 11 + 3) % 256) as u8).collect();
        let original_hash = ChunkedBroadcastManager::compute_hash(&data);

        let initial_msgs = chunked_mgr
            .broadcast("fault_test".to_string(), data.clone(), &mut managers[0])
            .unwrap();

        // 模拟node_6宕机：过滤掉发给node_6的消息
        let failed_node = "node_6";
        let filtered_msgs: Vec<_> = initial_msgs
            .into_iter()
            .filter(|(target, _)| target != failed_node)
            .collect();

        println!("模拟 {} 宕机", failed_node);

        // 运行协议，丢弃所有涉及故障节点的消息
        let mut pending = filtered_msgs;
        let mut rounds = 0;
        while !pending.is_empty() && rounds < 500 {
            rounds += 1;
            let mut new_msgs = Vec::new();
            for (target_id, msg) in pending {
                if target_id == failed_node {
                    continue;
                }
                if let Some(idx) = node_ids.iter().position(|id| id == &target_id) {
                    match managers[idx].handle_message(msg) {
                        Ok(msgs) => {
                            let filtered: Vec<_> = msgs
                                .into_iter()
                                .filter(|(t, _)| t != failed_node)
                                .collect();
                            new_msgs.extend(filtered);
                        }
                        Err(e) => eprintln!("处理失败: {}", e),
                    }
                }
            }
            pending = new_msgs;
        }
        println!("协议在{}轮后收敛", rounds);

        let mut all_chunked_mgrs: Vec<ChunkedBroadcastManager> = (0..n)
            .map(|_| ChunkedBroadcastManager::with_chunk_size(chunk_size))
            .collect();
        all_chunked_mgrs[0] = chunked_mgr;

        let mut success_count = 0;
        for (i, manager) in managers.iter_mut().enumerate() {
            if node_ids[i] == failed_node {
                continue;
            }
            let outputs = manager.drain_outputs();
            for output in &outputs {
                all_chunked_mgrs[i].handle_rbc_output(output);
            }
            let chunked_outputs = all_chunked_mgrs[i].drain_outputs();
            for co in &chunked_outputs {
                assert_eq!(co.data, data, "节点 {} 数据不一致", i);
                assert_eq!(co.file_hash, original_hash);
                success_count += 1;
            }
        }

        println!(
            "节点故障容错分块广播: {}/{} 个存活节点成功",
            success_count,
            n - 1
        );
        assert!(success_count >= 4, "至少4个存活节点应成功");
    }

    #[test]
    fn test_chunked_broadcast_non_aligned_size() {
        // 测试文件大小不是分块大小整数倍的情况
        println!("\n=== 分块广播测试: 非对齐文件大小（2500字节，分块1024字节）===");

        let n = 4;
        let node_ids: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();
        let mut managers = create_managers(n);

        let chunk_size = 1024;
        let mut chunked_mgr = ChunkedBroadcastManager::with_chunk_size(chunk_size);

        // 2500字节 → 3个分块（1024 + 1024 + 452）
        let data: Vec<u8> = (0..2500).map(|i| ((i * 3 + 1) % 256) as u8).collect();
        let original_hash = ChunkedBroadcastManager::compute_hash(&data);

        let initial_msgs = chunked_mgr
            .broadcast("non_aligned_test".to_string(), data.clone(), &mut managers[0])
            .unwrap();

        run_until_quiescent(&mut managers, initial_msgs, &node_ids);

        let mut all_chunked_mgrs: Vec<ChunkedBroadcastManager> = (0..n)
            .map(|_| ChunkedBroadcastManager::with_chunk_size(chunk_size))
            .collect();
        all_chunked_mgrs[0] = chunked_mgr;

        let mut success_count = 0;
        for (i, manager) in managers.iter_mut().enumerate() {
            let outputs = manager.drain_outputs();
            for output in &outputs {
                all_chunked_mgrs[i].handle_rbc_output(output);
            }
            let chunked_outputs = all_chunked_mgrs[i].drain_outputs();
            for co in &chunked_outputs {
                assert_eq!(co.data.len(), 2500, "节点 {} 数据大小不正确", i);
                assert_eq!(co.data, data, "节点 {} 数据不一致", i);
                assert_eq!(co.file_hash, original_hash);
                assert_eq!(co.total_chunks, 3);
                success_count += 1;
            }
        }

        println!("非对齐大小分块广播: {}/{} 个节点成功", success_count, n);
        assert!(success_count >= n - 1);
    }

    #[test]
    fn test_chunked_broadcast_exact_one_byte() {
        // 测试极端情况：1字节文件
        println!("\n=== 分块广播测试: 极小文件（1字节）===");

        let n = 4;
        let node_ids: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();
        let mut managers = create_managers(n);

        let chunk_size = 1024;
        let mut chunked_mgr = ChunkedBroadcastManager::with_chunk_size(chunk_size);

        let data = vec![42u8]; // 1字节
        let original_hash = ChunkedBroadcastManager::compute_hash(&data);

        let initial_msgs = chunked_mgr
            .broadcast("one_byte_test".to_string(), data.clone(), &mut managers[0])
            .unwrap();

        run_until_quiescent(&mut managers, initial_msgs, &node_ids);

        let mut all_chunked_mgrs: Vec<ChunkedBroadcastManager> = (0..n)
            .map(|_| ChunkedBroadcastManager::with_chunk_size(chunk_size))
            .collect();
        all_chunked_mgrs[0] = chunked_mgr;

        let mut success_count = 0;
        for (i, manager) in managers.iter_mut().enumerate() {
            let outputs = manager.drain_outputs();
            for output in &outputs {
                all_chunked_mgrs[i].handle_rbc_output(output);
            }
            let chunked_outputs = all_chunked_mgrs[i].drain_outputs();
            for co in &chunked_outputs {
                assert_eq!(co.data, data);
                assert_eq!(co.total_chunks, 1);
                success_count += 1;
            }
        }

        println!("1字节分块广播: {}/{} 个节点成功", success_count, n);
        assert!(success_count >= n - 1);
    }
}
