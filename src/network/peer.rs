use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::network::message::{Message, PeerInfo};

/// 对等节点状态
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerState {
    /// 正在连接/握手中
    Connecting,
    /// 已连接（正常通信中）
    Connected,
    /// 已断开
    Disconnected,
}

/// 对等节点数据
#[derive(Debug, Clone)]
pub struct Peer {
    /// 节点信息
    pub info: PeerInfo,
    /// 节点状态
    pub state: PeerState,
    /// 最后一次收到心跳的时间
    pub last_heartbeat: DateTime<Utc>,
    /// 连接建立时间
    pub connected_at: DateTime<Utc>,
    /// 消息发送通道
    pub sender: mpsc::Sender<Message>,
}

impl Peer {
    /// 创建新的对等节点
    pub fn new(info: PeerInfo, sender: mpsc::Sender<Message>) -> Self {
        let now = Utc::now();
        Self {
            info,
            state: PeerState::Connecting,
            last_heartbeat: now,
            connected_at: now,
            sender,
        }
    }

    /// 更新心跳时间
    pub fn update_heartbeat(&mut self) {
        self.last_heartbeat = Utc::now();
    }

    /// 设置为已连接状态
    pub fn set_connected(&mut self) {
        self.state = PeerState::Connected;
    }

    /// 设置为已断开状态
    pub fn set_disconnected(&mut self) {
        self.state = PeerState::Disconnected;
    }
}

/// 对等节点管理表
///
/// 使用 [`DashMap`] 实现无锁并发安全的节点表，
/// 支持多个异步任务同时读写而无需外部加锁。
#[derive(Debug, Clone)]
pub struct PeerTable {
    /// 节点ID -> 对等节点数据
    peers: Arc<DashMap<String, Peer>>,
    /// 最大连接数
    max_connections: usize,
}

impl PeerTable {
    /// 创建新的对等节点表
    pub fn new(max_connections: usize) -> Self {
        Self {
            peers: Arc::new(DashMap::new()),
            max_connections,
        }
    }

    /// 添加对等节点
    pub fn add_peer(&self, peer: Peer) -> bool {
        if self.peers.len() >= self.max_connections {
            log::warn!("已达到最大连接数 {}，拒绝新连接", self.max_connections);
            return false;
        }
        let node_id = peer.info.node_id.clone();
        self.peers.insert(node_id, peer);
        true
    }

    /// 移除对等节点
    pub fn remove_peer(&self, node_id: &str) -> Option<Peer> {
        self.peers.remove(node_id).map(|(_, peer)| peer)
    }

    /// 获取对等节点发送通道
    pub fn get_sender(&self, node_id: &str) -> Option<mpsc::Sender<Message>> {
        self.peers.get(node_id).map(|p| p.sender.clone())
    }

    /// 判断节点是否已存在
    pub fn contains(&self, node_id: &str) -> bool {
        self.peers.contains_key(node_id)
    }

    /// 更新节点心跳时间
    pub fn update_heartbeat(&self, node_id: &str) {
        if let Some(mut peer) = self.peers.get_mut(node_id) {
            peer.update_heartbeat();
        }
    }

    /// 设置节点为已连接状态
    pub fn set_connected(&self, node_id: &str) {
        if let Some(mut peer) = self.peers.get_mut(node_id) {
            peer.set_connected();
        }
    }

    /// 获取所有已连接节点的信息
    pub fn get_connected_peers(&self) -> Vec<PeerInfo> {
        self.peers
            .iter()
            .filter(|entry| entry.value().state == PeerState::Connected)
            .map(|entry| entry.value().info.clone())
            .collect()
    }

    /// 获取所有已连接节点的发送通道
    pub fn get_all_senders(&self) -> Vec<(String, mpsc::Sender<Message>)> {
        self.peers
            .iter()
            .filter(|entry| entry.value().state == PeerState::Connected)
            .map(|entry| (entry.key().clone(), entry.value().sender.clone()))
            .collect()
    }

    /// 获取已超时的节点ID列表
    pub fn get_timed_out_peers(&self, timeout: std::time::Duration) -> Vec<String> {
        let now = Utc::now();
        self.peers
            .iter()
            .filter(|entry| {
                let elapsed = now
                    .signed_duration_since(entry.value().last_heartbeat)
                    .to_std()
                    .unwrap_or(std::time::Duration::from_secs(0));
                entry.value().state == PeerState::Connected && elapsed > timeout
            })
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// 当前连接数
    pub fn connection_count(&self) -> usize {
        self.peers
            .iter()
            .filter(|entry| entry.value().state == PeerState::Connected)
            .count()
    }
}