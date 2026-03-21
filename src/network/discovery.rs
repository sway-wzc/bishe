use log::{debug, error, info, warn};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::network::message::PeerInfo;

/// 节点发现服务
/// 维护已知节点列表，支持基于种子节点的发现机制
#[derive(Debug, Clone)]
pub struct DiscoveryService {
    /// 已知节点集合（包括已连接和未连接的）
    known_peers: Arc<Mutex<HashSet<PeerInfo>>>,
    /// 本节点信息
    local_info: PeerInfo,
}

impl DiscoveryService {
    /// 创建节点发现服务
    pub fn new(local_info: PeerInfo) -> Self {
        Self {
            known_peers: Arc::new(Mutex::new(HashSet::new())),
            local_info,
        }
    }

    /// 添加种子节点
    pub async fn add_seed_nodes(&self, seeds: &[String]) {
        let mut known = self.known_peers.lock().await;
        for seed in seeds {
            let info = PeerInfo {
                node_id: String::new(), // 种子节点的ID在握手时确定
                address: seed.clone(),
            };
            known.insert(info);
        }
        info!("已添加 {} 个种子节点", seeds.len());
    }

    /// 合并从远端节点发现的节点列表
    pub async fn merge_peers(&self, peers: Vec<PeerInfo>) {
        let mut known = self.known_peers.lock().await;
        let before = known.len();
        for peer in peers {
            // 不添加自己
            if peer.node_id != self.local_info.node_id {
                known.insert(peer);
            }
        }
        let added = known.len() - before;
        if added > 0 {
            debug!("从节点发现中新增 {} 个节点", added);
        }
    }

    /// 获取所有已知节点地址（排除自身）
    pub async fn get_known_peers(&self) -> Vec<PeerInfo> {
        let known = self.known_peers.lock().await;
        known
            .iter()
            .filter(|p| p.node_id != self.local_info.node_id)
            .cloned()
            .collect()
    }

    /// 获取需要尝试连接的节点地址
    /// 传入已连接的节点ID集合，返回尚未连接的已知节点
    pub async fn get_unconnected_peers(&self, connected_ids: &HashSet<String>) -> Vec<PeerInfo> {
        let known = self.known_peers.lock().await;
        known
            .iter()
            .filter(|p| {
                !p.node_id.is_empty()
                    && p.node_id != self.local_info.node_id
                    && !connected_ids.contains(&p.node_id)
            })
            .cloned()
            .collect()
    }

    /// 获取种子节点中尚未连接的地址
    pub async fn get_unconnected_seed_addrs(&self, connected_addrs: &HashSet<String>) -> Vec<String> {
        let known = self.known_peers.lock().await;
        known
            .iter()
            .filter(|p| p.node_id.is_empty() && !connected_addrs.contains(&p.address))
            .map(|p| p.address.clone())
            .collect()
    }

    /// 更新节点ID（握手成功后调用）
    pub async fn update_peer_id(&self, address: &str, node_id: &str) {
        let mut known = self.known_peers.lock().await;

        // 移除旧的空ID记录
        let old_info = PeerInfo {
            node_id: String::new(),
            address: address.to_string(),
        };
        known.remove(&old_info);

        // 插入带有ID的新记录
        let new_info = PeerInfo {
            node_id: node_id.to_string(),
            address: address.to_string(),
        };
        known.insert(new_info);
    }

    /// 移除节点
    pub async fn remove_peer(&self, node_id: &str) {
        let mut known = self.known_peers.lock().await;
        known.retain(|p| p.node_id != node_id);
    }
}