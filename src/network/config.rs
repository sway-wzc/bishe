use serde::{Deserialize, Serialize};
use std::time::Duration;

/// P2P 节点配置
///
/// 控制节点的网络行为参数，包括监听地址、连接管理、心跳检测和节点发现。
/// 可通过 `Default::default()` 获取合理的默认配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// 节点监听端口
    pub listen_port: u16,
    /// 节点绑定地址
    pub listen_addr: String,
    /// 种子节点地址列表
    pub seed_nodes: Vec<String>,
    /// 最大连接数
    pub max_connections: usize,
    /// 心跳间隔
    pub heartbeat_interval: Duration,
    /// 连接超时时间
    pub connection_timeout: Duration,
    /// 心跳超时时间（超过此时间未收到心跳则认为对端离线）
    pub heartbeat_timeout: Duration,
    /// 节点发现间隔
    pub discovery_interval: Duration,
    /// 消息缓冲区大小
    pub message_buffer_size: usize,
}

impl Default for NodeConfig {
    /// 返回默认配置
    ///
    /// 默认值：
    /// - 监听端口：8000，绑定地址：0.0.0.0
    /// - 最大连接数：50
    /// - 心跳间隔：5秒，心跳超时：15秒
    /// - 连接超时：10秒，发现间隔：5秒
    /// - 消息缓冲区：1024
    fn default() -> Self {
        Self {
            listen_port: 8000,
            listen_addr: "0.0.0.0".to_string(),
            seed_nodes: Vec::new(),
            max_connections: 50,
            heartbeat_interval: Duration::from_secs(5),
            connection_timeout: Duration::from_secs(10),
            heartbeat_timeout: Duration::from_secs(15),
            discovery_interval: Duration::from_secs(5),
            message_buffer_size: 1024,
        }
    }
}