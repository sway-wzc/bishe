use anyhow::Result;
use log::{debug, error, info, warn};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};
use tokio::time;
use uuid::Uuid;

use crate::network::config::NodeConfig;
use crate::network::connection::Connection;
use crate::network::discovery::DiscoveryService;
use crate::network::message::{Message, PeerInfo};
use crate::network::peer::{Peer, PeerTable};
use crate::rbc::{RbcConfig, RbcManager, RbcMessage, ChunkedBroadcastManager};

/// 拜占庭（恶意）节点行为模式
///
/// 用于Docker测试中模拟不同类型的恶意节点攻击，
/// 验证RBC协议的BFT容错能力。
#[derive(Debug, Clone, PartialEq)]
pub enum ByzantineMode {
    /// 正常模式（诚实节点）
    None,
    /// 篡改分片数据：在ECHO和READY消息中发送随机垃圾数据替换真实分片
    CorruptShard,
    /// 伪造哈希：在ECHO和READY消息中使用错误的data_hash
    WrongHash,
    /// 选择性沉默：不转发任何RBC消息（模拟恶意丢弃）
    Silent,
}

impl ByzantineMode {
    /// 从环境变量字符串解析拜占庭模式
    pub fn from_env(s: &str) -> Self {
        match s.trim().to_lowercase().as_str() {
            "corrupt_shard" => ByzantineMode::CorruptShard,
            "wrong_hash" => ByzantineMode::WrongHash,
            "silent" => ByzantineMode::Silent,
            _ => ByzantineMode::None,
        }
    }
}

/// P2P节点内部事件
#[derive(Debug)]
enum NodeEvent {
    /// 收到来自某个节点的消息
    IncomingMessage {
        node_id: String,
        message: Message,
    },
    /// 某个节点连接断开
    PeerDisconnected {
        node_id: String,
    },
    /// 新的入站连接
    NewInboundConnection {
        stream: TcpStream,
        addr: SocketAddr,
    },
}

/// P2P节点
pub struct P2PNode {
    /// 本节点唯一ID
    node_id: String,
    /// 节点配置
    config: NodeConfig,
    /// 对等节点表
    peer_table: PeerTable,
    /// 节点发现服务
    discovery: DiscoveryService,
    /// RBC协议管理器（使用Arc<Mutex>支持异步共享访问）
    rbc_manager: Arc<Mutex<Option<RbcManager>>>,
    /// 分块广播管理器（超大文件支持）
    chunked_manager: Arc<Mutex<Option<ChunkedBroadcastManager>>>,
    /// 拜占庭模式（仅用于测试，正常节点为None）
    byzantine_mode: ByzantineMode,
}

impl P2PNode {
    /// 创建新的P2P节点
    pub async fn new(config: NodeConfig) -> Result<Self> {
        let node_id = Uuid::new_v4().to_string();
        let local_info = PeerInfo {
            node_id: node_id.clone(),
            address: format!("{}:{}", config.listen_addr, config.listen_port),
        };

        let peer_table = PeerTable::new(config.max_connections);
        let discovery = DiscoveryService::new(local_info);

        // 添加种子节点
        discovery.add_seed_nodes(&config.seed_nodes).await;

        // 从环境变量读取拜占庭模式
        let byzantine_mode = std::env::var("BYZANTINE_MODE")
            .map(|s| ByzantineMode::from_env(&s))
            .unwrap_or(ByzantineMode::None);

        if byzantine_mode != ByzantineMode::None {
            warn!("⚠️  节点以拜占庭模式启动: {:?}", byzantine_mode);
        }

        info!("节点创建成功，ID: {}", node_id);

        Ok(Self {
            node_id,
            config,
            peer_table,
            discovery,
            rbc_manager: Arc::new(Mutex::new(None)),
            chunked_manager: Arc::new(Mutex::new(None)),
            byzantine_mode,
        })
    }

    /// 启动P2P节点
    pub async fn start(self: Arc<Self>) -> Result<()> {
        let listen_addr = format!("{}:{}", self.config.listen_addr, self.config.listen_port);
        let listener = TcpListener::bind(&listen_addr).await?;
        info!("节点 {} 开始监听 {}", self.node_id, listen_addr);

        // 创建主事件通道
        let (event_tx, mut event_rx) = mpsc::channel::<NodeEvent>(self.config.message_buffer_size);

        // 启动监听协程：接受入站连接
        let event_tx_clone = event_tx.clone();
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        info!("收到入站连接: {}", addr);
                        let _ = event_tx_clone
                            .send(NodeEvent::NewInboundConnection { stream, addr })
                            .await;
                    }
                    Err(e) => {
                        error!("接受连接失败: {}", e);
                    }
                }
            }
        });

        // 启动种子节点连接协程
        self.connect_to_seeds(&event_tx).await;

        // 启动心跳定时器
        let heartbeat_interval = self.config.heartbeat_interval;
        let heartbeat_timeout = self.config.heartbeat_timeout;
        let peer_table_hb = self.peer_table.clone();
        let event_tx_hb = event_tx.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(heartbeat_interval);
            loop {
                interval.tick().await;
                // 发送心跳
                let senders = peer_table_hb.get_all_senders();
                let timestamp = chrono::Utc::now().timestamp() as u64;
                for (node_id, sender) in &senders {
                    if sender.send(Message::Ping { timestamp }).await.is_err() {
                        warn!("发送心跳到节点 {} 失败", node_id);
                    }
                }

                // 检查超时节点
                let timed_out = peer_table_hb.get_timed_out_peers(heartbeat_timeout);
                for node_id in timed_out {
                    warn!("节点 {} 心跳超时，断开连接", node_id);
                    let _ = event_tx_hb
                        .send(NodeEvent::PeerDisconnected {
                            node_id: node_id.clone(),
                        })
                        .await;
                }
            }
        });

        // 启动节点发现定时器（发送发现请求 + 主动连接新发现的节点）
        let discovery_interval = self.config.discovery_interval;
        let self_disc = self.clone();
        let event_tx_disc = event_tx.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(discovery_interval);
            loop {
                interval.tick().await;

                // 1. 向所有已连接节点发送发现请求
                let senders = self_disc.peer_table.get_all_senders();
                for (node_id, sender) in &senders {
                    if sender.send(Message::DiscoverRequest).await.is_err() {
                        warn!("发送发现请求到节点 {} 失败", node_id);
                    }
                }

                // 2. 尝试连接已发现但未连接的节点
                let connected_ids: std::collections::HashSet<String> = self_disc
                    .peer_table
                    .get_connected_peers()
                    .iter()
                    .map(|p| p.node_id.clone())
                    .collect();

                let unconnected = self_disc.discovery.get_unconnected_peers(&connected_ids).await;
                for peer_info in unconnected {
                    if peer_info.address.is_empty() {
                        continue;
                    }
                    debug!("尝试连接发现的节点: {} ({})", peer_info.node_id, peer_info.address);
                    match TcpStream::connect(&peer_info.address).await {
                        Ok(stream) => {
                            let addr = stream.peer_addr().unwrap_or_else(|_| {
                                peer_info.address.parse().unwrap_or_else(|_| {
                                    SocketAddr::from(([127, 0, 0, 1], 0))
                                })
                            });
                            self_disc
                                .setup_outbound_connection(stream, addr, &peer_info.address, &event_tx_disc)
                                .await;
                        }
                        Err(e) => {
                            debug!("连接发现节点 {} 失败: {}", peer_info.address, e);
                        }
                    }
                }
            }
        });

        // RBC初始化状态
        let mut rbc_initialized = false;
        let mut last_peer_count: usize = 0;
        let mut peer_stable_since: Option<tokio::time::Instant> = None;

        // 从环境变量读取预期节点数（由测试脚本设置）
        let expected_nodes: usize = std::env::var("EXPECTED_NODES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0); // 0表示未设置，使用稳定期策略

        // RBC初始化稳定等待时间（节点数不再变化后等待多久再初始化）
        let rbc_stable_wait = std::time::Duration::from_secs(
            std::env::var("RBC_STABLE_WAIT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(8) // 默认8秒稳定期
        );

        if expected_nodes > 0 {
            info!("RBC初始化策略: 等待 {} 个节点全部连接", expected_nodes);
        } else {
            info!("RBC初始化策略: 节点数稳定 {}秒 后初始化", rbc_stable_wait.as_secs());
        }

        // 主事件循环
        info!("节点 {} 主事件循环启动", self.node_id);
        while let Some(event) = event_rx.recv().await {
            match event {
                NodeEvent::NewInboundConnection { stream, addr } => {
                    self.handle_inbound_connection(stream, addr, &event_tx).await;
                }
                NodeEvent::IncomingMessage { node_id, message } => {
                    self.handle_message(&node_id, message, &event_tx).await;
                }
                NodeEvent::PeerDisconnected { node_id } => {
                    self.handle_peer_disconnected(&node_id).await;
                }
            }

            // RBC自动初始化逻辑
            if !rbc_initialized {
                let peer_count = self.peer_table.connection_count();
                let total_nodes = peer_count + 1; // 加上自己

                // 检测节点数变化
                if peer_count != last_peer_count {
                    if peer_count > last_peer_count {
                        info!("节点数变化: {} -> {}（总计{}），重置RBC初始化稳定计时",
                            last_peer_count, peer_count, total_nodes);
                    }
                    last_peer_count = peer_count;
                    peer_stable_since = Some(tokio::time::Instant::now());
                }

                if total_nodes >= 4 {
                    let should_init = if expected_nodes > 0 {
                        // 策略1: 等待预期节点数全部到齐
                        total_nodes >= expected_nodes
                    } else {
                        // 策略2: 节点数稳定一段时间后初始化
                        peer_stable_since
                            .map(|since| since.elapsed() >= rbc_stable_wait)
                            .unwrap_or(false)
                    };

                    if should_init {
                        // 收集所有节点ID（包括自己）
                        let mut node_ids: Vec<String> = self.peer_table
                            .get_connected_peers()
                            .iter()
                            .map(|p| p.node_id.clone())
                            .collect();
                        node_ids.push(self.node_id.clone());

                        match self.init_rbc(node_ids).await {
                            Ok(()) => {
                                info!("RBC协议自动初始化成功，参与节点数: {}", total_nodes);
                                rbc_initialized = true;
                            }
                            Err(e) => {
                                warn!("RBC协议自动初始化失败: {}", e);
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// 连接到种子节点
    async fn connect_to_seeds(&self, event_tx: &mpsc::Sender<NodeEvent>) {
        let seeds = self.config.seed_nodes.clone();
        for seed_addr in seeds {
            info!("正在连接种子节点: {}", seed_addr);
            match TcpStream::connect(&seed_addr).await {
                Ok(stream) => {
                    let addr = stream.peer_addr().unwrap_or_else(|_| {
                        seed_addr.parse().unwrap_or_else(|_| {
                            SocketAddr::from(([127, 0, 0, 1], 0))
                        })
                    });
                    self.setup_outbound_connection(stream, addr, &seed_addr, event_tx)
                        .await;
                }
                Err(e) => {
                    warn!("连接种子节点 {} 失败: {}", seed_addr, e);
                }
            }
        }
    }

    /// 处理出站连接（主动连接到远端）
    async fn setup_outbound_connection(
        &self,
        stream: TcpStream,
        addr: SocketAddr,
        target_addr: &str,
        event_tx: &mpsc::Sender<NodeEvent>,
    ) {
        let (sender, mut receiver) =
            Connection::spawn(stream, self.config.message_buffer_size);

        // 发送握手消息
        let handshake = Message::Handshake {
            node_id: self.node_id.clone(),
            listen_port: self.config.listen_port,
            version: "0.1.0".to_string(),
        };

        if let Err(e) = sender.send(handshake).await {
            error!("发送握手消息到 {} 失败: {}", target_addr, e);
            return;
        }

        // 等待握手响应
        match time::timeout(self.config.connection_timeout, receiver.recv()).await {
            Ok(Some(Message::HandshakeAck {
                        node_id: remote_id,
                        listen_port: remote_port,
                        version: _,
                    })) => {
                info!("与节点 {}({}) 握手成功", remote_id, addr);

                let remote_listen_addr = format!("{}:{}", addr.ip(), remote_port);
                let peer_info = PeerInfo {
                    node_id: remote_id.clone(),
                    address: remote_listen_addr,
                };

                // 更新发现服务
                self.discovery
                    .update_peer_id(target_addr, &remote_id)
                    .await;

                // 添加到对等节点表
                let peer = Peer::new(peer_info, sender);
                if self.peer_table.add_peer(peer) {
                    self.peer_table.set_connected(&remote_id);
                    info!(
                        "节点 {} 已连接，当前连接数: {}",
                        remote_id,
                        self.peer_table.connection_count()
                    );

                    // 启动消息接收循环
                    Self::spawn_message_reader(remote_id, receiver, event_tx.clone());
                }
            }
            Ok(Some(other)) => {
                warn!("收到非握手响应消息: {:?}", std::mem::discriminant(&other));
            }
            Ok(None) => {
                warn!("连接 {} 在握手阶段关闭", addr);
            }
            Err(_) => {
                warn!("等待 {} 握手响应超时", addr);
            }
        }
    }

    /// 处理入站连接（远端主动连接到本节点）
    async fn handle_inbound_connection(
        &self,
        stream: TcpStream,
        addr: SocketAddr,
        event_tx: &mpsc::Sender<NodeEvent>,
    ) {
        let (sender, mut receiver) =
            Connection::spawn(stream, self.config.message_buffer_size);

        // 等待对方握手
        match time::timeout(self.config.connection_timeout, receiver.recv()).await {
            Ok(Some(Message::Handshake {
                        node_id: remote_id,
                        listen_port: remote_port,
                        version: _,
                    })) => {
                // 检查是否已连接
                if self.peer_table.contains(&remote_id) {
                    warn!("节点 {} 已存在连接，拒绝重复连接", remote_id);
                    return;
                }

                info!("收到节点 {}({}) 的握手请求", remote_id, addr);

                // 回复握手确认
                let ack = Message::HandshakeAck {
                    node_id: self.node_id.clone(),
                    listen_port: self.config.listen_port,
                    version: "0.1.0".to_string(),
                };

                if let Err(e) = sender.send(ack).await {
                    error!("回复握手消息失败: {}", e);
                    return;
                }

                let remote_listen_addr = format!("{}:{}", addr.ip(), remote_port);
                let peer_info = PeerInfo {
                    node_id: remote_id.clone(),
                    address: remote_listen_addr.clone(),
                };

                // 更新发现服务
                self.discovery
                    .update_peer_id(&remote_listen_addr, &remote_id)
                    .await;

                // 添加到对等节点表
                let peer = Peer::new(peer_info, sender);
                if self.peer_table.add_peer(peer) {
                    self.peer_table.set_connected(&remote_id);
                    info!(
                        "节点 {} 已连接，当前连接数: {}",
                        remote_id,
                        self.peer_table.connection_count()
                    );

                    // 启动消息接收循环
                    Self::spawn_message_reader(remote_id, receiver, event_tx.clone());
                }
            }
            Ok(Some(other)) => {
                warn!(
                    "入站连接 {} 收到非握手消息: {:?}",
                    addr,
                    std::mem::discriminant(&other)
                );
            }
            Ok(None) => {
                warn!("入站连接 {} 在握手阶段关闭", addr);
            }
            Err(_) => {
                warn!("入站连接 {} 握手超时", addr);
            }
        }
    }

    /// 启动消息接收循环协程
    fn spawn_message_reader(
        node_id: String,
        mut receiver: mpsc::Receiver<Message>,
        event_tx: mpsc::Sender<NodeEvent>,
    ) {
        tokio::spawn(async move {
            while let Some(msg) = receiver.recv().await {
                if event_tx
                    .send(NodeEvent::IncomingMessage {
                        node_id: node_id.clone(),
                        message: msg,
                    })
                    .await
                    .is_err()
                {
                    break;
                }
            }
            // 连接关闭
            let _ = event_tx
                .send(NodeEvent::PeerDisconnected {
                    node_id: node_id.clone(),
                })
                .await;
            debug!("节点 {} 消息接收循环退出", node_id);
        });
    }

    /// 处理收到的消息
    async fn handle_message(
        &self,
        from: &str,
        message: Message,
        _event_tx: &mpsc::Sender<NodeEvent>,
    ) {
        match message {
            Message::Ping { timestamp } => {
                info!("收到节点 {} 的心跳请求", from);
                self.peer_table.update_heartbeat(from);
                // 回复Pong
                if let Some(sender) = self.peer_table.get_sender(from) {
                    let _ = sender.send(Message::Pong { timestamp }).await;
                }
            }
            Message::Pong { timestamp: _ } => {
                info!("收到节点 {} 的心跳响应", from);
                self.peer_table.update_heartbeat(from);
            }
            Message::DiscoverRequest => {
                info!("收到节点 {} 的发现请求", from);
                let peers = self.peer_table.get_connected_peers();
                if let Some(sender) = self.peer_table.get_sender(from) {
                    let _ = sender
                        .send(Message::DiscoverResponse { peers })
                        .await;
                }
            }
            Message::DiscoverResponse { peers } => {
                info!("收到节点 {} 的发现响应，包含 {} 个节点", from, peers.len());
                self.discovery.merge_peers(peers).await;
            }
            Message::Data { chunk_id, payload } => {
                info!(
                    "收到节点 {} 的数据块 {}，大小 {} 字节",
                    from,
                    chunk_id,
                    payload.len()
                );
                // TODO: 交给数据存储模块处理
            }
            Message::DataRequest { chunk_id } => {
                info!("收到节点 {} 的数据请求: {}", from, chunk_id);
                // TODO: 从存储模块获取数据并回复
            }
            Message::Broadcast {
                msg_id,
                origin,
                payload,
            } => {
                info!(
                    "收到广播消息 {}，来源: {}，大小 {} 字节",
                    msg_id,
                    origin,
                    payload.len()
                );
                // TODO: 去重后转发给其他节点
            }
            Message::Rbc { payload } => {
                debug!(
                    "收到节点 {} 的RBC协议消息，大小 {} 字节",
                    from,
                    payload.len()
                );
                // 反序列化RBC消息并交给RBC管理器处理
                match bincode::deserialize::<RbcMessage>(&payload) {
                    Ok(rbc_msg) => {
                        // 使用队列处理RBC消息，支持发给自己的消息递归处理
                        let mut pending_msgs = vec![rbc_msg];
                        while let Some(msg) = pending_msgs.pop() {
                            let mut manager_guard = self.rbc_manager.lock().await;
                            if let Some(ref mut manager) = *manager_guard {
                                match manager.handle_message(msg) {
                                    Ok(outgoing) => {
                                        drop(manager_guard); // 释放锁后再发送
                                        for (target, out_msg) in outgoing {
                                            if target == self.node_id {
                                                // 发给自己的消息加入待处理队列
                                                pending_msgs.push(out_msg);
                                            } else {
                                                // 应用拜占庭篡改
                                                let tampered = self.apply_byzantine_tampering(out_msg);
                                                if let Some(final_msg) = tampered {
                                                    if let Ok(serialized) = bincode::serialize(&final_msg) {
                                                        if let Some(sender) = self.peer_table.get_sender(&target) {
                                                            if let Err(e) = sender.send(Message::Rbc { payload: serialized }).await {
                                                                warn!("转发RBC消息到节点 {} 失败: {}", target, e);
                                                            }
                                                        } else {
                                                            debug!("RBC目标节点 {} 不在对等表中，跳过", target);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        error!("处理RBC消息失败: {}", e);
                                    }
                                }
                            } else {
                                warn!("RBC管理器未初始化，忽略RBC消息");
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        error!("反序列化RBC消息失败: {}", e);
                    }
                }
            }
            Message::Disconnect { reason } => {
                info!("节点 {} 主动断开，原因: {}", from, reason);
                self.handle_peer_disconnected(from).await;
            }
            // 握手消息在连接建立阶段已处理，这里不应收到
            Message::Handshake { .. } | Message::HandshakeAck { .. } => {
                warn!("收到意外的握手消息来自节点 {}", from);
            }
        }
    }

    /// 处理节点断开连接
    async fn handle_peer_disconnected(&self, node_id: &str) {
        if let Some(peer) = self.peer_table.remove_peer(node_id) {
            info!(
                "节点 {} ({}) 已断开，当前连接数: {}",
                node_id,
                peer.info.address,
                self.peer_table.connection_count()
            );
        }
    }

    /// 向指定节点发送消息（供外部调用）
    pub async fn send_to(&self, node_id: &str, message: Message) -> Result<()> {
        if let Some(sender) = self.peer_table.get_sender(node_id) {
            sender
                .send(message)
                .await
                .map_err(|e| anyhow::anyhow!("发送消息失败: {}", e))?;
            Ok(())
        } else {
            Err(anyhow::anyhow!("节点 {} 不存在或未连接", node_id))
        }
    }

    /// 广播消息到所有已连接节点
    pub async fn broadcast(&self, payload: Vec<u8>) {
        let msg_id = Uuid::new_v4().to_string();
        let senders = self.peer_table.get_all_senders();
        let message = Message::Broadcast {
            msg_id,
            origin: self.node_id.clone(),
            payload,
        };

        for (node_id, sender) in senders {
            if let Err(e) = sender.send(message.clone()).await {
                warn!("广播消息到节点 {} 失败: {}", node_id, e);
            }
        }
    }

    /// 获取当前已连接节点数
    pub fn connected_peers_count(&self) -> usize {
        self.peer_table.connection_count()
    }

    /// 获取本节点ID
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// 初始化RBC协议管理器
    ///
    /// 在所有节点连接建立后调用，传入所有参与节点的ID列表
    /// # 参数
    /// - `node_ids`: 所有参与RBC协议的节点ID列表
    pub async fn init_rbc(&self, node_ids: Vec<String>) -> Result<()> {
        let n = node_ids.len();
        if n < 4 {
            return Err(anyhow::anyhow!("RBC协议至少需要4个节点，当前: {}", n));
        }

        let config = RbcConfig::new(n)
            .map_err(|e| anyhow::anyhow!("创建RBC配置失败: {}", e))?;

        info!(
            "初始化RBC管理器: 本节点={}, {}",
            self.node_id,
            config.info()
        );

        let manager = RbcManager::new(
            self.node_id.clone(),
            config,
            node_ids,
        );

        let mut guard = self.rbc_manager.lock().await;
        *guard = Some(manager);

        // 同时初始化分块广播管理器
        let mut chunked_guard = self.chunked_manager.lock().await;
        *chunked_guard = Some(ChunkedBroadcastManager::new());

        Ok(())
    }

    /// 通过RBC协议广播数据到所有节点
    ///
    /// 自动判断数据大小：
    /// - 小于等于4MB：直接使用单次RBC广播
    /// - 大于4MB：使用分块广播（自动拆分为多个RBC实例）
    ///
    /// # 参数
    /// - `data`: 要广播的数据
    pub async fn rbc_broadcast(&self, data: Vec<u8>) -> Result<()> {
        let data_size = data.len();
        let chunk_threshold = crate::rbc::DEFAULT_CHUNK_SIZE;

        if data_size > chunk_threshold {
            // 超大文件：使用分块广播
            info!(
                "数据大小={}字节 ({:.2}MB) 超过阈值 {}字节，使用分块广播",
                data_size,
                data_size as f64 / 1024.0 / 1024.0,
                chunk_threshold
            );
            self.rbc_chunked_broadcast(data).await
        } else {
            // 普通大小：直接RBC广播
            self.rbc_direct_broadcast(data).await
        }
    }

    /// 直接RBC广播（适用于小于4MB的数据）
    async fn rbc_direct_broadcast(&self, data: Vec<u8>) -> Result<()> {
        let instance_id = Uuid::new_v4().to_string();
        info!(
            "发起RBC广播: instance={}, 数据大小={}字节",
            &instance_id[..8],
            data.len()
        );

        let outgoing = {
            let mut guard = self.rbc_manager.lock().await;
            let manager = guard.as_mut()
                .ok_or_else(|| anyhow::anyhow!("RBC管理器未初始化，请先调用init_rbc"))?;
            manager.broadcast(instance_id, data)?
        };

        self.send_rbc_messages(outgoing).await;
        Ok(())
    }

    /// 分块RBC广播（适用于超大文件）
    async fn rbc_chunked_broadcast(&self, data: Vec<u8>) -> Result<()> {
        let session_id = Uuid::new_v4().to_string();
        info!(
            "发起分块RBC广播: session={}, 数据大小={}字节 ({:.2}MB)",
            &session_id[..8],
            data.len(),
            data.len() as f64 / 1024.0 / 1024.0
        );

        let outgoing = {
            let mut rbc_guard = self.rbc_manager.lock().await;
            let rbc_manager = rbc_guard.as_mut()
                .ok_or_else(|| anyhow::anyhow!("RBC管理器未初始化"))?;

            let mut chunked_guard = self.chunked_manager.lock().await;
            let chunked_manager = chunked_guard.as_mut()
                .ok_or_else(|| anyhow::anyhow!("分块广播管理器未初始化"))?;

            chunked_manager.broadcast(session_id, data, rbc_manager)?
        };

        self.send_rbc_messages(outgoing).await;
        Ok(())
    }

    /// 对RBC消息应用拜占庭篡改（仅在恶意模式下生效）
    ///
    /// 根据 `byzantine_mode` 对即将发送的消息进行篡改：
    /// - CorruptShard: 将分片数据替换为随机垃圾数据
    /// - WrongHash: 将data_hash替换为伪造的哈希值
    /// - Silent: 返回None表示丢弃该消息
    fn apply_byzantine_tampering(&self, msg: RbcMessage) -> Option<RbcMessage> {
        match &self.byzantine_mode {
            ByzantineMode::None => Some(msg),
            ByzantineMode::Silent => {
                // 选择性沉默：丢弃所有ECHO和READY消息
                match &msg {
                    RbcMessage::Propose { .. } => {
                        // PROPOSE消息正常转发（广播者的PROPOSE不篡改，否则协议无法启动）
                        Some(msg)
                    }
                    RbcMessage::Echo { instance_id, .. } => {
                        warn!(
                            "[拜占庭-沉默] 丢弃ECHO消息: instance={}",
                            &instance_id[..8.min(instance_id.len())]
                        );
                        None
                    }
                    RbcMessage::Ready { instance_id, .. } => {
                        warn!(
                            "[拜占庭-沉默] 丢弃READY消息: instance={}",
                            &instance_id[..8.min(instance_id.len())]
                        );
                        None
                    }
                }
            }
            ByzantineMode::CorruptShard => {
                use rand::Rng;
                let mut rng = rand::thread_rng();
                match msg {
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
                        // 篡改分片数据：用随机数据替换
                        let mut corrupted = vec![0u8; shard_data.len()];
                        rng.fill(&mut corrupted[..]);
                        warn!(
                            "[拜占庭-篡改分片] 篡改ECHO分片: instance={}, shard_index={}",
                            &instance_id[..8.min(instance_id.len())],
                            shard_index
                        );
                        Some(RbcMessage::Echo {
                            instance_id,
                            sender,
                            data_hash,
                            shard_index,
                            shard_data: corrupted,
                            original_size,
                            data_shard_count,
                            parity_shard_count,
                        })
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
                        // 篡改分片数据：用随机数据替换
                        let mut corrupted = vec![0u8; shard_data.len()];
                        rng.fill(&mut corrupted[..]);
                        warn!(
                            "[拜占庭-篡改分片] 篡改READY分片: instance={}, shard_index={}",
                            &instance_id[..8.min(instance_id.len())],
                            shard_index
                        );
                        Some(RbcMessage::Ready {
                            instance_id,
                            sender,
                            data_hash,
                            shard_index,
                            shard_data: corrupted,
                            original_size,
                            data_shard_count,
                            parity_shard_count,
                        })
                    }
                    other => Some(other), // PROPOSE正常转发
                }
            }
            ByzantineMode::WrongHash => {
                match msg {
                    RbcMessage::Echo {
                        instance_id,
                        sender,
                        data_hash: _,
                        shard_index,
                        shard_data,
                        original_size,
                        data_shard_count,
                        parity_shard_count,
                    } => {
                        // 伪造哈希值
                        let fake_hash = format!(
                            "deadbeef{:056x}",
                            rand::random::<u64>()
                        );
                        warn!(
                            "[拜占庭-伪造哈希] 篡改ECHO哈希: instance={}, fake_h={}...",
                            &instance_id[..8.min(instance_id.len())],
                            &fake_hash[..16]
                        );
                        Some(RbcMessage::Echo {
                            instance_id,
                            sender,
                            data_hash: fake_hash,
                            shard_index,
                            shard_data,
                            original_size,
                            data_shard_count,
                            parity_shard_count,
                        })
                    }
                    RbcMessage::Ready {
                        instance_id,
                        sender,
                        data_hash: _,
                        shard_index,
                        shard_data,
                        original_size,
                        data_shard_count,
                        parity_shard_count,
                    } => {
                        let fake_hash = format!(
                            "deadbeef{:056x}",
                            rand::random::<u64>()
                        );
                        warn!(
                            "[拜占庭-伪造哈希] 篡改READY哈希: instance={}, fake_h={}...",
                            &instance_id[..8.min(instance_id.len())],
                            &fake_hash[..16]
                        );
                        Some(RbcMessage::Ready {
                            instance_id,
                            sender,
                            data_hash: fake_hash,
                            shard_index,
                            shard_data,
                            original_size,
                            data_shard_count,
                            parity_shard_count,
                        })
                    }
                    other => Some(other), // PROPOSE正常转发
                }
            }
        }
    }

    /// 发送RBC消息（处理发给自己的消息递归）
    ///
    /// 如果节点处于拜占庭模式，会在发送前对消息进行篡改
    async fn send_rbc_messages(&self, messages: Vec<(String, RbcMessage)>) {
        let mut pending = messages;
        while !pending.is_empty() {
            let current_batch: Vec<(String, RbcMessage)> = std::mem::take(&mut pending);
            for (target, msg) in current_batch {
                if target == self.node_id {
                    // 发给自己的消息直接处理（不篡改自己的消息）
                    let mut guard = self.rbc_manager.lock().await;
                    if let Some(ref mut manager) = *guard {
                        if let Ok(new_msgs) = manager.handle_message(msg) {
                            pending.extend(new_msgs);
                        }
                    }
                } else {
                    // 发给其他节点的消息：应用拜占庭篡改
                    let tampered_msg = self.apply_byzantine_tampering(msg);
                    if let Some(out_msg) = tampered_msg {
                        if let Ok(serialized) = bincode::serialize(&out_msg) {
                            if let Some(sender) = self.peer_table.get_sender(&target) {
                                if let Err(e) = sender.send(Message::Rbc { payload: serialized }).await {
                                    warn!("发送RBC消息到节点 {} 失败: {}", target, e);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// 获取RBC管理器的引用（用于外部查询状态）
    pub fn rbc_manager(&self) -> Arc<Mutex<Option<RbcManager>>> {
        self.rbc_manager.clone()
    }

    /// 获取分块广播管理器的引用
    pub fn chunked_manager(&self) -> Arc<Mutex<Option<ChunkedBroadcastManager>>> {
        self.chunked_manager.clone()
    }
}