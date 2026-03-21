use anyhow::Result;
use log::{debug, error, info, warn};
use std::collections::HashSet;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time;
use uuid::Uuid;

use crate::network::config::NodeConfig;
use crate::network::connection::Connection;
use crate::network::discovery::DiscoveryService;
use crate::network::message::{Message, PeerInfo};
use crate::network::peer::{Peer, PeerTable};

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

        info!("节点创建成功，ID: {}", node_id);

        Ok(Self {
            node_id,
            config,
            peer_table,
            discovery,
        })
    }

    /// 启动P2P节点
    pub async fn start(self) -> Result<()> {
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

        // 启动节点发现定时器
        let discovery_interval = self.config.discovery_interval;
        let peer_table_disc = self.peer_table.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(discovery_interval);
            loop {
                interval.tick().await;
                let senders = peer_table_disc.get_all_senders();
                for (node_id, sender) in &senders {
                    if sender.send(Message::DiscoverRequest).await.is_err() {
                        warn!("发送发现请求到节点 {} 失败", node_id);
                    }
                }
            }
        });

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
}