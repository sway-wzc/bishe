//! # P2P 网络层模块
//!
//! 提供去中心化的 P2P 网络通信基础设施，支持节点自动发现、
//! TCP 长连接管理、心跳保活以及消息帧编解码。
//!
//! ## 子模块
//!
//! - [`config`] — 节点配置参数（端口、超时、缓冲区等）
//! - [`message`] — 网络消息类型定义与帧编解码（魔数 + 长度 + 消息体）
//! - [`peer`] — 对等节点状态管理（`Peer`、`PeerTable`）
//! - [`connection`] — 单条 TCP 连接的异步读写协程
//! - [`discovery`] — 基于种子节点的节点发现服务
//! - [`node`] — P2P 节点主逻辑（事件循环、RBC 集成、拜占庭模拟）

pub mod config;
pub mod message;
pub mod peer;
pub mod connection;
pub mod node;
pub mod discovery;