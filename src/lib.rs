//! # 拜占庭容错可靠广播系统
//!
//! 本项目实现了基于 Reed-Solomon 纠删码的拜占庭容错可靠广播（RBC）协议，
//! 对应论文算法4：Four-round RBC protocol for long messages。
//!
//! ## 模块结构
//!
//! - [`erasure`] — Reed-Solomon 纠删码编解码器，提供数据分片与 Berlekamp-Welch 纠错能力
//! - [`network`] — P2P 网络层，包括节点发现、连接管理、消息编解码
//! - [`rbc`] — RBC 协议核心实现，包括单实例状态机、多实例管理器、超大文件分块广播

pub mod erasure;
pub mod network;
pub mod rbc;