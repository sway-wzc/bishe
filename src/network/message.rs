use serde::{Deserialize, Serialize};
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use thiserror::Error;

/// 消息协议错误类型
///
/// 涵盖网络 I/O、序列化、帧校验等各类通信异常。
#[derive(Error, Debug)]
pub enum MessageError {
    #[error("IO错误: {0}")]
    Io(#[from] io::Error),
    #[error("序列化错误: {0}")]
    Serialize(#[from] bincode::Error),
    #[error("消息体过大: {size} 字节，上限为 {max} 字节")]
    MessageTooLarge { size: usize, max: usize },
    #[error("连接已关闭")]
    ConnectionClosed,
    #[error("无效的魔数")]
    InvalidMagic,
}

/// 协议魔数（ASCII "P2PD"），用于标识合法消息帧并快速过滤非法连接
const PROTOCOL_MAGIC: u32 = 0x50325044;
/// 单条消息体最大长度：16MB（超过此大小应使用分块广播）
const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// 节点信息
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct PeerInfo {
    /// 节点唯一ID
    pub node_id: String,
    /// 节点监听地址 (ip:port)
    pub address: String,
}

/// P2P网络消息类型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    /// 握手请求：节点连接后首先交换身份信息
    Handshake {
        node_id: String,
        listen_port: u16,
        version: String,
    },
    /// 握手响应
    HandshakeAck {
        node_id: String,
        listen_port: u16,
        version: String,
    },
    /// 心跳请求
    Ping {
        timestamp: u64,
    },
    /// 心跳响应
    Pong {
        timestamp: u64,
    },
    /// 节点发现请求：请求对方返回已知节点列表
    DiscoverRequest,
    /// 节点发现响应：返回已知节点列表
    DiscoverResponse {
        peers: Vec<PeerInfo>,
    },
    /// 数据传输消息（供上层数据分发模块使用）
    Data {
        /// 数据块唯一标识
        chunk_id: String,
        /// 数据内容
        payload: Vec<u8>,
    },
    /// 数据请求
    DataRequest {
        chunk_id: String,
    },
    /// 广播消息
    Broadcast {
        /// 消息唯一ID，用于去重
        msg_id: String,
        /// 消息来源节点
        origin: String,
        /// 消息内容
        payload: Vec<u8>,
    },
    /// 节点断开通知
    Disconnect {
        reason: String,
    },

    /// RBC协议消息（四轮长消息可靠广播）
    Rbc {
        /// 序列化后的RBC消息（使用bincode序列化的RbcMessage）
        payload: Vec<u8>,
    },
}

/// 消息帧编解码器
///
/// 帧格式：`[魔数 4B][长度 4B][消息体 NB]`
///
/// - 魔数：固定值 `0x50325044`（"P2PD"），用于帧同步与合法性校验
/// - 长度：消息体字节数（大端序 u32），上限 16MB
/// - 消息体：bincode 序列化的 [`Message`] 枚举
pub struct MessageCodec;

impl MessageCodec {
    /// 将消息编码并写入到写端
    pub async fn write_message(
        writer: &mut OwnedWriteHalf,
        message: &Message,
    ) -> Result<(), MessageError> {
        let data = bincode::serialize(message)?;
        let size = data.len();

        if size > MAX_MESSAGE_SIZE {
            return Err(MessageError::MessageTooLarge {
                size,
                max: MAX_MESSAGE_SIZE,
            });
        }

        // 写入魔数
        writer.write_u32(PROTOCOL_MAGIC).await?;
        // 写入消息长度
        writer.write_u32(size as u32).await?;
        // 写入消息体
        writer.write_all(&data).await?;
        writer.flush().await?;

        Ok(())
    }

    /// 从读端读取并解码一条消息
    pub async fn read_message(
        reader: &mut OwnedReadHalf,
    ) -> Result<Message, MessageError> {
        // 读取魔数
        let magic = reader.read_u32().await.map_err(|e| {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                MessageError::ConnectionClosed
            } else {
                MessageError::Io(e)
            }
        })?;

        if magic != PROTOCOL_MAGIC {
            return Err(MessageError::InvalidMagic);
        }

        // 读取消息长度
        let size = reader.read_u32().await? as usize;

        if size > MAX_MESSAGE_SIZE {
            return Err(MessageError::MessageTooLarge {
                size,
                max: MAX_MESSAGE_SIZE,
            });
        }

        // 读取消息体
        let mut buf = vec![0u8; size];
        reader.read_exact(&mut buf).await.map_err(|e| {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                MessageError::ConnectionClosed
            } else {
                MessageError::Io(e)
            }
        })?;

        let message: Message = bincode::deserialize(&buf)?;
        Ok(message)
    }
}