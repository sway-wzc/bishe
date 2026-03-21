use log::{debug, error, info, warn};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::network::message::{Message, MessageCodec, MessageError};

/// 连接处理器
/// 负责管理单个TCP连接的消息收发
pub struct Connection;

impl Connection {
    /// 启动连接的读写协程
    /// 返回一个消息发送通道和一个消息接收通道
    ///
    /// - `tx_to_remote`: 通过此通道发送消息到远端
    /// - `rx_from_remote`: 通过此通道接收来自远端的消息
    pub fn spawn(
        stream: TcpStream,
        buffer_size: usize,
    ) -> (
        mpsc::Sender<Message>,
        mpsc::Receiver<Message>,
    ) {
        let (read_half, write_half) = stream.into_split();

        // 发送通道：上层 -> 写协程 -> 远端
        let (outbound_tx, mut outbound_rx) = mpsc::channel::<Message>(buffer_size);
        // 接收通道：远端 -> 读协程 -> 上层
        let (inbound_tx, inbound_rx) = mpsc::channel::<Message>(buffer_size);

        // 写协程：从outbound_rx获取消息，编码后发送到远端
        let mut write_half = write_half;
        tokio::spawn(async move {
            while let Some(msg) = outbound_rx.recv().await {
                match MessageCodec::write_message(&mut write_half, &msg).await {
                    Ok(()) => {
                        debug!("成功发送消息: {:?}", std::mem::discriminant(&msg));
                    }
                    Err(MessageError::ConnectionClosed) => {
                        info!("写连接已关闭");
                        break;
                    }
                    Err(e) => {
                        error!("发送消息失败: {}", e);
                        break;
                    }
                }
            }
            debug!("写协程退出");
        });

        // 读协程：从远端读取消息，解码后发送到inbound_tx
        let mut read_half = read_half;
        tokio::spawn(async move {
            loop {
                match MessageCodec::read_message(&mut read_half).await {
                    Ok(msg) => {
                        debug!("成功接收消息: {:?}", std::mem::discriminant(&msg));
                        if inbound_tx.send(msg).await.is_err() {
                            warn!("消息接收通道已关闭，读协程退出");
                            break;
                        }
                    }
                    Err(MessageError::ConnectionClosed) => {
                        info!("读连接已关闭");
                        break;
                    }
                    Err(e) => {
                        error!("接收消息失败: {}", e);
                        break;
                    }
                }
            }
            debug!("读协程退出");
        });

        (outbound_tx, inbound_rx)
    }
}