mod network;
pub mod erasure;

use anyhow::Result;
use log::info;
use network::node::P2PNode;
use network::config::NodeConfig;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let args: Vec<String> = env::args().collect();

    // 默认配置
    let mut config = NodeConfig::default();

    // 优先从环境变量读取监听端口
    if let Ok(port_str) = env::var("LISTEN_PORT") {
        if let Ok(port) = port_str.parse::<u16>() {
            config.listen_port = port;
        }
    }

    // 优先从环境变量读取种子节点地址
    if let Ok(seed_addr) = env::var("SEED_ADDR") {
        let seeds: Vec<String> = seed_addr
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        config.seed_nodes.extend(seeds);
    }

    // 命令行参数覆盖环境变量
    if args.len() > 1 {
        config.listen_port = args[1].parse().unwrap_or(config.listen_port);
    }
    if args.len() > 2 {
        let seed_addr = args[2].clone();
        config.seed_nodes.push(seed_addr);
    }

    info!("启动P2P节点，监听端口: {}", config.listen_port);
    if !config.seed_nodes.is_empty() {
        info!("种子节点列表: {:?}", config.seed_nodes);
    }

    // 创建并启动P2P节点
    let node = P2PNode::new(config).await?;
    node.start().await?;

    Ok(())
}