mod network;
pub mod erasure;
pub mod rbc;

use anyhow::Result;
use log::info;
use network::node::P2PNode;
use network::config::NodeConfig;
use std::env;
use std::sync::Arc;

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

    // 读取RBC测试相关环境变量
    let rbc_test_file = env::var("RBC_TEST_FILE").ok();
    let rbc_broadcast_delay: u64 = env::var("RBC_BROADCAST_DELAY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30); // 默认等待30秒让所有节点连接就绪
    let rbc_output_dir = env::var("RBC_OUTPUT_DIR")
        .unwrap_or_else(|_| "/app/rbc_output".to_string());

    if let Some(ref file) = rbc_test_file {
        info!("RBC测试模式: 将在{}秒后广播文件 {}", rbc_broadcast_delay, file);
    }

    // 创建RBC输出目录
    if let Err(e) = std::fs::create_dir_all(&rbc_output_dir) {
        log::warn!("创建RBC输出目录失败: {}", e);
    }

    // 创建并启动P2P节点
    let node = Arc::new(P2PNode::new(config).await?);

    // 如果设置了RBC测试文件，启动一个后台任务来触发广播
    if let Some(test_file) = rbc_test_file.filter(|f| !f.is_empty()) {
        let node_clone = node.clone();
        let output_dir = rbc_output_dir.clone();
        tokio::spawn(async move {
            // 等待网络就绪和RBC初始化
            info!("[RBC测试] 等待 {}秒 让网络就绪...", rbc_broadcast_delay);
            tokio::time::sleep(tokio::time::Duration::from_secs(rbc_broadcast_delay)).await;

            // 额外等待RBC管理器初始化完成（最多再等60秒）
            let mut rbc_ready = false;
            for i in 0..30 {
                let rbc_mgr = node_clone.rbc_manager();
                let guard = rbc_mgr.lock().await;
                if guard.is_some() {
                    rbc_ready = true;
                    break;
                }
                drop(guard);
                if i % 5 == 0 {
                    info!("[RBC测试] RBC管理器尚未初始化，继续等待... ({}秒)", i * 2);
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            }

            if !rbc_ready {
                log::error!("[RBC测试] RBC管理器初始化超时，无法发起广播");
                return;
            }

            info!("[RBC测试] RBC管理器已就绪，准备广播");

            // 读取测试文件
            match std::fs::read(&test_file) {
                Ok(data) => {
                    let data_size = data.len();
                    info!("[RBC测试] 读取文件成功: {}, 大小={}字节", test_file, data_size);

                    // 计算原始数据哈希
                    use sha2::{Digest, Sha256};
                    let mut hasher = Sha256::new();
                    hasher.update(&data);
                    let original_hash = hex::encode(hasher.finalize());
                    info!("[RBC测试] 原始数据SHA-256: {}", original_hash);

                    // 将原始哈希写入文件，供验证使用
                    let hash_file = format!("{}/original_hash.txt", output_dir);
                    if let Err(e) = std::fs::write(&hash_file, &original_hash) {
                        log::error!("[RBC测试] 写入原始哈希失败: {}", e);
                    }

                    // 发起RBC广播
                    match node_clone.rbc_broadcast(data).await {
                        Ok(()) => {
                            info!("[RBC测试] RBC广播发起成功，数据大小={}字节", data_size);
                        }
                        Err(e) => {
                            log::error!("[RBC测试] RBC广播失败: {}", e);
                        }
                    }
                }
                Err(e) => {
                    log::error!("[RBC测试] 读取文件 {} 失败: {}", test_file, e);
                }
            }
        });
    }

    // 启动RBC输出监控任务（同时处理普通RBC输出和分块广播输出）
    {
        let node_clone = node.clone();
        let output_dir = rbc_output_dir.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(2));
            // 记录是否已经写入过DONE文件（避免分块广播中间输出覆盖最终输出）
            let mut done_written = false;
            loop {
                interval.tick().await;

                // 1. 处理分块广播输出（优先级更高）
                {
                    let chunked_mgr = node_clone.chunked_manager();
                    let mut chunked_guard = chunked_mgr.lock().await;
                    if let Some(ref mut chunked_manager) = *chunked_guard {
                        // 先将RBC输出交给分块管理器处理
                        let rbc_mgr = node_clone.rbc_manager();
                        let mut rbc_guard = rbc_mgr.lock().await;
                        if let Some(ref mut rbc_manager) = *rbc_guard {
                            let rbc_outputs = rbc_manager.drain_outputs();
                            for output in &rbc_outputs {
                                let handled = chunked_manager.handle_rbc_output(output);
                                if !handled {
                                    // 不属于分块广播的普通RBC输出，直接写入文件
                                    info!(
                                        "[RBC输出] 实例={}, 数据大小={}字节, hash={}",
                                        &output.instance_id[..8.min(output.instance_id.len())],
                                        output.data.len(),
                                        &output.data_hash[..16.min(output.data_hash.len())]
                                    );

                                    let output_file = format!(
                                        "{}/output_{}.bin",
                                        output_dir,
                                        &output.instance_id[..8.min(output.instance_id.len())]
                                    );
                                    match std::fs::write(&output_file, &output.data) {
                                        Ok(()) => info!("[RBC输出] 数据已保存到 {}", output_file),
                                        Err(e) => log::error!("[RBC输出] 保存数据失败: {}", e),
                                    }

                                    let hash_file = format!(
                                        "{}/output_{}.hash",
                                        output_dir,
                                        &output.instance_id[..8.min(output.instance_id.len())]
                                    );
                                    if let Err(e) = std::fs::write(&hash_file, &output.data_hash) {
                                        log::error!("[RBC输出] 保存哈希失败: {}", e);
                                    }

                                    if !done_written {
                                        let done_file = format!("{}/DONE", output_dir);
                                        let _ = std::fs::write(&done_file, format!(
                                            "instance={}\nsize={}\nhash={}\n",
                                            output.instance_id,
                                            output.data.len(),
                                            output.data_hash
                                        ));
                                        done_written = true;
                                    }
                                }
                            }
                        }

                        // 检查分块广播是否有完成的输出
                        let chunked_outputs = chunked_manager.drain_outputs();
                        for co in chunked_outputs {
                            info!(
                                "[分块广播输出] 会话={}, 文件大小={}字节 ({:.2}MB), 分块数={}, hash={}...",
                                &co.session_id[..8.min(co.session_id.len())],
                                co.total_size,
                                co.total_size as f64 / 1024.0 / 1024.0,
                                co.total_chunks,
                                &co.file_hash[..16.min(co.file_hash.len())]
                            );

                            // 将完整文件写入输出目录
                            let output_file = format!(
                                "{}/output_{}.bin",
                                output_dir,
                                &co.session_id[..8.min(co.session_id.len())]
                            );
                            match std::fs::write(&output_file, &co.data) {
                                Ok(()) => info!("[分块广播输出] 文件已保存到 {}", output_file),
                                Err(e) => log::error!("[分块广播输出] 保存文件失败: {}", e),
                            }

                            let hash_file = format!(
                                "{}/output_{}.hash",
                                output_dir,
                                &co.session_id[..8.min(co.session_id.len())]
                            );
                            if let Err(e) = std::fs::write(&hash_file, &co.file_hash) {
                                log::error!("[分块广播输出] 保存哈希失败: {}", e);
                            }

                            // 写入完成标记
                            let done_file = format!("{}/DONE", output_dir);
                            let _ = std::fs::write(&done_file, format!(
                                "session={}\nsize={}\nhash={}\nchunks={}\nmode=chunked\n",
                                co.session_id,
                                co.total_size,
                                co.file_hash,
                                co.total_chunks
                            ));
                            done_written = true;
                        }
                    } else {
                        // 分块管理器未初始化，按原有逻辑处理普通RBC输出
                        let rbc_mgr = node_clone.rbc_manager();
                        let mut rbc_guard = rbc_mgr.lock().await;
                        if let Some(ref mut manager) = *rbc_guard {
                            let outputs = manager.drain_outputs();
                            for output in outputs {
                                info!(
                                    "[RBC输出] 实例={}, 数据大小={}字节, hash={}",
                                    &output.instance_id[..8.min(output.instance_id.len())],
                                    output.data.len(),
                                    &output.data_hash[..16.min(output.data_hash.len())]
                                );

                                let output_file = format!(
                                    "{}/output_{}.bin",
                                    output_dir,
                                    &output.instance_id[..8.min(output.instance_id.len())]
                                );
                                match std::fs::write(&output_file, &output.data) {
                                    Ok(()) => info!("[RBC输出] 数据已保存到 {}", output_file),
                                    Err(e) => log::error!("[RBC输出] 保存数据失败: {}", e),
                                }

                                let hash_file = format!(
                                    "{}/output_{}.hash",
                                    output_dir,
                                    &output.instance_id[..8.min(output.instance_id.len())]
                                );
                                if let Err(e) = std::fs::write(&hash_file, &output.data_hash) {
                                    log::error!("[RBC输出] 保存哈希失败: {}", e);
                                }

                                if !done_written {
                                    let done_file = format!("{}/DONE", output_dir);
                                    let _ = std::fs::write(&done_file, format!(
                                        "instance={}\nsize={}\nhash={}\n",
                                        output.instance_id,
                                        output.data.len(),
                                        output.data_hash
                                    ));
                                    done_written = true;
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    // 启动P2P节点主循环（此方法会阻塞）
    node.start().await?;

    Ok(())
}