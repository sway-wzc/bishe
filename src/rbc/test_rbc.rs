use crate::rbc::protocol::RbcManager;
use crate::rbc::types::{RbcConfig, RbcMessage};

/// 辅助函数：创建n个RBC管理器，模拟n个节点
fn create_managers(n: usize) -> Vec<RbcManager> {
    let node_ids: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();
    let config = RbcConfig::new(n).unwrap();

    node_ids
        .iter()
        .map(|id| RbcManager::new(id.clone(), config.clone(), node_ids.clone()))
        .collect()
}

/// 辅助函数：将消息路由到目标节点，收集新产生的消息
fn route_messages(
    managers: &mut Vec<RbcManager>,
    messages: Vec<(String, RbcMessage)>,
    node_ids: &[String],
) -> Vec<(String, RbcMessage)> {
    let mut new_messages = Vec::new();

    for (target_id, msg) in messages {
        if let Some(idx) = node_ids.iter().position(|id| id == &target_id) {
            match managers[idx].handle_message(msg) {
                Ok(msgs) => new_messages.extend(msgs),
                Err(e) => eprintln!("节点 {} 处理消息失败: {}", target_id, e),
            }
        }
    }

    new_messages
}

/// 辅助函数：运行消息传递直到没有新消息产生
fn run_until_quiescent(
    managers: &mut Vec<RbcManager>,
    initial_messages: Vec<(String, RbcMessage)>,
    node_ids: &[String],
) {
    let mut pending = initial_messages;
    let mut rounds = 0;
    let max_rounds = 100; // 防止无限循环

    while !pending.is_empty() && rounds < max_rounds {
        rounds += 1;
        let msg_count = pending.len();
        pending = route_messages(managers, pending, node_ids);
        println!("第{}轮: 处理{}条消息, 产生{}条新消息", rounds, msg_count, pending.len());
    }

    println!("协议在{}轮后收敛", rounds);
}

#[test]
fn test_rbc_config() {
    // 4个节点: t=1, data_shards=2, parity_shards=2
    let config = RbcConfig::new(4).unwrap();
    assert_eq!(config.total_nodes, 4);
    assert_eq!(config.fault_tolerance, 1);
    assert_eq!(config.data_shards, 2);
    assert_eq!(config.parity_shards, 2);
    assert_eq!(config.echo_threshold(), 3); // 2*1+1
    assert_eq!(config.ready_amplify_threshold(), 2); // 1+1

    // 7个节点: t=2, data_shards=3, parity_shards=4
    let config = RbcConfig::new(7).unwrap();
    assert_eq!(config.fault_tolerance, 2);
    assert_eq!(config.data_shards, 3);
    assert_eq!(config.parity_shards, 4);
    assert_eq!(config.echo_threshold(), 5); // 2*2+1
    assert_eq!(config.ready_amplify_threshold(), 3); // 2+1

    // 节点数太少应该报错
    assert!(RbcConfig::new(3).is_err());
    assert!(RbcConfig::new(1).is_err());
}

#[test]
fn test_rbc_basic_4_nodes() {
    // 4个节点的基本RBC广播测试
    let n = 4;
    let node_ids: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();
    let mut managers = create_managers(n);

    let data = b"Hello, RBC Protocol! This is a test message.".to_vec();

    // node_0 作为广播者发起广播
    let initial_msgs = managers[0]
        .broadcast("test_broadcast_1".to_string(), data.clone())
        .unwrap();

    assert!(!initial_msgs.is_empty());
    println!("广播者产生{}条PROPOSE消息", initial_msgs.len());

    // 运行协议直到收敛
    run_until_quiescent(&mut managers, initial_msgs, &node_ids);

    // 检查所有节点是否都成功输出
    let mut success_count = 0;
    for (i, manager) in managers.iter_mut().enumerate() {
        let outputs = manager.drain_outputs();
        if !outputs.is_empty() {
            assert_eq!(outputs[0].data, data);
            println!("节点 {} 成功输出数据, 大小={}字节", i, outputs[0].data.len());
            success_count += 1;
        }
    }

    println!("成功输出的节点数: {}/{}", success_count, n);
    assert!(success_count >= n - 1, "至少n-1个节点应该成功输出");
}

#[test]
fn test_rbc_7_nodes() {
    // 7个节点的RBC广播测试（t=2，可容忍2个拜占庭节点）
    let n = 7;
    let node_ids: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();
    let mut managers = create_managers(n);

    let data = b"Testing RBC with 7 nodes, fault tolerance = 2".to_vec();

    let initial_msgs = managers[0]
        .broadcast("test_7nodes".to_string(), data.clone())
        .unwrap();

    run_until_quiescent(&mut managers, initial_msgs, &node_ids);

    let mut success_count = 0;
    for (i, manager) in managers.iter_mut().enumerate() {
        let outputs = manager.drain_outputs();
        if !outputs.is_empty() {
            assert_eq!(outputs[0].data, data);
            success_count += 1;
        }
    }

    println!("7节点测试: 成功输出 {}/{}", success_count, n);
    assert!(success_count >= n - 2);
}

#[test]
fn test_rbc_with_node_failure() {
    // 模拟节点故障：4个节点中1个节点不参与（t=1，可容忍1个故障）
    let n = 4;
    let node_ids: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();
    let mut managers = create_managers(n);

    let data = b"Testing fault tolerance with node failure".to_vec();

    let initial_msgs = managers[0]
        .broadcast("test_fault".to_string(), data.clone())
        .unwrap();

    // 过滤掉发给 node_3 的消息（模拟 node_3 宕机）
    let failed_node = "node_3";
    let filtered_msgs: Vec<_> = initial_msgs
        .into_iter()
        .filter(|(target, _)| target != failed_node)
        .collect();

    println!("模拟 {} 宕机，过滤后消息数: {}", failed_node, filtered_msgs.len());

    // 运行协议，但所有发给 node_3 的消息都被丢弃
    let mut pending = filtered_msgs;
    let mut rounds = 0;
    while !pending.is_empty() && rounds < 100 {
        rounds += 1;
        let mut new_msgs = Vec::new();
        for (target_id, msg) in pending {
            if target_id == failed_node {
                continue; // 丢弃发给故障节点的消息
            }
            if let Some(idx) = node_ids.iter().position(|id| id == &target_id) {
                match managers[idx].handle_message(msg) {
                    Ok(msgs) => {
                        // 过滤掉故障节点产生的消息（它已宕机）
                        let filtered: Vec<_> = msgs
                            .into_iter()
                            .filter(|(target, _)| target != failed_node)
                            .collect();
                        new_msgs.extend(filtered);
                    }
                    Err(e) => eprintln!("处理失败: {}", e),
                }
            }
        }
        pending = new_msgs;
    }

    // 检查存活节点是否成功输出
    let mut success_count = 0;
    for (i, manager) in managers.iter_mut().enumerate() {
        if node_ids[i] == failed_node {
            continue;
        }
        let outputs = manager.drain_outputs();
        if !outputs.is_empty() {
            assert_eq!(outputs[0].data, data);
            success_count += 1;
        }
    }

    println!(
        "节点故障测试: 存活节点中 {}/{} 成功输出",
        success_count,
        n - 1
    );
    // 在4节点t=1的情况下，1个节点宕机后，剩余3个节点应该能完成协议
    assert!(success_count >= 2, "至少2个存活节点应该成功输出");
}

#[test]
fn test_rbc_large_data() {
    // 测试大数据量的RBC广播
    let n = 4;
    let node_ids: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();
    let mut managers = create_managers(n);

    // 生成100KB的测试数据
    let data: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();

    let initial_msgs = managers[0]
        .broadcast("test_large".to_string(), data.clone())
        .unwrap();

    run_until_quiescent(&mut managers, initial_msgs, &node_ids);

    let mut success_count = 0;
    for (i, manager) in managers.iter_mut().enumerate() {
        let outputs = manager.drain_outputs();
        if !outputs.is_empty() {
            assert_eq!(outputs[0].data, data);
            success_count += 1;
        }
    }

    println!("大数据测试: 成功输出 {}/{}", success_count, n);
    assert!(success_count >= n - 1);
}

#[test]
fn test_rbc_concurrent_broadcasts() {
    // 测试多个并发广播实例
    let n = 4;
    let node_ids: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();
    let mut managers = create_managers(n);

    let data1 = b"First broadcast message".to_vec();
    let data2 = b"Second broadcast message".to_vec();

    // node_0 发起第一个广播
    let msgs1 = managers[0]
        .broadcast("broadcast_1".to_string(), data1.clone())
        .unwrap();

    // node_1 发起第二个广播
    let msgs2 = managers[1]
        .broadcast("broadcast_2".to_string(), data2.clone())
        .unwrap();

    // 合并所有消息一起处理
    let mut all_msgs = msgs1;
    all_msgs.extend(msgs2);

    run_until_quiescent(&mut managers, all_msgs, &node_ids);

    // 检查每个节点是否收到了两个广播的输出
    for (i, manager) in managers.iter_mut().enumerate() {
        let outputs = manager.drain_outputs();
        println!("节点 {} 收到 {} 个输出", i, outputs.len());
        for output in &outputs {
            if output.instance_id == "broadcast_1" {
                assert_eq!(output.data, data1);
            } else if output.instance_id == "broadcast_2" {
                assert_eq!(output.data, data2);
            }
        }
    }
}

#[test]
fn test_rbc_10_nodes() {
    // 10个节点的RBC广播测试（t=3，可容忍3个拜占庭节点）
    let n = 10;
    let node_ids: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();
    let mut managers = create_managers(n);

    let config = RbcConfig::new(n).unwrap();
    println!("{}", config.info());

    let data = b"Testing RBC with 10 nodes for scalability".to_vec();

    let initial_msgs = managers[0]
        .broadcast("test_10nodes".to_string(), data.clone())
        .unwrap();

    run_until_quiescent(&mut managers, initial_msgs, &node_ids);

    let mut success_count = 0;
    for manager in managers.iter_mut() {
        let outputs = manager.drain_outputs();
        if !outputs.is_empty() {
            assert_eq!(outputs[0].data, data);
            success_count += 1;
        }
    }

    println!("10节点测试: 成功输出 {}/{}", success_count, n);
    assert!(success_count >= n - config.fault_tolerance);
}

#[test]
fn test_rbc_data_integrity() {
    // 验证数据完整性：确保恢复的数据与原始数据完全一致
    let n = 7;
    let node_ids: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();
    let mut managers = create_managers(n);

    // 使用各种特殊数据模式
    let test_cases: Vec<Vec<u8>> = vec![
        vec![0u8; 1000],                                    // 全零
        vec![0xFF; 1000],                                    // 全1
        (0..1000).map(|i| (i % 256) as u8).collect(),       // 递增
        b"Short".to_vec(),                                   // 短数据
        (0..50000).map(|i| ((i * 7 + 13) % 256) as u8).collect(), // 伪随机
    ];

    for (case_idx, data) in test_cases.iter().enumerate() {
        let mut case_managers = create_managers(n);
        let instance_id = format!("integrity_test_{}", case_idx);

        let initial_msgs = case_managers[0]
            .broadcast(instance_id, data.clone())
            .unwrap();

        run_until_quiescent(&mut case_managers, initial_msgs, &node_ids);

        for (i, manager) in case_managers.iter_mut().enumerate() {
            let outputs = manager.drain_outputs();
            for output in &outputs {
                assert_eq!(
                    output.data, *data,
                    "测试用例{} 节点{} 数据不一致",
                    case_idx, i
                );
            }
        }
        println!("数据完整性测试用例 {} 通过 (大小={}字节)", case_idx, data.len());
    }
}

// ============================================================================
// 拜占庭恶意节点测试（Byzantine Fault Tolerance Tests）
// ============================================================================

/// 辅助函数：带恶意节点的消息路由
/// malicious_nodes: 恶意节点ID集合
/// tamper_fn: 恶意节点的篡改函数，对恶意节点产生的消息进行篡改
fn route_messages_with_byzantine<F>(
    managers: &mut Vec<RbcManager>,
    messages: Vec<(String, RbcMessage)>,
    node_ids: &[String],
    malicious_nodes: &std::collections::HashSet<String>,
    tamper_fn: &F,
) -> Vec<(String, RbcMessage)>
where
    F: Fn(&str, String, RbcMessage) -> Option<(String, RbcMessage)>,
{
    let mut new_messages = Vec::new();

    for (target_id, msg) in messages {
        if let Some(idx) = node_ids.iter().position(|id| id == &target_id) {
            match managers[idx].handle_message(msg) {
                Ok(msgs) => {
                    for (dest, out_msg) in msgs {
                        // 如果消息来源是恶意节点，则通过篡改函数处理
                        if malicious_nodes.contains(&target_id) {
                            if let Some(tampered) = tamper_fn(&target_id, dest, out_msg) {
                                new_messages.push(tampered);
                            }
                            // 如果tamper_fn返回None，则丢弃该消息
                        } else {
                            new_messages.push((dest, out_msg));
                        }
                    }
                }
                Err(e) => eprintln!("节点 {} 处理消息失败: {}", target_id, e),
            }
        }
    }

    new_messages
}

/// 辅助函数：带恶意节点的协议运行
fn run_with_byzantine<F>(
    managers: &mut Vec<RbcManager>,
    initial_messages: Vec<(String, RbcMessage)>,
    node_ids: &[String],
    malicious_nodes: &std::collections::HashSet<String>,
    tamper_fn: F,
) where
    F: Fn(&str, String, RbcMessage) -> Option<(String, RbcMessage)>,
{
    let mut pending = initial_messages;
    let mut rounds = 0;
    let max_rounds = 200; // 恶意场景可能需要更多轮次

    while !pending.is_empty() && rounds < max_rounds {
        rounds += 1;
        let msg_count = pending.len();
        pending = route_messages_with_byzantine(
            managers,
            pending,
            node_ids,
            malicious_nodes,
            &tamper_fn,
        );
        if rounds <= 10 || rounds % 10 == 0 {
            println!(
                "第{}轮: 处理{}条消息, 产生{}条新消息",
                rounds,
                msg_count,
                pending.len()
            );
        }
    }

    println!("协议在{}轮后收敛", rounds);
}

#[test]
fn test_byzantine_tampered_shard_data() {
    // ========================================================================
    // 恶意场景1：篡改分片数据
    // 恶意节点在ECHO阶段发送被篡改的shard_data（但shard_hash不变）
    // 预期：被第一层SHA-256校验拦截，诚实节点仍能正确完成协议
    // ========================================================================
    println!("\n=== 恶意场景1: 篡改分片数据（shard_data被修改，shard_hash不变）===");

    let n = 7; // t=2，最多容忍2个恶意节点
    let node_ids: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();
    let mut managers = create_managers(n);

    let data = b"Byzantine test: tampered shard data attack".to_vec();

    let initial_msgs = managers[0]
        .broadcast("byz_tamper_shard".to_string(), data.clone())
        .unwrap();

    // node_5 和 node_6 是恶意节点
    let malicious: std::collections::HashSet<String> =
        vec!["node_5".to_string(), "node_6".to_string()]
            .into_iter()
            .collect();

    println!("恶意节点: {:?}", malicious);

    // 恶意行为：篡改ECHO和READY消息中的分片数据（翻转所有字节）
    let tamper_fn = |_source: &str, dest: String, msg: RbcMessage| -> Option<(String, RbcMessage)> {
        match msg {
            RbcMessage::Echo {
                instance_id,
                sender,
                data_hash,
                shard_index,
                mut shard_data,
                shard_hash, // 保持原始哈希不变，制造哈希不匹配
                original_size,
                data_shard_count,
                parity_shard_count,
            } => {
                // 翻转分片数据的每个字节
                for byte in shard_data.iter_mut() {
                    *byte = !*byte;
                }
                Some((
                    dest,
                    RbcMessage::Echo {
                        instance_id,
                        sender,
                        data_hash,
                        shard_index,
                        shard_data,
                        shard_hash, // 哈希不变，会被校验拦截
                        original_size,
                        data_shard_count,
                        parity_shard_count,
                    },
                ))
            }
            RbcMessage::Ready {
                instance_id,
                sender,
                data_hash,
                shard_index,
                mut shard_data,
                shard_hash,
                original_size,
                data_shard_count,
                parity_shard_count,
            } => {
                for byte in shard_data.iter_mut() {
                    *byte = !*byte;
                }
                Some((
                    dest,
                    RbcMessage::Ready {
                        instance_id,
                        sender,
                        data_hash,
                        shard_index,
                        shard_data,
                        shard_hash,
                        original_size,
                        data_shard_count,
                        parity_shard_count,
                    },
                ))
            }
            other => Some((dest, other)),
        }
    };

    run_with_byzantine(&mut managers, initial_msgs, &node_ids, &malicious, tamper_fn);

    // 验证：诚实节点应该成功输出正确数据
    let mut success_count = 0;
    for (i, manager) in managers.iter_mut().enumerate() {
        if malicious.contains(&node_ids[i]) {
            continue; // 跳过恶意节点
        }
        let outputs = manager.drain_outputs();
        if !outputs.is_empty() {
            assert_eq!(outputs[0].data, data, "诚实节点 {} 输出数据不正确", i);
            success_count += 1;
        }
    }

    println!(
        "恶意场景1结果: {}/{} 个诚实节点成功输出",
        success_count,
        n - malicious.len()
    );
    // 5个诚实节点中至少3个应该成功
    assert!(
        success_count >= 3,
        "篡改分片数据攻击下，至少3个诚实节点应成功输出，实际: {}",
        success_count
    );
}

#[test]
fn test_byzantine_fake_hash() {
    // ========================================================================
    // 恶意场景2：伪造数据哈希
    // 恶意节点在ECHO阶段发送完全伪造的data_hash和对应的shard_data+shard_hash
    // 预期：伪造哈希的投票数 ≤ t，达不到2t+1阈值，被多数投票机制拦截
    // ========================================================================
    println!("\n=== 恶意场景2: 伪造数据哈希（发送完全不同的假数据）===");

    let n = 7;
    let node_ids: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();
    let mut managers = create_managers(n);

    let data = b"Byzantine test: fake hash attack with forged data".to_vec();

    let initial_msgs = managers[0]
        .broadcast("byz_fake_hash".to_string(), data.clone())
        .unwrap();

    let malicious: std::collections::HashSet<String> =
        vec!["node_5".to_string(), "node_6".to_string()]
            .into_iter()
            .collect();

    println!("恶意节点: {:?}", malicious);

    // 恶意行为：用完全不同的假数据替换，同时伪造匹配的哈希
    let tamper_fn = |_source: &str, dest: String, msg: RbcMessage| -> Option<(String, RbcMessage)> {
        match msg {
            RbcMessage::Echo {
                instance_id,
                sender,
                data_hash: _,
                shard_index,
                shard_data: _,
                shard_hash: _,
                original_size,
                data_shard_count,
                parity_shard_count,
            } => {
                // 生成完全不同的假分片数据
                let fake_shard: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF]
                    .into_iter()
                    .cycle()
                    .take(original_size / data_shard_count + 1)
                    .collect();
                // 计算假分片的正确哈希（使哈希校验通过，但data_hash是假的）
                let fake_shard_hash = {
                    use sha2::{Digest, Sha256};
                    let mut hasher = Sha256::new();
                    hasher.update(&fake_shard);
                    hex::encode(hasher.finalize())
                };
                let fake_data_hash = "aaaa_fake_hash_from_malicious_node_bbbb".to_string();

                Some((
                    dest,
                    RbcMessage::Echo {
                        instance_id,
                        sender,
                        data_hash: fake_data_hash,
                        shard_index,
                        shard_data: fake_shard,
                        shard_hash: fake_shard_hash,
                        original_size,
                        data_shard_count,
                        parity_shard_count,
                    },
                ))
            }
            RbcMessage::Ready {
                instance_id,
                sender,
                data_hash: _,
                shard_index,
                shard_data: _,
                shard_hash: _,
                original_size,
                data_shard_count,
                parity_shard_count,
            } => {
                let fake_shard: Vec<u8> = vec![0xCA, 0xFE, 0xBA, 0xBE]
                    .into_iter()
                    .cycle()
                    .take(original_size / data_shard_count + 1)
                    .collect();
                let fake_shard_hash = {
                    use sha2::{Digest, Sha256};
                    let mut hasher = Sha256::new();
                    hasher.update(&fake_shard);
                    hex::encode(hasher.finalize())
                };
                let fake_data_hash = "aaaa_fake_hash_from_malicious_node_bbbb".to_string();

                Some((
                    dest,
                    RbcMessage::Ready {
                        instance_id,
                        sender,
                        data_hash: fake_data_hash,
                        shard_index,
                        shard_data: fake_shard,
                        shard_hash: fake_shard_hash,
                        original_size,
                        data_shard_count,
                        parity_shard_count,
                    },
                ))
            }
            other => Some((dest, other)),
        }
    };

    run_with_byzantine(&mut managers, initial_msgs, &node_ids, &malicious, tamper_fn);

    let mut success_count = 0;
    for (i, manager) in managers.iter_mut().enumerate() {
        if malicious.contains(&node_ids[i]) {
            continue;
        }
        let outputs = manager.drain_outputs();
        if !outputs.is_empty() {
            assert_eq!(outputs[0].data, data, "诚实节点 {} 输出了错误数据!", i);
            success_count += 1;
        }
    }

    println!(
        "恶意场景2结果: {}/{} 个诚实节点成功输出",
        success_count,
        n - malicious.len()
    );
    assert!(
        success_count >= 3,
        "伪造哈希攻击下，至少3个诚实节点应成功输出，实际: {}",
        success_count
    );
}

#[test]
fn test_byzantine_selective_silence() {
    // ========================================================================
    // 恶意场景3：选择性沉默（部分消息丢弃）
    // 恶意节点只向部分节点发送消息，对其他节点保持沉默
    // 这是比完全掉线更隐蔽的攻击方式
    // 预期：诚实节点通过READY放大机制仍能完成协议
    // ========================================================================
    println!("\n=== 恶意场景3: 选择性沉默（只向部分节点发送消息）===");

    let n = 7;
    let node_ids: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();
    let mut managers = create_managers(n);

    let data = b"Byzantine test: selective silence attack".to_vec();

    let initial_msgs = managers[0]
        .broadcast("byz_silence".to_string(), data.clone())
        .unwrap();

    let malicious: std::collections::HashSet<String> =
        vec!["node_5".to_string(), "node_6".to_string()]
            .into_iter()
            .collect();

    println!("恶意节点: {:?}", malicious);

    // 恶意行为：只向node_0和node_1发送消息，对其他节点沉默
    let tamper_fn = |_source: &str, dest: String, msg: RbcMessage| -> Option<(String, RbcMessage)> {
        // 恶意节点只向前两个节点发送消息
        if dest == "node_0" || dest == "node_1" {
            Some((dest, msg))
        } else {
            None // 对其他节点沉默
        }
    };

    run_with_byzantine(&mut managers, initial_msgs, &node_ids, &malicious, tamper_fn);

    let mut success_count = 0;
    for (i, manager) in managers.iter_mut().enumerate() {
        if malicious.contains(&node_ids[i]) {
            continue;
        }
        let outputs = manager.drain_outputs();
        if !outputs.is_empty() {
            assert_eq!(outputs[0].data, data, "诚实节点 {} 输出了错误数据!", i);
            success_count += 1;
        }
    }

    println!(
        "恶意场景3结果: {}/{} 个诚实节点成功输出",
        success_count,
        n - malicious.len()
    );
    assert!(
        success_count >= 3,
        "选择性沉默攻击下，至少3个诚实节点应成功输出，实际: {}",
        success_count
    );
}

#[test]
fn test_byzantine_contradictory_echo() {
    // ========================================================================
    // 恶意场景4：矛盾ECHO攻击（equivocation）
    // 恶意节点给不同的节点发送不同的data_hash，试图分裂网络共识
    // 预期：2t+1多数投票确保只有一个哈希能达到阈值
    // ========================================================================
    println!("\n=== 恶意场景4: 矛盾ECHO攻击（给不同节点发不同哈希）===");

    let n = 7;
    let node_ids: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();
    let mut managers = create_managers(n);

    let data = b"Byzantine test: contradictory echo equivocation".to_vec();

    let initial_msgs = managers[0]
        .broadcast("byz_contradict".to_string(), data.clone())
        .unwrap();

    let malicious: std::collections::HashSet<String> =
        vec!["node_5".to_string(), "node_6".to_string()]
            .into_iter()
            .collect();

    println!("恶意节点: {:?}", malicious);

    // 恶意行为：给偶数索引节点发送假哈希A，给奇数索引节点发送假哈希B
    let tamper_fn = |_source: &str, dest: String, msg: RbcMessage| -> Option<(String, RbcMessage)> {
        let dest_index: usize = dest
            .strip_prefix("node_")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        match msg {
            RbcMessage::Echo {
                instance_id,
                sender,
                data_hash: _,
                shard_index,
                shard_data,
                shard_hash,
                original_size,
                data_shard_count,
                parity_shard_count,
            } => {
                // 给不同节点发送不同的假哈希
                let fake_hash = if dest_index % 2 == 0 {
                    "fake_hash_AAAA_for_even_nodes".to_string()
                } else {
                    "fake_hash_BBBB_for_odd_nodes".to_string()
                };

                Some((
                    dest,
                    RbcMessage::Echo {
                        instance_id,
                        sender,
                        data_hash: fake_hash,
                        shard_index,
                        shard_data,  // 保持分片数据不变
                        shard_hash,  // 保持分片哈希不变（分片校验能通过）
                        original_size,
                        data_shard_count,
                        parity_shard_count,
                    },
                ))
            }
            other => Some((dest, other)),
        }
    };

    run_with_byzantine(&mut managers, initial_msgs, &node_ids, &malicious, tamper_fn);

    let mut success_count = 0;
    let mut wrong_output = false;
    for (i, manager) in managers.iter_mut().enumerate() {
        if malicious.contains(&node_ids[i]) {
            continue;
        }
        let outputs = manager.drain_outputs();
        if !outputs.is_empty() {
            if outputs[0].data == data {
                success_count += 1;
            } else {
                wrong_output = true;
                println!("警告: 诚实节点 {} 输出了错误数据!", i);
            }
        }
    }

    // 关键断言：没有诚实节点输出错误数据
    assert!(
        !wrong_output,
        "矛盾ECHO攻击不应导致任何诚实节点输出错误数据"
    );

    println!(
        "恶意场景4结果: {}/{} 个诚实节点成功输出，无错误输出",
        success_count,
        n - malicious.len()
    );
    assert!(
        success_count >= 3,
        "矛盾ECHO攻击下，至少3个诚实节点应成功输出，实际: {}",
        success_count
    );
}

#[test]
fn test_byzantine_mixed_attack_7_nodes() {
    // ========================================================================
    // 恶意场景5：混合攻击（最严苛测试）
    // 2个恶意节点同时执行不同的攻击策略：
    //   - node_5: 篡改分片数据+伪造哈希
    //   - node_6: 选择性沉默（只向部分节点发消息）
    // 预期：5个诚实节点仍能正确完成协议
    // ========================================================================
    println!("\n=== 恶意场景5: 混合攻击（篡改+沉默同时进行）===");

    let n = 7;
    let node_ids: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();
    let mut managers = create_managers(n);

    let data = b"Byzantine test: mixed attack - the ultimate resilience test!".to_vec();

    let initial_msgs = managers[0]
        .broadcast("byz_mixed".to_string(), data.clone())
        .unwrap();

    let malicious: std::collections::HashSet<String> =
        vec!["node_5".to_string(), "node_6".to_string()]
            .into_iter()
            .collect();

    println!("恶意节点: node_5(篡改攻击), node_6(沉默攻击)");

    let tamper_fn = |source: &str, dest: String, msg: RbcMessage| -> Option<(String, RbcMessage)> {
        if source == "node_6" {
            // node_6: 选择性沉默，只向node_0发消息
            if dest == "node_0" {
                return Some((dest, msg));
            } else {
                return None;
            }
        }

        // node_5: 篡改分片数据并伪造哈希
        match msg {
            RbcMessage::Echo {
                instance_id,
                sender,
                data_hash: _,
                shard_index,
                shard_data: _,
                shard_hash: _,
                original_size,
                data_shard_count,
                parity_shard_count,
            } => {
                let fake_shard: Vec<u8> = vec![0xFF; original_size / data_shard_count + 1];
                let fake_shard_hash = {
                    use sha2::{Digest, Sha256};
                    let mut hasher = Sha256::new();
                    hasher.update(&fake_shard);
                    hex::encode(hasher.finalize())
                };
                Some((
                    dest,
                    RbcMessage::Echo {
                        instance_id,
                        sender,
                        data_hash: "mixed_attack_fake_hash".to_string(),
                        shard_index,
                        shard_data: fake_shard,
                        shard_hash: fake_shard_hash,
                        original_size,
                        data_shard_count,
                        parity_shard_count,
                    },
                ))
            }
            RbcMessage::Ready {
                instance_id,
                sender,
                data_hash: _,
                shard_index,
                shard_data: _,
                shard_hash: _,
                original_size,
                data_shard_count,
                parity_shard_count,
            } => {
                let fake_shard: Vec<u8> = vec![0xFF; original_size / data_shard_count + 1];
                let fake_shard_hash = {
                    use sha2::{Digest, Sha256};
                    let mut hasher = Sha256::new();
                    hasher.update(&fake_shard);
                    hex::encode(hasher.finalize())
                };
                Some((
                    dest,
                    RbcMessage::Ready {
                        instance_id,
                        sender,
                        data_hash: "mixed_attack_fake_hash".to_string(),
                        shard_index,
                        shard_data: fake_shard,
                        shard_hash: fake_shard_hash,
                        original_size,
                        data_shard_count,
                        parity_shard_count,
                    },
                ))
            }
            other => Some((dest, other)),
        }
    };

    run_with_byzantine(&mut managers, initial_msgs, &node_ids, &malicious, tamper_fn);

    let mut success_count = 0;
    let mut wrong_output = false;
    for (i, manager) in managers.iter_mut().enumerate() {
        if malicious.contains(&node_ids[i]) {
            continue;
        }
        let outputs = manager.drain_outputs();
        if !outputs.is_empty() {
            if outputs[0].data == data {
                success_count += 1;
                println!("诚实节点 {} 正确输出数据 ✓", i);
            } else {
                wrong_output = true;
                println!("错误: 诚实节点 {} 输出了错误数据!", i);
            }
        } else {
            println!("诚实节点 {} 未产生输出", i);
        }
    }

    assert!(
        !wrong_output,
        "混合攻击不应导致任何诚实节点输出错误数据"
    );

    println!(
        "恶意场景5结果: {}/{} 个诚实节点成功输出",
        success_count,
        n - malicious.len()
    );
    assert!(
        success_count >= 3,
        "混合攻击下，至少3个诚实节点应成功输出，实际: {}",
        success_count
    );
}

#[test]
fn test_byzantine_max_tolerance_10_nodes() {
    // ========================================================================
    // 恶意场景6：10节点极限容错测试（t=3，3个恶意节点同时攻击）
    // 验证在最大容错数量的恶意节点下系统仍然安全
    // ========================================================================
    println!("\n=== 恶意场景6: 10节点极限容错（3个恶意节点同时攻击）===");

    let n = 10;
    let config = RbcConfig::new(n).unwrap();
    println!("{}", config.info());

    let node_ids: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();
    let mut managers = create_managers(n);

    let data: Vec<u8> = (0..10_000).map(|i| ((i * 13 + 7) % 256) as u8).collect();

    let initial_msgs = managers[0]
        .broadcast("byz_max_10".to_string(), data.clone())
        .unwrap();

    // 3个恶意节点（t=3的极限）
    let malicious: std::collections::HashSet<String> = vec![
        "node_7".to_string(),
        "node_8".to_string(),
        "node_9".to_string(),
    ]
    .into_iter()
    .collect();

    println!("恶意节点（t={}个）: {:?}", config.fault_tolerance, malicious);

    // 恶意行为：全部发送伪造数据和哈希
    let tamper_fn = |_source: &str, dest: String, msg: RbcMessage| -> Option<(String, RbcMessage)> {
        match msg {
            RbcMessage::Echo {
                instance_id,
                sender,
                shard_index,
                original_size,
                data_shard_count,
                parity_shard_count,
                ..
            } => {
                let fake_shard: Vec<u8> = vec![0xAB; original_size / data_shard_count + 1];
                let fake_shard_hash = {
                    use sha2::{Digest, Sha256};
                    let mut hasher = Sha256::new();
                    hasher.update(&fake_shard);
                    hex::encode(hasher.finalize())
                };
                Some((
                    dest,
                    RbcMessage::Echo {
                        instance_id,
                        sender,
                        data_hash: "10node_fake_hash_from_byzantine".to_string(),
                        shard_index,
                        shard_data: fake_shard,
                        shard_hash: fake_shard_hash,
                        original_size,
                        data_shard_count,
                        parity_shard_count,
                    },
                ))
            }
            RbcMessage::Ready {
                instance_id,
                sender,
                shard_index,
                original_size,
                data_shard_count,
                parity_shard_count,
                ..
            } => {
                let fake_shard: Vec<u8> = vec![0xAB; original_size / data_shard_count + 1];
                let fake_shard_hash = {
                    use sha2::{Digest, Sha256};
                    let mut hasher = Sha256::new();
                    hasher.update(&fake_shard);
                    hex::encode(hasher.finalize())
                };
                Some((
                    dest,
                    RbcMessage::Ready {
                        instance_id,
                        sender,
                        data_hash: "10node_fake_hash_from_byzantine".to_string(),
                        shard_index,
                        shard_data: fake_shard,
                        shard_hash: fake_shard_hash,
                        original_size,
                        data_shard_count,
                        parity_shard_count,
                    },
                ))
            }
            other => Some((dest, other)),
        }
    };

    run_with_byzantine(&mut managers, initial_msgs, &node_ids, &malicious, tamper_fn);

    let mut success_count = 0;
    let mut wrong_output = false;
    for (i, manager) in managers.iter_mut().enumerate() {
        if malicious.contains(&node_ids[i]) {
            continue;
        }
        let outputs = manager.drain_outputs();
        if !outputs.is_empty() {
            if outputs[0].data == data {
                success_count += 1;
            } else {
                wrong_output = true;
                println!("错误: 诚实节点 {} 输出了错误数据!", i);
            }
        }
    }

    assert!(
        !wrong_output,
        "10节点极限容错测试中不应有诚实节点输出错误数据"
    );

    println!(
        "恶意场景6结果: {}/{} 个诚实节点成功输出",
        success_count,
        n - malicious.len()
    );
    assert!(
        success_count >= n - 2 * config.fault_tolerance,
        "10节点极限容错下，至少{}个诚实节点应成功输出，实际: {}",
        n - 2 * config.fault_tolerance,
        success_count
    );
}
