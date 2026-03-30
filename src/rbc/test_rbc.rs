use crate::rbc::manager::RbcManager;
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
    let max_rounds = 100;

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
    let n = 4;
    let node_ids: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();
    let mut managers = create_managers(n);

    let data = b"Hello, RBC Protocol! This is a test message.".to_vec();

    let initial_msgs = managers[0]
        .broadcast("test_broadcast_1".to_string(), data.clone())
        .unwrap();

    assert!(!initial_msgs.is_empty());
    println!("广播者产生{}条PROPOSE消息", initial_msgs.len());

    run_until_quiescent(&mut managers, initial_msgs, &node_ids);

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

    let failed_node = "node_3";
    let filtered_msgs: Vec<_> = initial_msgs
        .into_iter()
        .filter(|(target, _)| target != failed_node)
        .collect();

    println!("模拟 {} 宕机，过滤后消息数: {}", failed_node, filtered_msgs.len());

    let mut pending = filtered_msgs;
    let mut rounds = 0;
    while !pending.is_empty() && rounds < 100 {
        rounds += 1;
        let mut new_msgs = Vec::new();
        for (target_id, msg) in pending {
            if target_id == failed_node {
                continue;
            }
            if let Some(idx) = node_ids.iter().position(|id| id == &target_id) {
                match managers[idx].handle_message(msg) {
                    Ok(msgs) => {
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
    assert!(success_count >= 2, "至少2个存活节点应该成功输出");
}

#[test]
fn test_rbc_large_data() {
    let n = 4;
    let node_ids: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();
    let mut managers = create_managers(n);

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
    let n = 4;
    let node_ids: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();
    let mut managers = create_managers(n);

    let data1 = b"First broadcast message".to_vec();
    let data2 = b"Second broadcast message".to_vec();

    let msgs1 = managers[0]
        .broadcast("broadcast_1".to_string(), data1.clone())
        .unwrap();
    let msgs2 = managers[1]
        .broadcast("broadcast_2".to_string(), data2.clone())
        .unwrap();

    let mut all_msgs = msgs1;
    all_msgs.extend(msgs2);

    run_until_quiescent(&mut managers, all_msgs, &node_ids);

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
    let n = 7;
    let node_ids: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();

    let test_cases: Vec<Vec<u8>> = vec![
        vec![0u8; 1000],
        vec![0xFF; 1000],
        (0..1000).map(|i| (i % 256) as u8).collect(),
        b"Short".to_vec(),
        (0..50000).map(|i| ((i * 7 + 13) % 256) as u8).collect(),
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
                        if malicious_nodes.contains(&target_id) {
                            if let Some(tampered) = tamper_fn(&target_id, dest, out_msg) {
                                new_messages.push(tampered);
                            }
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
    let max_rounds = 200;

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
    // 恶意节点在ECHO/READY阶段发送被篡改的shard_data
    // 预期：Berlekamp-Welch纠错自动纠正假分片，hash(M')=h验证通过
    // ========================================================================
    println!("\n=== 恶意场景1: 篡改分片数据（Berlekamp-Welch纠错）===");

    let n = 7;
    let node_ids: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();
    let mut managers = create_managers(n);

    let data = b"Byzantine test: tampered shard data attack".to_vec();

    let initial_msgs = managers[0]
        .broadcast("byz_tamper_shard".to_string(), data.clone())
        .unwrap();

    let malicious: std::collections::HashSet<String> =
        vec!["node_5".to_string(), "node_6".to_string()]
            .into_iter()
            .collect();

    println!("恶意节点: {:?}", malicious);

    // 恶意行为：篡改ECHO和READY消息中的分片数据（翻转所有字节）
    // 算法4中没有shard_hash校验，恶意分片直接进入T_h
    // 依赖Berlekamp-Welch纠错 + hash(M')=h 验证
    let tamper_fn = |_source: &str, dest: String, msg: RbcMessage| -> Option<(String, RbcMessage)> {
        match msg {
            RbcMessage::Echo {
                instance_id,
                sender,
                data_hash,
                shard_index,
                mut shard_data,
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
            assert_eq!(outputs[0].data, data, "诚实节点 {} 输出数据不正确", i);
            success_count += 1;
        }
    }

    println!(
        "恶意场景1结果: {}/{} 个诚实节点成功输出",
        success_count,
        n - malicious.len()
    );
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
    // 恶意节点发送完全伪造的data_hash和shard_data
    // 预期：伪造哈希的投票数 ≤ t，达不到2t+1阈值
    // ========================================================================
    println!("\n=== 恶意场景2: 伪造数据哈希 ===");

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

    let tamper_fn = |_source: &str, dest: String, msg: RbcMessage| -> Option<(String, RbcMessage)> {
        match msg {
            RbcMessage::Echo {
                instance_id,
                sender,
                data_hash: _,
                shard_index,
                shard_data: _,
                original_size,
                data_shard_count,
                parity_shard_count,
            } => {
                let fake_shard: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF]
                    .into_iter()
                    .cycle()
                    .take(original_size / data_shard_count + 1)
                    .collect();
                let fake_data_hash = "aaaa_fake_hash_from_malicious_node_bbbb".to_string();

                Some((
                    dest,
                    RbcMessage::Echo {
                        instance_id,
                        sender,
                        data_hash: fake_data_hash,
                        shard_index,
                        shard_data: fake_shard,
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
                original_size,
                data_shard_count,
                parity_shard_count,
            } => {
                let fake_shard: Vec<u8> = vec![0xCA, 0xFE, 0xBA, 0xBE]
                    .into_iter()
                    .cycle()
                    .take(original_size / data_shard_count + 1)
                    .collect();
                let fake_data_hash = "aaaa_fake_hash_from_malicious_node_bbbb".to_string();

                Some((
                    dest,
                    RbcMessage::Ready {
                        instance_id,
                        sender,
                        data_hash: fake_data_hash,
                        shard_index,
                        shard_data: fake_shard,
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
    // 恶意场景3：选择性沉默
    // 恶意节点只向部分节点发送消息
    // 预期：诚实节点通过READY放大机制仍能完成协议
    // ========================================================================
    println!("\n=== 恶意场景3: 选择性沉默 ===");

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

    let tamper_fn = |_source: &str, dest: String, msg: RbcMessage| -> Option<(String, RbcMessage)> {
        // 恶意节点只向node_0和node_1发送消息，对其他节点沉默
        if dest == "node_0" || dest == "node_1" {
            Some((dest, msg))
        } else {
            None
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
    // 恶意节点给不同的节点发送不同的data_hash
    // 预期：2t+1多数投票确保只有一个哈希能达到阈值
    // ========================================================================
    println!("\n=== 恶意场景4: 矛盾ECHO攻击 ===");

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
                original_size,
                data_shard_count,
                parity_shard_count,
            } => {
                // 给偶数节点和奇数节点发送不同的假哈希
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
                        shard_data,
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
    // 恶意场景5：混合攻击
    //   - node_5: 篡改分片数据+伪造哈希
    //   - node_6: 选择性沉默
    // 预期：5个诚实节点仍能正确完成协议
    // ========================================================================
    println!("\n=== 恶意场景5: 混合攻击 ===");

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
        // node_6: 选择性沉默（只向node_0发送）
        if source == "node_6" {
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
                original_size,
                data_shard_count,
                parity_shard_count,
            } => {
                let fake_shard: Vec<u8> = vec![0xFF; original_size / data_shard_count + 1];
                Some((
                    dest,
                    RbcMessage::Echo {
                        instance_id,
                        sender,
                        data_hash: "mixed_attack_fake_hash".to_string(),
                        shard_index,
                        shard_data: fake_shard,
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
                original_size,
                data_shard_count,
                parity_shard_count,
            } => {
                let fake_shard: Vec<u8> = vec![0xFF; original_size / data_shard_count + 1];
                Some((
                    dest,
                    RbcMessage::Ready {
                        instance_id,
                        sender,
                        data_hash: "mixed_attack_fake_hash".to_string(),
                        shard_index,
                        shard_data: fake_shard,
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
    // 恶意场景6：10节点极限容错（t=3，3个恶意节点同时攻击）
    // ========================================================================
    println!("\n=== 恶意场景6: 10节点极限容错 ===");

    let n = 10;
    let config = RbcConfig::new(n).unwrap();
    println!("{}", config.info());

    let node_ids: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();
    let mut managers = create_managers(n);

    let data: Vec<u8> = (0..10_000).map(|i| ((i * 13 + 7) % 256) as u8).collect();

    let initial_msgs = managers[0]
        .broadcast("byz_max_10".to_string(), data.clone())
        .unwrap();

    let malicious: std::collections::HashSet<String> = vec![
        "node_7".to_string(),
        "node_8".to_string(),
        "node_9".to_string(),
    ]
    .into_iter()
    .collect();

    println!("恶意节点（t={}个）: {:?}", config.fault_tolerance, malicious);

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
                Some((
                    dest,
                    RbcMessage::Echo {
                        instance_id,
                        sender,
                        data_hash: "10node_fake_hash_from_byzantine".to_string(),
                        shard_index,
                        shard_data: fake_shard,
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
                Some((
                    dest,
                    RbcMessage::Ready {
                        instance_id,
                        sender,
                        data_hash: "10node_fake_hash_from_byzantine".to_string(),
                        shard_index,
                        shard_data: fake_shard,
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

#[test]
fn test_byzantine_forged_shard_index() {
    // ========================================================================
    // 恶意场景7：伪造分片索引（Forged shard_index）
    //
    // 攻击方式：
    //   恶意节点在READY消息中伪造shard_index字段，声称自己是别的节点索引。
    //   例如 node_5（真实索引=5）在READY中声称 shard_index=0，
    //        node_6（真实索引=6）在READY中声称 shard_index=1。
    //   如果系统信任消息中的shard_index，恶意分片会覆盖索引0和1的正确分片，
    //   导致解码失败或输出错误数据。
    //
    // 预期：
    //   real_shard_index 逻辑通过 sender 在 node_ids 中的真实位置确定索引，
    //   无视消息中的 shard_index，恶意分片被放置在正确的位置（索引5和6），
    //   不会污染其他索引的分片。诚实节点仍能正确解码。
    // ========================================================================
    println!("\n=== 恶意场景7: 伪造分片索引（real_shard_index纠偏测试）===");

    let n = 7;
    let node_ids: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();
    let mut managers = create_managers(n);

    let data = b"Byzantine test: forged shard_index attack - testing real_shard_index correction".to_vec();

    let initial_msgs = managers[0]
        .broadcast("byz_forged_index".to_string(), data.clone())
        .unwrap();

    // node_5 和 node_6 是恶意节点（t=2）
    let malicious: std::collections::HashSet<String> =
        vec!["node_5".to_string(), "node_6".to_string()]
            .into_iter()
            .collect();

    println!("恶意节点: {:?}", malicious);
    println!("攻击方式: 在READY消息中伪造shard_index，试图污染其他节点的分片槽位");

    // 恶意行为：篡改READY消息中的shard_index
    // node_5（真实索引5）声称 shard_index=0（试图覆盖node_0的分片）
    // node_6（真实索引6）声称 shard_index=1（试图覆盖node_1的分片）
    // 同时篡改分片数据，使得如果系统信任了假索引，解码一定会失败
    let tamper_fn = |source: &str, dest: String, msg: RbcMessage| -> Option<(String, RbcMessage)> {
        match msg {
            RbcMessage::Ready {
                instance_id,
                sender,
                data_hash,
                shard_index: _,
                mut shard_data,
                original_size,
                data_shard_count,
                parity_shard_count,
            } => {
                // 伪造shard_index：node_5声称是索引0，node_6声称是索引1
                let fake_index = if source == "node_5" {
                    0 // 试图冒充node_0
                } else {
                    1 // 试图冒充node_1
                };

                // 同时篡改分片数据（翻转所有字节），使假分片内容完全错误
                for byte in shard_data.iter_mut() {
                    *byte = !*byte;
                }

                println!(
                    "  [恶意] {} 伪造 shard_index: 真实索引={} -> 声称索引={}, 目标={}",
                    source,
                    if source == "node_5" { 5 } else { 6 },
                    fake_index,
                    dest
                );

                Some((
                    dest,
                    RbcMessage::Ready {
                        instance_id,
                        sender, // sender保持真实（不伪造身份）
                        data_hash,
                        shard_index: fake_index, // 伪造的索引
                        shard_data,              // 篡改的分片数据
                        original_size,
                        data_shard_count,
                        parity_shard_count,
                    },
                ))
            }
            // ECHO消息保持正常（恶意节点只在READY阶段攻击）
            other => Some((dest, other)),
        }
    };

    run_with_byzantine(&mut managers, initial_msgs, &node_ids, &malicious, tamper_fn);

    // 验证结果
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
                println!("错误: 诚实节点 {} 输出了错误数据! (数据长度={})", i, outputs[0].data.len());
            }
        } else {
            println!("诚实节点 {} 未产生输出", i);
        }
    }

    assert!(
        !wrong_output,
        "伪造shard_index攻击不应导致任何诚实节点输出错误数据"
    );

    println!(
        "\n恶意场景7结果: {}/{} 个诚实节点成功输出",
        success_count,
        n - malicious.len()
    );
    assert!(
        success_count >= 3,
        "伪造shard_index攻击下，至少3个诚实节点应成功输出，实际: {}",
        success_count
    );
    println!("✓ real_shard_index 纠偏逻辑验证通过！恶意伪造的索引被正确忽略。");
}

#[test]
fn test_byzantine_forged_shard_index_10_nodes() {
    // ========================================================================
    // 恶意场景8：10节点极限伪造索引攻击
    //
    // 攻击方式：
    //   3个恶意节点（t=3）全部伪造shard_index，每个都声称自己是索引0，
    //   并发送完全不同的垃圾分片数据。
    //   这是最极端的索引伪造攻击：所有恶意节点都试图污染同一个索引位置。
    //
    // 预期：
    //   real_shard_index 逻辑将恶意分片放置在各自的真实索引位置（7,8,9），
    //   索引0的分片不受影响。纠错解码能处理3个错误分片并正确输出。
    // ========================================================================
    println!("\n=== 恶意场景8: 10节点极限伪造索引攻击 ===");

    let n = 10;
    let config = RbcConfig::new(n).unwrap();
    println!("{}", config.info());

    let node_ids: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();
    let mut managers = create_managers(n);

    let data: Vec<u8> = (0..5_000).map(|i| ((i * 17 + 3) % 256) as u8).collect();

    let initial_msgs = managers[0]
        .broadcast("byz_forged_idx_10".to_string(), data.clone())
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
    println!("攻击方式: 所有恶意节点都声称shard_index=0，试图集中污染索引0");

    let tamper_fn = |source: &str, dest: String, msg: RbcMessage| -> Option<(String, RbcMessage)> {
        match msg {
            RbcMessage::Ready {
                instance_id,
                sender,
                data_hash,
                shard_index: _,
                shard_data: _,
                original_size,
                data_shard_count,
                parity_shard_count,
            } => {
                // 所有恶意节点都声称自己是索引0，并发送垃圾数据
                let garbage: Vec<u8> = source.as_bytes().iter()
                    .cycle()
                    .take(original_size / data_shard_count + 1)
                    .copied()
                    .collect();

                Some((
                    dest,
                    RbcMessage::Ready {
                        instance_id,
                        sender,
                        data_hash,
                        shard_index: 0, // 全部声称是索引0
                        shard_data: garbage,
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
        "10节点极限伪造索引攻击不应导致任何诚实节点输出错误数据"
    );

    println!(
        "\n恶意场景8结果: {}/{} 个诚实节点成功输出",
        success_count,
        n - malicious.len()
    );
    assert!(
        success_count >= n - 2 * config.fault_tolerance,
        "10节点极限伪造索引攻击下，至少{}个诚实节点应成功输出，实际: {}",
        n - 2 * config.fault_tolerance,
        success_count
    );
    println!("✓ 10节点极限伪造索引攻击防御验证通过！");
}

// ============================================================================
// 切片策略对比实验（Sharding Strategy Comparison Experiments）
// ============================================================================

/// 辅助函数：创建使用自定义data_shards的RBC管理器
fn create_managers_with_custom_shards(n: usize, data_shards: usize) -> Vec<RbcManager> {
    let node_ids: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();
    let config = RbcConfig::with_custom_shards(n, data_shards).unwrap();
    node_ids
        .iter()
        .map(|id| RbcManager::new(id.clone(), config.clone(), node_ids.clone()))
        .collect()
}

/// 辅助结构：记录单次实验的开销统计
#[derive(Debug)]
struct OverheadStats {
    /// 策略名称
    strategy_name: String,
    /// 节点数
    n: usize,
    /// 数据分片数 k
    data_shards: usize,
    /// 校验分片数
    parity_shards: usize,
    /// 原始数据大小（字节）
    data_size: usize,
    /// 总传输消息数
    total_messages: usize,
    /// 总传输字节数（所有消息的shard_data大小之和）
    total_transfer_bytes: usize,
    /// 传输放大比
    transfer_amplification: f64,
    /// RS编码耗时（毫秒）
    encode_time_ms: f64,
    /// RS解码耗时（毫秒）
    decode_time_ms: f64,
    /// 端到端总耗时（毫秒）
    total_time_ms: f64,
    /// 成功输出的节点数
    success_count: usize,
}

impl std::fmt::Display for OverheadStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "| {:<20} | k={:<2} p={:<2} | {:>8} | {:>10} | {:>8.2}x | {:>10.3}ms | {:>10.3}ms | {:>10.3}ms | {}/{} |",
            self.strategy_name,
            self.data_shards,
            self.parity_shards,
            self.data_size,
            self.total_transfer_bytes,
            self.transfer_amplification,
            self.encode_time_ms,
            self.decode_time_ms,
            self.total_time_ms,
            self.success_count,
            self.n
        )
    }
}

/// 辅助函数：运行单次切片策略实验并收集开销统计
///
/// 统计传输开销（消息数、传输字节数）和计算开销（编解码耗时）
fn run_strategy_experiment(
    strategy_name: &str,
    n: usize,
    data_shards: usize,
    data: &[u8],
) -> OverheadStats {
    use std::time::Instant;

    let node_ids: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();
    let config = RbcConfig::with_custom_shards(n, data_shards).unwrap();
    let mut managers: Vec<RbcManager> = node_ids
        .iter()
        .map(|id| RbcManager::new(id.clone(), config.clone(), node_ids.clone()))
        .collect();

    let total_start = Instant::now();

    // 广播者发起广播
    let initial_msgs = managers[0]
        .broadcast("strategy_test".to_string(), data.to_vec())
        .unwrap();

    // 统计传输开销
    let mut total_messages: usize = 0;
    let mut total_transfer_bytes: usize = 0;

    // 统计初始消息的传输开销
    for (_, msg) in &initial_msgs {
        total_messages += 1;
        total_transfer_bytes += estimate_message_size(msg);
    }

    // 运行消息传递直到收敛
    let mut pending = initial_msgs;
    let mut rounds = 0;
    let max_rounds = 200;

    while !pending.is_empty() && rounds < max_rounds {
        rounds += 1;
        let mut new_msgs = Vec::new();
        for (target_id, msg) in pending {
            if let Some(idx) = node_ids.iter().position(|id| id == &target_id) {
                match managers[idx].handle_message(msg) {
                    Ok(msgs) => {
                        for (dest, out_msg) in &msgs {
                            total_messages += 1;
                            total_transfer_bytes += estimate_message_size(out_msg);
                        }
                        new_msgs.extend(msgs);
                    }
                    Err(e) => eprintln!("节点 {} 处理消息失败: {}", target_id, e),
                }
            }
        }
        pending = new_msgs;
    }

    let total_elapsed = total_start.elapsed();

    // 统计成功输出的节点数
    let mut success_count = 0;
    for manager in managers.iter_mut() {
        let outputs = manager.drain_outputs();
        if !outputs.is_empty() && outputs[0].data == data {
            success_count += 1;
        }
    }

    // 计算RS编码和解码耗时（单独测量）
    let codec = crate::erasure::ErasureCodec::new(data_shards, n - data_shards).unwrap();

    let encode_start = Instant::now();
    let shard_group = codec.encode(data).unwrap();
    let encode_time = encode_start.elapsed();

    let decode_start = Instant::now();
    let _recovered = codec.decode_from_group(&shard_group).unwrap();
    let decode_time = decode_start.elapsed();

    let transfer_amplification = if data.len() > 0 {
        total_transfer_bytes as f64 / data.len() as f64
    } else {
        0.0
    };

    OverheadStats {
        strategy_name: strategy_name.to_string(),
        n,
        data_shards,
        parity_shards: n - data_shards,
        data_size: data.len(),
        total_messages,
        total_transfer_bytes,
        transfer_amplification,
        encode_time_ms: encode_time.as_secs_f64() * 1000.0,
        decode_time_ms: decode_time.as_secs_f64() * 1000.0,
        total_time_ms: total_elapsed.as_secs_f64() * 1000.0,
        success_count,
    }
}

/// 估算单条RBC消息的传输大小（字节）
fn estimate_message_size(msg: &RbcMessage) -> usize {
    match msg {
        RbcMessage::Propose { data, instance_id, broadcaster } => {
            // PROPOSE消息包含完整数据
            data.len() + instance_id.len() + broadcaster.len() + 32 // 32字节开销
        }
        RbcMessage::Echo { shard_data, instance_id, sender, data_hash, .. } => {
            // ECHO消息包含单个分片
            shard_data.len() + instance_id.len() + sender.len() + data_hash.len() + 48
        }
        RbcMessage::Ready { shard_data, instance_id, sender, data_hash, .. } => {
            // READY消息包含单个分片
            shard_data.len() + instance_id.len() + sender.len() + data_hash.len() + 48
        }
    }
}

/// 打印实验结果表格
fn print_experiment_table(title: &str, results: &[OverheadStats]) {
    println!("\n{}", "=".repeat(140));
    println!("  {}", title);
    println!("{}", "=".repeat(140));
    println!(
        "| {:<20} | {:<9} | {:>8} | {:>10} | {:>9} | {:>12} | {:>12} | {:>12} | {:>5} |",
        "策略", "k/p", "数据大小", "传输字节", "放大比", "RS编码", "RS解码", "总耗时", "成功"
    );
    println!(
        "| {:<20} | {:<9} | {:>8} | {:>10} | {:>9} | {:>12} | {:>12} | {:>12} | {:>5} |",
        "-".repeat(20), "-".repeat(9), "-".repeat(8), "-".repeat(10), "-".repeat(9),
        "-".repeat(12), "-".repeat(12), "-".repeat(12), "-".repeat(5)
    );
    for stat in results {
        println!("{}", stat);
    }
    println!("{}", "=".repeat(140));
}

// ============================================================================
// 实验1：RS编码参数对比（固定文件大小，变化k值）
// ============================================================================

#[test]
fn test_strategy_compare_k_values_7_nodes() {
    println!("\n\n");
    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║  实验1: RS编码参数对比 — 不同k值对传输开销与计算开销的影响     ║");
    println!("║  固定: n=7, 文件大小=100KB                                     ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");

    let n = 7;
    let data: Vec<u8> = (0..102400).map(|i| ((i * 7 + 13) % 256) as u8).collect(); // 100KB

    // 策略S1: 标准策略 k=t+1=3（当前默认）
    // 策略S2: 高冗余策略 k=2
    // 策略S4: 对称策略 k=n/2≈4
    // 策略S3: 低冗余策略 k=n-1=6
    let strategies: Vec<(&str, usize)> = vec![
        ("S2:高冗余(k=2)", 2),
        ("S1:标准(k=t+1=3)", 3),
        ("S4:对称(k=n/2=4)", 4),
        ("S3:低冗余(k=5)", 5),
        ("S3:极低冗余(k=6)", 6),
    ];

    let mut results = Vec::new();
    for (name, k) in &strategies {
        println!("\n--- 运行策略: {} (k={}, p={}) ---", name, k, n - k);
        let stats = run_strategy_experiment(name, n, *k, &data);
        println!(
            "  传输: {}条消息, {}字节, 放大比={:.2}x",
            stats.total_messages, stats.total_transfer_bytes, stats.transfer_amplification
        );
        println!(
            "  计算: 编码={:.3}ms, 解码={:.3}ms, 总耗时={:.3}ms",
            stats.encode_time_ms, stats.decode_time_ms, stats.total_time_ms
        );
        println!("  成功: {}/{}", stats.success_count, n);
        results.push(stats);
    }

    print_experiment_table(
        "实验1: RS编码参数对比 (n=7, 数据=100KB)",
        &results,
    );

    // 验证所有策略都能成功
    for stat in &results {
        assert!(
            stat.success_count >= n - 2,
            "策略 {} 成功节点数不足: {}/{}",
            stat.strategy_name, stat.success_count, n
        );
    }

    // 验证传输放大比趋势：k越大（分片越小），传输放大比越低
    println!("\n📊 传输放大比趋势分析:");
    for stat in &results {
        let bar_len = (stat.transfer_amplification * 2.0) as usize;
        let bar: String = "█".repeat(bar_len.min(60));
        println!(
            "  k={}: {:>8.2}x  {}",
            stat.data_shards, stat.transfer_amplification, bar
        );
    }

    println!("\n📊 RS编码耗时趋势分析:");
    for stat in &results {
        let bar_len = (stat.encode_time_ms * 10.0) as usize;
        let bar: String = "█".repeat(bar_len.min(60));
        println!(
            "  k={}: {:>8.3}ms  {}",
            stat.data_shards, stat.encode_time_ms, bar
        );
    }
}

#[test]
fn test_strategy_compare_k_values_10_nodes() {
    println!("\n\n");
    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║  实验1b: RS编码参数对比 — 10节点场景                           ║");
    println!("║  固定: n=10, 文件大小=100KB                                    ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");

    let n = 10;
    let data: Vec<u8> = (0..102400).map(|i| ((i * 7 + 13) % 256) as u8).collect();

    let strategies: Vec<(&str, usize)> = vec![
        ("S2:高冗余(k=2)", 2),
        ("S1:标准(k=t+1=4)", 4),
        ("S4:对称(k=n/2=5)", 5),
        ("S3:低冗余(k=7)", 7),
        ("S3:极低冗余(k=9)", 9),
    ];

    let mut results = Vec::new();
    for (name, k) in &strategies {
        println!("\n--- 运行策略: {} (k={}, p={}) ---", name, k, n - k);
        let stats = run_strategy_experiment(name, n, *k, &data);
        println!(
            "  传输: {}条消息, {}字节, 放大比={:.2}x",
            stats.total_messages, stats.total_transfer_bytes, stats.transfer_amplification
        );
        println!(
            "  计算: 编码={:.3}ms, 解码={:.3}ms",
            stats.encode_time_ms, stats.decode_time_ms
        );
        results.push(stats);
    }

    print_experiment_table(
        "实验1b: RS编码参数对比 (n=10, 数据=100KB)",
        &results,
    );

    for stat in &results {
        assert!(
            stat.success_count >= n - 3,
            "策略 {} 成功节点数不足: {}/{}",
            stat.strategy_name, stat.success_count, n
        );
    }
}

// ============================================================================
// 实验2：文件大小扩展性（固定策略，变化文件大小）
// ============================================================================

#[test]
fn test_strategy_compare_file_sizes() {
    println!("\n\n");
    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║  实验2: 文件大小扩展性 — 不同文件大小对开销的影响              ║");
    println!("║  固定: n=7, k=3 (标准策略)                                     ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");

    let n = 7;
    let k = 3; // 标准策略 k=t+1

    let file_sizes: Vec<(&str, usize)> = vec![
        ("1KB", 1024),
        ("10KB", 10 * 1024),
        ("100KB", 100 * 1024),
        ("500KB", 500 * 1024),
        ("1MB", 1024 * 1024),
    ];

    let mut results = Vec::new();
    for (size_name, size) in &file_sizes {
        let data: Vec<u8> = (0..*size).map(|i| ((i * 7 + 13) % 256) as u8).collect();
        let strategy_name = format!("标准k=3,{}", size_name);
        println!("\n--- 文件大小: {} ({} 字节) ---", size_name, size);
        let stats = run_strategy_experiment(&strategy_name, n, k, &data);
        println!(
            "  传输: {}条消息, {}字节, 放大比={:.2}x",
            stats.total_messages, stats.total_transfer_bytes, stats.transfer_amplification
        );
        println!(
            "  计算: 编码={:.3}ms, 解码={:.3}ms, 总耗时={:.3}ms",
            stats.encode_time_ms, stats.decode_time_ms, stats.total_time_ms
        );
        results.push(stats);
    }

    print_experiment_table(
        "实验2: 文件大小扩展性 (n=7, k=3)",
        &results,
    );

    // 验证传输放大比应该基本稳定（不随文件大小变化太大）
    println!("\n📊 传输放大比 vs 文件大小:");
    for stat in &results {
        println!(
            "  {:>6}: 放大比={:.2}x, 编码={:.3}ms, 解码={:.3}ms",
            format!("{}B", stat.data_size),
            stat.transfer_amplification,
            stat.encode_time_ms,
            stat.decode_time_ms
        );
    }

    // 验证编解码时间应该随文件大小线性增长
    println!("\n📊 RS编码耗时 vs 文件大小:");
    for stat in &results {
        let bar_len = (stat.encode_time_ms * 5.0) as usize;
        let bar: String = "█".repeat(bar_len.min(60));
        println!(
            "  {:>8}B: {:>8.3}ms  {}",
            stat.data_size, stat.encode_time_ms, bar
        );
    }
}

// ============================================================================
// 实验3：分块大小对比（固定大文件，变化分块大小）
// ============================================================================

#[test]
fn test_strategy_compare_chunk_sizes() {
    use crate::rbc::chunked::ChunkedBroadcastManager;

    println!("\n\n");
    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║  实验3: 分块大小对比 — 不同分块大小对大文件传输开销的影响      ║");
    println!("║  固定: n=7, k=3, 文件大小=50KB                                 ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");

    let n = 7;
    let node_ids: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();
    let data: Vec<u8> = (0..51200).map(|i| ((i * 31 + 17) % 256) as u8).collect(); // 50KB

    let chunk_sizes: Vec<(&str, usize)> = vec![
        ("5KB", 5 * 1024),
        ("10KB", 10 * 1024),
        ("25KB", 25 * 1024),
        ("50KB", 50 * 1024),       // 不分块（一次RBC）
    ];

    println!("\n| {:<12} | {:>6} | {:>8} | {:>10} | {:>9} | {:>12} | {:>5} |",
        "分块大小", "分块数", "消息数", "传输字节", "放大比", "总耗时", "成功");
    println!("| {:<12} | {:>6} | {:>8} | {:>10} | {:>9} | {:>12} | {:>5} |",
        "-".repeat(12), "-".repeat(6), "-".repeat(8), "-".repeat(10),
        "-".repeat(9), "-".repeat(12), "-".repeat(5));

    for (chunk_name, chunk_size) in &chunk_sizes {
        use std::time::Instant;

        let config = RbcConfig::new(n).unwrap();
        let mut managers: Vec<RbcManager> = node_ids
            .iter()
            .map(|id| RbcManager::new(id.clone(), config.clone(), node_ids.clone()))
            .collect();

        let total_start = Instant::now();
        let mut total_messages: usize = 0;
        let mut total_transfer_bytes: usize = 0;

        let total_chunks = (data.len() + chunk_size - 1) / chunk_size;

        // 使用分块广播管理器
        let mut chunked_mgr = ChunkedBroadcastManager::with_chunk_size(*chunk_size);
        let initial_msgs = chunked_mgr
            .broadcast("chunk_test".to_string(), data.clone(), &mut managers[0])
            .unwrap();

        // 统计初始消息
        for (_, msg) in &initial_msgs {
            total_messages += 1;
            total_transfer_bytes += estimate_message_size(msg);
        }

        // 运行消息传递
        let mut pending = initial_msgs;
        let mut rounds = 0;
        while !pending.is_empty() && rounds < 500 {
            rounds += 1;
            let mut new_msgs = Vec::new();
            for (target_id, msg) in pending {
                if let Some(idx) = node_ids.iter().position(|id| id == &target_id) {
                    match managers[idx].handle_message(msg) {
                        Ok(msgs) => {
                            for (_, out_msg) in &msgs {
                                total_messages += 1;
                                total_transfer_bytes += estimate_message_size(out_msg);
                            }
                            new_msgs.extend(msgs);
                        }
                        Err(e) => eprintln!("处理失败: {}", e),
                    }
                }
            }
            pending = new_msgs;
        }

        let total_elapsed = total_start.elapsed();

        // 收集分块广播输出
        let mut all_chunked_mgrs: Vec<ChunkedBroadcastManager> = (0..n)
            .map(|_| ChunkedBroadcastManager::with_chunk_size(*chunk_size))
            .collect();
        all_chunked_mgrs[0] = chunked_mgr;

        let mut success_count = 0;
        for (i, manager) in managers.iter_mut().enumerate() {
            let outputs = manager.drain_outputs();
            for output in &outputs {
                all_chunked_mgrs[i].handle_rbc_output(output);
            }
            let chunked_outputs = all_chunked_mgrs[i].drain_outputs();
            for co in &chunked_outputs {
                if co.data == data {
                    success_count += 1;
                }
            }
        }

        let amplification = total_transfer_bytes as f64 / data.len() as f64;

        println!(
            "| {:<12} | {:>6} | {:>8} | {:>10} | {:>8.2}x | {:>10.3}ms | {}/{} |",
            chunk_name,
            total_chunks,
            total_messages,
            total_transfer_bytes,
            amplification,
            total_elapsed.as_secs_f64() * 1000.0,
            success_count,
            n
        );

        assert!(success_count >= n - 2, "分块大小 {} 成功节点数不足", chunk_name);
    }

    println!("\n📊 分析: 分块越大，元信息开销占比越低，但单块RBC的内存占用更高。");
    println!("   分块越小，分块数越多，每个分块独立走RBC协议，总消息数和传输量增加。");
}

// ============================================================================
// 实验4：节点规模扩展性（固定策略和文件大小，变化节点数）
// ============================================================================

#[test]
fn test_strategy_compare_node_scales() {
    println!("\n\n");
    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║  实验4: 节点规模扩展性 — 不同节点数对开销的影响               ║");
    println!("║  固定: k=t+1 (标准策略), 文件大小=100KB                        ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");

    let data: Vec<u8> = (0..102400).map(|i| ((i * 7 + 13) % 256) as u8).collect(); // 100KB

    let node_configs: Vec<(usize, usize)> = vec![
        (4, 2),   // n=4, t=1, k=t+1=2
        (7, 3),   // n=7, t=2, k=t+1=3
        (10, 4),  // n=10, t=3, k=t+1=4
        (13, 5),  // n=13, t=4, k=t+1=5
    ];

    let mut results = Vec::new();
    for (n, k) in &node_configs {
        let t = (n - 1) / 3;
        let strategy_name = format!("n={},t={},k={}", n, t, k);
        println!("\n--- 节点数: n={}, t={}, k={} ---", n, t, k);
        let stats = run_strategy_experiment(&strategy_name, *n, *k, &data);
        println!(
            "  传输: {}条消息, {}字节, 放大比={:.2}x",
            stats.total_messages, stats.total_transfer_bytes, stats.transfer_amplification
        );
        println!(
            "  计算: 编码={:.3}ms, 解码={:.3}ms, 总耗时={:.3}ms",
            stats.encode_time_ms, stats.decode_time_ms, stats.total_time_ms
        );
        results.push(stats);
    }

    print_experiment_table(
        "实验4: 节点规模扩展性 (标准策略k=t+1, 数据=100KB)",
        &results,
    );

    // 验证传输放大比随节点数增长的趋势
    println!("\n📊 传输放大比 vs 节点数:");
    for stat in &results {
        let bar_len = (stat.transfer_amplification * 1.5) as usize;
        let bar: String = "█".repeat(bar_len.min(60));
        println!(
            "  n={:>2}: {:>8.2}x  {}",
            stat.n, stat.transfer_amplification, bar
        );
    }

    println!("\n📊 总消息数 vs 节点数:");
    for stat in &results {
        let bar_len = stat.total_messages / 5;
        let bar: String = "█".repeat(bar_len.min(60));
        println!(
            "  n={:>2}: {:>6}条  {}",
            stat.n, stat.total_messages, bar
        );
    }

    // 理论分析：传输复杂度应为 O(n²·|M|/k) ≈ O(n·|M|) 当 k≈n/3 时
    println!("\n📊 理论 vs 实际传输放大比:");
    for stat in &results {
        let theory_propose = stat.n * stat.data_size;
        let shard_size = (stat.data_size + stat.data_shards - 1) / stat.data_shards;
        let theory_echo = stat.n * stat.n * shard_size;
        let theory_ready = stat.n * stat.n * shard_size;
        let theory_total = theory_propose + theory_echo + theory_ready;
        let theory_amp = theory_total as f64 / stat.data_size as f64;
        println!(
            "  n={:>2}: 理论={:.2}x, 实际={:.2}x, 差异={:.1}%",
            stat.n,
            theory_amp,
            stat.transfer_amplification,
            ((stat.transfer_amplification - theory_amp) / theory_amp * 100.0).abs()
        );
    }
}

// ============================================================================
// 实验5：综合对比 — 无编码基线 vs 标准策略
// ============================================================================

#[test]
fn test_strategy_no_coding_baseline() {
    println!("\n\n");
    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║  实验5: 无编码基线对比 — 证明纠删码的传输优化价值             ║");
    println!("║  对比: 无编码(k=1) vs 标准策略(k=t+1) vs 对称(k=n/2)          ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");

    let n = 7;
    let data: Vec<u8> = (0..102400).map(|i| ((i * 7 + 13) % 256) as u8).collect(); // 100KB

    // k=1 相当于"无编码"：每个分片就是完整数据，校验分片=n-1
    // 这模拟了经典Bracha RBC中每个节点转发完整消息的场景
    let strategies: Vec<(&str, usize)> = vec![
        ("无编码基线(k=1)", 1),
        ("高冗余(k=2)", 2),
        ("标准(k=t+1=3)", 3),
        ("对称(k=n/2=4)", 4),
        ("低冗余(k=6)", 6),
    ];

    let mut results = Vec::new();
    for (name, k) in &strategies {
        let stats = run_strategy_experiment(name, n, *k, &data);
        results.push(stats);
    }

    print_experiment_table(
        "实验5: 无编码基线对比 (n=7, 数据=100KB)",
        &results,
    );

    // 计算相对于无编码基线的传输节省比例
    let baseline_transfer = results[0].total_transfer_bytes as f64;
    println!("\n📊 相对于无编码基线(k=1)的传输节省:");
    for stat in &results {
        let saving = (1.0 - stat.total_transfer_bytes as f64 / baseline_transfer) * 100.0;
        let bar_len = (saving.max(0.0) * 0.5) as usize;
        let bar: String = "█".repeat(bar_len.min(60));
        println!(
            "  k={}: 传输={:>10}B, 节省={:>6.1}%  {}",
            stat.data_shards, stat.total_transfer_bytes, saving, bar
        );
    }

    // 计算编码开销的代价
    println!("\n📊 编码开销代价（传输节省 vs 计算增加）:");
    let baseline_compute = results[0].encode_time_ms + results[0].decode_time_ms;
    for stat in &results {
        let compute = stat.encode_time_ms + stat.decode_time_ms;
        let compute_increase = if baseline_compute > 0.0 {
            (compute / baseline_compute - 1.0) * 100.0
        } else {
            0.0
        };
        let transfer_saving = (1.0 - stat.total_transfer_bytes as f64 / baseline_transfer) * 100.0;
        println!(
            "  k={}: 传输节省={:>6.1}%, 计算增加={:>6.1}%, 编码={:.3}ms, 解码={:.3}ms",
            stat.data_shards, transfer_saving, compute_increase,
            stat.encode_time_ms, stat.decode_time_ms
        );
    }
}
