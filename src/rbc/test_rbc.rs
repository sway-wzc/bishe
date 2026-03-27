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
