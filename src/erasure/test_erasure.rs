use crate::erasure::{ErasureCodec, ShardGroup};

#[test]
fn test_basic_encode_decode() {
    let codec = ErasureCodec::new(4, 4).unwrap();
    let data = b"Hello, Erasure Coding! This is a test for Reed-Solomon encoding.";

    // 编码
    let group = codec.encode(data).unwrap();
    assert_eq!(group.shards.len(), 8);
    assert_eq!(group.data_shard_count, 4);
    assert_eq!(group.parity_shard_count, 4);
    assert_eq!(group.original_size, data.len());

    // 完整解码
    let recovered = codec.decode_from_group(&group).unwrap();
    assert_eq!(recovered, data.to_vec());
}

#[test]
fn test_recover_with_lost_data_shards() {
    let codec = ErasureCodec::new(4, 4).unwrap();
    let data = b"Testing recovery after losing data shards!";

    let group = codec.encode(data).unwrap();

    // 丢失2个数据分片（索引0和1）
    let recovered = codec.simulate_loss_and_recover(&group, &[0, 1]).unwrap();
    assert_eq!(recovered, data.to_vec());
}

#[test]
fn test_recover_with_lost_parity_shards() {
    let codec = ErasureCodec::new(4, 4).unwrap();
    let data = b"Testing recovery after losing parity shards!";

    let group = codec.encode(data).unwrap();

    // 丢失2个校验分片（索引4和5）
    let recovered = codec.simulate_loss_and_recover(&group, &[4, 5]).unwrap();
    assert_eq!(recovered, data.to_vec());
}

#[test]
fn test_recover_with_mixed_loss() {
    let codec = ErasureCodec::new(4, 4).unwrap();
    let data = b"Testing recovery with mixed shard loss!";

    let group = codec.encode(data).unwrap();

    // 丢失1个数据分片 + 1个校验分片
    let recovered = codec.simulate_loss_and_recover(&group, &[2, 5]).unwrap();
    assert_eq!(recovered, data.to_vec());
}

#[test]
fn test_too_many_lost_shards() {
    let codec = ErasureCodec::new(4, 4).unwrap();
    let data = b"This should fail with too many lost shards";

    let group = codec.encode(data).unwrap();

    // 丢失5个分片（超过容错能力4）
    let result = codec.simulate_loss_and_recover(&group, &[0, 1, 2, 3, 4]);
    assert!(result.is_err());
}

#[test]
fn test_small_data() {
    let codec = ErasureCodec::new(2, 2).unwrap();
    let data = b"Hi";

    let group = codec.encode(data).unwrap();
    let recovered = codec.decode_from_group(&group).unwrap();
    assert_eq!(recovered, data.to_vec());
}

#[test]
fn test_large_data() {
    let codec = ErasureCodec::new(6, 3).unwrap();
    // 生成1MB的测试数据
    let data: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();

    let group = codec.encode(&data).unwrap();
    assert_eq!(group.shards.len(), 9);

    // 丢失3个分片后恢复
    let recovered = codec.simulate_loss_and_recover(&group, &[0, 4, 8]).unwrap();
    assert_eq!(recovered, data);
}

#[test]
fn test_shard_integrity_verification() {
    let codec = ErasureCodec::new(4, 4).unwrap();
    let data = b"Integrity check test data";

    let group = codec.encode(data).unwrap();

    // 验证所有分片完整性
    let results = group.verify_all();
    for (id, valid) in &results {
        assert!(valid, "分片 {} 完整性校验失败", id);
    }
}

#[test]
fn test_verify_roundtrip() {
    let codec = ErasureCodec::new(4, 4).unwrap();
    let data = b"Roundtrip verification test";
    assert!(codec.verify_roundtrip(data).unwrap());
}

#[test]
fn test_storage_overhead() {
    let codec = ErasureCodec::new(4, 4).unwrap();
    let ratio = codec.storage_overhead_ratio();
    assert!((ratio - 2.0).abs() < f64::EPSILON);
}

#[test]
fn test_fault_tolerance_info() {
    let codec = ErasureCodec::new(4, 4).unwrap();
    let info = codec.fault_tolerance_info();
    assert!(info.contains("4"));
    assert!(info.contains("8"));
    assert!(info.contains("Berlekamp-Welch"));
}

#[test]
fn test_shard_serialization() {
    let codec = ErasureCodec::new(3, 3).unwrap();
    let data = b"Serialization test for network transfer";

    let group = codec.encode(data).unwrap();

    // 测试单个分片的序列化/反序列化
    for shard in &group.shards {
        let serialized = serde_json::to_string(shard).unwrap();
        let deserialized: crate::erasure::DataShard =
            serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.data, shard.data);
        assert_eq!(deserialized.id.index, shard.id.index);
        assert!(deserialized.verify());
    }

    // 测试整个分片组的序列化/反序列化
    let group_json = serde_json::to_string(&group).unwrap();
    let recovered_group: ShardGroup = serde_json::from_str(&group_json).unwrap();
    let recovered = codec.decode_from_group(&recovered_group).unwrap();
    assert_eq!(recovered, data.to_vec());
}

#[test]
fn test_empty_data_error() {
    let codec = ErasureCodec::new(4, 4).unwrap();
    let result = codec.encode(b"");
    assert!(result.is_err());
}

#[test]
fn test_invalid_codec_params() {
    assert!(ErasureCodec::new(0, 2).is_err());
    assert!(ErasureCodec::new(4, 0).is_err());
    assert!(ErasureCodec::new(200, 100).is_err()); // 超过256
}

#[test]
fn test_different_codec_configurations() {
    let configs = vec![
        (2, 2),  // 最小配置
        (3, 3),  // 对称配置
        (4, 4),  // 常用配置
        (6, 3),  // 高数据比
        (10, 4), // 大规模
    ];

    let data = b"Testing various erasure coding configurations for robustness!";

    for (d, p) in configs {
        let codec = ErasureCodec::new(d, p).unwrap();
        let group = codec.encode(data).unwrap();

        // 完整恢复
        let recovered = codec.decode_from_group(&group).unwrap();
        assert_eq!(recovered, data.to_vec(), "配置({},{})完整恢复失败", d, p);

        // 丢失 p-1 个分片后恢复（确保剩余分片数 > k，可走纠错解码路径）
        if p > 1 {
            let lost: Vec<usize> = (0..p - 1).collect();
            let recovered = codec.simulate_loss_and_recover(&group, &lost).unwrap();
            assert_eq!(recovered, data.to_vec(), "配置({},{})容错恢复失败", d, p);
        }

        // 丢失全部 p 个校验分片后恢复（剩余的全是数据分片，直接拼接）
        let lost_parity: Vec<usize> = (d..d + p).collect();
        let recovered = codec.simulate_loss_and_recover(&group, &lost_parity).unwrap();
        assert_eq!(recovered, data.to_vec(), "配置({},{})丢失全部校验分片恢复失败", d, p);
    }
}

// ============================================================================
// 纠错能力测试（Berlekamp-Welch 算法）
// ============================================================================

#[test]
fn test_error_correction_single_corruption() {
    // 测试单个分片损坏的纠错恢复
    // 配置: k=4, n=8, 纠错能力 = ⌊4/2⌋ = 2
    let codec = ErasureCodec::new(4, 4).unwrap();
    assert_eq!(codec.error_correction_capacity(), 2);

    let data = b"Error correction test: single corruption";
    let group = codec.encode(data).unwrap();

    // 损坏1个分片（在纠错能力范围内）
    let recovered = codec
        .simulate_corruption_and_recover(&group, &[0], 0xFF)
        .unwrap();
    assert_eq!(recovered, data.to_vec());
}

#[test]
fn test_error_correction_two_corruptions() {
    // 测试2个分片损坏的纠错恢复
    let codec = ErasureCodec::new(4, 4).unwrap();

    let data = b"Error correction test: two corruptions";
    let group = codec.encode(data).unwrap();

    // 损坏2个分片（纠错能力极限）
    let recovered = codec
        .simulate_corruption_and_recover(&group, &[0, 1], 0xAB)
        .unwrap();
    assert_eq!(recovered, data.to_vec());
}

#[test]
fn test_error_correction_exceeds_capacity() {
    // 测试超过纠错能力的损坏（应该失败或返回错误数据）
    let codec = ErasureCodec::new(4, 4).unwrap();

    let data = b"Error correction test: too many corruptions";
    let group = codec.encode(data).unwrap();

    // 损坏3个分片（超过纠错能力2）
    let result = codec.simulate_corruption_and_recover(&group, &[0, 1, 2], 0xDE);
    // 要么解码失败，要么返回的数据与原始数据不同
    match result {
        Ok(recovered) => {
            assert_ne!(
                recovered,
                data.to_vec(),
                "超过纠错能力时不应恢复出正确数据"
            );
        }
        Err(_) => {
            // 解码失败也是预期行为
        }
    }
}

#[test]
fn test_error_correction_with_larger_parity() {
    // 更多校验分片 = 更强的纠错能力
    // k=4, parity=6, n=10, 纠错能力 = ⌊6/2⌋ = 3
    let codec = ErasureCodec::new(4, 6).unwrap();
    assert_eq!(codec.error_correction_capacity(), 3);

    let data = b"Higher parity means stronger error correction!";
    let group = codec.encode(data).unwrap();

    // 损坏3个分片（纠错能力极限）
    let recovered = codec
        .simulate_corruption_and_recover(&group, &[0, 3, 7], 0xCC)
        .unwrap();
    assert_eq!(recovered, data.to_vec());
}

#[test]
fn test_error_correction_mixed_loss_and_corruption() {
    // 同时存在丢失和损坏的场景
    // k=4, parity=6, n=10
    // 丢失2个 + 损坏1个 = 需要至少 k + 2*1 = 6 个正确分片
    // 剩余 10-2=8 个分片中有1个损坏，纠错能力 = ⌊(8-4)/2⌋ = 2，足够
    let codec = ErasureCodec::new(4, 6).unwrap();

    let data = b"Mixed loss and corruption scenario test!";
    let group = codec.encode(data).unwrap();

    // 先丢失2个分片
    let remaining: Vec<_> = group
        .shards
        .iter()
        .filter(|s| s.id.index != 8 && s.id.index != 9)
        .cloned()
        .collect();

    // 再损坏1个分片
    let mut corrupted = remaining;
    corrupted[0].data.iter_mut().for_each(|b| *b = 0xEE);

    let recovered = codec.decode(&corrupted).unwrap();
    assert_eq!(recovered, data.to_vec());
}

#[test]
fn test_error_correction_capacity_values() {
    // 验证不同配置下的纠错能力计算
    assert_eq!(ErasureCodec::new(4, 2).unwrap().error_correction_capacity(), 1);
    assert_eq!(ErasureCodec::new(4, 4).unwrap().error_correction_capacity(), 2);
    assert_eq!(ErasureCodec::new(4, 6).unwrap().error_correction_capacity(), 3);
    assert_eq!(ErasureCodec::new(3, 4).unwrap().error_correction_capacity(), 2);
    assert_eq!(ErasureCodec::new(2, 2).unwrap().error_correction_capacity(), 1);
}

#[test]
fn test_error_correction_large_data() {
    // 大数据量下的纠错测试
    let codec = ErasureCodec::new(6, 6).unwrap();
    // 生成100KB的测试数据
    let data: Vec<u8> = (0..100_000).map(|i| ((i * 7 + 13) % 256) as u8).collect();

    let group = codec.encode(&data).unwrap();

    // 损坏3个分片（纠错能力极限 = ⌊6/2⌋ = 3）
    let recovered = codec
        .simulate_corruption_and_recover(&group, &[0, 5, 11], 0x42)
        .unwrap();
    assert_eq!(recovered, data);
}

// ============================================================================
// RS 纠错解码性能测试（本地直接运行，无需 Docker）
//
// 用法：cargo test --release test_rs_correction_perf -- --nocapture
//       cargo test --release perf -- --nocapture  （模糊匹配所有性能测试）
// ============================================================================

/// 辅助函数：测量 RS 纠错解码性能
fn bench_rs_correction(
    data_shards: usize,
    parity_shards: usize,
    data_size: usize,
    corrupt_count: usize,
    label: &str,
) {
    use std::time::Instant;

    let n = data_shards + parity_shards;
    let codec = ErasureCodec::new(data_shards, parity_shards).unwrap();

    // 生成测试数据
    let data: Vec<u8> = (0..data_size).map(|i| ((i * 7 + 13) % 256) as u8).collect();

    // 编码
    let encode_start = Instant::now();
    let group = codec.encode(&data).unwrap();
    let encode_elapsed = encode_start.elapsed();

    let shard_size = group.shards[0].data.len();

    // 构造损坏的分片索引（从索引0开始损坏）
    let corrupt_indices: Vec<usize> = (0..corrupt_count).collect();

    // 损坏分片并解码（纠错）
    let mut corrupted_shards = group.shards.clone();
    for shard in corrupted_shards.iter_mut() {
        if corrupt_indices.contains(&shard.id.index) {
            for byte in shard.data.iter_mut() {
                *byte = 0xFF; // 完全篡改
            }
        }
    }

    let decode_start = Instant::now();
    let recovered = codec.decode(&corrupted_shards).unwrap();
    let decode_elapsed = decode_start.elapsed();

    // 验证正确性
    assert_eq!(recovered, data, "纠错解码结果不正确！");

    let throughput = data_size as f64 / decode_elapsed.as_secs_f64() / 1024.0 / 1024.0;

    println!(
        "[{}] n={}, k={}, 数据={}KB, 分片={}KB, 损坏={}/{}, \
         编码={:.1}ms, 纠错解码={:.1}ms, 吞吐量={:.2} MB/s",
        label,
        n,
        data_shards,
        data_size / 1024,
        shard_size / 1024,
        corrupt_count,
        n,
        encode_elapsed.as_secs_f64() * 1000.0,
        decode_elapsed.as_secs_f64() * 1000.0,
        throughput,
    );
}

/// 辅助函数：测量无损坏场景下的 RS rebuild 性能（作为对照组）
fn bench_rs_rebuild_no_error(
    data_shards: usize,
    parity_shards: usize,
    data_size: usize,
    label: &str,
) {
    use std::time::Instant;

    let n = data_shards + parity_shards;
    let codec = ErasureCodec::new(data_shards, parity_shards).unwrap();

    let data: Vec<u8> = (0..data_size).map(|i| ((i * 7 + 13) % 256) as u8).collect();
    let group = codec.encode(&data).unwrap();
    let shard_size = group.shards[0].data.len();

    // 无损坏，直接解码
    let decode_start = Instant::now();
    let recovered = codec.decode(&group.shards).unwrap();
    let decode_elapsed = decode_start.elapsed();

    assert_eq!(recovered, data);

    let throughput = data_size as f64 / decode_elapsed.as_secs_f64() / 1024.0 / 1024.0;

    println!(
        "[{}] n={}, k={}, 数据={}KB, 分片={}KB, 无损坏, \
         解码={:.1}ms, 吞吐量={:.2} MB/s",
        label,
        n,
        data_shards,
        data_size / 1024,
        shard_size / 1024,
        decode_elapsed.as_secs_f64() * 1000.0,
        throughput,
    );
}

#[test]
fn test_rs_correction_perf_n13_k5() {
    // 模拟实际 BFT 配置：n=13, k=5 (t=4)
    // 纠错能力 = ⌊8/2⌋ = 4
    println!("\n========== RS 纠错性能测试: n=13, k=5 (t=4) ==========");

    let data_shards = 5;
    let parity_shards = 8;
    let corrupt_count = 4; // BFT 极限：t=4 个恶意节点

    // 对照组：无损坏
    for &size in &[100 * 1024, 1024 * 1024, 5 * 1024 * 1024] {
        bench_rs_rebuild_no_error(data_shards, parity_shards, size, "对照组-无损坏");
    }

    println!("---");

    // 实验组：4个恶意分片（BFT极限）
    // 注意：大文件可能非常慢（这正是我们要优化的！），先从小文件开始
    for &size in &[
        10 * 1024,        // 10KB - 快速验证
        100 * 1024,       // 100KB - 小文件
        500 * 1024,       // 500KB - 中等文件
        1024 * 1024,      // 1MB - 大文件（可能需要几秒）
        // 5 * 1024 * 1024,  // 5MB - 超大文件（可能需要几十秒，按需取消注释）
    ] {
        bench_rs_correction(data_shards, parity_shards, size, corrupt_count, "BFT极限-4损坏");
    }
}

#[test]
fn test_rs_correction_perf_n10_k4() {
    // 模拟实际 BFT 配置：n=10, k=4 (t=3)
    // 纠错能力 = ⌊6/2⌋ = 3
    println!("\n========== RS 纠错性能测试: n=10, k=4 (t=3) ==========");

    let data_shards = 4;
    let parity_shards = 6;
    let corrupt_count = 3; // BFT 极限：t=3 个恶意节点

    for &size in &[100 * 1024, 500 * 1024, 1024 * 1024] {
        bench_rs_rebuild_no_error(data_shards, parity_shards, size, "对照组-无损坏");
    }

    println!("---");

    for &size in &[10 * 1024, 100 * 1024, 500 * 1024, 1024 * 1024] {
        bench_rs_correction(data_shards, parity_shards, size, corrupt_count, "BFT极限-3损坏");
    }
}

#[test]
fn test_rs_correction_perf_scaling() {
    // 测试纠错耗时随数据大小的增长趋势
    println!("\n========== RS 纠错耗时增长趋势 (n=13, k=5, 4损坏) ==========");

    let data_shards = 5;
    let parity_shards = 8;
    let corrupt_count = 4;

    for &size in &[
        1024,             // 1KB
        10 * 1024,        // 10KB
        50 * 1024,        // 50KB
        100 * 1024,       // 100KB
        200 * 1024,       // 200KB
        500 * 1024,       // 500KB
    ] {
        bench_rs_correction(data_shards, parity_shards, size, corrupt_count, "增长趋势");
    }
}

