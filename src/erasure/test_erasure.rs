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

    let data_mb = data_size as f64 / 1024.0 / 1024.0;
    let shard_mb = shard_size as f64 / 1024.0 / 1024.0;

    println!(
        "[{}] n={}, k={}, 数据={:.1}MB, 分片={:.1}MB, 损坏={}/{}, \
         编码={:.1}ms, 纠错解码={:.1}ms, 吞吐量={:.2} MB/s",
        label,
        n,
        data_shards,
        data_mb,
        shard_mb,
        corrupt_count,
        n,
        encode_elapsed.as_secs_f64() * 1000.0,
        decode_elapsed.as_secs_f64() * 1000.0,
        throughput,
    );
}

/// 辅助函数：测量无损坏场景下的 RS 解码性能（作为对照组）
///
/// 传入全部 n 个正确分片，走与实验组相同的 fec.decode() 路径
/// （correct() 检测无错误快速返回 → rebuild()），确保公平对比。
fn bench_rs_rebuild_no_error(
    data_shards: usize,
    parity_shards: usize,
    data_size: usize,
    corrupt_count: usize,
    label: &str,
) {
    use std::time::Instant;

    let n = data_shards + parity_shards;
    let codec = ErasureCodec::new(data_shards, parity_shards).unwrap();

    let data: Vec<u8> = (0..data_size).map(|i| ((i * 7 + 13) % 256) as u8).collect();
    let group = codec.encode(&data).unwrap();
    let shard_size = group.shards[0].data.len();

    // 模拟与实验组相同的条件：传入 n - corrupt_count 个正确分片
    // 实验组 correct() 会丢弃 corrupt_count 个错误分片后剩余相同数量
    let shards_to_use: Vec<_> = group.shards.iter()
        .take(n - corrupt_count)
        .cloned()
        .collect();

    let decode_start = Instant::now();
    let recovered = codec.decode(&shards_to_use).unwrap();
    let decode_elapsed = decode_start.elapsed();

    assert_eq!(recovered, data);

    let throughput = data_size as f64 / decode_elapsed.as_secs_f64() / 1024.0 / 1024.0;

    let data_mb = data_size as f64 / 1024.0 / 1024.0;
    let shard_mb = shard_size as f64 / 1024.0 / 1024.0;

    println!(
        "[{}] n={}, k={}, 数据={:.1}MB, 分片={:.1}MB, 无损坏({}个分片), \
         解码={:.1}ms, 吞吐量={:.2} MB/s",
        label,
        n,
        data_shards,
        data_mb,
        shard_mb,
        n - corrupt_count,
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

    // 统一测试数据大小，聚焦大文件（20MB以上），确保对照组和实验组可公平对比
    let test_sizes = [
        20 * 1024 * 1024,   // 20MB
        50 * 1024 * 1024,   // 50MB
        100 * 1024 * 1024,  // 100MB
    ];

    // 对照组：无损坏（传入 n - corrupt_count 个正确分片，与实验组 rebuild 阶段条件一致）
    for &size in &test_sizes {
        bench_rs_rebuild_no_error(data_shards, parity_shards, size, corrupt_count, "对照组-无损坏");
    }

    println!("---");

    // 实验组：4个恶意分片（BFT极限）
    for &size in &test_sizes {
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

    // 统一测试数据大小，聚焦大文件（20MB以上），确保对照组和实验组可公平对比
    let test_sizes = [
        20 * 1024 * 1024,   // 20MB
        50 * 1024 * 1024,   // 50MB
        100 * 1024 * 1024,  // 100MB
    ];

    // 对照组：无损坏（传入 n - corrupt_count 个正确分片，与实验组 rebuild 阶段条件一致）
    for &size in &test_sizes {
        bench_rs_rebuild_no_error(data_shards, parity_shards, size, corrupt_count, "对照组-无损坏");
    }

    println!("---");

    for &size in &test_sizes {
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
        1024 * 1024,        // 1MB
        5 * 1024 * 1024,    // 5MB
        10 * 1024 * 1024,   // 10MB
        20 * 1024 * 1024,   // 20MB
        50 * 1024 * 1024,   // 50MB
        100 * 1024 * 1024,  // 100MB
    ] {
        bench_rs_correction(data_shards, parity_shards, size, corrupt_count, "增长趋势");
    }
}

// ============================================================================
// master vs dev1 在 RBC 算法 4 渐进 r 循环下的成败界界与成本差异对比测试
//
// 背景：
//   - master 分支：correct() 对每个 syndrome 非零字节位置独立调用 BW，修好字节后
//                  保留全部分片（逐字节字节级修复）。
//   - dev1 分支：correct() 多轮 BW 定位错误分片集合后整片丢弃。
//
// 结论（本测试印证）：
//   1. 关键指标不是“分片级错误数 e_actual”，而是“单字节位置上的错误数”。
//      BW 在字节位置 j 上的纠错能力为 (|T_h|-k)/2，它只要求“在这个具体字节
//      列上实际错误的分片数”≤此数即可。
//   2. 当错误字节在分片内稀疏分布时（例如每片只翻转极少数字节），
//      单个字节列上的并发错误数远小于分片级总错误数 e_actual。
//      **此时 master 的逐字节修复能在 dev1 整片丢弃失败的更小 r 值上真成功**
//      —— master 在这种稀疏错误场景确实能比 dev1 提前退出渐进循环。
//   3. 在错误密集场景（整片被篡改，所有字节列都有错）：
//      两者成败判定等价，但 master 单轮耗时是 dev1 的 数百至上千倍。
//   4. 在真实 RBC 协议中，拜占庭节点会选择“密集模式”（整片无意义或高密度篡改）以最大化
//      破坏性，其 Docker 测试 9/12/13 的损坏模式也属此类——故 dev1 的“成败与 master 等价
//      + 耗时减少几个数量级”是真实场景下的大获利。
//
// 用法：
//   cargo test --release master_vs_dev1 -- --nocapture
// ============================================================================

/// master 风格的 legacy correct() 实现：对每个 syndrome 非零的字节位置
/// 独立调用一次 `FEC::berlekamp_welch()`，用返回的正确字节写回所有分片。
/// 这精确复现了 reed_solomon_rs 0.1.2 原始库 `correct()` 的行为。
///
/// 返回 `Ok(())` 表示修复成功（所有字节 syndrome 已归零），
/// 返回 `Err(..)` 表示错误超过纠错能力。
fn correct_master_legacy(
    fec: &reed_solomon_rs::fec::fec::FEC,
    shares: &mut Vec<reed_solomon_rs::fec::fec::Share>,
    k: usize,
) -> Result<usize, Box<dyn std::error::Error>> {
    use reed_solomon_rs::math::addmul::addmul;

    if shares.len() < k {
        return Err("Must specify at least k shares".into());
    }
    shares.sort();

    let buf_len = shares[0].data.len();
    let mut buf = vec![0u8; buf_len];
    let mut bw_call_count = 0usize;

    // 外层无限循环：只要还有 syndrome 非零的字节就继续修复
    // （master 原库写成 for i in 0..synd.r 的双层循环，本质等价）
    loop {
        // 计算 syndrome 矩阵（每次修复后都需要重算）
        let synd = fec.syndrome_matrix(&*shares)?;
        let mut found_error = false;

        for i in 0..synd.r {
            for j in 0..buf_len {
                buf[j] = 0;
            }
            for j in 0..synd.c {
                addmul(
                    buf.as_mut_slice(),
                    shares[j].data.as_slice(),
                    synd.get(i, j).0,
                );
            }
                for j in 0..buf_len {
                    if buf[j] != 0 {
                        // 对每个出错字节位置 j 独立运行 BW，取回 n 个字节的正确值
                        let data = fec.berlekamp_welch(&*shares, j)?;
                        bw_call_count += 1;
                    for sh in shares.iter_mut() {
                        sh.data[j] = data[sh.number];
                    }
                    found_error = true;
                }
            }
        }

        if !found_error {
            break;
        }
    }

    Ok(bw_call_count)
}

/// 对给定分片集合 T_h 构造损坏场景，并在同一批分片上分别运行两种 correct() 实现。
/// 进一步：对 correct 后的分片进行 rebuild，对比恢复出的数据与原始数据是否一致（“真成功”）。
/// 返回 (dev1 耗时, master 耗时, master BW 调用次数, dev1 真成功?, master 真成功?).
fn bench_one_th_round(
    data_shards: usize,
    parity_shards: usize,
    shard_size: usize,
    shares_count: usize,      // |T_h|
    corrupt_count: usize,      // 错误分片数
    corrupt_bytes_per_shard: usize,  // 每个错误分片被篡改的字节数（>=1）
) -> (std::time::Duration, std::time::Duration, usize, bool, bool) {
    use reed_solomon_rs::fec::fec::{Share, FEC};
    use std::time::Instant;

    let n = data_shards + parity_shards;
    let fec = FEC::new(data_shards, n).expect("FEC::new");

    // 生成原始数据 & 完整编码所有 n 个分片
    let data: Vec<u8> = (0..shard_size * data_shards)
        .map(|i| ((i * 7 + 13) % 256) as u8)
        .collect();

    let mut all_shares: Vec<Share> = Vec::with_capacity(n);
    fec.encode(&data, |s: Share| {
        all_shares.push(s);
    })
    .expect("encode");
    assert_eq!(all_shares.len(), n);

    // 取前 shares_count 个分片作为 T_h（模拟网络上陆续到达的分片）
    let mut th_shares: Vec<Share> = all_shares.into_iter().take(shares_count).collect();

    // 对前 corrupt_count 个分片做损坏：只翻转若干字节（模拟精巧的字节级攻击）
    // 每个错误分片选不同的字节列，最大化“字节级错误分散”的压力
    for (idx, sh) in th_shares.iter_mut().enumerate().take(corrupt_count) {
        for b in 0..corrupt_bytes_per_shard {
            // 每个错误分片的错误字节列错开
            let pos = (idx * 131 + b * 7) % shard_size;
            sh.data[pos] ^= 0xFF;
        }
    }

    // 分别克隆给两种实现使用（保证输入严格一致）
    let mut shares_for_dev1 = th_shares.clone();
    let mut shares_for_master = th_shares.clone();

    // --- dev1 版本 ---
    let start = Instant::now();
    let dev1_correct_ok = fec.correct(&mut shares_for_dev1).is_ok();
    let dev1_elapsed = start.elapsed();

    // --- master 风格 legacy 版本 ---
    let start = Instant::now();
    let master_result = correct_master_legacy(&fec, &mut shares_for_master, data_shards);
    let master_elapsed = start.elapsed();
    let (master_correct_ok, master_bw_calls) = match master_result {
        Ok(calls) => (true, calls),
        Err(_) => (false, 0),
    };

    // --- 关键：rebuild 两者修复后的分片，对比是否真实恢复出原始数据 ---
    let dev1_true_ok = dev1_correct_ok
        && rebuild_and_verify(&fec, &shares_for_dev1, data_shards, shard_size, &data);
    let master_true_ok = master_correct_ok
        && rebuild_and_verify(&fec, &shares_for_master, data_shards, shard_size, &data);

    (
        dev1_elapsed,
        master_elapsed,
        master_bw_calls,
        dev1_true_ok,
        master_true_ok,
    )
}

/// rebuild 一批 correct() 后的分片，验证恢复出的数据是否等于原始数据。
fn rebuild_and_verify(
    fec: &reed_solomon_rs::fec::fec::FEC,
    shares: &[reed_solomon_rs::fec::fec::Share],
    data_shards: usize,
    shard_size: usize,
    original: &[u8],
) -> bool {
    if shares.len() < data_shards {
        return false;
    }
    let result_len = shard_size * data_shards;
    let mut dst = vec![0u8; result_len];
    let rebuild_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        fec.rebuild(shares.to_vec(), |s: reed_solomon_rs::fec::fec::Share| {
            if s.number < data_shards {
                let start = s.number * shard_size;
                let end = start + shard_size;
                if end <= dst.len() {
                    dst[start..end].copy_from_slice(&s.data);
                }
            }
        })
        .map(|_| dst)
    }));
    match rebuild_result {
        Ok(Ok(recovered)) => recovered == original,
        _ => false,
    }
}

/// 模拟 RBC 算法 4 的渐进 r 循环：
///   for r = 0 .. t:
///     |T_h| = 2t + r + 1（分片陆续到达，每轮比上一轮多 1 个）
///     在 T_h 上运行 correct() 尝试解码
/// 对同一批损坏场景，分别跑两种 correct() 实现，记录每轮成败 + 耗时。
fn simulate_rbc_progressive_loop(
    scenario_name: &str,
    data_shards: usize,
    parity_shards: usize,
    shard_size: usize,
    actual_errors: usize,         // 实际错误分片数
    corrupt_bytes_per_shard: usize,
) {
    let n = data_shards + parity_shards;
    let t = (n - 1) / 3;
    assert!(data_shards == t + 1, "本测试要求 k = t + 1（RBC 标准参数）");

    println!(
        "\n========== {} ==========",
        scenario_name
    );
    println!(
        "参数: n={}, k={}, t={}, 分片大小={}B, 实际错误分片数={}, 每片篡改字节数={}",
        n, data_shards, t, shard_size, actual_errors, corrupt_bytes_per_shard
    );
    println!(
        "{:<5} {:<10} {:<10} {:<18} {:<18} {:<12} {:<10}",
        "r",
        "|T_h|",
        "BW可纠e",
        "dev1:真成功?/耗时",
        "master:真成功?/耗时",
        "BW调用次数",
        "加速比"
    );
    println!("{}", "-".repeat(95));

    let mut dev1_first_success_r: Option<usize> = None;
    let mut master_first_success_r: Option<usize> = None;
    let mut dev1_cumulative = std::time::Duration::ZERO;
    let mut master_cumulative = std::time::Duration::ZERO;
    let mut master_fake_success_rounds: Vec<usize> = Vec::new(); // master correct Ok 但 rebuild 数据错误的轮

    for r in 0..=t {
        let shares_count = 2 * t + r + 1;
        if shares_count > n {
            break;
        }
        let bw_capacity = (shares_count.saturating_sub(data_shards)) / 2;

        let (dev1_ms, master_ms, master_bw_calls, dev1_true_ok, master_true_ok) =
            bench_one_th_round(
                data_shards,
                parity_shards,
                shard_size,
                shares_count,
                actual_errors,
                corrupt_bytes_per_shard,
            );

        // 识别 master 的“假成功”：BW 超容量时返回内部自洽但污染分片的解
        // 判定方法：actual_errors > bw_capacity 但 master 在本轮也没真成功（rebuild 数据错误），
            // 而 master_bw_calls > 0 表明它确实把“解”写回分片了——这时则浪费了计算但最终也被协议层 hash 拒绝
        if actual_errors > bw_capacity && master_bw_calls > 0 && !master_true_ok {
            master_fake_success_rounds.push(r);
        }

        dev1_cumulative += dev1_ms;
        master_cumulative += master_ms;

        if dev1_true_ok && dev1_first_success_r.is_none() {
            dev1_first_success_r = Some(r);
        }
        if master_true_ok && master_first_success_r.is_none() {
            master_first_success_r = Some(r);
        }

        let speedup = if dev1_ms.as_secs_f64() > 0.0 {
            master_ms.as_secs_f64() / dev1_ms.as_secs_f64()
        } else {
            0.0
        };

        println!(
            "{:<5} {:<10} {:<10} {:<3} {:>10.3}ms    {:<3} {:>10.3}ms    {:<12} ×{:.1}",
            r,
            shares_count,
            bw_capacity,
            if dev1_true_ok { "✅" } else { "❌" },
            dev1_ms.as_secs_f64() * 1000.0,
            if master_true_ok { "✅" } else { "❌" },
            master_ms.as_secs_f64() * 1000.0,
            master_bw_calls,
            speedup,
        );
    }

    println!("{}", "-".repeat(95));
    println!(
        "【首次真成功轮次】dev1: r={:?}, master: r={:?} {}",
        dev1_first_success_r,
        master_first_success_r,
        match (dev1_first_success_r, master_first_success_r) {
            (Some(a), Some(b)) if a == b => {
                "✅ 相等（密集错误场景：两者成败判定等价）"
            }
            (Some(a), Some(b)) if a > b => {
                "ℹ️  master 更早成功（稀疏错误场景：master 逐字节修复能处理“单列错误数≤BW容量”的分布）"
            }
            _ => "⚠️  其他情况",
        }
    );
    if !master_fake_success_rounds.is_empty() {
        println!(
            "【master “假成功”轮次】r={:?}：correct() 返回 Ok 但 rebuild 数据已被污染，会被协议层 hash 验证拒绝",
            master_fake_success_rounds
        );
    }
    println!(
        "【累计耗时】dev1: {:.3}ms, master: {:.3}ms, 总加速比: ×{:.1}",
        dev1_cumulative.as_secs_f64() * 1000.0,
        master_cumulative.as_secs_f64() * 1000.0,
        if dev1_cumulative.as_secs_f64() > 0.0 {
            master_cumulative.as_secs_f64() / dev1_cumulative.as_secs_f64()
        } else {
            0.0
        },
    );

    // 断言：在 r=t（最终轮）两者都必须真成功——算法协议的基础容错保证。
    // （不再硬性要求 dev1 == master，因为稀疏错误场景下 master 有更小 r 上退出的理论优势）
    if actual_errors <= t {
        assert!(
            dev1_first_success_r.is_some(),
            "【断言失败】dev1 在错误数≤t时以告失败，违反 RBC 容错理论保证！"
        );
        assert!(
            master_first_success_r.is_some(),
            "【断言失败】master 在错误数≤t时以告失败，违反 RBC 容错理论保证！"
        );
    }
}

#[test]
fn test_master_vs_dev1_progressive_r_loop_n7() {
    // 场景：n=7, k=3, t=2，10KB 分片
    // 重点对比两种分片级错误分布对两者“首次真成功 r”的影响：
    //   - 稀疏错误（每片仅翻转 100 字节）：单字节列上的错误数远小于 e_actual，
        //                     master 能在更小 r 上退出（有理论优势）
    //   - 密集错误（每片全片篡改）：单字节列上的错误数 = e_actual，
    //                     两者成败判定严格等价，差异全部回到“单轮耗时”上

    // —— 子组 A：稀疏错误场景（每片仅 1% 字节被翻转）——
    for actual_errors in [1usize, 2] {
        simulate_rbc_progressive_loop(
            &format!(
                "n=7,k=3,t=2,shard=10KB,错误分片={},每片篡改100字节[稀疏]",
                actual_errors
            ),
            3,       // k
            4,       // parity
            10_000,  // shard_size
            actual_errors,
            100,
        );
    }

    // —— 子组 B：密集错误场景（全片篡改，每个字节列都有错）——
    // 这是 Docker 拜占庭测试（测试 9/12/13）的模式。
    for actual_errors in [1usize, 2] {
        simulate_rbc_progressive_loop(
            &format!(
                "n=7,k=3,t=2,shard=10KB,错误分片={},每片全片篡改[密集]",
                actual_errors
            ),
            3,
            4,
            10_000,
            actual_errors,
            10_000,
        );
    }
}

#[test]
fn test_master_vs_dev1_progressive_r_loop_n13_large() {
    // 大场景：n=13, k=5, t=4，100KB 分片（BFT 极限）
    //
    // 这是最贴近 Docker 测试12/13 的场景，用于量化在真实大分片条件下
    // master 的逐字节 BW 有多么昂贵。
    //
    // 预期：两者首次成功 r 值相等（通常是 r = actual_errors 左右），
    //       但 master 每成功一轮的耗时可能是 dev1 的 100× 以上。

    // 用较小的数据规模（100KB / k=5 = 每片 20KB）避免测试过慢，
    // 但仍能清晰观察量级差异。
    for actual_errors in [1usize, 2, 4] {
        simulate_rbc_progressive_loop(
            &format!(
                "n=13,k=5,t=4,shard=20KB,错误分片={},每片全片篡改",
                actual_errors
            ),
            5,       // k
            8,       // parity
            20_000,  // shard_size
            actual_errors,
            20_000,  // 全片篡改 —— 模拟 Docker 测试中的恶意节点行为（覆盖所有字节列）
        );
    }
}