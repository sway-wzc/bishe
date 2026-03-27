use crate::erasure::{ErasureCodec, ShardGroup};

#[test]
fn test_basic_encode_decode() {
    let codec = ErasureCodec::new(4, 2).unwrap();
    let data = b"Hello, Erasure Coding! This is a test for Reed-Solomon encoding.";

    // 编码
    let group = codec.encode(data).unwrap();
    assert_eq!(group.shards.len(), 6);
    assert_eq!(group.data_shard_count, 4);
    assert_eq!(group.parity_shard_count, 2);
    assert_eq!(group.original_size, data.len());

    // 完整解码
    let recovered = codec.decode_from_group(&group).unwrap();
    assert_eq!(recovered, data.to_vec());
}

#[test]
fn test_recover_with_lost_data_shards() {
    let codec = ErasureCodec::new(4, 2).unwrap();
    let data = b"Testing recovery after losing data shards!";

    let group = codec.encode(data).unwrap();

    // 丢失2个数据分片（索引0和1）
    let recovered = codec.simulate_loss_and_recover(&group, &[0, 1]).unwrap();
    assert_eq!(recovered, data.to_vec());
}

#[test]
fn test_recover_with_lost_parity_shards() {
    let codec = ErasureCodec::new(4, 2).unwrap();
    let data = b"Testing recovery after losing parity shards!";

    let group = codec.encode(data).unwrap();

    // 丢失2个校验分片（索引4和5）
    let recovered = codec.simulate_loss_and_recover(&group, &[4, 5]).unwrap();
    assert_eq!(recovered, data.to_vec());
}

#[test]
fn test_recover_with_mixed_loss() {
    let codec = ErasureCodec::new(4, 2).unwrap();
    let data = b"Testing recovery with mixed shard loss!";

    let group = codec.encode(data).unwrap();

    // 丢失1个数据分片 + 1个校验分片
    let recovered = codec.simulate_loss_and_recover(&group, &[2, 5]).unwrap();
    assert_eq!(recovered, data.to_vec());
}

#[test]
fn test_too_many_lost_shards() {
    let codec = ErasureCodec::new(4, 2).unwrap();
    let data = b"This should fail with too many lost shards";

    let group = codec.encode(data).unwrap();

    // 丢失3个分片（超过容错能力2）
    let result = codec.simulate_loss_and_recover(&group, &[0, 1, 2]);
    assert!(result.is_err());
}

#[test]
fn test_small_data() {
    let codec = ErasureCodec::new(2, 1).unwrap();
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
    let codec = ErasureCodec::new(4, 2).unwrap();
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
    let codec = ErasureCodec::new(4, 2).unwrap();
    let data = b"Roundtrip verification test";
    assert!(codec.verify_roundtrip(data).unwrap());
}

#[test]
fn test_storage_overhead() {
    let codec = ErasureCodec::new(4, 2).unwrap();
    let ratio = codec.storage_overhead_ratio();
    assert!((ratio - 1.5).abs() < f64::EPSILON);
}

#[test]
fn test_fault_tolerance_info() {
    let codec = ErasureCodec::new(4, 2).unwrap();
    let info = codec.fault_tolerance_info();
    assert!(info.contains("4"));
    assert!(info.contains("2"));
    assert!(info.contains("6"));
}

#[test]
fn test_shard_serialization() {
    let codec = ErasureCodec::new(3, 2).unwrap();
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
    let codec = ErasureCodec::new(4, 2).unwrap();
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
        (2, 1),  // 最小配置
        (3, 1),  // 低冗余
        (4, 2),  // 常用配置
        (6, 3),  // 高冗余
        (10, 4), // 大规模
    ];

    let data = b"Testing various erasure coding configurations for robustness!";

    for (d, p) in configs {
        let codec = ErasureCodec::new(d, p).unwrap();
        let group = codec.encode(data).unwrap();

        // 完整恢复
        let recovered = codec.decode_from_group(&group).unwrap();
        assert_eq!(recovered, data.to_vec(), "配置({},{})完整恢复失败", d, p);

        // 丢失最大容忍数量的分片后恢复
        let lost: Vec<usize> = (0..p).collect();
        let recovered = codec.simulate_loss_and_recover(&group, &lost).unwrap();
        assert_eq!(recovered, data.to_vec(), "配置({},{})容错恢复失败", d, p);
    }
}