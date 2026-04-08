# fix 分支：修复 RS 纠错解码逐字节冗余调用 Berlekamp-Welch 算法

> 本分支从 `master` 签出，专注于修复 `reed_solomon_rs` 库中 Berlekamp-Welch 纠错算法的**逐字节冗余计算**问题，该问题是恶意节点场景下系统吞吐量的首要瓶颈。

## 问题背景

在 master 分支的 Docker 端到端测试中发现：**大文件传输场景下，当存在恶意节点时，RS 纠错解码耗时暴增，成为系统吞吐量的绝对瓶颈。**

### 实测数据（master 分支，优化前）

**n=13（t=4）场景，50MB 文件：**

| 场景 | RS 解码耗时 | 端到端延迟 | 吞吐量 |
|------|-----------|-----------|--------|
| 正常（无恶意节点） | 469ms | 8,581ms | 5.83 MB/s |
| 4 个恶意节点（BFT 极限） | **48,629ms** | 53,348ms | 0.94 MB/s |
| **退化倍数** | **×103.7** | **×6.2** | **↓83.9%** |

恶意场景下 RS 解码耗时占端到端延迟的 **91.2%**。

---

## 根因分析

### 核心缺陷：逐字节独立调用 Berlekamp-Welch 算法

`reed_solomon_rs 0.1.2` 库的 `correct()` 方法在检测到错误后，**对分片数据的每个字节位置独立调用一次完整的 `berlekamp_welch()` 函数**：

```rust
// 原始实现（简化）
pub fn correct(&self, shares: &mut Vec<Share>) -> Result<(), ...> {
    let synd = self.syndrome_matrix(&shares)?;
    let mut buf = vec![0u8; shares[0].data.len()];

    for i in 0..synd.r {
        // ... syndrome 计算 ...
        for j in 0..buf.len() {       // buf.len() = 分片大小（可达数十万字节）
            if buf[j] != 0 {
                // 🔥 对每个出错的字节位置，独立运行一次完整的 BW 算法！
                let data = self.berlekamp_welch(&shares, j)?;
                for i in 0..shares.len() {
                    shares[i].data[j] = data[shares[i].number];
                }
            }
        }
    }
}
```

以 n=13, k=5 配置为例，50MB 文件的每个分片约 800KB：

- **800,000 个字节位置** × 每次 dim×dim 矩阵求逆（dim ≈ 13，O(dim³)）
- 总计算量：**800,000 × 13³ ≈ 17.6 亿次 GF(2⁸) 运算**

这就是纠错解码耗时暴增的根本原因。

---

## 优化方案

### 数学原理

Berlekamp-Welch 算法的关键数学性质：**错误定位多项式 E(x) 与具体字节位置无关**。

E(x) 的根只取决于**哪些分片是错误的**（即错误的位置），而不取决于错误的具体内容（即每个字节被篡改成了什么值）。因此：

1. 只需对**一个字节位置**运行 BW 算法，即可得到错误定位多项式 E(x)
2. 通过 E(x) 的根确定哪些分片是错误的
3. 丢弃错误分片，用剩余正确分片调用 `rebuild()` 恢复数据

### 复杂度对比

| | 原始实现 | 优化后 |
|---|---|---|
| BW 调用次数 | shard_size 次（~800,000） | **1 次** |
| 矩阵求逆 | shard_size × O(dim³) | **1 × O(dim³)** |
| rebuild | 无（逐字节修复） | 1 × O(k² × shard_size) |
| **总复杂度** | O(shard_size × dim³) | **O(dim³ + k² × shard_size)** |

### 优化后的 correct() 流程

```
correct(shares):
  1. syndrome 矩阵快速检测 → 无错误则直接返回（快速路径）
  2. 找到一个 syndrome 非零的字节位置 error_byte_index
  3. 对该字节位置运行一次 BW 算法 → 得到错误定位多项式 E(x)
  4. 遍历所有分片，E(eval_point(share.number)) == 0 的即为错误分片
  5. 丢弃错误分片（shares.retain）
  6. 剩余正确分片数 ≥ k → 后续 rebuild() 可正常恢复数据
```

---

## 修改文件清单

本分支相对于 master 的改动：

| 文件 | 改动说明 |
|------|---------|
| `Cargo.toml` | `reed_solomon_rs` 依赖从 crates.io 版本改为本地 path 依赖 |
| `deps/reed_solomon_rs/` | 从 cargo registry 复制的库源码（完整保留） |
| `deps/reed_solomon_rs/src/decoder/berlekamp_welch.rs` | **核心改动**：重写 `correct()` 方法，新增 `find_error_byte_index()` 和 `berlekamp_welch_locate_errors()` |
| `src/erasure/test_erasure.rs` | 性能测试数据大小聚焦大文件（20MB~100MB） |

### 核心改动详情（berlekamp_welch.rs）

**新增方法：**

| 方法 | 说明 |
|------|------|
| `find_error_byte_index()` | 遍历 syndrome 矩阵，找到第一个非零的字节位置 |
| `berlekamp_welch_locate_errors()` | 对单个字节位置运行 BW 算法，通过 E(x) 的根定位所有错误分片 |

**修改方法：**

| 方法 | 改动 |
|------|------|
| `correct()` | 从"逐字节修复"改为"一次定位 + 丢弃错误分片" |

**保留方法：**

| 方法 | 说明 |
|------|------|
| `berlekamp_welch()` | 保留原始实现，维持 API 兼容性 |

---

## 性能测试

### 运行方式

```bash
# 运行所有 RS 纠错性能测试（聚焦 20MB~100MB 大文件）
cargo test --release perf -- --nocapture

# 单独运行
cargo test --release test_rs_correction_perf_n13_k5 -- --nocapture  # n=13, BFT极限
cargo test --release test_rs_correction_perf_n10_k4 -- --nocapture  # n=10, BFT极限
cargo test --release test_rs_correction_perf_scaling -- --nocapture  # 1MB~100MB 增长趋势

# 运行正确性测试（确保纠错功能无回归）
cargo test --release test_error_correction -- --nocapture
```

### 优化前后性能对比（n=13, k=5, 4 个恶意分片）

#### 纠错解码耗时

| 数据大小 | 优化前 | 优化后 | 加速比 |
|:---:|:---:|:---:|:---:|
| 10KB | 22.9ms | 0.1ms | **×229** |
| 100KB | 228.8ms | 0.4ms | **×572** |
| 500KB | 1,139ms | 1.8ms | **×633** |
| 1MB | 2,518ms | 3.2ms | **×787** |

#### 纠错解码吞吐量

| 数据大小 | 优化前 | 优化后 | 提升 |
|:---:|:---:|:---:|:---:|
| 1MB | 0.40 MB/s | **312 MB/s** | **×780** |

#### 大文件测试结果（优化后）

**n=13, k=5, 4 损坏（BFT 极限）：**

| 数据大小 | 对照组（无损坏） | 实验组（4 损坏纠错） | 纠错额外开销 |
|:---:|:---:|:---:|:---:|
| 20MB | 58ms / 346 MB/s | 60ms / 334 MB/s | ~2ms |
| 50MB | 150ms / 335 MB/s | 152ms / 330 MB/s | ~2ms |
| 100MB | 302ms / 331 MB/s | 355ms / 282 MB/s | ~53ms |

**n=10, k=4, 3 损坏（BFT 极限）：**

| 数据大小 | 对照组（无损坏） | 实验组（3 损坏纠错） | 纠错额外开销 |
|:---:|:---:|:---:|:---:|
| 20MB | 43ms / 467 MB/s | 53ms / 375 MB/s | ~10ms |
| 50MB | 132ms / 379 MB/s | 144ms / 346 MB/s | ~12ms |
| 100MB | 270ms / 370 MB/s | 306ms / 327 MB/s | ~36ms |

#### 增长趋势（n=13, k=5, 4 损坏）

| 数据大小 | 纠错解码耗时 | 吞吐量 |
|:---:|:---:|:---:|
| 1MB | 3.2ms | 309 MB/s |
| 5MB | 16.3ms | 306 MB/s |
| 10MB | 29.9ms | 334 MB/s |
| 20MB | 63.5ms | 315 MB/s |
| 50MB | 186.2ms | 269 MB/s |
| 100MB | 364.0ms | 275 MB/s |

### 关键结论

1. **纠错解码性能提升 500~800 倍**：从恒定 ~0.42 MB/s 提升到 275~334 MB/s
2. **纠错开销几乎可忽略**：优化后纠错解码耗时与无损坏解码几乎一致，额外开销仅为毫秒级的一次 BW 调用
3. **吞吐量随数据大小线性扩展**：不再受逐字节 BW 调用的限制
4. **所有 25 个 erasure 测试通过**：包括 7 个纠错正确性测试，无任何回归

---

## 正确性验证

```bash
# 运行全部 erasure 模块测试（25 个用例）
cargo test --release erasure -- --nocapture
```

测试覆盖：
- ✅ 基本编解码、丢失恢复、混合丢失
- ✅ 单分片纠错、双分片纠错、超过纠错能力的损坏
- ✅ 高冗余配置纠错、丢失+篡改混合纠错
- ✅ 大数据量纠错（100KB）
- ✅ 不同编码配置（2+2, 3+3, 4+4, 6+3, 10+4）
- ✅ 分片序列化/反序列化、完整性校验