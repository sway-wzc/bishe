use crate::galois_field::{gf_alg::GfVal, tables::GF_MUL_TABLE};

// ============================================================================
// SIMD 加速 GF(2⁸) 乘法
//
// 原理：GF(2⁸) 中 mul(x, y) 可以拆分为高低 4 位分别查表再 XOR：
//   mul(x, y) = table_low[x & 0x0F] ^ table_high[x >> 4]
// 其中 table_low 和 table_high 各 16 字节，恰好适合 SSSE3 pshufb 指令
// 一次 SSE 操作处理 16 字节，AVX2 处理 32 字节
// ============================================================================

/// 为给定的 GF(2⁸) 乘数 y 生成低 4 位和高 4 位查找表
///
/// 返回 (table_low, table_high)，各 16 字节
/// table_low[i]  = GF_MUL(i, y)        对 i ∈ [0, 15]
/// table_high[i] = GF_MUL(i << 4, y)   对 i ∈ [0, 15]
#[inline(always)]
fn build_mul_tables(y: u8) -> ([u8; 16], [u8; 16]) {
    let gf_mul_y = &GF_MUL_TABLE[y as usize];
    let mut table_low = [0u8; 16];
    let mut table_high = [0u8; 16];

    for i in 0..16 {
        table_low[i] = gf_mul_y[i];
        table_high[i] = gf_mul_y[i << 4];
    }

    (table_low, table_high)
}

// ============================================================================
// AVX2 实现：一次处理 32 字节
// ============================================================================

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn addmul_avx2(z: &mut [u8], x: &[u8], y: u8) {
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    let (table_low, table_high) = build_mul_tables(y);

    // 将 16 字节查找表扩展为 32 字节（高低 128 位相同）
    let tbl_lo = _mm256_broadcastsi128_si256(_mm_loadu_si128(table_low.as_ptr() as *const __m128i));
    let tbl_hi = _mm256_broadcastsi128_si256(_mm_loadu_si128(table_high.as_ptr() as *const __m128i));
    let mask_0f = _mm256_set1_epi8(0x0F);

    let len = z.len();
    let mut i = 0;

    // 每次处理 32 字节
    while i + 32 <= len {
        let xv = _mm256_loadu_si256(x.as_ptr().add(i) as *const __m256i);
        let zv = _mm256_loadu_si256(z.as_ptr().add(i) as *const __m256i);

        // 拆分高低 4 位
        let x_lo = _mm256_and_si256(xv, mask_0f);
        let x_hi = _mm256_and_si256(_mm256_srli_epi16(xv, 4), mask_0f);

        // pshufb 查表
        let mul_lo = _mm256_shuffle_epi8(tbl_lo, x_lo);
        let mul_hi = _mm256_shuffle_epi8(tbl_hi, x_hi);

        // GF 乘法结果 = table_low[x_lo] ^ table_high[x_hi]
        let product = _mm256_xor_si256(mul_lo, mul_hi);

        // z[i] ^= product
        let result = _mm256_xor_si256(zv, product);
        _mm256_storeu_si256(z.as_mut_ptr().add(i) as *mut __m256i, result);

        i += 32;
    }

    // 处理剩余的 16~31 字节（使用 SSE）
    if i + 16 <= len {
        let tbl_lo_128 = _mm_loadu_si128(table_low.as_ptr() as *const __m128i);
        let tbl_hi_128 = _mm_loadu_si128(table_high.as_ptr() as *const __m128i);
        let mask_0f_128 = _mm_set1_epi8(0x0F);

        let xv = _mm_loadu_si128(x.as_ptr().add(i) as *const __m128i);
        let zv = _mm_loadu_si128(z.as_ptr().add(i) as *const __m128i);

        let x_lo = _mm_and_si128(xv, mask_0f_128);
        let x_hi = _mm_and_si128(_mm_srli_epi16(xv, 4), mask_0f_128);

        let mul_lo = _mm_shuffle_epi8(tbl_lo_128, x_lo);
        let mul_hi = _mm_shuffle_epi8(tbl_hi_128, x_hi);

        let product = _mm_xor_si128(mul_lo, mul_hi);
        let result = _mm_xor_si128(zv, product);
        _mm_storeu_si128(z.as_mut_ptr().add(i) as *mut __m128i, result);

        i += 16;
    }

    // 处理剩余不足 16 字节的尾部（标量回退）
    let gf_mul_y = &GF_MUL_TABLE[y as usize];
    while i < len {
        z[i] ^= gf_mul_y[x[i] as usize];
        i += 1;
    }
}

// ============================================================================
// SSSE3 实现：一次处理 16 字节（AVX2 不可用时的回退）
// ============================================================================

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "ssse3")]
unsafe fn addmul_ssse3(z: &mut [u8], x: &[u8], y: u8) {
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    let (table_low, table_high) = build_mul_tables(y);

    let tbl_lo = _mm_loadu_si128(table_low.as_ptr() as *const __m128i);
    let tbl_hi = _mm_loadu_si128(table_high.as_ptr() as *const __m128i);
    let mask_0f = _mm_set1_epi8(0x0F);

    let len = z.len();
    let mut i = 0;

    // 每次处理 16 字节
    while i + 16 <= len {
        let xv = _mm_loadu_si128(x.as_ptr().add(i) as *const __m128i);
        let zv = _mm_loadu_si128(z.as_ptr().add(i) as *const __m128i);

        let x_lo = _mm_and_si128(xv, mask_0f);
        let x_hi = _mm_and_si128(_mm_srli_epi16(xv, 4), mask_0f);

        let mul_lo = _mm_shuffle_epi8(tbl_lo, x_lo);
        let mul_hi = _mm_shuffle_epi8(tbl_hi, x_hi);

        let product = _mm_xor_si128(mul_lo, mul_hi);
        let result = _mm_xor_si128(zv, product);
        _mm_storeu_si128(z.as_mut_ptr().add(i) as *mut __m128i, result);

        i += 16;
    }

    // 处理剩余不足 16 字节的尾部
    let gf_mul_y = &GF_MUL_TABLE[y as usize];
    while i < len {
        z[i] ^= gf_mul_y[x[i] as usize];
        i += 1;
    }
}

// ============================================================================
// 标量实现（无 SIMD 指令集时的回退）
// ============================================================================

#[inline(always)]
fn addmul_scalar(z: &mut [u8], x: &[u8], y: u8) {
    let x = &x[..z.len()];
    let gf_mul_y = &GF_MUL_TABLE[y as usize];
    for (zi, &xi) in z.iter_mut().zip(x.iter()) {
        *zi ^= gf_mul_y[xi as usize];
    }
}

// ============================================================================
// 公开接口：运行时自动选择最优 SIMD 路径
// ============================================================================

/// GF(2⁸) 向量乘加运算：z[i] ^= GF_MUL(x[i], y)
///
/// 运行时自动检测 CPU 指令集，选择最优实现：
/// - AVX2:  一次处理 32 字节（理论加速 ×32）
/// - SSSE3: 一次处理 16 字节（理论加速 ×16）
/// - 标量:  逐字节查表（回退方案）
pub fn addmul(z: &mut [u8], x: &[u8], y: u8) {
    if y == 0 {
        return;
    }

    // Safety: x 长度至少与 z 相同
    let x = &x[..z.len()];

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            // Safety: 已通过运行时检测确认 AVX2 可用
            unsafe { addmul_avx2(z, x, y); }
            return;
        }
        if is_x86_feature_detected!("ssse3") {
            // Safety: 已通过运行时检测确认 SSSE3 可用
            unsafe { addmul_ssse3(z, x, y); }
            return;
        }
    }

    // 标量回退
    addmul_scalar(z, x, y);
}

/// GfVal 版本的向量乘加运算（用于矩阵运算等场景）
pub fn addmul_gfval(z: &mut [GfVal], x: &[GfVal], y: GfVal) {
    if y.0 == 0 {
        return;
    }

    let x = &x[..z.len()];

    // GfVal 是 repr 透明的 u8 包装，可以安全地转换为 u8 切片进行 SIMD 运算
    // 由于 GfVal 是 #[derive(Copy, Clone)] 的 newtype(pub u8)，内存布局与 u8 相同
    let z_bytes = unsafe {
        std::slice::from_raw_parts_mut(z.as_mut_ptr() as *mut u8, z.len())
    };
    let x_bytes = unsafe {
        std::slice::from_raw_parts(x.as_ptr() as *const u8, x.len())
    };

    // 复用 SIMD 加速的 addmul
    addmul(z_bytes, x_bytes, y.0);
}

// ============================================================================
// 单元测试：验证 SIMD 实现的正确性
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// 标量参考实现，用于对比验证
    fn addmul_reference(z: &mut [u8], x: &[u8], y: u8) {
        let gf_mul_y = &GF_MUL_TABLE[y as usize];
        for (zi, &xi) in z.iter_mut().zip(x.iter()) {
            *zi ^= gf_mul_y[xi as usize];
        }
    }

    #[test]
    fn test_addmul_correctness_small() {
        // 小数据：验证基本正确性
        for y in 0..=255u8 {
            let x: Vec<u8> = (0..64).collect();
            let mut z_ref = vec![0u8; 64];
            let mut z_simd = vec![0u8; 64];

            addmul_reference(&mut z_ref, &x, y);
            addmul(&mut z_simd, &x, y);

            assert_eq!(z_ref, z_simd, "y={} 时 SIMD 结果与标量不一致", y);
        }
    }

    #[test]
    fn test_addmul_correctness_large() {
        // 大数据：覆盖 AVX2 主循环 + SSE 尾部 + 标量尾部
        let sizes = [1, 15, 16, 17, 31, 32, 33, 47, 48, 63, 64, 100, 255, 256, 1000, 4096];

        for &size in &sizes {
            let x: Vec<u8> = (0..size).map(|i| (i * 7 + 13) as u8).collect();

            for y in [0u8, 1, 2, 127, 128, 255] {
                let mut z_ref: Vec<u8> = (0..size).map(|i| (i * 3 + 5) as u8).collect();
                let mut z_simd = z_ref.clone();

                addmul_reference(&mut z_ref, &x, y);
                addmul(&mut z_simd, &x, y);

                assert_eq!(
                    z_ref, z_simd,
                    "size={}, y={} 时 SIMD 结果与标量不一致",
                    size, y
                );
            }
        }
    }

    #[test]
    fn test_addmul_gfval_correctness() {
        // 验证 GfVal 版本与标量版本一致
        let x_vals: Vec<GfVal> = (0..100).map(|i| GfVal((i * 11 + 3) as u8)).collect();
        let y = GfVal(42);

        let mut z_ref: Vec<GfVal> = (0..100).map(|i| GfVal((i * 5 + 7) as u8)).collect();
        let mut z_simd = z_ref.clone();

        // 标量参考
        let gf_mul_y = &GF_MUL_TABLE[y.0 as usize];
        for (zi, &GfVal(xi)) in z_ref.iter_mut().zip(x_vals.iter()) {
            zi.0 ^= gf_mul_y[xi as usize];
        }

        // SIMD 版本
        addmul_gfval(&mut z_simd, &x_vals, y);

        for i in 0..100 {
            assert_eq!(
                z_ref[i].0, z_simd[i].0,
                "index={} 时 GfVal SIMD 结果不一致",
                i
            );
        }
    }

    #[test]
    fn test_addmul_zero_multiplier() {
        let x = vec![1u8, 2, 3, 4, 5];
        let mut z = vec![10u8, 20, 30, 40, 50];
        let z_orig = z.clone();

        addmul(&mut z, &x, 0);
        assert_eq!(z, z_orig, "乘数为 0 时 z 不应被修改");
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn test_simd_feature_detection() {
        // 验证 SIMD 指令集检测正常工作
        println!("SSSE3 支持: {}", is_x86_feature_detected!("ssse3"));
        println!("AVX2 支持:  {}", is_x86_feature_detected!("avx2"));

        // 无论哪种路径，结果都应正确
        let x: Vec<u8> = (0..128).map(|i| i as u8).collect();
        let mut z = vec![0u8; 128];
        addmul(&mut z, &x, 0x53);

        let mut z_ref = vec![0u8; 128];
        addmul_reference(&mut z_ref, &x, 0x53);
        assert_eq!(z, z_ref);
    }
}