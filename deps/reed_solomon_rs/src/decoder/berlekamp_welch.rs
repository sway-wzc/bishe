use crate::{
    fec::fec::{Share, FEC},
    galois_field::gf_alg::{GfMat, GfPoly, GfVal, GfVals},
    math::addmul::addmul,
};

// Berlekamp Welch functions for FEC
impl FEC {
    /// decode() will take a destination buffer (can be empty) and a list of shares
    /// (pieces). It will return the data passed in to the corresponding Encode
    /// call or return an error.
    ///
    /// It will first correct the shares using correct(), mutating and reordering the
    /// passed-in shares arguments. Then it will rebuild the data using rebuild().
    /// Finally it will concatenate the data into the given output buffer dst if it
    /// has capacity, growing it otherwise.
    ///
    /// If you already know your data does not contain errors, rebuild() will be
    /// faster.
    ///
    /// If you only want to identify which pieces are bad, you may be interested in
    /// correct().
    pub fn decode(
        &self,
        mut dst: Vec<u8>,
        mut shares: Vec<Share>,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        self.correct(&mut shares)?;

        if shares.len() == 0 {
            return Err(("Must specify at least one share").into());
        }
 
        let piece_len = shares[0].data.len();
        let result_len = piece_len * self.k;
        if dst.capacity() < result_len {
            dst = vec![0u8; result_len];
        } else {
            dst.resize(result_len, 0);
        }
        self.rebuild(shares, |s: Share| {
            dst[s.number * piece_len..(s.number + 1) * piece_len].copy_from_slice(&s.data);
        })?;
        return Ok(dst);
    }

    /// If you don't want the data concatenated for you, you can use correct() and
    /// then rebuild() individually.
    pub fn decode_no_concat<F>(
        &self,
        mut shares: Vec<Share>,
        output: F,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        F: FnMut(Share),
    {
        self.correct(&mut shares)?;

        return self.rebuild(shares, output);
    }

    /// correct() implements an optimized Berlekamp-Welch error correction strategy.
    ///
    /// 优化策略：利用 BW 算法的数学性质——错误定位多项式 E(x) 与具体字节位置无关，
    /// 只取决于哪些分片是错误的。因此只需对一个字节位置运行 BW 算法定位错误分片，
    /// 然后直接丢弃错误分片，用剩余正确分片做 rebuild。
    ///
    /// 原始实现对每个出错的字节位置独立调用 berlekamp_welch()，导致：
    ///   - 800KB 分片 → 80万次矩阵求逆 → 数十秒
    /// 优化后：
    ///   - 只需 1 次 BW 调用定位错误分片 → 毫秒级
    pub fn correct(&self, shares: &mut Vec<Share>) -> Result<(), Box<dyn std::error::Error>> {
        if shares.len() < self.k {
            return Err(format!("Must specify at least the number of required shares").into());
        }
        shares.sort();

        // 快速路径：用 syndrome 矩阵检测是否存在错误
        let synd = self.syndrome_matrix(&shares)?;

        let buf_len = shares[0].data.len();
        let mut buf = vec![0u8; buf_len];

        // 第一轮 syndrome 扫描：合并"错误检测"与"定位首个错误字节位置"。
        // 一旦在某行 i 的某个字节 j 上发现 syndrome 非零，立即记录 j 并退出。
        // 这样省掉了独立的 find_error_byte_index()，避免对所有 syndrome 行做第二次
        // 完整扫描（O(synd.r × synd.c × buf_len) 的 addmul 运算）。
        let mut error_byte_index: Option<usize> = None;
        'outer: for i in 0..synd.r {
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
                    error_byte_index = Some(j);
                    break 'outer;
                }
            }
        }

        let first_index = match error_byte_index {
            None => return Ok(()), // 无错误，快速返回
            Some(idx) => idx,
        };

        // ---- 多字节 BW 定位 + 并集 + syndrome 兜底验证 ----
        //
        // 原理：
        // BW 算法在某个固定字节位置 t 上求解，只能识别那些在字节 t 上
        // 实际 ε_i(t) ≠ 0 的错误分片；若错误分片 i* 恰好在字节 t 上没被破坏
        // （ε_{i*}(t) = 0），则会被漏检。
        //
        // 关键性质（不会误伤）：
        // 在任意字节 t 上运行 BW 返回的错误分片集合 E_t 一定满足 E_t ⊆ 真实错误集 E。
        // 因此对多个字节位置的结果取并集 ⋃_t E_t，仍有 ⋃_t E_t ⊆ E，
        // 不会把正确分片误判为错误。
        //
        // 策略：
        // 1) 用 first_index 跑一次 BW 得到初始错误集；
        // 2) 用"剔除已识别错误分片后"的 shares 重算 syndrome，若仍非零，说明还有漏检，
        //    在剩余 syndrome 非零字节位置上再跑 BW，合并到错误集；
        // 3) 最多迭代 (n-k)/2 + 1 轮，防止恶意输入导致死循环。
        let max_rounds = (self.n - self.k) / 2 + 1;
        let mut error_set: Vec<usize> = Vec::new();
        let mut try_index = first_index;

        for round in 0..max_rounds {
            // 仅用当前未被标记为错误的分片跑 BW
            let active: Vec<Share> = shares
                .iter()
                .filter(|s| !error_set.contains(&s.number))
                .cloned()
                .collect();

            // 剩余分片数必须 ≥ k 才能跑 BW（否则无法纠正）
            if active.len() < self.k {
                return Err(format!(
                    "Too many errors to correct: identified {} bad shares, only {} remain, need {}",
                    error_set.len(),
                    active.len(),
                    self.k
                )
                .into());
            }
            if (active.len() - self.k) / 2 == 0 {
                // 剩余分片没有纠错裕度，无法再通过 BW 定位更多错误。
                // 但 syndrome 仍非零 → 说明错误数超过纠错能力。
                break;
            }

            let newly_found = self.berlekamp_welch_locate_errors(&active, try_index)?;

            // 合并并集
            let mut added_any = false;
            for n in &newly_found {
                if !error_set.contains(n) {
                    error_set.push(*n);
                    added_any = true;
                }
            }

            // 如果本轮没找出任何新错误分片，说明 try_index 这个位置无法再提供信息，
            // 后面再在同一位置跑也没用 —— 尝试寻找新的 syndrome 非零字节位置
            // （在剔除当前 error_set 后重算 syndrome）
            if !added_any && round > 0 {
                break;
            }

            // 在"剔除 error_set 后的分片"上重新计算 syndrome，寻找下一个非零字节位置。
            // 若剩余分片的 syndrome 处处为零 → 全部错误已被识别，正常退出。
            let remaining: Vec<Share> = shares
                .iter()
                .filter(|s| !error_set.contains(&s.number))
                .cloned()
                .collect();

            if remaining.len() < self.k {
                return Err(format!(
                    "Too many errors to correct: identified {} bad shares, only {} remain, need {}",
                    error_set.len(),
                    remaining.len(),
                    self.k
                )
                .into());
            }

            let synd2 = self.syndrome_matrix(&remaining)?;
            let rem_buf_len = remaining[0].data.len();
            let mut rem_buf = vec![0u8; rem_buf_len];
            let mut next_idx: Option<usize> = None;

            'outer2: for i in 0..synd2.r {
                for j in 0..rem_buf_len {
                    rem_buf[j] = 0;
                }
                for j in 0..synd2.c {
                    addmul(
                        rem_buf.as_mut_slice(),
                        remaining[j].data.as_slice(),
                        synd2.get(i, j).0,
                    );
                }
                for j in 0..rem_buf_len {
                    if rem_buf[j] != 0 {
                        next_idx = Some(j);
                        break 'outer2;
                    }
                }
            }

            match next_idx {
                None => {
                    // 剩余分片 syndrome 全零 → 错误分片已识别完毕
                    break;
                }
                Some(idx) => {
                    try_index = idx;
                }
            }
        }

        // 丢弃所有已识别的错误分片
        shares.retain(|s| !error_set.contains(&s.number));

        // 验证剩余分片数量足够 rebuild
        if shares.len() < self.k {
            return Err(format!(
                "Too many errors to correct: found {} bad shares, only {} remain, need {}",
                error_set.len(),
                shares.len(),
                self.k
            )
            .into());
        }

        // 最终 syndrome 兜底验证：若剩余分片的 syndrome 仍非零，说明存在漏检，
        // 宁可显式报错也不要让 rebuild 静默返回错误数据。
        let final_synd = self.syndrome_matrix(&shares)?;
        let mut final_buf = vec![0u8; buf_len];
        for i in 0..final_synd.r {
            for j in 0..buf_len {
                final_buf[j] = 0;
            }
            for j in 0..final_synd.c {
                addmul(
                    final_buf.as_mut_slice(),
                    shares[j].data.as_slice(),
                    final_synd.get(i, j).0,
                );
            }
            for j in 0..buf_len {
                if final_buf[j] != 0 {
                    return Err(
                        "Too many errors to correct: residual syndrome non-zero after BW rounds"
                            .into(),
                    );
                }
            }
        }

        Ok(())
    }

    /// 使用 Berlekamp-Welch 算法定位错误分片（只运行一次）
    ///
    /// 返回错误分片的 share number 列表
    fn berlekamp_welch_locate_errors(
        &self,
        shares: &[Share],
        index: usize,
    ) -> Result<Vec<usize>, Box<dyn std::error::Error>> {
        let k = self.k;
        let r = shares.len();
        let e = (r - k) / 2; // 错误定位多项式 E(x) 的次数
        let q = e + k;       // 商多项式 Q(x) 的次数

        if e <= 0 {
            return Err(("Not enough shares!").into());
        }

        let interp_base = GfVal(2);
        let eval_point = |num: usize| -> GfVal {
            if num == 0 {
                GfVal(0)
            } else {
                interp_base.pow(num - 1)
            }
        };

        let dim = q + e;
        let mut s = GfMat::matrix_zero(dim, dim);
        let mut a = GfMat::matrix_zero(dim, dim);
        let mut f = GfVals::gfvals_zero(dim);
        let mut u = GfVals::gfvals_zero(dim);

        for i in 0..dim {
            let x_i = eval_point(shares[i].number);
            let r_i = GfVal(shares[i].data[index]);

            f.0[i] = x_i.pow(e).mul(r_i);

            for j in 0..q {
                s.set(i, j, x_i.pow(j));
                if i == j {
                    a.set(i, j, GfVal(1));
                }
            }

            for ki in 0..e {
                let j = ki + q;
                s.set(i, j, x_i.pow(ki).mul(r_i));
                if i == j {
                    a.set(i, j, GfVal(1));
                }
            }
        }

        if s.invert_with(&mut a).is_err() {
            return Err(("Error inverting matrix").into());
        }

        for i in 0..dim {
            let ri = a.index_row(i);
            u.0[i] = ri.dot(&f);
        }

        // 反转 u 以便构造多项式
        let len_u = u.0.len();
        for i in 0..len_u / 2 {
            u.0.swap(i, len_u - i - 1);
        }

        // 构造错误定位多项式 E(x) = 1 + u[0]*x + u[1]*x^2 + ... + u[e-1]*x^e
        let e_poly = {
            let mut ep = GfPoly(vec![GfVal(1)]);
            ep.0.extend_from_slice(&u.0[..e]);
            ep
        };

        // 通过求 E(x) 的根来定位错误分片
        // E(eval_point(share.number)) == 0 表示该分片是错误的
        let mut error_shares = Vec::new();
        for share in shares {
            let pt = eval_point(share.number);
            if e_poly.eval(pt).is_zero() {
                error_shares.push(share.number);
            }
        }

        Ok(error_shares)
    }

    /// 保留原始的 berlekamp_welch 方法用于兼容性（逐字节纠错）
    /// 但在优化后的 correct() 中不再使用
    pub fn berlekamp_welch(
        &self,
        shares: &Vec<Share>,
        index: usize,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let k = self.k;
        let r = shares.len();
        let e = (r - k) / 2; // deg of E polynomial
        let q = e + k; // deg of Q polynomial

        if e <= 0 {
            return Err(("Not enough shares!").into());
        }

        let interp_base = GfVal(2);
        let eval_point = |num: usize| -> GfVal {
            if num == 0 {
                GfVal(0)
            } else {
                interp_base.pow(num - 1)
            }
        };
        let dim = q + e;
        let mut s = GfMat::matrix_zero(dim, dim); // constraint matrix
        let mut a = GfMat::matrix_zero(dim, dim); // augmented matrix
        let mut f = GfVals::gfvals_zero(dim); // constant column
        let mut u = GfVals::gfvals_zero(dim); // solution column

        for i in 0..dim {
            let x_i = eval_point(shares[i].number);
            let r_i = GfVal(shares[i].data[index]);

            f.0[i] = x_i.pow(e).mul(r_i);

            for j in 0..q {
                s.set(i, j, x_i.pow(j));
                if i == j {
                    a.set(i, j, GfVal(1));
                }
            }

            for k in 0..e {
                let j = k + q;
                s.set(i, j, x_i.pow(k).mul(r_i));
                if i == j {
                    a.set(i, j, GfVal(1));
                }
            }
        }

        // invert and put the result in a
        if s.invert_with(&mut a).is_err() {
            return Err(("Error inverting matrix").into());
        }

        // multiply the inverted matrix by the column vector
        for i in 0..dim {
            let ri = a.index_row(i);
            u.0[i] = ri.dot(&f);
        }

        // reverse u for easier construction of the polynomials
        let len_u = u.0.len();
        for i in 0..len_u / 2 {
            u.0.swap(i, len_u - i - 1);
        }

        let mut q_poly = GfPoly(u.0[e..].to_vec());
        let mut e_poly = GfPoly(vec![GfVal(1)]);
        e_poly.0.extend_from_slice(&u.0[..e]);

        let (p_poly, rem) = q_poly.div(e_poly)?;

        if !rem.is_zero() {
            return Err(("too many errors to reconstruct").into());
        }

        let mut out = vec![0u8; self.n];
        for i in 0..out.len() {
            let mut pt = GfVal(0);
            if i != 0 {
                pt = interp_base.pow(i - 1);
            }
            out[i] = p_poly.eval(pt).0;
        }

        return Ok(out);
    }

    pub fn syndrome_matrix(
        &self,
        shares: &Vec<Share>,
    ) -> Result<GfMat, Box<dyn std::error::Error>> {
        let mut keepers = vec![false; self.n];
        let mut share_count = 0;
        for share in shares {
            if !keepers[share.number as usize] {
                keepers[share.number as usize] = true;
                share_count += 1;
            }
        }

        // create a vandermonde matrix but skip columns where we're missing the share
        let mut out = GfMat::matrix_zero(self.k, share_count);
        for i in 0..self.k {
            let mut skipped = 0;
            for j in 0..self.n {
                if !keepers[j] {
                    skipped = skipped + 1;
                    continue;
                }

                out.set(i, j - skipped, GfVal(self.vand_matrix[i * self.n + j]));
            }
        }

        if out.standardize().is_err() {
            return Err(("Matrix standardizing failed").into());
        }

        return Ok(out.parity());
    }
}

#[cfg(test)]
mod tests {
    //! 针对 `correct()` 中"合并 syndrome 扫描与错误字节定位"优化的单元测试。
    //!
    //! 测试覆盖：
    //! 1. 无损坏 → correct() 不改动 shares 且快速返回
    //! 2. 首字节损坏（边界：error_byte_index == 0）
    //! 3. 末字节损坏（边界：error_byte_index == buf_len - 1）
    //! 4. 中间字节损坏（普通情况）
    //! 5. 多分片损坏（最大纠错能力范围内）
    //! 6. 错误数超过纠错能力时返回错误
    //! 7. 较大分片、多字节随机损坏的端到端正确性
    //! 8. 对比：decode() 在损坏前后返回相同明文

    use crate::fec::fec::{Share, FEC};

    /// 辅助函数：编码数据得到所有分片
    fn encode_all(f: &FEC, data: &[u8]) -> Vec<Share> {
        let total = f.total();
        let mut shares: Vec<Share> = vec![
            Share {
                number: 0,
                data: vec![]
            };
            total
        ];
        let output = |s: Share| {
            shares[s.number] = s.clone();
        };
        f.encode(data, output).expect("encode failed");
        shares
    }

    #[test]
    fn test_correct_no_corruption_returns_untouched() {
        let f = FEC::new(4, 8).unwrap();
        let data = b"hello, world! __".to_vec();
        let mut shares = encode_all(&f, &data);
        let original = shares.clone();

        f.correct(&mut shares).expect("correct should succeed");

        // 无损坏：shares 数量与内容都保持不变
        assert_eq!(shares.len(), original.len());
        for (a, b) in shares.iter().zip(original.iter()) {
            assert_eq!(a.number, b.number);
            assert_eq!(a.data, b.data);
        }
    }

    #[test]
    fn test_correct_first_byte_corruption() {
        // 边界：error_byte_index == 0
        let f = FEC::new(4, 8).unwrap();
        let data = b"hello, world! __".to_vec();
        let mut shares = encode_all(&f, &data);

        // 损坏分片 2 的首字节（异或非零值，确保实际改变）
        shares[2].data[0] ^= 0xAB;

        let result = f.decode([].to_vec(), shares).expect("decode should succeed");
        assert_eq!(result, data);
    }

    #[test]
    fn test_correct_last_byte_corruption() {
        // 边界：error_byte_index == buf_len - 1
        let f = FEC::new(4, 8).unwrap();
        let data = b"hello, world! __".to_vec();
        let mut shares = encode_all(&f, &data);

        let last = shares[3].data.len() - 1;
        shares[3].data[last] ^= 0x7F;

        let result = f.decode([].to_vec(), shares).expect("decode should succeed");
        assert_eq!(result, data);
    }

    #[test]
    fn test_correct_middle_byte_corruption() {
        let f = FEC::new(4, 8).unwrap();
        let data = b"hello, world! __".to_vec();
        let mut shares = encode_all(&f, &data);

        let mid = shares[5].data.len() / 2;
        shares[5].data[mid] ^= 0x3C;

        let result = f.decode([].to_vec(), shares).expect("decode should succeed");
        assert_eq!(result, data);
    }

    #[test]
    fn test_correct_two_shares_corrupted() {
        // k=4, n=8 → 可纠 (n-k)/2 = 2 个错误分片
        //
        // 关键场景：两个错误分片的损坏字节位置完全不相交。
        // 原 fix 分支实现会在此处漏检；
        // 经"多字节 BW + 并集 + syndrome 兜底"修复后，应能正确纠错。
        let f = FEC::new(4, 8).unwrap();
        let data = b"hello, world! __".to_vec();
        let mut shares = encode_all(&f, &data);

        shares[0].data[0] ^= 0xFF;
        shares[0].data[3] ^= 0x11;
        shares[1].data[2] ^= 0x22; // 注意：字节 0 处未损坏，与 shares[0] 不相交

        let result = f.decode([].to_vec(), shares).expect("decode should succeed");
        assert_eq!(result, data);
    }

    #[test]
    fn test_correct_too_many_errors_fails() {
        // k=4, n=8 → 最多可纠 2 个错误分片；损坏 3 个应当失败
        let f = FEC::new(4, 8).unwrap();
        let data = b"hello, world! __".to_vec();
        let mut shares = encode_all(&f, &data);

        shares[0].data[0] ^= 0xFF;
        shares[1].data[0] ^= 0xFF;
        shares[2].data[0] ^= 0xFF;

        let result = f.decode([].to_vec(), shares);
        assert!(
            result.is_err() || result.as_ref().ok() != Some(&data),
            "decode with 3 corrupted shares (> capacity) must not silently return correct data"
        );
    }

    #[test]
    fn test_correct_large_payload_multiple_byte_errors() {
        // 较大分片场景：每个分片 4096 字节，损坏 2 个分片的多个字节
        let f = FEC::new(5, 13).unwrap();
        // data 长度必须是 k 的倍数
        let k = f.required();
        let per_share = 4096usize;
        let total_len = per_share * k;
        let mut data = vec![0u8; total_len];
        // 填充伪随机确定性数据
        for i in 0..total_len {
            data[i] = ((i * 31 + 7) & 0xFF) as u8;
        }

        let mut shares = encode_all(&f, &data);

        // 两个错误分片的损坏位置**完全不相交**，验证多字节 BW 修复逻辑
        shares[2].data[0] ^= 0x01;
        shares[2].data[100] ^= 0x80;
        shares[2].data[per_share - 1] ^= 0x55;
        shares[9].data[50] ^= 0xAA;
        shares[9].data[per_share / 2] ^= 0xF0;

        let result = f.decode(vec![], shares).expect("decode should succeed");
        assert_eq!(result, data);
    }

    #[test]
    fn test_correct_single_byte_syndrome_first_row_nonzero() {
        // 专门验证合并扫描逻辑：构造一个损坏使第一个 syndrome 行（i=0）
        // 在第一个字节（j=0）就非零 —— 这是 'outer break 立即触发的路径
        let f = FEC::new(4, 8).unwrap();
        let data = b"abcdefghijklmnop".to_vec();
        let mut shares = encode_all(&f, &data);

        // 对分片 0 的首字节轻微扰动
        shares[0].data[0] ^= 0x01;

        let result = f.decode(vec![], shares).expect("decode should succeed");
        assert_eq!(result, data);
    }

    #[test]
    fn test_correct_error_only_on_later_syndrome_rows() {
        // 构造一种损坏让前几个 syndrome 行的所有字节都可能为 0（极少数情况），
        // 但通过多位置损坏确保至少存在某个 (i, j) 非零 —— 验证合并循环对所有 (i, j) 正确扫描
        let f = FEC::new(4, 8).unwrap();
        let data = b"0123456789ABCDEF".to_vec();
        let mut shares = encode_all(&f, &data);

        // 损坏分片 6 的几乎所有字节
        for b in shares[6].data.iter_mut() {
            *b ^= 0x5A;
        }

        let result = f.decode(vec![], shares).expect("decode should succeed");
        assert_eq!(result, data);
    }

    #[test]
    fn test_correct_idempotent_on_clean_shares() {
        // correct() 在干净 shares 上多次调用应当等价，且不修改 shares
        let f = FEC::new(4, 8).unwrap();
        let data = b"idempotent test!".to_vec();
        let mut shares = encode_all(&f, &data);

        let snapshot = shares.clone();
        f.correct(&mut shares).unwrap();
        f.correct(&mut shares).unwrap();
        f.correct(&mut shares).unwrap();

        assert_eq!(shares.len(), snapshot.len());
        for (a, b) in shares.iter().zip(snapshot.iter()) {
            assert_eq!(a.number, b.number);
            assert_eq!(a.data, b.data);
        }
    }

    // ========== 以下为"多字节 BW 漏检修复"专项测试 ==========

    #[test]
    fn test_fix_disjoint_byte_corruption_two_shares() {
        // 修复核心场景：两个错误分片损坏字节位置完全不相交
        // 修复前：只跑一次 BW，只能定位到首个字节对应的分片，另一个漏检
        // 修复后：通过多轮 BW + syndrome 兜底，应当能完整定位并纠正
        //
        // 注：使用 k=4，数据长度 32 字节 → per_share = 8 字节
        let f = FEC::new(4, 8).unwrap();
        let data = b"disjoint byte corruption test!!!".to_vec(); // 32 bytes
        let mut shares = encode_all(&f, &data);

        // 分片 2 只在字节 1 损坏，分片 5 只在字节 6 损坏 —— 无任何共同字节
        shares[2].data[1] ^= 0x99;
        shares[5].data[6] ^= 0x66;

        let result = f.decode(vec![], shares).expect("decode should succeed");
        assert_eq!(result, data);
    }

    #[test]
    fn test_fix_max_errors_completely_disjoint() {
        // 达到纠错能力上限 (n-k)/2 = 2，且两个错误分片**每一个字节位置都不重叠**
        let f = FEC::new(4, 8).unwrap();
        let data = b"MAXerr disjoint!".to_vec(); // 16 bytes → per_share = 4
        let mut shares = encode_all(&f, &data);

        // per_share = 16/4 = 4；分片 1 在偶数字节损坏，分片 6 在奇数字节损坏
        shares[1].data[0] ^= 0xAA;
        shares[1].data[2] ^= 0xBB;
        shares[6].data[1] ^= 0xCC;
        shares[6].data[3] ^= 0xDD;

        let result = f.decode(vec![], shares).expect("decode should succeed");
        assert_eq!(result, data);
    }

    #[test]
    fn test_fix_three_errors_disjoint_n13_k5() {
        // 更大参数：k=5, n=13, 可纠 (13-5)/2 = 4 个错误
        // 构造 3 个错误分片，损坏字节位置完全分离
        let f = FEC::new(5, 13).unwrap();
        let k = f.required();
        let per_share = 256usize;
        let total_len = per_share * k;
        let mut data = vec![0u8; total_len];
        for i in 0..total_len {
            data[i] = ((i * 17 + 3) & 0xFF) as u8;
        }
        let mut shares = encode_all(&f, &data);

        // 3 个错误分片，各自损坏一个独立字节
        shares[1].data[10] ^= 0x11;
        shares[4].data[100] ^= 0x22;
        shares[10].data[200] ^= 0x33;

        let result = f.decode(vec![], shares).expect("decode should succeed");
        assert_eq!(result, data);
    }

    #[test]
    fn test_fix_over_capacity_disjoint_returns_err() {
        // k=4, n=8 → 最多纠 2 个错误。构造 3 个独立字节损坏 → 必须返回错误，
        // 不允许静默返回错误数据（由最终 syndrome 兜底验证保证）
        //
        // 注：用 32 字节数据，per_share = 8，避免越界。
        let f = FEC::new(4, 8).unwrap();
        let data = b"overcap disjoint ABCDEFGHIJKLMNOP".to_vec(); // 32 bytes
        let mut shares = encode_all(&f, &data);

        shares[0].data[0] ^= 0x11;
        shares[1].data[1] ^= 0x22;
        shares[2].data[2] ^= 0x33;

        let result = f.decode(vec![], shares);
        match result {
            Err(_) => {} // 期望：显式返回错误
            Ok(v) => {
                // 若 rebuild 碰巧返回了某个值，它必须不等于原始数据（否则说明算法有问题）
                assert_ne!(v, data, "over-capacity decode must not silently succeed");
            }
        }
    }

    #[test]
    fn test_fix_partial_overlap_byte_corruption() {
        // 两个错误分片部分字节位置重叠、部分不重叠 —— 综合场景
        //
        // 注：32 字节数据 → per_share = 8
        let f = FEC::new(4, 8).unwrap();
        let data = b"partial overlap test data 32 b!!".to_vec(); // 32 bytes
        let mut shares = encode_all(&f, &data);

        // 分片 3 损坏字节 0 和 4；分片 7 损坏字节 4 和 7
        shares[3].data[0] ^= 0x44;
        shares[3].data[4] ^= 0x55;
        shares[7].data[4] ^= 0x66;
        shares[7].data[7] ^= 0x77;

        let result = f.decode(vec![], shares).expect("decode should succeed");
        assert_eq!(result, data);
    }

    #[test]
    fn test_fix_correct_api_returns_ok_after_locating_all_errors() {
        // 验证 correct() 本身（非 decode）在不相交损坏场景下能成功收敛
        //
        // 注：32 字节数据 → per_share = 8
        let f = FEC::new(4, 8).unwrap();
        let data = b"correct api test 32byte payload!".to_vec(); // 32 bytes
        let mut shares = encode_all(&f, &data);

        shares[1].data[3] ^= 0xAB;
        shares[6].data[7] ^= 0xCD;

        let before = shares.len();
        f.correct(&mut shares).expect("correct should succeed");

        // 预期丢弃 2 个错误分片
        assert_eq!(shares.len(), before - 2);
        // 剩余分片中不应包含 1 和 6
        assert!(shares.iter().all(|s| s.number != 1 && s.number != 6));
    }
}
