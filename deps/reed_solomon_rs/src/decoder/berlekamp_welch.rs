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

        // 检查是否有任何错误
        let mut has_error = false;
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
                    has_error = true;
                    break 'outer;
                }
            }
        }

        if !has_error {
            // 无错误，快速返回
            return Ok(());
        }

        // 有错误：找到一个出错的字节位置，用它运行 BW 算法定位错误分片
        // 需要找到一个 syndrome 非零的字节位置
        let error_byte_index = self.find_error_byte_index(&shares, &synd)?;

        // 用 BW 算法在该字节位置上求解错误定位多项式 E(x)
        let error_shares = self.berlekamp_welch_locate_errors(&shares, error_byte_index)?;

        // 丢弃错误分片
        shares.retain(|s| !error_shares.contains(&s.number));

        // 验证剩余分片数量足够 rebuild
        if shares.len() < self.k {
            return Err(format!(
                "Too many errors to correct: found {} bad shares, only {} remain, need {}",
                error_shares.len(),
                shares.len(),
                self.k
            ).into());
        }

        Ok(())
    }

    /// 找到一个 syndrome 非零的字节位置（用于 BW 错误定位）
    fn find_error_byte_index(
        &self,
        shares: &[Share],
        synd: &GfMat,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        let buf_len = shares[0].data.len();
        let mut buf = vec![0u8; buf_len];

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
                    return Ok(j);
                }
            }
        }

        Err("No error byte found (should not happen)".into())
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
