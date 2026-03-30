use serde::{Deserialize, Serialize};

/// RBC协议消息类型
///
/// 严格对应论文算法4：Four-round RBC protocol for long messages
/// 消息格式：
///   - PROPOSE: ⟨PROPOSE, M⟩
///   - ECHO:    ⟨ECHO, m_j, h⟩
///   - READY:   ⟨READY, m_i, h⟩
///
/// 注意：ECHO和READY消息中**不包含shard_hash**。
/// 因为底层RS码已具备Berlekamp-Welch纠错能力，恶意分片会被自动纠正，
/// 最终通过 hash(M') == h 的整体哈希验证确保正确性。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RbcMessage {
    /// 第1轮：提议（PROPOSE）
    /// 广播者向所有节点发送完整消息 M
    Propose {
        /// 广播实例唯一标识（支持多个并发广播）
        instance_id: String,
        /// 广播者节点ID
        broadcaster: String,
        /// 完整消息数据
        data: Vec<u8>,
    },

    /// 第2轮：回声（ECHO）— 算法第6-10行
    /// 节点收到 M 后：h := hash(M)，RS编码为 n 个分片，
    /// 向节点 j 发送 ⟨ECHO, m_j, h⟩
    Echo {
        /// 广播实例唯一标识
        instance_id: String,
        /// 发送者节点ID
        sender: String,
        /// 原始数据的SHA-256哈希 h = hash(M)
        data_hash: String,
        /// 目标节点专属的分片索引 j
        shard_index: usize,
        /// 目标节点专属的分片数据 m_j
        shard_data: Vec<u8>,
        /// 原始数据大小（解码时需要）
        original_size: usize,
        /// 数据分片数量 k = t+1（RS编码参数）
        data_shard_count: usize,
        /// 校验分片数量 n-k（RS编码参数）
        parity_shard_count: usize,
    },

    /// 第3轮：就绪（READY）— 算法第11-15行
    /// 当节点 i 收到 2t+1 个匹配的 ⟨ECHO, m_i, h⟩ 时，
    /// 确立自己的分片 m_i，并向全网广播 ⟨READY, m_i, h⟩
    Ready {
        /// 广播实例唯一标识
        instance_id: String,
        /// 发送者节点ID（即节点 j）
        sender: String,
        /// 原始数据的SHA-256哈希 h
        data_hash: String,
        /// 发送者自己的分片索引（= sender在节点列表中的索引）
        shard_index: usize,
        /// 发送者自己的分片数据 m_j
        shard_data: Vec<u8>,
        /// 原始数据大小
        original_size: usize,
        /// 数据分片数量
        data_shard_count: usize,
        /// 校验分片数量
        parity_shard_count: usize,
    },
}

/// RBC协议配置
#[derive(Debug, Clone)]
pub struct RbcConfig {
    /// 网络中的总节点数 n
    pub total_nodes: usize,
    /// 最大容错拜占庭节点数 t（需满足 n >= 3t + 1）
    pub fault_tolerance: usize,
    /// RS编码的数据分片数（= t + 1，算法中的 RSEnc(M_i, n, t+1)）
    pub data_shards: usize,
    /// RS编码的校验分片数（= n - data_shards）
    pub parity_shards: usize,
}

impl RbcConfig {
    /// 根据总节点数创建RBC配置
    ///
    /// # 参数
    /// - `total_nodes`: 网络中的总节点数 n（需 >= 4）
    ///
    /// # 计算规则
    /// - t = (n - 1) / 3  （最大容错拜占庭节点数）
    /// - data_shards = t + 1（RS编码参数，对应算法中 RSEnc(M, n, t+1)）
    /// - parity_shards = n - data_shards
    pub fn new(total_nodes: usize) -> Result<Self, String> {
        if total_nodes < 4 {
            return Err(format!(
                "节点数必须 >= 4 以满足 n >= 3t+1，当前: {}",
                total_nodes
            ));
        }

        let fault_tolerance = (total_nodes - 1) / 3; // t = floor((n-1)/3)
        let data_shards = fault_tolerance + 1; // t + 1
        let parity_shards = total_nodes - data_shards; // n - (t+1)

        Ok(Self {
            total_nodes,
            fault_tolerance,
            data_shards,
            parity_shards,
        })
    }

    /// 根据总节点数和自定义数据分片数创建RBC配置
    ///
    /// 用于切片策略对比实验，允许自定义 data_shards (k) 值。
    ///
    /// # 参数
    /// - `total_nodes`: 网络中的总节点数 n（需 >= 4）
    /// - `data_shards`: 自定义数据分片数 k（需满足 1 <= k < n）
    ///
    /// # 注意
    /// 自定义 k 值可能不满足 BFT 安全性要求（标准要求 k = t+1），
    /// 仅用于实验对比传输开销与计算开销。
    pub fn with_custom_shards(total_nodes: usize, data_shards: usize) -> Result<Self, String> {
        if total_nodes < 4 {
            return Err(format!(
                "节点数必须 >= 4 以满足 n >= 3t+1，当前: {}",
                total_nodes
            ));
        }
        if data_shards == 0 || data_shards >= total_nodes {
            return Err(format!(
                "数据分片数必须满足 1 <= k < n，当前: k={}, n={}",
                data_shards, total_nodes
            ));
        }

        let fault_tolerance = (total_nodes - 1) / 3;
        let parity_shards = total_nodes - data_shards;

        Ok(Self {
            total_nodes,
            fault_tolerance,
            data_shards,
            parity_shards,
        })
    }

    /// ECHO消息的确认阈值：2t + 1
    pub fn echo_threshold(&self) -> usize {
        2 * self.fault_tolerance + 1
    }

    /// READY消息的放大阈值：t + 1
    pub fn ready_amplify_threshold(&self) -> usize {
        self.fault_tolerance + 1
    }

    /// 重建所需的最小分片数（尝试纠错时的阈值）：2t + r + 1
    /// 其中 r 从 0 到 t 递增尝试
    pub fn reconstruct_threshold(&self, error_count: usize) -> usize {
        2 * self.fault_tolerance + error_count + 1
    }

    /// 获取容错描述信息
    pub fn info(&self) -> String {
        format!(
            "RBC配置: n={}, t={}, RS({},{}), ECHO阈值={}, READY放大阈值={}",
            self.total_nodes,
            self.fault_tolerance,
            self.data_shards,
            self.parity_shards,
            self.echo_threshold(),
            self.ready_amplify_threshold(),
        )
    }
}

/// RBC广播实例的状态
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RbcInstanceState {
    /// 初始状态，等待PROPOSE
    Init,
    /// 已收到PROPOSE，正在收集ECHO
    EchoPhase,
    /// 已发送READY，正在收集READY
    ReadyPhase,
    /// 已成功输出消息
    Completed,
    /// 协议失败
    Failed(String),
}

/// RBC协议的输出结果
#[derive(Debug, Clone)]
pub struct RbcOutput {
    /// 广播实例ID
    pub instance_id: String,
    /// 广播者节点ID
    pub broadcaster: String,
    /// 恢复的原始消息
    pub data: Vec<u8>,
    /// 消息哈希
    pub data_hash: String,
}

// ============================================================================
// 超大文件分块广播相关类型
// ============================================================================

/// 默认分块大小：4MB（确保单条消息在16MB限制内，PROPOSE消息包含完整块数据）
pub const DEFAULT_CHUNK_SIZE: usize = 4 * 1024 * 1024;

/// 分块广播元信息
/// 广播者在发起分块广播前，先通过RBC广播此元信息，
/// 让所有节点知道即将接收的文件的分块数量和校验信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkedBroadcastMeta {
    /// 分块广播会话唯一标识
    pub session_id: String,
    /// 原始文件总大小（字节）
    pub total_size: usize,
    /// 分块大小（字节）
    pub chunk_size: usize,
    /// 总分块数
    pub total_chunks: usize,
    /// 原始文件的SHA-256哈希（用于最终完整性校验）
    pub file_hash: String,
    /// 每个分块的SHA-256哈希列表（按顺序）
    pub chunk_hashes: Vec<String>,
}

/// 分块广播的输出结果（完整文件重组后）
#[derive(Debug, Clone)]
pub struct ChunkedBroadcastOutput {
    /// 分块广播会话ID
    pub session_id: String,
    /// 恢复的完整文件数据
    pub data: Vec<u8>,
    /// 文件SHA-256哈希
    pub file_hash: String,
    /// 文件总大小
    pub total_size: usize,
    /// 分块数量
    pub total_chunks: usize,
}