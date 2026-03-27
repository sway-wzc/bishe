use serde::{Deserialize, Serialize};

/// RBC协议消息类型
///
/// 实现四轮长消息RBC协议（融合优化方案），将ADD的"编码与分发"
/// 过程融入到Bracha RBC的投票（ECHO和READY）消息中。
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

    /// 第2轮：回声（ECHO）— 融合编码与分发
    /// 节点收到 M 后计算 h=hash(M)，用RS码编码为 n 个分片，
    /// 向节点 j 发送其专属的分片 m_j 和哈希 h
    Echo {
        /// 广播实例唯一标识
        instance_id: String,
        /// 发送者节点ID
        sender: String,
        /// 原始数据的SHA-256哈希 h = hash(M)
        data_hash: String,
        /// 目标节点专属的分片索引
        shard_index: usize,
        /// 目标节点专属的分片数据
        shard_data: Vec<u8>,
        /// 分片的SHA-256哈希（用于完整性校验）
        shard_hash: String,
        /// 原始数据大小（解码时需要）
        original_size: usize,
        /// 数据分片数量（RS编码参数）
        data_shard_count: usize,
        /// 校验分片数量（RS编码参数）
        parity_shard_count: usize,
    },

    /// 第3轮：就绪（READY）— 确认与Reconstruction共享
    /// 当节点收到 2t+1 个匹配的 ECHO 消息时，确立自己的分片，
    /// 并向全网广播带有该分片的就绪消息
    Ready {
        /// 广播实例唯一标识
        instance_id: String,
        /// 发送者节点ID
        sender: String,
        /// 原始数据的SHA-256哈希
        data_hash: String,
        /// 发送者自己的分片索引
        shard_index: usize,
        /// 发送者自己的分片数据
        shard_data: Vec<u8>,
        /// 分片的SHA-256哈希
        shard_hash: String,
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
