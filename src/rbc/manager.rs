use anyhow::Result;
use log::info;
use std::collections::HashMap;

use super::protocol::RbcInstance;
use super::types::{RbcConfig, RbcMessage, RbcOutput};

/// RBC协议管理器
///
/// 管理多个并发的RBC广播实例，提供统一的消息处理接口
pub struct RbcManager {
    /// 本节点ID
    local_node_id: String,
    /// RBC协议配置
    config: RbcConfig,
    /// 所有节点ID列表（有序）
    node_ids: Vec<String>,
    /// 活跃的RBC实例：instance_id -> RbcInstance
    instances: HashMap<String, RbcInstance>,
    /// 已完成的输出队列
    completed_outputs: Vec<RbcOutput>,
}

impl RbcManager {
    /// 创建RBC协议管理器
    pub fn new(
        local_node_id: String,
        config: RbcConfig,
        mut node_ids: Vec<String>,
    ) -> Self {
        // 对节点ID排序，确保所有节点的顺序一致
        node_ids.sort();

        info!(
            "[RBC管理器] 初始化: 本节点={}, 节点数={}, {}",
            local_node_id,
            node_ids.len(),
            config.info()
        );

        Self {
            local_node_id,
            config,
            node_ids,
            instances: HashMap::new(),
            completed_outputs: Vec::new(),
        }
    }

    /// 获取或创建RBC实例
    fn get_or_create_instance(&mut self, instance_id: &str) -> Result<&mut RbcInstance> {
        if !self.instances.contains_key(instance_id) {
            let instance = RbcInstance::new(
                instance_id.to_string(),
                self.local_node_id.clone(),
                self.config.clone(),
                self.node_ids.clone(),
            )?;
            self.instances.insert(instance_id.to_string(), instance);
        }
        Ok(self.instances.get_mut(instance_id).unwrap())
    }

    /// 作为广播者发起一次RBC广播
    pub fn broadcast(
        &mut self,
        instance_id: String,
        data: Vec<u8>,
    ) -> Result<Vec<(String, RbcMessage)>> {
        let instance = RbcInstance::new(
            instance_id.clone(),
            self.local_node_id.clone(),
            self.config.clone(),
            self.node_ids.clone(),
        )?;
        self.instances.insert(instance_id.clone(), instance);
        let instance = self.instances.get_mut(&instance_id).unwrap();
        instance.broadcast(data)
    }

    /// 处理收到的RBC消息
    pub fn handle_message(
        &mut self,
        message: RbcMessage,
    ) -> Result<Vec<(String, RbcMessage)>> {
        match message {
            RbcMessage::Propose {
                instance_id,
                broadcaster,
                data,
            } => {
                let instance = self.get_or_create_instance(&instance_id)?;
                let msgs = instance.handle_propose(&broadcaster, &data)?;
                self.check_completion(&instance_id);
                Ok(msgs)
            }
            RbcMessage::Echo {
                instance_id,
                sender,
                data_hash,
                shard_index,
                shard_data,
                original_size,
                data_shard_count,
                parity_shard_count,
            } => {
                let instance = self.get_or_create_instance(&instance_id)?;
                let msgs = instance.handle_echo(
                    &sender,
                    &data_hash,
                    shard_index,
                    &shard_data,
                    original_size,
                    data_shard_count,
                    parity_shard_count,
                )?;
                self.check_completion(&instance_id);
                Ok(msgs)
            }
            RbcMessage::Ready {
                instance_id,
                sender,
                data_hash,
                shard_index,
                shard_data,
                original_size,
                data_shard_count,
                parity_shard_count,
            } => {
                let instance = self.get_or_create_instance(&instance_id)?;
                let msgs = instance.handle_ready(
                    &sender,
                    &data_hash,
                    shard_index,
                    &shard_data,
                    original_size,
                    data_shard_count,
                    parity_shard_count,
                )?;
                self.check_completion(&instance_id);
                Ok(msgs)
            }
        }
    }

    /// 检查实例是否完成，如果完成则收集输出
    fn check_completion(&mut self, instance_id: &str) {
        if let Some(instance) = self.instances.get(instance_id) {
            if instance.is_completed() {
                if let Some(output) = instance.output().cloned() {
                    info!(
                        "[RBC管理器] 实例 {} 已完成，数据大小={}字节",
                        &instance_id[..8.min(instance_id.len())],
                        output.data.len()
                    );
                    self.completed_outputs.push(output);
                }
            }
        }
    }

    /// 取出所有已完成的输出
    pub fn drain_outputs(&mut self) -> Vec<RbcOutput> {
        std::mem::take(&mut self.completed_outputs)
    }

    /// 获取活跃实例数量
    pub fn active_instance_count(&self) -> usize {
        self.instances
            .values()
            .filter(|i| !i.is_completed())
            .count()
    }

    /// 清理已完成的实例
    pub fn cleanup_completed(&mut self) {
        self.instances.retain(|_, i| !i.is_completed());
    }
}
