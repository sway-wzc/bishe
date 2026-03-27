pub mod protocol;
pub mod types;

// 重新导出常用类型
pub use protocol::{RbcInstance, RbcManager};
pub use types::{RbcConfig, RbcMessage, RbcOutput, RbcInstanceState};

#[cfg(test)]
mod test_rbc;
