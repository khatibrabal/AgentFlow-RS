// src/error.rs
use thiserror::Error;

/// 工作流执行过程中的领域级错误枚举。
///
/// 采用 `thiserror` 派生宏，提供了底层错误到领域错误的无缝转换。
/// 这保证了在庞大的 DAG 调度或异步执行中，依然能提供精确的错误堆溯源。
#[derive(Error, Debug)]
pub enum WorkflowError {
    /// 触发于有向无环图 (DAG) 构建阶段。
    /// 当解析出的 YAML 拓扑图中存在首尾相连的环路（如 A -> B -> A）时抛出。
    #[error("☠️ 构建失败：检测到配置的工作流中存在死循环(环路)，不符合 DAG 要求！")]
    CycleDetected,

    /// 触发于配置文件反序列化阶段。
    /// 自动拦截并包装来自 `serde_yaml` 的底层解析错误。
    #[error("📄 YAML 配置文件解析失败: {0}")]
    ParseYamlError(#[from] serde_yaml::Error),

    /// 触发于工厂模式实例化节点时。
    /// 当配置文件中声明了引擎未注册的 `node_type` 时抛出。
    #[error("❓ 未知的节点类型: '{0}'")]
    UnknownNodeType(String),

    /// 触发于运行时节点执行阶段。
    /// 携带具体发生崩溃的节点 ID 和底层的失败原因，便于 TUI 界面提取并在控制台展示。
    #[allow(dead_code)] // 作为系统预留的高级错误载体
    #[error("❌ 节点执行失败 [{node_id}]: {reason}")]
    NodeExecutionFailed { node_id: String, reason: String },
}
