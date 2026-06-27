//! # 工作流并发执行调度模块 (Workflow Executor)
//!
//! 本模块提供了系统底层的核心异步调度引擎。
//! 引擎通过分析有向无环图 (DAG) 的入度表，结合 Kahn 算法计算全局拓扑排序，
//! 实现了严格基于数据依赖的无锁并发调度。同时，采用 `tokio::sync::mpsc` 通道，
//! 实现了底层计算层与上层 UI 渲染层（如 TUI 或 Web 视图）的彻底解耦。

// src/executor.rs
use crate::node::ExecutableNode;
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

/// 执行引擎向前端视图（如 TUI 或 Web API）派发的异步生命周期事件枚举。
///
/// 利用 MPSC (多生产者单消费者) 通道机制，允许底层并发任务在执行的任意阶段
/// 安全地向主线程或渲染层推送状态变更，彻底隔离了计算逻辑与展现逻辑。
pub enum ExecutionEvent {
    /// 节点已成功分配到 Tokio 工作线程池，进入异步执行态。
    Started {
        node_id: String,
        node_name: String,
    },
    /// 节点执行完毕，并产出了最终的字符串载荷。
    Finished {
        node_id: String,
        node_name: String,
        result: String,
    },
    /// 节点在执行期间发生崩溃或遭遇不可恢复的错误。
    Failed {
        node_id: String,
        error: String,
    },
    /// 系统的底层调试、遥测与监控数据流。
    DebugLog {
        node_id: String,
        message: String,
    },
    /// 针对长时运算（如 ReAct Agent 的推演过程）提供的流式增量输出事件。
    StreamLog {
        #[allow(dead_code)]
        node_id: String,
        message: String,
    },
    /// 触发人工干预流程，请求外部授权指令。
    #[allow(dead_code)]
    RequireApproval {
        node_id: String,
        prompt: String,
    },
    /// 由于前置条件分支判断未命中，该节点及后续分支被调度器标记为跳过。
    Skipped {
        node_id: String,
    },
}

/// 引擎内部状态机使用的隐式控制流事件。
///
/// 用于协调各个并发子任务完成后的图状态推进。
enum InternalEvent {
    /// 节点正常完成执行。
    Success(String),
    /// 节点执行失败。
    Failed(String),
    /// 节点因路由条件未满足被跳过。
    Skipped(String),
}

/// 核心工作流调度中枢。
///
/// 基于入度表（Indegree Table）与 Kahn 拓扑排序算法，实现动态的依赖驱动并发。
/// 确保没有任何子节点会早于其依赖的父节点执行，同时最大化无依赖任务的并行度。
pub struct WorkflowExecutor {
    /// 托管所有堆分配的、实现了 `ExecutableNode` 契约的动态分发节点实例。
    nodes: HashMap<String, Arc<dyn ExecutableNode>>,
    /// 图的邻接表表示，包含目标节点 ID 以及可选的路由触发条件。
    edges: HashMap<String, Vec<(String, Option<String>)>>,
    /// 引擎是否处于深度调试输出模式（开启后将触发额外的资源遥测事件）。
    debug: bool,
    /// 预计算的节点拓扑排序列表，主要供前端 UI 进行树状图的顺序渲染。
    pub topological_order: Vec<String>,
    /// 当前工作流执行生命周期的全局时间戳标识。
    pub run_timestamp: String,
    /// 注入到 DAG 入度为零的根节点的初始数据负载。
    pub initial_payload: String,
}

impl WorkflowExecutor {
    /// 构造一个新的工作流执行器实例。
    ///
    /// # Arguments
    ///
    /// * `debug` - 是否开启底层调试模式。
    /// * `run_timestamp` - 全局执行流水号，用于文件隔离和日志追踪。
    /// * `initial_payload` - 初始化数据，将分发给所有入度为 0 的节点。
    pub fn new(debug: bool, run_timestamp: String, initial_payload: String) -> Self {
        Self {
            nodes: HashMap::new(),
            edges: HashMap::new(),
            debug,
            topological_order: Vec::new(),
            run_timestamp,
            initial_payload,
        }
    }

    /// 计算有向无环图 (DAG) 的拓扑排序。
    ///
    /// 该算法通过统计各节点的入度，生成初始就绪队列，并逐步剥离图的边。
    /// 排序结果将缓存于 `self.topological_order` 字段中。
    ///
    /// # Errors
    ///
    /// 算法假设图在进入此阶段前已通过了严格的环路检测，若执行异常则返回领域级错误。
    pub fn calculate_topology(&mut self) -> Result<()> {
        let mut indegrees: HashMap<String, usize> = HashMap::new();
        for node_id in self.nodes.keys() {
            indegrees.insert(node_id.clone(), 0);
        }

        for tos in self.edges.values() {
            for (to, _condition) in tos {
                *indegrees.entry(to.clone()).or_insert(0) += 1;
            }
        }

        // 采用 Kahn 算法进行拓扑排序
        let mut queue: Vec<String> = indegrees
            .iter()
            .filter(|&(_, &deg)| deg == 0)
            .map(|(id, _)| id.clone())
            .collect();

        // 保证同一层级的节点按字母序一致排列，避免重复执行时顺序发生跳动
        queue.sort();

        let mut order = Vec::new();
        let mut local_indegrees = indegrees.clone();

        while let Some(node) = queue.pop() {
            order.push(node.clone());
            if let Some(neighbors) = self.edges.get(&node) {
                let mut next_batch = Vec::new();
                for (neighbor_id, _condition) in neighbors {
                    let deg = local_indegrees.get_mut(neighbor_id).unwrap();
                    *deg -= 1;
                    if *deg == 0 {
                        next_batch.push(neighbor_id.clone());
                    }
                }
                next_batch.sort();

                for next in next_batch.into_iter().rev() {
                    queue.push(next);
                }
            }
        }

        self.topological_order = order;
        Ok(())
    }

    /// 向调度器注册一个新的可执行节点实例。
    pub fn add_node(&mut self, node: Arc<dyn ExecutableNode>) {
        self.nodes.insert(node.id().to_string(), node);
    }

    /// 在给定的上下游节点之间建立有向依赖边，并可附带控制流的判断条件。
    pub fn add_edge(&mut self, from: &str, to: &str, condition: Option<String>) {
        self.edges
            .entry(from.to_string())
            .or_default()
            .push((to.to_string(), condition));
    }

    /// 导出 DAG 拓扑模型为标准的 Graphviz DOT 文件。
    ///
    /// 允许系统在不实际运行节点的前提下，对复杂的 Agentic 工作流进行
    /// 静态结构分析与离线渲染验证。
    ///
    /// # Arguments
    ///
    /// * `file_path` - DOT 文件的目标输出路径。
    ///
    /// # Errors
    ///
    /// 若无法获得文件系统的写入权限，返回 I/O 错误。
    pub fn export_to_dot(&self, file_path: &str) -> Result<()> {
        let mut out = String::new();
        out.push_str("digraph AgentFlow {\n");
        out.push_str("    rankdir=LR; \n");
        out.push_str("    node [shape=box, style=filled, fillcolor=\"#E3F2FD\", fontname=\"Microsoft YaHei, SimHei, sans-serif\", penwidth=1.5];\n");
        out.push_str("    edge [color=\"#546E7A\", penwidth=1.5, arrowsize=0.8];\n\n");

        for (id, node) in &self.nodes {
            out.push_str(&format!(
                "    \"{}\" [label=\"{}\\n{}\"];\n",
                id,
                id,
                node.name()
            ));
        }

        out.push('\n');

        for (from, tos) in &self.edges {
            for (to, condition) in tos {
                if let Some(cond) = condition {
                    // 带有逻辑条件的路由分支渲染为带标签的橙色虚线
                    out.push_str(&format!(
                        "    \"{}\" -> \"{}\" [label=\" {} \", fontcolor=\"#E65100\", color=\"#FF9800\", style=\"dashed\", penwidth=2.0];\n",
                        from, to, cond
                    ));
                } else {
                    out.push_str(&format!("    \"{}\" -> \"{}\";\n", from, to));
                }
            }
        }

        out.push_str("}\n");

        std::fs::write(file_path, out).context("无法写入 DOT 可视化文件")?;
        Ok(())
    }

    /// 导出 DAG 拓扑模型为 Mermaid 图表文本格式。
    ///
    /// 生成的文件可直接粘贴至支持 Mermaid 渲染的 Markdown 引擎中实现可视化。
    ///
    /// # Arguments
    ///
    /// * `file_path` - Markdown 文件的目标输出路径。
    ///
    /// # Errors
    ///
    /// 若底层文件读写异常则返回错误。
    pub fn export_to_mermaid(&self, file_path: &str) -> Result<()> {
        let mut out = String::new();

        out.push_str("```mermaid\n");
        out.push_str("graph TD\n");
        out.push_str(
            "    classDef default fill:#E3F2FD,stroke:#546E7A,stroke-width:2px,color:#333;\n\n",
        );

        for (id, node) in &self.nodes {
            out.push_str(&format!("    {}[\"{} | {}\"]\n", id, id, node.name()));
        }

        out.push('\n');

        for (from, tos) in &self.edges {
            for (to, condition) in tos {
                if let Some(cond) = condition {
                    out.push_str(&format!("    {} -->|\"{}\"| {}\n", from, cond, to));
                } else {
                    out.push_str(&format!("    {} --> {}\n", from, to));
                }
            }
        }

        out.push_str("```\n");

        std::fs::write(file_path, out).context("无法写入 Mermaid 可视化文件")?;
        Ok(())
    }

    /// 触发整个 DAG 图谱的异步流式计算过程。
    ///
    /// 此方法将在当前上下文中阻塞，直至网络上的所有节点全部执行完毕。
    /// 执行期间，调度器会动态派生并发的 `tokio` 任务，并持续向外推流状态变更。
    ///
    /// # Arguments
    ///
    /// * `ui_tx` - 用于向外部环境（如 UI 层）通信的 MPSC 发送端。
    ///
    /// # Errors
    ///
    /// 当系统任务队列异常或信道断开时可能抛出致命错误。
    pub async fn execute_dag(&self, ui_tx: mpsc::Sender<ExecutionEvent>) -> Result<()> {
        let mut indegrees: HashMap<String, usize> = HashMap::new();
        let mut reverse_edges: HashMap<String, Vec<String>> = HashMap::new();

        for node_id in self.nodes.keys() {
            indegrees.insert(node_id.clone(), 0);
        }

        for (from, tos) in &self.edges {
            for (to, _) in tos {
                *indegrees.entry(to.clone()).or_insert(0) += 1;
                reverse_edges
                    .entry(to.clone())
                    .or_default()
                    .push(from.clone());
            }
        }

        let context = Arc::new(RwLock::new(HashMap::<String, String>::new()));
        let (internal_tx, mut internal_rx) = mpsc::channel::<InternalEvent>(32);
        let mut running_tasks = 0;

        // 初始化入度为 0 的节点加入执行队列
        for (id, deg) in &indegrees {
            if *deg == 0 {
                self.spawn_node(
                    id,
                    &reverse_edges,
                    &context,
                    ui_tx.clone(),
                    internal_tx.clone(),
                    self.debug,
                );
                running_tasks += 1;
            }
        }

        while running_tasks > 0 {
            if let Some(event) = internal_rx.recv().await {
                running_tasks -= 1;

                match event {
                    InternalEvent::Success(node_id) => {
                        let output = context.read().await.get(&node_id).unwrap().clone();
                        let is_skip = output == "__ROUTER_SKIP__";

                        if let Some(children) = self.edges.get(&node_id) {
                            for (child, condition) in children {
                                let mut should_skip = is_skip;

                                // 处理条件路由逻辑
                                if !should_skip && condition.is_some() {
                                    let cond = condition.as_ref().unwrap();
                                    // 校验节点输出与定义的路由条件是否匹配，若不匹配则标记跳过
                                    if (cond == "true" && output != "__CONDITION_TRUE__")
                                        || (cond == "false" && output != "__CONDITION_FALSE__")
                                    {
                                        should_skip = true;
                                    }
                                }

                                let deg = indegrees.get_mut(child).unwrap();
                                *deg -= 1;

                                if *deg == 0 {
                                    if should_skip {
                                        let _ = ui_tx
                                            .send(ExecutionEvent::Skipped {
                                                node_id: child.clone(),
                                            })
                                            .await;
                                        let _ = internal_tx
                                            .send(InternalEvent::Skipped(child.clone()))
                                            .await;
                                        running_tasks += 1;
                                    } else {
                                        self.spawn_node(
                                            child,
                                            &reverse_edges,
                                            &context,
                                            ui_tx.clone(),
                                            internal_tx.clone(),
                                            self.debug,
                                        );
                                        running_tasks += 1;
                                    }
                                }
                            }
                        }
                    }
                    InternalEvent::Skipped(skipped_id) => {
                        // 级联跳过逻辑：当前节点跳过后，其独占的下游节点也会被自动掐断
                        if let Some(children) = self.edges.get(&skipped_id) {
                            for (child_id, _) in children {
                                let deg = indegrees.get_mut(child_id).unwrap();
                                *deg -= 1;

                                if *deg == 0 {
                                    let _ = ui_tx
                                        .send(ExecutionEvent::Skipped {
                                            node_id: child_id.clone(),
                                        })
                                        .await;
                                    let _ = internal_tx
                                        .send(InternalEvent::Skipped(child_id.clone()))
                                        .await;
                                    running_tasks += 1;
                                }
                            }
                        }
                    }
                    InternalEvent::Failed(_failed_id) => {}
                }
            }
        }

        Ok(())
    }

    /// [Web API 专用] 将当前的拓扑在内存中动态渲染为 Mermaid 字符串。
    pub fn generate_mermaid(&self) -> String {
        let mut out = String::new();

        out.push_str("```mermaid\n");
        out.push_str("graph TD\n");
        out.push_str(
            "    classDef default fill:#E3F2FD,stroke:#546E7A,stroke-width:2px,color:#333;\n\n",
        );

        for (id, node) in &self.nodes {
            out.push_str(&format!("    {}[\"{} | {}\"]\n", id, id, node.name()));
        }

        out.push('\n');

        for (from, tos) in &self.edges {
            for (to, _) in tos {
                out.push_str(&format!("    {} --> {}\n", from, to));
            }
        }

        out.push_str("```\n");
        out
    }

    /// 从当前上下文中剥离出独立的异步协程，用于触发目标计算节点的执行。
    ///
    /// 内部封装了上下文读写锁分离，并将流式信道注入到底层方法调用中。
    fn spawn_node(
        &self,
        node_id: &str,
        reverse_edges: &HashMap<String, Vec<String>>,
        context: &Arc<RwLock<HashMap<String, String>>>,
        ui_tx: mpsc::Sender<ExecutionEvent>,
        internal_tx: mpsc::Sender<InternalEvent>,
        is_debug: bool,
    ) {
        let node = Arc::clone(self.nodes.get(node_id).unwrap());
        let id = node_id.to_string();
        let parents = reverse_edges.get(node_id).cloned().unwrap_or_default();
        let ctx_clone = Arc::clone(context);
        let timestamp_clone = self.run_timestamp.clone();
        let payload_clone = self.initial_payload.clone();

        tokio::spawn(async move {
            let name = node.name().to_string();
            let _ = ui_tx
                .send(ExecutionEvent::Started {
                    node_id: id.clone(),
                    node_name: name.clone(),
                })
                .await;

            let mut input_strings = Vec::new();
            {
                let read_guard = ctx_clone.read().await;
                for p in &parents {
                    if let Some(parent_out) = read_guard.get(p) {
                        input_strings.push(parent_out.clone());
                    }
                }
            }

            let final_input = if input_strings.is_empty() {
                payload_clone
            } else {
                input_strings.join("\n")
            };

            // 构建单向数据流通道，专用于捕获节点内部的细粒度流式输出
            let (log_tx, mut log_rx) = mpsc::channel::<String>(100);
            let ui_tx_log = ui_tx.clone();
            let node_id_log = id.clone();

            // 派生独立的协程负责监控数据通道，将增量日志实时封装为流事件并推送至外层主循环
            tokio::spawn(async move {
                while let Some(msg) = log_rx.recv().await {
                    let _ = ui_tx_log
                        .send(ExecutionEvent::StreamLog {
                            node_id: node_id_log.clone(),
                            message: msg,
                        })
                        .await;
                }
            });

            let start_time = std::time::Instant::now();

            // 启动带有流式接口通信支持的核心运算链路
            match node
                .execute_with_stream(&final_input, is_debug, &timestamp_clone, log_tx)
                .await
            {
                Ok(res) => {
                    if is_debug {
                        let elapsed = start_time.elapsed().as_millis();
                        let _ = ui_tx
                            .send(ExecutionEvent::DebugLog {
                                node_id: id.clone(),
                                message: format!("⏱️ 底层网络与计算总耗时: {} ms", elapsed),
                            })
                            .await;
                    }

                    let mut write_guard = ctx_clone.write().await;
                    write_guard.insert(id.clone(), res.clone());
                    drop(write_guard);

                    let _ = ui_tx
                        .send(ExecutionEvent::Finished {
                            node_id: id.clone(),
                            node_name: name.clone(),
                            result: res,
                        })
                        .await;
                    let _ = internal_tx.send(InternalEvent::Success(id)).await;
                }
                Err(e) => {
                    let _ = ui_tx
                        .send(ExecutionEvent::Failed {
                            node_id: id.clone(),
                            error: format!("{:#}", e),
                        })
                        .await;
                    let _ = internal_tx.send(InternalEvent::Failed(id)).await;
                }
            }
        });
    }
}

// 模块测试组件
#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Mutex;
    use tokio::sync::mpsc;

    struct MockNode {
        id: String,
        execution_order: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl ExecutableNode for MockNode {
        fn id(&self) -> &str {
            &self.id
        }
        fn name(&self) -> &str {
            "Mock"
        }
        // 实现执行契约并模拟延时计算
        async fn execute(&self, _input: &str, _debug: bool, _timestamp: &str) -> Result<String> {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            self.execution_order.lock().unwrap().push(self.id.clone());
            Ok("Mock OK".to_string())
        }
    }

    #[tokio::test]
    async fn test_dag_execution_order() {
        let mut engine = WorkflowExecutor::new(
            false,
            "test_timestamp".to_string(),
            "Mock Payload".to_string(),
        );
        let order_record = Arc::new(Mutex::new(Vec::new()));

        engine.add_node(Arc::new(MockNode {
            id: "A".to_string(),
            execution_order: order_record.clone(),
        }));
        engine.add_node(Arc::new(MockNode {
            id: "B".to_string(),
            execution_order: order_record.clone(),
        }));
        engine.add_node(Arc::new(MockNode {
            id: "C".to_string(),
            execution_order: order_record.clone(),
        }));
        engine.add_node(Arc::new(MockNode {
            id: "D".to_string(),
            execution_order: order_record.clone(),
        }));

        engine.add_edge("A", "B", None);
        engine.add_edge("A", "C", None);
        engine.add_edge("B", "D", None);
        engine.add_edge("C", "D", None);

        let (tx, _rx) = mpsc::channel(32);
        let result = engine.execute_dag(tx).await;
        assert!(result.is_ok(), "引擎执行任务图应当无错误返回");

        let final_order = order_record.lock().unwrap();
        assert_eq!(final_order.len(), 4, "预期所有四个注册节点都必须完成执行");
        assert_eq!(final_order[0], "A", "节点 A 不具备前置依赖，应当首位执行");
        assert_eq!(
            final_order[3], "D",
            "节点 D 的入度约束要求必须等待 B 与 C 执行完毕，应当末位执行"
        );
    }
}
