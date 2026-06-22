// src/executor.rs
use crate::node::ExecutableNode;
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};

/// 执行引擎向 TUI 前端视图派发的异步事件枚举。
///
/// 利用 MPSC (多生产者单消费者) 通道，实现了底层计算引擎与上层
/// 渲染视图的彻底解耦。
pub enum ExecutionEvent {
    /// 节点已分配到 CPU 线程，开始异步执行
    Started {
        node_id: String,
        node_name: String,
    },
    /// 节点成功执行完毕，并产出了结果负载
    Finished {
        node_id: String,
        node_name: String,
        result: String,
    },
    /// 节点执行崩溃或遭遇不可恢复错误
    Failed {
        node_id: String,
        error: String,
    },
    /// 底层 Debug 遥测与监控数据流
    DebugLog {
        node_id: String,
        message: String,
    },
    /// 专为 Agent 实时思考流打造的事件
    StreamLog {
        #[allow(dead_code)]
        node_id: String,
        message: String,
    },
    #[allow(dead_code)]
    /// 请求人工审批事件
    RequireApproval {
        node_id: String,
        prompt: String,
    },
    // 专用的 Skipped 事件，通过通道告诉前端更新状态
    Skipped {
        node_id: String,
    },
}

/// 引擎内部状态机使用的隐式事件
enum InternalEvent {
    Success(String),
    Failed(String),
    Skipped(String),
}

/// AgentFlow-RS 的核心工作流调度大脑。
///
/// 基于入度表计算（Indegree Table）与 Kahn 算法，实现依赖驱动的并发。
/// 确保没有任何子节点会早于它的父节点执行，同时最大化并行无依赖任务。
pub struct WorkflowExecutor {
    /// 托管所有堆分配的动态分发节点闭包
    nodes: HashMap<String, Arc<dyn ExecutableNode>>,
    /// 图的邻接表表示
    edges: HashMap<String, Vec<(String, Option<String>)>>,
    /// 引擎是否处于深度调试输出模式
    debug: bool,
    /// 预计算的节点拓扑排序列表，专供前端 TUI UI 树状图渲染使用
    pub topological_order: Vec<String>,
    pub run_timestamp: String,
    pub initial_payload: String,
}

impl WorkflowExecutor {
    /// 构造一个新的执行器引擎实例。
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

    /// 计算有向无环图的拓扑排序。
    ///
    /// 算法过程会生成一个入度为零的初始队列，并逐步剥离图边。
    /// 结果被缓存于 `self.topological_order` 中。
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

        // 使用 Kahn 算法进行简单的拓扑排序
        let mut queue: Vec<String> = indegrees
            .iter()
            .filter(|&(_, &deg)| deg == 0)
            .map(|(id, _)| id.clone())
            .collect();

        // 保证在同一层级的节点按字母序排，避免每次运行显示不一致
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
                        // Push just the String ID
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

    /// 向调度器注册一个新的可执行节点。
    pub fn add_node(&mut self, node: Arc<dyn ExecutableNode>) {
        self.nodes.insert(node.id().to_string(), node);
    }

    /// 在给定的上下游节点之间建立有向依赖边。
    pub fn add_edge(&mut self, from: &str, to: &str, condition: Option<String>) {
        self.edges
            .entry(from.to_string())
            .or_default()
            .push((to.to_string(), condition)); // ✨ 将条件与目标节点绑定存入
    }
    // 一键导出 DAG 拓扑可视化
    /// 将当前的内部工作流引擎内存模型，静态编译导出为标准的 Graphviz DOT 文件。
    ///
    /// 这是一个极强的工程与学术分析工具，允许在不实际运行节点(Dry-Run)
    /// 的前提下，校验、分析并可视化复杂的 Agentic 编排拓扑。
    pub fn export_to_dot(&self, file_path: &str) -> Result<()> {
        let mut out = String::new();
        // 设置 DOT 文件的基本样式：从左到右布局，节点带底色，工业风字体
        out.push_str("digraph AgentFlow {\n");
        out.push_str("    rankdir=LR; // 从左到右渲染\n");
        out.push_str("    node [shape=box, style=filled, fillcolor=\"#E3F2FD\", fontname=\"Microsoft YaHei, SimHei, sans-serif\", penwidth=1.5];\n");
        out.push_str("    edge [color=\"#546E7A\", penwidth=1.5, arrowsize=0.8];\n\n");

        // 1. 声明所有的图节点
        for (id, node) in &self.nodes {
            // 将节点的 ID 和其人类可读的 name() 合并展示
            out.push_str(&format!(
                "    \"{}\" [label=\"{}\\n{}\"];\n",
                id,
                id,
                node.name()
            ));
        }

        out.push('\n');

        // 2. 声明所有的依赖有向边
        for (from, tos) in &self.edges {
            for (to, condition) in tos {
                if let Some(cond) = condition {
                    // ✨ 智能路由分支：使用带 Label 的橙色虚线
                    out.push_str(&format!(
                        "    \"{}\" -> \"{}\" [label=\" {} \", fontcolor=\"#E65100\", color=\"#FF9800\", style=\"dashed\", penwidth=2.0];\n",
                        from, to, cond
                    ));
                } else {
                    // 普通数据流连线
                    out.push_str(&format!("    \"{}\" -> \"{}\";\n", from, to));
                }
            }
        }

        out.push_str("}\n");

        // 将组装好的 DOT 文本写入磁盘
        std::fs::write(file_path, out).context("无法写入 DOT 可视化文件")?;
        Ok(())
    }

    // 一键导出 Mermaid 拓扑 (现代 Markdown 标配)
    /// 将当前的内部工作流编译导出为标准的 Mermaid 图表文本。
    ///
    /// 生成的文件可以直接粘贴至 GitHub README、Notion、Obsidian
    /// 或任意支持 Mermaid 的 Markdown 渲染器中，实现开箱即用的前端可视化。
    pub fn export_to_mermaid(&self, file_path: &str) -> Result<()> {
        let mut out = String::new();

        // ✨ 新增：Markdown 代码块的开头
        out.push_str("```mermaid\n");

        // TD 表示 Top to Down (从上到下)，也可改为 LR (从左到右)
        out.push_str("graph TD\n");
        out.push_str(
            "    classDef default fill:#E3F2FD,stroke:#546E7A,stroke-width:2px,color:#333;\n\n",
        );

        for (id, node) in &self.nodes {
            // Mermaid 节点语法: id["ID | 名称"]
            out.push_str(&format!("    {}[\"{} | {}\"]\n", id, id, node.name()));
        }

        out.push('\n');

        for (from, tos) in &self.edges {
            for (to, condition) in tos {
                if let Some(cond) = condition {
                    // ✨ 智能路由分支：在 Mermaid 箭头上增加条件 Label
                    out.push_str(&format!("    {} -->|\"{}\"| {}\n", from, cond, to));
                } else {
                    // 普通数据流连线
                    out.push_str(&format!("    {} --> {}\n", from, to));
                }
            }
        }

        // ✨ 新增：Markdown 代码块的结尾
        out.push_str("```\n");

        std::fs::write(file_path, out).context("无法写入 Mermaid 可视化文件")?;
        Ok(())
    }

    /// 触发整个 DAG 引擎的异步流式计算过程。
    ///
    /// 此方法会阻塞至图谱上的所有节点全部执行完毕，期间不断向
    /// `ui_tx` 发送实时的节点状态变更以驱动 TUI 更新。
    ///
    /// # Arguments
    /// * `ui_tx` - 与 TUI 线程通信的多生产者发送端
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
                            // ✨ 注意这里解构出了 child 和 condition
                            for (child, condition) in children {
                                let mut should_skip = is_skip;

                                // 🚀 核心魔法：判断路由条件！
                                if !should_skip && condition.is_some() {
                                    let cond = condition.as_ref().unwrap();
                                    // 如果上游输出的信号与设定的路由条件 (true/false) 不匹配，则跳过执行下游分支
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
                                        // 发送跳过事件给 UI，并且通知调度器它的下游也全跳过
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
                        // 🚀 核心魔法：级联掐断所有下游！
                        if let Some(children) = self.edges.get(&skipped_id) {
                            // ✨ FIX: Destructure the tuple to get the child_id
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

    /// [Web API 专用] 将当前的图谱在内存中动态渲染为 Mermaid 字符串，不涉及任何磁盘 I/O。
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

    /// 通过 `tokio::spawn` 剥离出一个独立的异步任务来执行具体节点。
    ///
    /// 该函数处理了极为复杂的锁与并发问题：使用 `Arc<RwLock<HashMap>>` 来
    /// 实现安全的跨线程上下游数据读写。
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

            // ✨ 核心魔法 1：建立一条微型单向数据水管
            let (log_tx, mut log_rx) = tokio::sync::mpsc::channel::<String>(100);
            let ui_tx_log = ui_tx.clone();
            let node_id_log = id.clone();

            // ✨ 开启一个无阻塞搬运工线程：它死守在出水口，只要 Agent 发出一句思考，
            // 它就秒包成 DebugLog 事件发给 TUI 的主消息循环！
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

            // ✨ 核心魔法 2：不再调用老旧死板的 execute，而是启动带有流水线的 execute_with_stream！
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
                            result: res, // 因为 Agent 的拦截，这个 res 现在变得纯净无比！
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

// ✨ 同步修复底部的单元测试接口
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
        // ✨ 同步增加 _debug 参数
        async fn execute(&self, _input: &str, _debug: bool, _timestamp: &str) -> Result<String> {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            self.execution_order.lock().unwrap().push(self.id.clone());
            Ok("Mock OK".to_string())
        }
    }

    #[tokio::test]
    async fn test_dag_execution_order() {
        // ✨ 同步修改：传入 false 作为 debug 模式
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
        assert!(result.is_ok(), "引擎执行应该成功");

        let final_order = order_record.lock().unwrap();
        assert_eq!(final_order.len(), 4, "所有四个节点都必须被执行");
        assert_eq!(final_order[0], "A", "节点 A 没有前置依赖，必须第一个执行");
        assert_eq!(
            final_order[3], "D",
            "节点 D 必须等 B 和 C 结束，所以必然最后一个执行"
        );
    }
}
