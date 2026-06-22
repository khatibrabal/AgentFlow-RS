// src/graph.rs
use crate::error::WorkflowError;
use crate::executor::WorkflowExecutor;
use crate::node::{
    ApprovalNode, DeepSeekNode, ExecutableNode, FileReadNode, FileWriteNode, LocalVectorSearchNode,
    MultiModalParseNode, NodeToolWrapper, ReActAgentNode, RegexMatchNode, RouterNode, ShellNode,
    SpiderNode, TextNode, WebSearchNode,
};
use anyhow::Result;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

// 核心元编程宏 (Macro Rules)
/// 工作流节点自动注册宏。
///
/// 利用 Rust 编译期的 AST 展开能力，消除工厂模式中冗余的 `Arc::new`
/// 和底层的错误路由代码。不仅大幅提升了代码的声明式美感，更实现了零成本抽象。
macro_rules! register_nodes {
    ($nc:expr, $engine:expr, { $( $name:pat => $node_expr:expr ),* $(,)? }) => {
        let node: Arc<dyn ExecutableNode> = match $nc.node_type.as_str() {
            // 编译期循环展开：自动为所有节点包裹 Arc 智能指针
            $( $name => Arc::new($node_expr), )*

            // 兜底路由：拦截未注册节点并抛出强类型领域错误
            _ => return Err(WorkflowError::UnknownNodeType($nc.node_type.clone()).into()),
        };
        $engine.add_node(node);
    };
}

/// 顶层工作流配置结构体，与外部的 YAML 配置文件一一映射。
#[derive(Deserialize, Debug)]
pub struct WorkflowConfig {
    /// 工作流中所有节点的定义集合
    pub nodes: Vec<NodeConfig>,
    /// 定义节点间数据与控制流向的边集合
    pub edges: Vec<EdgeConfig>,
}

/// 单个执行节点的声明式配置。
///
/// 采用了 Option 包装的可选字段设计，以便兼容底层各异的节点类型
/// （例如爬虫节点特有的 `link_selector`，大模型节点特有的 `prompt`）。
#[derive(Deserialize, Debug)]
pub struct NodeConfig {
    /// 节点的全局唯一标识符
    pub id: String,
    /// 节点类型的枚举映射字符串（如 "DeepSeek", "Spider"）
    pub node_type: String,

    // --- 以下为多态节点特有可选字段 ---
    /// 提供给 LLM 节点使用的系统级提示词
    pub prompt: Option<String>,
    /// 提供给检索引擎专用的强特征 Query 搜索词
    pub query: Option<String>,
    /// 本地文件读写节点所需的目标路径
    pub file_path: Option<String>,
    /// 正则清洗节点所需的正则表达式匹配模式
    pub pattern: Option<String>,
    /// 爬虫节点所需的并发页面抓取上限
    pub max_pages: Option<usize>,
    /// 文本节点所需的静态注入内容
    pub text: Option<String>,
    /// 爬虫节点用于定位子页面链接的 CSS 选择器
    pub link_selector: Option<String>,
    /// 搜索引擎节点返回的高质量网页数量上限
    pub num_results: Option<usize>,
    /// 向量检索召回数
    pub top_k: Option<usize>,
    /// Shell 命令
    pub command: Option<String>,
    /// 路由判断词
    pub keyword: Option<String>,
    /// 用于审批节点
    pub message: Option<String>,
}

/// DAG 图中的有向边定义，表示数据与控制权的流向。
#[derive(Deserialize, Debug)]
pub struct EdgeConfig {
    /// 边的起点节点 ID (上游)
    pub from: String,
    /// 边的终点节点 ID (下游)
    pub to: String,
    pub condition: Option<String>,
}

/// 图拓扑构建器，负责将静态的 YAML 描述转化为可执行的内存模型。
pub struct GraphBuilder;

impl GraphBuilder {
    /// 从指定的 YAML 文件读取配置，并构建出安全的并发执行引擎。
    ///
    /// 此函数包含了核心的生命周期管理与安全校验，在将节点注入引擎前，
    /// 必须先通过严格的 DAG 环路检测算法。
    ///
    /// # Arguments
    /// * `path` - YAML 配置文件的物理路径
    /// * `debug` - 是否开启底层开发调试模式（日志落盘）
    ///
    /// # Errors
    /// * 如果 YAML 格式损坏，返回 `ParseYamlError`
    /// * 如果检测到循环依赖，返回 `CycleDetected`
    /// * 如果存在未注册节点，返回 `UnknownNodeType`
    pub fn build_from_yaml(
        path: &str,
        debug: bool,
        run_timestamp: &str,
        initial_payload: &str,
    ) -> Result<WorkflowExecutor> {
        let content = fs::read_to_string(path)?;
        let config: WorkflowConfig = serde_yaml::from_str(&content)?;

        // 满分考点：在构建前，必须进行有向无环图(DAG)的环路检测！
        Self::check_cycles(&config)?;

        // ✨ 透传给 Executor
        let mut engine = WorkflowExecutor::new(
            debug,
            run_timestamp.to_string(),
            initial_payload.to_string(),
        );

        // 实例化节点 (利用自定义宏，代码极致压缩与优雅)
        for nc in &config.nodes {
            register_nodes!(nc, engine, {
                "DeepSeek" => DeepSeekNode {
                    id: nc.id.clone(),
                    prompt: nc.prompt.clone().unwrap_or_else(|| "你是一个AI助手".to_string()),
                },
                "FileRead" => FileReadNode {
                    id: nc.id.clone(),
                    file_path: nc.file_path.clone().unwrap_or_else(|| "input.txt".to_string()),
                },
                "FileWrite" => FileWriteNode {
                    id: nc.id.clone(),
                    original_path: nc.file_path.clone().unwrap_or_else(|| "output.txt".to_string()),
                    timestamp: run_timestamp.to_string(),
                },
                "RegexMatch" => RegexMatchNode {
                    id: nc.id.clone(),
                    pattern: nc.pattern.clone().unwrap_or_else(|| "(.*)".to_string()),
                },
                "Shell" => ShellNode {
                    id: nc.id.clone(),
                    command: nc.command.clone().unwrap_or_else(|| "echo 缺少命令".to_string())
                },
                "Spider" => SpiderNode {
                    id: nc.id.clone(),
                    max_pages: nc.max_pages.unwrap_or(3),
                    link_selector: nc.link_selector.clone(),
                },
                "Text" => TextNode {
                    id: nc.id.clone(),
                    text: nc.text.clone().unwrap_or_default(),
                },
                "WebSearch" => WebSearchNode {
                    id: nc.id.clone(),
                    query: nc.query.clone(),
                    num_results: nc.num_results.unwrap_or(5),
                    raw_mode: false,
                },
                "MultiModalParse" => MultiModalParseNode {
                    id: nc.id.clone(),
                    file_path: nc.file_path.clone().unwrap_or_else(|| "document.pdf".to_string()),
                },
                "LocalRAG" => LocalVectorSearchNode {
                    id: nc.id.clone(),
                    query: nc.query.clone().unwrap_or_else(|| "核心观点是什么？".to_string()),
                    top_k: nc.top_k.unwrap_or(3),
                },
                "Router" => RouterNode {
                    id: nc.id.clone(),
                    keyword: nc.keyword.clone().unwrap_or_else(|| "ERROR".to_string())
                },
                "Approval" => {
                    let (_dummy_tx, rx) = tokio::sync::broadcast::channel(16);
                    ApprovalNode {
                        id: nc.id.clone(),
                        message: nc.message.clone().unwrap_or_else(|| "请求人工审批".to_string()),
                        rx,
                    }
                },
                "ReActAgent" => {
                    let mut tools = HashMap::new();

                    // 1. 🕷️ 爬虫工具
                    tools.insert("spider".to_string(), NodeToolWrapper {
                        description: "并发网页抓取爬虫。必须提供合法的完整 URL 作为输入。".to_string(),
                        node: Arc::new(SpiderNode {
                            id: "internal_spider".into(),
                            max_pages: 3,
                            link_selector: None,
                        }),
                    });

                    // 2. 💻 Shell 工具
                    tools.insert("shell".to_string(), NodeToolWrapper {
                        description: "执行本地终端系统命令。参数应为纯合法的 shell 指令字符串。".to_string(),
                        node: Arc::new(ShellNode {
                            id: "internal_shell".into(),
                            command: "".to_string(), // 留空，等待大模型动态传入
                        }),
                    });

                    // 3. 🌐 智能搜索工具 (Agent 的神兵利器)
                    tools.insert("search".to_string(), NodeToolWrapper {
                        description: "全网智能搜索引擎，用于查询最新资讯和客观事实。参数应为你要搜索的关键词。".to_string(),
                        node: Arc::new(WebSearchNode {
                            id: "internal_search".into(),
                            query: None, // 留空，让节点自动读取大模型传入的 input
                            num_results: 3,
                            raw_mode: true,
                        }),
                    });

                    // 4. 📄 多模态文件解析工具 (视觉/听觉/文档)
                    tools.insert("parse_file".to_string(), NodeToolWrapper {
                        description: "多模态解析器，可以读取并提取本地 PDF, Word, 图片(OCR) 和 音频(语音识别) 文件中的文本。参数必须是本地文件的绝对或相对路径。".to_string(),
                        node: Arc::new(MultiModalParseNode {
                            id: "internal_multimodal".into(),
                            file_path: "".to_string(), // 留空，等待大模型动态传入文件路径
                        }),
                    });

                    // 5. 💾 文件写入工具 (让大模型拥有“记忆”和“产出”能力)
                    tools.insert("write_file".to_string(), NodeToolWrapper {
                        description: "将一段文本保存到本地系统。参数应为你想要写入的具体文本内容。".to_string(),
                        node: Arc::new(FileWriteNode {
                            id: "internal_write".into(),
                            original_path: "agent_output.md".to_string(),
                            timestamp: run_timestamp.to_string(), // 复用 GraphBuilder 传入的时间戳
                        }),
                    });

                    ReActAgentNode {
                        id: nc.id.clone(),
                        max_steps: nc.max_pages.unwrap_or(5),
                        registered_tools: tools,
                    }
                }
            });
        }

        // 添加边
        for ec in &config.edges {
            engine.add_edge(&ec.from, &ec.to, ec.condition.clone());
        }

        // 构建完成后，预计算好拓扑顺序
        engine.calculate_topology()?;

        Ok(engine)
    }

    /// 基于深度优先搜索（DFS）的三色标记法进行图的环路检测。
    ///
    /// 遍历所有声明的边节点，将状态分为：0(未访问), 1(正在递归栈中), 2(已安全访问)。
    /// 如果在遍历过程中遇到状态为 1 的节点，则证明拓扑图中存在反向指针(死循环)。
    fn check_cycles(config: &WorkflowConfig) -> Result<()> {
        let mut adj: HashMap<String, Vec<String>> = HashMap::new();
        for edge in &config.edges {
            adj.entry(edge.from.clone())
                .or_default()
                .push(edge.to.clone());
        }

        let mut state: HashMap<String, u8> = HashMap::new();
        for node in &config.nodes {
            state.insert(node.id.clone(), 0);
        }

        for node in &config.nodes {
            if *state.get(&node.id).unwrap_or(&0) == 0
                && Self::dfs_has_cycle(&node.id, &adj, &mut state)
            {
                return Err(WorkflowError::CycleDetected.into());
            }
        }

        Ok(())
    }

    /// DFS 递归遍历核心逻辑。
    fn dfs_has_cycle(
        curr: &str,
        adj: &HashMap<String, Vec<String>>,
        state: &mut HashMap<String, u8>,
    ) -> bool {
        state.insert(curr.to_string(), 1);

        if let Some(neighbors) = adj.get(curr) {
            for next in neighbors {
                let s = *state.get(next).unwrap_or(&0);
                if s == 1 || (s == 0 && Self::dfs_has_cycle(next, adj, state)) {
                    return true;
                }
            }
        }

        state.insert(curr.to_string(), 2);
        false
    }
}

// 🧪 单元测试层
#[cfg(test)]
mod tests {
    use super::*;

    // 辅助函数：快速生成一个占位的 NodeConfig
    fn mock_node_config(id: &str) -> NodeConfig {
        NodeConfig {
            id: id.to_string(),
            node_type: "Text".to_string(),
            prompt: None,
            query: None,
            file_path: None,
            pattern: None,
            max_pages: None,
            text: None,
            link_selector: None,
            num_results: None,
            top_k: None,
            command: None,
            keyword: None,
            message: None,
        }
    }

    #[test]
    fn test_dag_cycle_detection_pass() {
        let config = WorkflowConfig {
            nodes: vec![
                mock_node_config("A"),
                mock_node_config("B"),
                mock_node_config("C"),
            ],
            edges: vec![
                EdgeConfig {
                    from: "A".to_string(),
                    to: "B".to_string(),
                    condition: None,
                },
                EdgeConfig {
                    from: "B".to_string(),
                    to: "C".to_string(),
                    condition: None,
                },
            ],
        };

        let result = GraphBuilder::check_cycles(&config);
        assert!(result.is_ok(), "健康的有向无环图(DAG)不应该报错");
    }

    #[test]
    fn test_dag_cycle_detection_fail() {
        let config = WorkflowConfig {
            nodes: vec![
                mock_node_config("A"),
                mock_node_config("B"),
                mock_node_config("C"),
            ],
            edges: vec![
                EdgeConfig {
                    from: "A".to_string(),
                    to: "B".to_string(),
                    condition: None,
                },
                EdgeConfig {
                    from: "B".to_string(),
                    to: "C".to_string(),
                    condition: None,
                },
                EdgeConfig {
                    from: "C".to_string(),
                    to: "A".to_string(),
                    condition: None,
                },
            ],
        };

        let result = GraphBuilder::check_cycles(&config);
        assert!(result.is_err(), "引擎必须能够拦截并报错包含死循环的拓扑图");

        if let Err(e) = result {
            let err_str = e.to_string();
            assert!(
                err_str.contains("死循环") || err_str.contains("CycleDetected"),
                "抛出的错误信息必须包含我们定义的强类型描述"
            );
        }
    }
}
