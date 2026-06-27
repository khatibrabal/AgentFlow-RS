//! # Headless API 服务器模块 (Web Server)
//!
//! 本模块基于 `axum` 框架，为工作流引擎提供 RESTful API 接口。
//! 允许系统以无头（Headless）模式运行，支持通过 HTTP 请求动态加载 YAML 配置、
//! 触发异步工作流执行，并支持在离线状态下静态解析 DAG 的 Mermaid 拓扑结构。

// src/server.rs
use axum::{
    Json, Router,
    extract::{Query, State},
    response::IntoResponse,
    http::header,
    routing::{get, post},
};
use chrono::Local;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tower_http::cors::CorsLayer;

use crate::executor::ExecutionEvent;
use crate::graph::GraphBuilder;

/// Web 服务器的全局共享状态。
///
/// 在 Axum 路由层之间共享，用于保存引擎启动时的默认配置路径和运行时模式参数。
#[derive(Clone)]
pub struct AppState {
    /// 默认的工作流 YAML 配置文件挂载路径。
    pub default_config_path: String,
    /// 是否开启底层执行器的深度调试与遥测模式。
    pub debug_mode: bool,
}

/// 接收外部工作流触发请求的 JSON 反序列化结构体。
#[derive(Deserialize)]
pub struct WorkflowRunRequest {
    /// 允许外部传入临时参数以覆盖工作流根节点的初始输入。若留空则使用默认上下文。
    pub initial_input: Option<String>,
    /// 允许在单次 API 调用中动态指定要解析运行的 YAML 配置文件路径。
    pub config_path: Option<String>,
}

/// 接收外部拓扑图查询请求的 URL 传参结构体。
#[derive(Deserialize)]
pub struct TopologyQuery {
    /// 指定待解析拓扑的目标 YAML 配置文件路径。
    pub config_path: Option<String>,
}

/// 返回给外部调用方的工作流执行结果序列化结构体。
#[derive(Serialize)]
pub struct WorkflowRunResponse {
    /// 本次执行的全局状态标识（如 "Success", "Build_Error"）。
    pub status: String,
    /// 记录工作流触发时的全局流水线时间戳。
    pub execution_time: String,
    /// 在 Headless 模式下静默收集的各节点生命周期状态流日志。
    pub logs: Vec<String>,
    /// 收集所有已完成节点的最终输出载荷，以列表形式返回。
    pub final_results: Vec<String>,
}

/// 实例化并启动 Axum 异步 Web 服务器。
///
/// 该方法将绑定指定的网络端口，初始化路由表，挂载 CORS 中间件与优雅停机监听器，
/// 并最终阻塞当前线程以持续监听外部入站请求。
///
/// # Arguments
///
/// * `port` - HTTP 服务器监听的本地网络端口号。
/// * `config_path` - 全局默认的 YAML 工作流配置路径。
/// * `debug` - 是否开启底层引擎的调试模式。
///
/// # Errors
///
/// 若指定的端口被占用或底层套接字绑定失败，将返回 I/O 层面的 `anyhow::Error`。
pub async fn start_api_server(port: u16, config_path: String, debug: bool) -> anyhow::Result<()> {
    let state = AppState {
        default_config_path: config_path.clone(),
        debug_mode: debug,
    };

    let app = Router::new()
        .route(
            "/health",
            get(|| async { "AgentFlow-RS API is healthy! 🟢" }),
        )
        .route("/api/v1/workflow/run", post(handle_run_workflow))
        .route("/api/v1/workflow/topology", get(handle_get_topology))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await?;

    println!(
        r#"
===================================================================
 🚀 AgentFlow-RS | Headless Engine Active
===================================================================
 📡 监听网络 : http://{}
 📄 默认挂载 : {}
 🛡️  CORS跨域 : 已全局放行 (Permissive)

 📌 路由表 (Endpoints):
    [GET]  http://127.0.0.1:{}/health                   -> 微服务健康心跳检查
    [GET]  http://127.0.0.1:{}/api/v1/workflow/topology -> 实时拉取 DAG 拓扑图 (支持 ?config_path=...)
    [POST] http://127.0.0.1:{}/api/v1/workflow/run      -> 异步触发 Agent 工作流

 💡 终端极速测试指令 (支持动态覆盖 YAML 路径):
    curl -X POST http://127.0.0.1:{}/api/v1/workflow/run \
         -H "Content-Type: application/json" \
         -d "{{\\\"initial_input\\\": \\\"帮我总结科研进展\\\", \\\"config_path\\\": \\\"my_spider.yaml\\\"}}"

 🛑 关闭并退出微服务：Ctrl + C
===================================================================
"#,
        addr, config_path, port, port, port, port
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    println!("👋 微服务已安全关闭，再见！");
    Ok(())
}

/// 核心工作流执行路由控制器。
///
/// 响应 POST 请求，依据传入的参数动态构建内存 DAG 模型，并派生异步任务执行图谱。
/// 期间静默收集引擎内部产生的所有生命周期事件，最终聚合为格式化的 JSON 报文返回。
async fn handle_run_workflow(
    State(state): State<AppState>,
    Json(payload): Json<WorkflowRunRequest>,
) -> impl IntoResponse {
    let run_timestamp = Local::now().format("%Y%m%d_%H%M%S").to_string();
    let initial_input = payload
        .initial_input
        .unwrap_or_else(|| "User Request: 开始运行！".to_string());

    // 优先使用请求体中携带的配置路径，否则降级回滚至系统全局默认路径
    let target_config = payload.config_path.unwrap_or(state.default_config_path);

    // 动态反序列化并构建具有环路校验的安全执行引擎
    let engine_result = GraphBuilder::build_from_yaml(
        &target_config,
        state.debug_mode,
        &run_timestamp,
        &initial_input,
    );

    let engine = match engine_result {
        Ok(e) => e,
        Err(err) => {
            let response = WorkflowRunResponse {
                status: "Build_Error".to_string(),
                execution_time: run_timestamp,
                logs: vec![format!("读取配置 [{}] 失败: {}", target_config, err)],
                final_results: vec![],
            };

            // 构建失败时，返回格式化后的 JSON 异常信息
            let pretty_json = serde_json::to_string_pretty(&response).unwrap_or_default();
            return (
                [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
                pretty_json,
            )
                .into_response();
        }
    };

    let (tx, mut rx) = mpsc::channel::<ExecutionEvent>(32);

    // 将引擎图执行上下文移交至后台任务池
    tokio::spawn(async move {
        let _ = engine.execute_dag(tx).await;
    });

    let mut logs = Vec::new();
    let mut final_results = Vec::new();

    // 在 Headless 模式下消费执行事件流水，剥离渲染需求，纯粹归档数据
    while let Some(event) = rx.recv().await {
        match event {
            ExecutionEvent::Started { node_id, node_name } => {
                logs.push(format!("▶️ [{}] {} 开始执行", node_id, node_name));
            }
            ExecutionEvent::Finished {
                node_id,
                node_name,
                result,
            } => {
                logs.push(format!("✔️ [{}] {} 执行成功", node_id, node_name));
                final_results.push(format!("==== Node: {} ====\n{}", node_id, result));
            }
            ExecutionEvent::Failed { node_id, error } => {
                logs.push(format!("❌ [{}] 执行崩溃: {}", node_id, error));
            }
            ExecutionEvent::Skipped { node_id } => {
                logs.push(format!("⏭️ [{}] 被路由跳过", node_id));
            }
            _ => {} // Debug 与流式日志在同步 API 返回模式中暂不暴露以压缩报文体积
        }
    }

    let response = WorkflowRunResponse {
        status: "Success".to_string(),
        execution_time: run_timestamp,
        logs,
        final_results,
    };

    // 工作流执行成功后，强制序列化为带缩进的 Pretty JSON 格式返回
    let pretty_json = serde_json::to_string_pretty(&response).unwrap_or_default();
    (
        [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
        pretty_json,
    )
        .into_response()
}

/// DAG 拓扑图离线提取路由控制器。
///
/// 响应 GET 请求，读取给定的 YAML 配置文件，通过内置工厂解析，
/// 在不触发任何实际计算的前提下，输出该工作流的静态 Mermaid 图表文本。
async fn handle_get_topology(
    State(state): State<AppState>,
    Query(query): Query<TopologyQuery>,
) -> String {
    let run_timestamp = Local::now().format("%Y%m%d_%H%M%S").to_string();

    // 解析目标配置文件路径
    let target_config = query.config_path.unwrap_or(state.default_config_path);

    // 实例化引擎构建图谱（包含底层的拓扑校验逻辑）
    let engine_result = GraphBuilder::build_from_yaml(
        &target_config,
        state.debug_mode,
        &run_timestamp,
        "API Topology Check",
    );

    match engine_result {
        Ok(engine) => engine.generate_mermaid(),
        Err(e) => format!("⚠️ 拓扑图解析失败，请检查 [{}] YAML 语法: {}", target_config, e),
    }
}

/// POSIX 信号监听器，用于实现 Web 服务的优雅停机 (Graceful Shutdown)。
///
/// 注册对 `SIGINT` (Ctrl+C) 和 `SIGTERM` 的系统底层监听。接收到信号后，
/// 会通知 Axum 拒绝新连接，并在退出前等待既有请求的任务安全释放。
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("未能安装 Ctrl+C 信号监听器");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("未能安装 POSIX 终止信号监听器")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            println!("\n🛑 [退出指令] 收到 Ctrl+C (SIGINT)，准备安全关闭服务器...");
        },
        _ = terminate => {
            println!("\n🛑 [退出指令] 收到系统终止信号 (SIGTERM)，准备安全关闭服务器...");
        },
    }

    println!("⏳ 正在等待当前处理中的工作流任务安全着陆...");
}
