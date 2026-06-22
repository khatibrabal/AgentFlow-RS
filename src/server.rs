// src/server.rs
use axum::{
    Json, Router,
    extract::{Query, State}, // ✨ 引入 Query 用于 GET 请求参数
    routing::{get, post},
};
use chrono::Local;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tower_http::cors::CorsLayer;

use crate::executor::ExecutionEvent;
use crate::graph::GraphBuilder;

// 1. 共享的服务器全局状态
#[derive(Clone)]
pub struct AppState {
    pub default_config_path: String,
    pub debug_mode: bool,
}

// 2. 接收外部 POST 请求的 JSON 格式
#[derive(Deserialize)]
pub struct WorkflowRunRequest {
    // 允许外部传入临时参数覆盖工作流初始输入（可选）
    pub initial_input: Option<String>,
    // ✨ 新增：允许单次请求动态指定要运行的 YAML 文件路径
    pub config_path: Option<String>,
}

// ✨ 新增：接收外部 GET 请求的 Query 格式
#[derive(Deserialize)]
pub struct TopologyQuery {
    pub config_path: Option<String>,
}

// 3. 返回给外部的 JSON 响应格式
#[derive(Serialize)]
pub struct WorkflowRunResponse {
    pub status: String,
    pub execution_time: String,
    pub logs: Vec<String>,
    pub final_results: Vec<String>,
}

/// 启动 Axum Web 服务器
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
    [POST] http://127.0.0.1:{}/api/v1/workflow/run      -> 异步触发 Agent 工作流 (支持 ?config_path=...)

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
        .with_graceful_shutdown(shutdown_signal()) // ✨ 挂载停机监听器
        .await?;

    println!("👋 微服务已安全关闭，再见！");
    Ok(())
}

/// 处理工作流执行的核心控制器
async fn handle_run_workflow(
    State(state): State<AppState>,
    Json(payload): Json<WorkflowRunRequest>,
) -> Json<WorkflowRunResponse> {
    let run_timestamp = Local::now().format("%Y%m%d_%H%M%S").to_string();
    let initial_input = payload
        .initial_input
        .unwrap_or_else(|| "User Request: 开始运行！".to_string());

    // ✨ 核心逻辑：如果请求传了特定的路径，就用请求的；否则降级使用系统默认的
    let target_config = payload.config_path.unwrap_or(state.default_config_path);

    // 1. 动态构建 DAG 引擎
    let engine_result = GraphBuilder::build_from_yaml(
        &target_config,
        state.debug_mode,
        &run_timestamp,
        &initial_input,
    );

    let engine = match engine_result {
        Ok(e) => e,
        Err(err) => {
            return Json(WorkflowRunResponse {
                status: "Build_Error".to_string(),
                execution_time: run_timestamp,
                logs: vec![format!("读取配置 [{}] 失败: {}", target_config, err)],
                final_results: vec![],
            });
        }
    };

    let (tx, mut rx) = mpsc::channel::<ExecutionEvent>(32);

    // 2. 将引擎投入后台计算池
    tokio::spawn(async move {
        let _ = engine.execute_dag(tx).await;
    });

    let mut logs = Vec::new();
    let mut final_results = Vec::new();

    // 3. 静默收集执行事件（Headless 模式，不需要 TUI 界面）
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
                // 将执行结果存入数组准备返回给前端
                final_results.push(format!("==== Node: {} ====\n{}", node_id, result));
            }
            ExecutionEvent::Failed { node_id, error } => {
                logs.push(format!("❌ [{}] 执行崩溃: {}", node_id, error));
            }
            ExecutionEvent::Skipped { node_id } => {
                logs.push(format!("⏭️ [{}] 被路由跳过", node_id));
            }
            _ => {} // Debug 等事件在 API 返回中暂时忽略
        }
    }

    // 4. 返回包含完整日志和结果的 JSON
    Json(WorkflowRunResponse {
        status: "Success".to_string(),
        execution_time: run_timestamp,
        logs,
        final_results,
    })
}

async fn handle_get_topology(
    State(state): State<AppState>,
    Query(query): Query<TopologyQuery>, // ✨ 解析 URL 后的 ?config_path=xxx
) -> String {
    let run_timestamp = Local::now().format("%Y%m%d_%H%M%S").to_string();

    // ✨ 解析用户想要拉取哪个文件的拓扑图
    let target_config = query.config_path.unwrap_or(state.default_config_path);

    // 1. 每次收到请求时，动态去读取指定的 YAML 配置文件
    let engine_result = GraphBuilder::build_from_yaml(
        &target_config,
        state.debug_mode,
        &run_timestamp,
        "API Topology Check", // 占位用的 Payload，因为我们不执行，只是建图
    );

    // 2. 匹配结果：如果 YAML 正确，直接在内存中生成 Mermaid 字符串并通过 HTTP 返回！
    match engine_result {
        Ok(engine) => engine.generate_mermaid(),
        Err(e) => format!("⚠️ 拓扑图解析失败，请检查 [{}] YAML 语法: {}", target_config, e),
    }
}

/// 优雅停机信号监听器
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