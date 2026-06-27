//! # AgentFlow-RS 应用程序主入口 (Main Entrypoint)
//!
//! 本模块作为工作流引擎的启动容器，负责解析系统命令行参数、加载全局环境上下文，
//! 并根据传入的指令将系统路由至不同的运行模式：
//! - 图形化设计模式 (GUI)
//! - 静态拓扑导出模式 (DOT / Mermaid 静态分析)
//! - 无头网络服务模式 (Headless Web API Server)
//! - 终端交互监控模式 (TUI)

// src/main.rs
mod error;
mod executor;
mod graph;
mod gui;
mod node;
mod server;
mod tui;

use executor::ExecutionEvent;
use tui::{AppState, draw_ui};

use crossterm::{
    cursor::{Hide, Show},
    event::{self, DisableMouseCapture, EnableMouseCapture, KeyCode, KeyEventKind},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};

use chrono::Local;
use clap::Parser;
use dotenvy::dotenv;
use ratatui::{Terminal, backend::CrosstermBackend};
use std::{
    io::{self, BufWriter},
    time::Duration,
};
use tokio::sync::{broadcast, mpsc};

/// 应用程序命令行启动参数配置定义。
#[derive(Parser, Debug)]
#[command(author, version, about = "AgentFlow-RS: 高性能异步工作流调度引擎", long_about = None)]
struct Args {
    /// 指定目标工作流的 YAML 配置文件路径。
    #[arg(short, long)]
    config: Option<String>,

    /// 启用调试模式，将底层网络遥测与节点执行明细输出至日志系统。
    #[arg(short, long, default_value_t = false)]
    debug: bool,

    /// [静态分析] 将当前工作流的 DAG 拓扑图导出为 Graphviz (.dot) 格式文件，并在导出后自动退出。
    #[arg(long)]
    export_dot: Option<String>,

    /// [静态分析] 将当前工作流的 DAG 拓扑图导出为 Mermaid (.md) 格式文件，并在导出后自动退出。
    #[arg(long)]
    export_mermaid: Option<String>,

    /// [网络服务] 启动 Web API 服务器，以 Headless 无界面的后台守护模式运行。
    #[arg(long, default_value_t = false)]
    server: bool,

    /// [网络服务] 指定 API 服务器监听的本地网络端口号。
    #[arg(short, long, default_value_t = 8080)]
    port: u16,

    /// [图形界面] 启动本地图形化编辑器 (GUI)，支持通过可视化拖拽构建工作流配置。
    #[arg(long, default_value_t = false)]
    gui: bool,
}

/// 应用程序异步主入口。
///
/// 负责系统初始化、环境上下文装载，并根据参数解析将执行流路由至不同子系统。
///
/// # Errors
///
/// 当底层套接字网络绑定失败、终端环境初始化异常或工作流配置格式损坏时，将返回 `anyhow::Error`。
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 针对 Windows 平台，在引擎初始化初期强制切换终端活动代码页至 UTF-8 (65001)，
    // 以防止派生子进程 (如 cmd.exe) 时发生输出编码解码错乱。
    if cfg!(target_os = "windows") {
        std::process::Command::new("cmd")
            .args(["/C", "chcp 65001 > nul"])
            .output()
            .ok();
    }

    // 解析命令行参数并加载本地环境变量配置文件 (.env)。
    let args = Args::parse();
    dotenv().ok();

    let run_timestamp = Local::now().format("%Y%m%d_%H%M%S").to_string();

    // 路由分支：图形化编辑器模式
    if args.gui {
        println!("🎨 正在启动原生图形化节点编辑器 (Design-Time)...");

        let config_path = args.config.unwrap_or_else(|| "".to_string());

        // 阻塞当前异步运行时，将控制权安全移交至同步的 GUI 主事件循环。
        let _ = tokio::task::block_in_place(|| gui::start_gui(&config_path));

        println!("👋 图形化设计器已关闭，GUI 安全退出！");

        return Ok(());
    }

    let target_config = args.config.unwrap_or_else(|| "workflow.yaml".to_string());

    let (broadcast_tx, _broadcast_rx) = broadcast::channel::<char>(16);

    // 读取配置文件，反序列化并构建具有循环依赖校验的 DAG 执行引擎内存拓扑模型。
    let engine = graph::GraphBuilder::build_from_yaml(
        &target_config,
        args.debug,
        &run_timestamp,
        "User Request: 开始运行！",
        broadcast_tx.clone(),
    )?;

    // 路由分支：静态拓扑分析与导出模式
    // 若命中此分支，引擎仅输出模型静态图，不触发任何实际的节点调度。
    if let Some(dot_name) = args.export_dot {
        let out_name = format!(
            "{}_{}.dot",
            dot_name.trim_end_matches(".dot"),
            run_timestamp
        );
        let out_path = format!("outputs/visualizations/{}", out_name);

        std::fs::create_dir_all("outputs/visualizations").ok();

        engine.export_to_dot(&out_path)?;
        println!("✅ 成功: DAG 拓扑结构已导出至 `{}`", out_path);
        println!("💡 提示: 这是一个标准的 Graphviz 文件。");
        println!("   - 命令行渲染: `dot -Tpng {} -o graph.png`", out_path);
        println!(
            "   - 在线免安装查看: 请访问 https://dreampuf.github.io/GraphvizOnline/ 并粘贴文件内容。"
        );
        return Ok(());
    }

    if let Some(mmd_name) = args.export_mermaid {
        let out_name = format!("{}_{}.md", mmd_name.trim_end_matches(".md"), run_timestamp);
        let out_path = format!("outputs/visualizations/{}", out_name);

        std::fs::create_dir_all("outputs/visualizations").ok();

        engine.export_to_mermaid(&out_path)?;
        println!("✅ 成功: DAG 已导出为 Mermaid 格式至 `{}`", out_path);
        println!(
            "💡 提示: 你可以直接将里面的内容粘贴到 GitHub 的 Markdown 文件中，或者访问 https://mermaid.live/ 在线预览！"
        );
        return Ok(());
    }

    // 路由分支：无头 (Headless) 服务端模式
    // 挂起控制台，初始化 Web API 监听队列。
    if args.server {
        return server::start_api_server(args.port, target_config.clone(), args.debug).await;
    }

    // 路由分支：终端用户界面 (TUI) 监控模式
    // 初始化终端状态句柄，进入备用屏幕并启用终端原生输入捕获。
    enable_raw_mode()?;
    let mut terminal_backend = BufWriter::new(io::stderr());
    execute!(
        terminal_backend,
        EnterAlternateScreen,
        EnableMouseCapture,
        Hide
    )?;
    let backend = CrosstermBackend::new(terminal_backend);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    // 获取已计算的 DAG 拓扑执行顺序，以便初始化 UI 面板状态。
    let topology_order = engine.topological_order.clone();

    // 建立执行引擎与 UI 之间的异步通信管道。
    let (tx, mut rx) = mpsc::channel::<ExecutionEvent>(32);

    // 将执行调度任务提交至 Tokio 异步后台线程池。
    tokio::spawn(async move {
        let _ = engine.execute_dag(tx).await;
    });

    // 计算列表项总数（包含一个用于展示系统全局聚合状态的虚拟首节点）。
    let total_list_items = topology_order.len() + 1;

    // 提取各节点的原始 YAML 配置声明特征，用于在 TUI 的侧边监控面板中展示详细运行时属性。
    let config_content = std::fs::read_to_string(&target_config).unwrap_or_default();
    let parsed_yaml: Result<serde_yaml::Value, _> = serde_yaml::from_str(&config_content);
    let mut node_configs_map = std::collections::HashMap::new();

    // 递归解析 YAML 语法树节点映射体系。
    if let Ok(value) = parsed_yaml
        && let Some(nodes) = value.get("nodes").and_then(|n| n.as_sequence()) {
            for node in nodes {
                if let Some(id) = node.get("id").and_then(|i| i.as_str()) {
                    let mut yaml_str = serde_yaml::to_string(node).unwrap_or_default();
                    if yaml_str.starts_with("---\n") {
                        yaml_str = yaml_str[4..].to_string();
                    }
                    node_configs_map.insert(id.to_string(), yaml_str);
                }
            }
        }

    // 初始化 TUI 全局状态机。
    let mut app_state = AppState::new(topology_order, &run_timestamp, node_configs_map);

    // 调试模式系统前置日志注入。
    if args.debug {
        app_state.logs.push(format!(
            "🔧 [Debug模式已开启] 当前加载配置: {}",
            target_config
        ));
    }

    let mut ticker = tokio::time::interval(Duration::from_millis(50));

    // 启动终端界面的主事件轮询循环。
    'mainLoop: loop {
        tokio::select! {
            // 监听并处理执行引擎向上传递的状态流转信令事件。
            Some(event) = rx.recv() => {
                match event {
                    ExecutionEvent::Started { node_id, node_name } => {
                        app_state.running_tasks += 1;
                        app_state.node_status.insert(node_id.clone(), format!("⏳ [{}] 思考中...", node_name));
                        app_state.add_log(format!("▶️ 节点 {} 开始运行...", node_id));
                    }
                    ExecutionEvent::Finished { node_id, node_name, result } => {
                        app_state.running_tasks = app_state.running_tasks.saturating_sub(1);
                        app_state.node_status.insert(node_id.clone(), "✅ 成功完成".to_string());
                        app_state.node_results.insert(node_id.clone(), result.clone());

                        let char_count = result.chars().count();

                        // 应用大文本载荷防刷屏截断策略。特定语义组件（如 AI 与搜索中枢）默认完全放行以保证信息溯源度。
                        let display_result = if char_count > 300
                            && !node_name.contains("AI Agent")
                            && !node_name.contains("智能搜索")
                        {
                            let truncated: String = result.chars().take(300).collect();
                            format!("{} ...\n\n  (✨ 截断保护: 真实文本总计 {} 字，已传递给下游计算中枢)", truncated, char_count)
                        } else {
                            result.clone()
                        };

                        app_state.add_log(format!("✔️ 节点 {} 输出:\n{}", node_id, display_result));
                    }
                    ExecutionEvent::Failed { node_id, error } => {
                        app_state.running_tasks = app_state.running_tasks.saturating_sub(1);
                        app_state.node_status.insert(node_id.clone(), "❌ 运行失败".to_string());
                        app_state.add_log(format!("⚠️ 节点 {} 报错: {}", node_id, error));
                    }
                    ExecutionEvent::RequireApproval { node_id, prompt } => {
                        app_state.awaiting_approval = Some(node_id.clone());
                        app_state.add_log(format!("⚠️ 节点 {} 暂停！请求人工审批: {}", node_id, prompt));
                    }
                    ExecutionEvent::DebugLog { node_id, message } => {
                        app_state.add_log(format!("   🔍 [Debug-{}] {}", node_id, message));
                    }
                    ExecutionEvent::StreamLog { node_id: _, message } => {
                        app_state.add_log(message);
                    }
                    ExecutionEvent::Skipped { node_id } => {
                        // 节点因未命中路由网关判断条件被跳过。
                        // 跳过逻辑并未触发 `Started` 状态，故无需扣减并发任务计数器。
                        app_state.node_status.insert(node_id.clone(), "⏭️ 被路由跳过".to_string());
                        app_state.add_log(format!("⏭️ 节点 {} 因条件不符被自动跳过", node_id));
                    }
                }
            }

            // 处理时钟中断信号以及外部控制台外设事件。
            _ = ticker.tick() => {
                app_state.spinner_tick = app_state.spinner_tick.wrapping_add(1);

                while event::poll(Duration::from_millis(0))? {
                    match event::read()? {
                        // 处理键盘终端中断与逻辑映射事件。
                        event::Event::Key(key) if key.kind == KeyEventKind::Press => {
                            if let Some(ref _node_id) = app_state.awaiting_approval {
                                match key.code {
                                    KeyCode::Char('y') => { let _ = broadcast_tx.send('y'); app_state.awaiting_approval = None; }
                                    KeyCode::Char('n') => { let _ = broadcast_tx.send('n'); app_state.awaiting_approval = None; }
                                    _ => {}
                                }
                            } else {
                                match key.code {
                                    KeyCode::Char('q') => break 'mainLoop,
                                    KeyCode::Esc => {
                                        break 'mainLoop;
                                    }
                                    KeyCode::Up => app_state.previous_node(),
                                    KeyCode::Down => app_state.next_node(total_list_items),
                                    KeyCode::PageUp | KeyCode::Char('w') => app_state.previous_node(),
                                    KeyCode::PageDown | KeyCode::Char('s') => app_state.next_node(total_list_items),
                                    _ => {}
                                }
                            }
                        }
                        // 处理鼠标外设位置检测与交互事件。
                        event::Event::Mouse(mouse) => {
                            match mouse.kind {
                                event::MouseEventKind::ScrollUp => app_state.scroll_logs_up(),
                                event::MouseEventKind::ScrollDown => app_state.scroll_logs_down(),
                                // 捕获鼠标左键点击信号，计算锚点并映射为侧边栏界面的焦点切换动作。
                                event::MouseEventKind::Down(event::MouseButton::Left)
                                    if mouse.column < 45 && mouse.row > 0 => {
                                        let clicked_index = (mouse.row - 1) as usize;
                                        if clicked_index < total_list_items {
                                            app_state.node_list_state.select(Some(clicked_index));
                                            // 焦点切换后，将右侧详细日志视图的滚动条重置回显底部。
                                            app_state.log_scroll_offset = 0;
                                            app_state.auto_scroll = true;
                                        }
                                    }
                                _ => {}
                            }
                        }
                        event::Event::Resize(_, _) => {}
                        _ => {}
                    }
                }
            }
        }

        terminal.draw(|f| draw_ui(f, &mut app_state))?;
    }

    // 退出前挂起的资源清理与终端模式还原操作。
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture,
        Show
    )?;

    println!("👋 AgentFlow-RS 已安全退出！");
    Ok(())
}
