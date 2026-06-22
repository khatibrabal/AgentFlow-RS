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
    event::{self, DisableMouseCapture, EnableMouseCapture, KeyCode, KeyEventKind}, // ✨ 新增
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

// ✨ 2. 定义高颜值的命令行参数结构体
/// AgentFlow-RS: 极客终端大模型异步工作流引擎
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// 指定工作流 YAML 配置文件路径
    #[arg(short, long)]
    config: Option<String>,

    /// 开启 Debug 模式，在 TUI 中显示更多底层调试日志
    #[arg(short, long, default_value_t = false)]
    debug: bool,

    /// [静态分析] 导出当前工作流的 DAG 拓扑图为 Graphviz .dot 文件，完成后自动退出
    #[arg(long)]
    export_dot: Option<String>,

    /// [静态分析] 导出当前工作流的 DAG 拓扑图为 Mermaid .md 文件
    #[arg(long)]
    export_mermaid: Option<String>,

    /// [网络服务] 启动 Web API 服务器 (Headless 模式，无终端UI)
    #[arg(long, default_value_t = false)]
    server: bool,

    /// [网络服务] 指定 API 服务器监听的端口号
    #[arg(short, long, default_value_t = 8080)]
    port: u16,

    /// [可视化设计态] 启动本地图形界面，拖拽生成工作流配置
    #[arg(long, default_value_t = false)]
    gui: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. 解析命令行参数 & 加载环境变量
    // ✨ 这行代码会自动拦截 --help，或者把用户输入的参数映射到 args 变量中
    let args = Args::parse();
    dotenv().ok();

    let run_timestamp = Local::now().format("%Y%m%d_%H%M%S").to_string();

    if args.gui {
        println!("🎨 正在启动原生图形化节点编辑器 (Design-Time)...");
        // ✨ 修改：通过闭包将用户传入的 config 路径传递给 GUI！
        let config_path = args.config.unwrap_or_else(|| "".to_string());

        let _ = tokio::task::block_in_place(|| gui::start_gui(&config_path));
        return Ok(());
    }

    let target_config = args.config.unwrap_or_else(|| "workflow.yaml".to_string());

    // 2. 从 YAML 动态加载图调度引擎 (这里提前把图建好)
    let engine = graph::GraphBuilder::build_from_yaml(
        &target_config,
        args.debug,
        &run_timestamp,
        "User Request: 开始运行！",
    )?;

    // 🛑 静态分析模式拦截
    // 如果用户使用了 --export-dot 参数，我们只生成可视化文件，然后优雅退出，不进入 TUI
    if let Some(dot_name) = args.export_dot {
        // 如果用户传了名字比如 "my_graph"，我们就把它拼成 "my_graph_20260619_153022.dot"
        let out_name = format!(
            "{}_{}.dot",
            dot_name.trim_end_matches(".dot"),
            run_timestamp
        );
        let out_path = format!("outputs/visualizations/{}", out_name);

        // 自动创建目录
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

    // ✨ 核心分支路由：如果开启了 server 模式，进入 Web 服务挂起，不再初始化 TUI！
    if args.server {
        // 调用我们新写的服务端模块
        return server::start_api_server(args.port, target_config.clone(), args.debug).await;
    }

    // 3. 初始化终端环境
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

    // 4. 从 YAML 动态加载图调度引擎
    // 提前克隆一份排好的顺序给 UI
    let topology_order = engine.topological_order.clone();

    let (tx, mut rx) = mpsc::channel::<ExecutionEvent>(32);

    let (broadcast_tx, _broadcast_rx) = broadcast::channel::<char>(16);

    tokio::spawn(async move {
        let _ = engine.execute_dag(tx).await;
    });

    // ✨ 获取列表总项数（真实的节点数 + 1个系统全局节点）
    let total_list_items = topology_order.len() + 1;

    // 🚀 新增：为 Node Inspector 提取每个节点的 YAML 配置明细
    let config_content = std::fs::read_to_string(&target_config).unwrap_or_default();
    let parsed_yaml: Result<serde_yaml::Value, _> = serde_yaml::from_str(&config_content);
    let mut node_configs_map = std::collections::HashMap::new();

    // ✨ 修复：拆分不稳定的 if-let-chain，改为向后兼容的嵌套写法
    if let Ok(value) = parsed_yaml
        && let Some(nodes) = value.get("nodes").and_then(|n| n.as_sequence()) {
            for node in nodes {
                if let Some(id) = node.get("id").and_then(|i| i.as_str()) {
                    let mut yaml_str = serde_yaml::to_string(node).unwrap_or_default();
                    // 去掉 serde_yaml 自动生成的 "---" 文件头
                    if yaml_str.starts_with("---\n") {
                        yaml_str = yaml_str[4..].to_string();
                    }
                    node_configs_map.insert(id.to_string(), yaml_str);
                }
            }
        }

    // 5. TUI 事件主循环
    let mut app_state = AppState::new(topology_order, &run_timestamp, node_configs_map);

    // ✨ 如果用户加了 --debug 参数，我们在界面上打印一条特殊日志
    if args.debug {
        app_state.logs.push(format!(
            "🔧 [Debug模式已开启] 当前加载配置: {}",
            target_config
        ));
    }

    let mut ticker = tokio::time::interval(Duration::from_millis(50));

    'mainloop: loop {
        tokio::select! {
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

                        // ✨ 智能截断白名单：如果是 AI Agent 或 智能搜索 节点，不予截断！
                        let display_result = if char_count > 300
                            && !node_name.contains("AI Agent")
                            && !node_name.contains("智能搜索")
                        {
                            let truncated: String = result.chars().take(300).collect();
                            format!("{} ...\n\n  (✨ 截断保护: 真实文本总计 {} 字，已传递给下游计算中枢)", truncated, char_count)
                        } else {
                            result.clone() // 白名单节点，原样完整保留！
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
                        // 极客细节：被路由跳过的节点从来没有执行 Started，
                        // 所以绝对不能去减 running_tasks，否则会导致负数溢出崩溃！
                        app_state.node_status.insert(node_id.clone(), "⏭️ 被路由跳过".to_string());
                        app_state.add_log(format!("⏭️ 节点 {} 因条件不符被自动跳过", node_id));
                    }
                }
            }

            _ = ticker.tick() => {
                app_state.spinner_tick = app_state.spinner_tick.wrapping_add(1);


                while event::poll(Duration::from_millis(0))? {
                    match event::read()? {
                        // 1. 处理键盘事件
                        event::Event::Key(key) if key.kind == KeyEventKind::Press => {
                            if let Some(ref _node_id) = app_state.awaiting_approval {
                                match key.code {
                                    KeyCode::Char('y') => { let _ = broadcast_tx.send('y'); app_state.awaiting_approval = None; }
                                    KeyCode::Char('n') => { let _ = broadcast_tx.send('n'); app_state.awaiting_approval = None; }
                                    _ => {}
                                }
                            } else {
                                match key.code {
                                    KeyCode::Char('q') => break 'mainloop,
                                    KeyCode::Esc => {
                                        // if app_state.node_list_state.selected().unwrap_or(0) != 0 {
                                        //     app_state.node_list_state.select(Some(0));
                                        //     app_state.log_scroll_offset = 0;
                                        //     app_state.auto_scroll = true;
                                        // } else {
                                        break 'mainloop;
                                        // }
                                    }
                                    KeyCode::Up => app_state.previous_node(),
                                    KeyCode::Down => app_state.next_node(total_list_items),
                                    KeyCode::PageUp | KeyCode::Char('w') => app_state.previous_node(),
                                    KeyCode::PageDown | KeyCode::Char('s') => app_state.next_node(total_list_items),
                                    _ => {}
                                }
                            }
                        }
                        // 2. ✨ 处理鼠标滚轮与点击事件！
                        event::Event::Mouse(mouse) => {
                            match mouse.kind {
                                event::MouseEventKind::ScrollUp => app_state.scroll_logs_up(),
                                event::MouseEventKind::ScrollDown => app_state.scroll_logs_down(),
                                // ✨ 捕获鼠标左键点击！
                                event::MouseEventKind::Down(event::MouseButton::Left)
                                    if mouse.column < 45 && mouse.row > 0 => {
                                        let clicked_index = (mouse.row - 1) as usize;
                                        if clicked_index < total_list_items {
                                            app_state.node_list_state.select(Some(clicked_index));
                                            // 点击切换后，将右侧滚动条复位
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

    // 6. 安全退出
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
