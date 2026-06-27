//! # 终端用户界面模块 (TUI)
//!
//! 本模块基于 `ratatui` 构建，负责在终端环境中渲染交互式的有向无环图 (DAG) 工作流执行状态。
//! 核心功能涵盖：运行时长的智能格式化、Markdown 语法的实时解析与高亮、
//! 系统全局状态管理，以及高度定制化的控制台布局切割与重绘逻辑。

// src/tui.rs
use chrono::Local;
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout},
    style::{Color, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Wrap},
};
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::Write,
    time::Instant,
};
use unicode_width::UnicodeWidthStr;

/// 将总秒数转换为符合人类阅读习惯的“天/时/分/秒”格式。
///
/// # Arguments
///
/// * `total_seconds` - 经过的总耗时（秒），支持浮点数以提供毫秒级的精度。
///
/// # Returns
///
/// 返回格式化后的字符串。对于秒数部分，固定保留两位小数以展示运行精度。
fn format_duration(total_seconds: f32) -> String {
    if total_seconds < 0.0 {
        return "0s".to_string();
    }

    let total_secs_int = total_seconds as u64;

    let days = total_secs_int / 86400;
    let hours = (total_secs_int % 86400) / 3600;
    let minutes = (total_secs_int % 3600) / 60;
    let seconds = total_seconds % 60.0;

    if days > 0 {
        format!("{}d {}h {}m {:.2}s", days, hours, minutes, seconds)
    } else if hours > 0 {
        format!("{}h {}m {:.2}s", hours, minutes, seconds)
    } else if minutes > 0 {
        format!("{}m {:.2}s", minutes, seconds)
    } else {
        format!("{:.2}s", seconds)
    }
}

/// Markdown 语法与结构化日志解析器。
///
/// 将原始字符串解析为带有 `ratatui` 样式信息的 `Line` 对象，支持标题、引用、
/// 时间戳过滤、特殊标签前缀着色以及双星号 (`**`) 的内联加粗语法。
///
/// # Arguments
///
/// * `line` - 待解析的单行纯文本字符串。
///
/// # Returns
///
/// 返回带有分段样式 (`Span`) 的 `Line` 对象，可直接交由 UI 组件进行渲染。
fn highlight_log_line(line: &str) -> Line<'_> {
    // 解析 Markdown 标题层级，独占整行并使用统一高亮
    if line.starts_with("### ") || line.starts_with("## ") || line.starts_with("# ") {
        return Line::from(Span::styled(line, Style::default().fg(Color::Cyan).bold()));
    }

    let mut spans = Vec::new();
    let mut remaining = line;

    // 提升基础样式的定义域，以实现全局基准色的统一管理
    let mut base_style = Style::default().fg(Color::White);

    // 解析 Markdown 引用区块，剥离前缀并覆盖后续的全局基准色
    if let Some(rest) = remaining.strip_prefix("> ") {
        base_style = Style::default().fg(Color::Rgb(171, 215, 223)).italic();
        spans.push(Span::styled("> ", base_style));
        remaining = rest;
    }

    // 提取并弱化头部时间戳显示 (例如 [2026-06-19 12:00:00])
    if remaining.starts_with('[')
        && remaining.chars().nth(1).is_some_and(|c| c.is_ascii_digit())
        && let Some(end_idx) = remaining.find(']')
    {
        let prefix = &remaining[..=end_idx];
        spans.push(Span::styled(prefix, Style::default().fg(Color::DarkGray)));
        remaining = &remaining[end_idx + 1..];
        if let Some(rest) = remaining.strip_prefix(' ') {
            spans.push(Span::raw(" "));
            remaining = rest;
        }
    }

    // 基于系统日志的特定前缀标识符进行动态的基准色路由
    if let Some(rest) = remaining.strip_prefix("▶️ ") {
        spans.push(Span::styled("▶️ ", Style::default().fg(Color::LightGreen)));
        base_style = Style::default().fg(Color::LightGreen);
        remaining = rest;
    } else if let Some(rest) = remaining.strip_prefix("✔️ ") {
        spans.push(Span::styled(
            "✔️ ",
            Style::default().fg(Color::Rgb(145, 213, 241)).bold(),
        ));
        base_style = Style::default().fg(Color::Rgb(145, 213, 241));
        remaining = rest;
    } else if let Some(rest) = remaining.strip_prefix("❌ ") {
        spans.push(Span::styled(
            "❌ ",
            Style::default().fg(Color::LightRed).bold(),
        ));
        base_style = Style::default().fg(Color::LightRed);
        remaining = rest;
    } else if let Some(rest) = remaining.strip_prefix("⚠️ ") {
        spans.push(Span::styled(
            "⚠️ ",
            Style::default().fg(Color::Rgb(241, 187, 62)).bold(),
        ));
        base_style = Style::default().fg(Color::Rgb(241, 187, 62));
        remaining = rest;
    } else if let Some(rest) = remaining.strip_prefix("🔍 ") {
        spans.push(Span::styled("🔍 ", Style::default().fg(Color::LightCyan)));
        base_style = Style::default().fg(Color::LightCyan);
        remaining = rest;
    } else if let Some(rest) = remaining.strip_prefix("🧠 ") {
        spans.push(Span::styled(
            "🧠 ",
            Style::default().fg(Color::Rgb(243, 198, 213)),
        ));
        base_style = Style::default().fg(Color::Rgb(243, 198, 213));
        remaining = rest;
    } else if let Some(rest) = remaining.strip_prefix("🛠️ ") {
        spans.push(Span::styled(
            "🛠️ ",
            Style::default().fg(Color::Rgb(241, 187, 62)).bold(),
        ));
        base_style = Style::default().fg(Color::Rgb(241, 187, 62));
        remaining = rest;
    } else if let Some(rest) = remaining.strip_prefix("✨ ") {
        spans.push(Span::styled(
            "✨ ",
            Style::default().fg(Color::Rgb(145, 213, 241)).bold(),
        ));
        base_style = Style::default().fg(Color::Rgb(145, 213, 241));
        remaining = rest;
    } else if let Some(rest) = remaining.strip_prefix("🔄 ") {
        spans.push(Span::styled(
            "🔄 ",
            Style::default().fg(Color::LightCyan).bold(),
        ));
        base_style = Style::default().fg(Color::LightCyan);
        remaining = rest;
    } else if let Some(rest) = remaining.strip_prefix("🌐 ") {
        spans.push(Span::styled(
            "🌐 ",
            Style::default().fg(Color::Rgb(241, 131, 122)).bold(),
        ));
        base_style = Style::default().fg(Color::Rgb(241, 131, 122));
        remaining = rest;
    } else if let Some(rest) = remaining.strip_prefix("[系统] ") {
        spans.push(Span::styled(
            "[系统] ",
            Style::default().fg(Color::LightCyan),
        ));
        base_style = Style::default().fg(Color::LightCyan);
        remaining = rest;
    }

    // 拦截无序与有序列表项结构
    if remaining.starts_with("- ") || remaining.starts_with("* ") {
        spans.push(Span::styled(
            &remaining[..2],
            Style::default().fg(Color::Yellow).bold(),
        ));
        remaining = &remaining[2..];
        base_style = Style::default().fg(Color::Rgb(200, 234, 213));
    } else {
        let mut digit_count = 0;
        for c in remaining.chars() {
            if c.is_ascii_digit() {
                digit_count += 1;
            } else {
                break;
            }
        }
        if digit_count > 0 && remaining[digit_count..].starts_with('.') {
            let mut space_end = digit_count + 1;
            while space_end < remaining.len() && remaining[space_end..].starts_with(' ') {
                space_end += 1;
            }
            if space_end > digit_count + 1 {
                spans.push(Span::styled(
                    &remaining[..space_end],
                    Style::default().fg(Color::Yellow).bold(),
                ));
                remaining = &remaining[space_end..];
                base_style = Style::default().fg(Color::Rgb(200, 234, 213));
            }
        }
    }

    // 解析内联加粗语法 (**关键词**)
    let parts: Vec<&str> = remaining.split("**").collect();
    if parts.len() > 1 {
        for (i, part) in parts.iter().enumerate() {
            if part.is_empty() {
                continue;
            }

            if i % 2 == 1 {
                spans.push(Span::styled(
                    *part,
                    base_style.fg(Color::LightYellow).bold(),
                ));
            } else {
                spans.push(Span::styled(*part, base_style));
            }
        }
    } else if !remaining.is_empty() {
        spans.push(Span::styled(remaining, base_style));
    }

    Line::from(spans)
}

/// TUI 客户端全局状态机。
///
/// 维护当前界面的选中焦点、滚动偏移量、运行监控指标以及磁盘日志文件句柄。
pub struct AppState {
    /// 存储所有聚合的系统全局日志。
    pub logs: Vec<String>,
    /// 跟踪各节点的运行状态摘要。
    pub node_status: HashMap<String, String>,
    /// 跟踪各节点执行完毕后的输出载荷，以供选中审阅。
    pub node_results: HashMap<String, String>,
    /// 缓存从底层读取的各节点原始 YAML 配置声明。
    pub node_configs: HashMap<String, String>,
    /// 左侧节点清单组件的内置焦点状态管理。
    pub node_list_state: ListState,
    /// 右侧日志视图距离底部的滚动偏移量（行数）。
    pub log_scroll_offset: u16,
    /// 记录 TUI 启动的时间，用于推算引擎生命周期运行总耗时。
    pub start_time: Instant,
    /// 监控底层执行引擎当前正在并发处理的任务数。
    pub running_tasks: usize,
    /// 调试日志追加写入的底层持久化文件句柄。
    pub log_file: File,
    /// 各节点在界面的标准展示顺序（基于引擎提供的拓扑排序）。
    pub display_order: Vec<String>,
    /// 终端动画占位符的帧索引计数器。
    pub spinner_tick: usize,
    /// 控制日志区域是否自动跟随最新输出滚动的开关。
    pub auto_scroll: bool,
    /// 记录当前正在等待人工干预授权的节点 ID。
    pub awaiting_approval: Option<String>,
}

impl AppState {
    /// 构造并初始化全局 TUI 状态。
    ///
    /// 在初始化内存状态的同时，建立对指定目录下日志文件的持久化写入连接。
    ///
    /// # Arguments
    ///
    /// * `display_order` - 经过拓扑排序的节点标识符列表。
    /// * `timestamp` - 引擎本次启动的全局时间戳。
    /// * `node_configs` - 预解析的全量节点配置静态映射表。
    ///
    /// # Panics
    ///
    /// 若无法在 `outputs/debug_logs` 目录下创建或打开日志文件，此函数将直接触发恐慌。
    pub fn new(
        display_order: Vec<String>,
        timestamp: &str,
        node_configs: HashMap<String, String>,
    ) -> Self {
        let log_dir = "outputs/debug_logs";
        fs::create_dir_all(log_dir).ok();

        let file_path = format!("{}/{}_ui_history.log", log_dir, timestamp);

        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
            .unwrap_or_else(|_| panic!("无法创建或打开工作流日志文件: {}", file_path));

        let mut state = Self {
            logs: Vec::new(),
            node_status: HashMap::new(),
            node_results: HashMap::new(),
            node_configs,
            node_list_state: ListState::default(),
            log_scroll_offset: 0,
            start_time: Instant::now(),
            running_tasks: 0,
            log_file,
            display_order,
            spinner_tick: 0,
            auto_scroll: true,
            awaiting_approval: None,
        };

        // 初始化节点状态表，预置默认等待描述
        for id in &state.display_order {
            state
                .node_status
                .insert(id.clone(), "💤 等待调度".to_string());
        }

        state.node_list_state.select(Some(0));
        state.add_log(
            "[系统] 初始化完成，按 'q' 退出，'↑/↓' 选择节点，'鼠标滚动' 滚屏...".to_string(),
        );
        state
    }

    /// 追加记录系统全局日志，执行双写策略（同时存入内存与磁盘）。
    pub fn add_log(&mut self, msg: String) {
        self.logs.push(msg.clone());
        if self.auto_scroll {
            self.log_scroll_offset = 0;
        }

        let now = Local::now().format("%Y-%m-%d %H:%M:%S");
        let _ = writeln!(self.log_file, "[{}] {}", now, msg);
    }

    /// 将节点列表的焦点向上移动一项。
    pub fn previous_node(&mut self) {
        let i = match self.node_list_state.selected() {
            Some(i) => {
                if i == 0 {
                    0
                } else {
                    i - 1
                }
            }
            None => 0,
        };
        self.node_list_state.select(Some(i));
        self.log_scroll_offset = 0;
        self.auto_scroll = true;
    }

    /// 将节点列表的焦点向下移动一项。
    pub fn next_node(&mut self, total_len: usize) {
        let i = match self.node_list_state.selected() {
            Some(i) => {
                if i >= total_len.saturating_sub(1) {
                    total_len.saturating_sub(1)
                } else {
                    i + 1
                }
            }
            None => 0,
        };
        self.node_list_state.select(Some(i));
        self.log_scroll_offset = 0;
        self.auto_scroll = true;
    }

    /// 向上翻阅历史日志（增加针对底部基准的偏移行数）。
    pub fn scroll_logs_up(&mut self) {
        self.log_scroll_offset = self.log_scroll_offset.saturating_add(2);
        self.auto_scroll = false;
    }

    /// 向下翻阅最新日志（减少针对底部基准的偏移行数）。
    pub fn scroll_logs_down(&mut self) {
        self.log_scroll_offset = self.log_scroll_offset.saturating_sub(2);
        if self.log_scroll_offset == 0 {
            self.auto_scroll = true;
        }
    }

    /// 获取当前动画占位符的具体字符形态。
    pub fn get_spinner_frame(&self) -> &str {
        let frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
        frames[self.spinner_tick % frames.len()]
    }
}

/// 终端界面渲染的主入口调用。
///
/// 此方法在事件循环中触发，负责按比例划分屏幕布局、抽取系统状态，
/// 并调度相应的 `ratatui` 组件进行控制台绘图。
///
/// # Arguments
///
/// * `f` - 控制台渲染帧上下文。
/// * `app_state` - 系统的全局共享状态机引用。
pub fn draw_ui(f: &mut Frame, app_state: &mut AppState) {
    // 垂直切分布局：主视区占用多数空间，底部状态栏固定长度
    let main_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(10), Constraint::Length(3)].as_ref())
        .split(f.size());

    // 水平切分布局：左侧导航面板固定宽度，右侧日志面板自适应填充剩余空间
    let top_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(45), Constraint::Min(0)].as_ref())
        .split(main_chunks[0]);

    let mut node_items = Vec::new();
    let global_status = if app_state.running_tasks > 0 {
        "⚡ 引擎运转中"
    } else {
        "🌟 待机/结束"
    };
    node_items.push(ListItem::new(format!(
        " 🌐 系统全局监控 | {}",
        global_status
    )));

    // 基于引擎预计算的拓扑顺序进行结构渲染
    for id in &app_state.display_order {
        let mut status = app_state.node_status.get(id).unwrap().clone();
        if status.contains("思考中...") {
            status = status.replace("⏳", app_state.get_spinner_frame());
        }
        let display_text = format!(" 📦 {} | {}", id, status);
        node_items.push(ListItem::new(display_text));
    }

    let nodes_list = List::new(node_items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" 拓扑节点状态 (DAG) "),
        )
        .highlight_style(
            Style::default()
                .bg(Color::DarkGray)
                .fg(Color::Rgb(241, 187, 62))
                .bold(),
        )
        .highlight_symbol(">> ");

    f.render_stateful_widget(nodes_list, top_chunks[0], &mut app_state.node_list_state);

    // 基于左侧焦点的视图路由：若选择全局节点则呈现系统日志，若选择独立节点则渲染组件详情
    let selected_idx = app_state.node_list_state.selected().unwrap_or(0);

    let log_text = if selected_idx == 0 {
        app_state.logs.join("\n\n")
    } else {
        let real_node_id = &app_state.display_order[selected_idx - 1];

        // 抽取原始配置文件信息，附加引用块前缀以触发 Markdown 高亮渲染
        let raw_yaml = app_state
            .node_configs
            .get(real_node_id)
            .map(|s| s.as_str())
            .unwrap_or("无配置信息");
        let formatted_yaml = raw_yaml
            .lines()
            .map(|l| format!("> {}", l))
            .collect::<Vec<_>>()
            .join("\n");

        if let Some(result) = app_state.node_results.get(real_node_id) {
            format!(
                "### 📦 节点 [{}] 详细信息\n\n**⚙️ 节点配置:**\n{}\n\n**✨ 最终输出:**\n\n{}",
                real_node_id, formatted_yaml, result
            )
        } else {
            let status = app_state
                .node_status
                .get(real_node_id)
                .unwrap_or(&String::new())
                .clone();
            format!(
                "### 📦 节点 [{}] 详细信息\n\n**⚙️ 节点配置:**\n{}\n\n**⏳ 当前状态:** {}\n> 请等待该节点完成系统调度...",
                real_node_id, formatted_yaml, status
            )
        }
    };

    // 遍历文本行，按序转换为带有颜色及样式特征的格式化段落组件
    let mut colored_lines = Vec::new();
    for line in log_text.lines() {
        if line.is_empty() {
            colored_lines.push(Line::default());
        } else {
            colored_lines.push(highlight_log_line(line));
        }
    }

    let inner_width = top_chunks[1].width.saturating_sub(2);
    let mut real_total_lines = 0;

    // 边界值安全校验：防止终端窗口被拉伸过小导致的除零异常
    if inner_width > 0 {
        for line in log_text.lines() {
            let line_width = line.width() as u16;

            if line_width <= inner_width {
                real_total_lines += 1;
                continue;
            }

            // 模拟底层渲染机制计算基于单词拆分的折行排版逻辑
            let mut current_line_width = 0;

            for word in line.split(' ') {
                let word_width = word.width() as u16;
                let space_width = if current_line_width > 0 { 1 } else { 0 };

                if current_line_width + space_width + word_width > inner_width {
                    if current_line_width > 0 {
                        real_total_lines += 1;
                    }

                    if word_width > inner_width {
                        // 防御性折行：直接切断超长且无空格的连续文本串
                        real_total_lines += word_width.div_ceil(inner_width) - 1;
                        current_line_width = word_width % inner_width;
                    } else {
                        current_line_width = word_width;
                    }
                } else {
                    current_line_width += space_width + word_width;
                }
            }
            if current_line_width > 0 {
                real_total_lines += 1;
            }
        }
    }

    real_total_lines += 1;

    let viewport_height = top_chunks[1].height.saturating_sub(2);
    let max_scroll = real_total_lines.saturating_sub(viewport_height);

    let scroll_y = if app_state.auto_scroll {
        max_scroll
    } else {
        max_scroll.saturating_sub(app_state.log_scroll_offset)
    };

    let logs_paragraph = Paragraph::new(colored_lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" AI Agent 工作流实时追踪 "),
        )
        .wrap(Wrap { trim: true })
        .scroll((scroll_y, 0));

    f.render_widget(logs_paragraph, top_chunks[1]);

    // 绘制底部系统全局运行状态监控栏
    let elapsed = app_state.start_time.elapsed().as_secs_f32();
    let formatted_time = format_duration(elapsed);

    let status_text = vec![Line::from(vec![
        Span::styled(
            format!("⏱️ 运行总耗时: {}  |  ", formatted_time),
            Style::default().fg(Color::Cyan),
        ),
        Span::styled(
            format!("🚀 当前活跃并发任务数: {}  |  ", app_state.running_tasks),
            Style::default().fg(Color::Yellow),
        ),
        Span::styled("⌨️ 按 'q' 退出 | ", Style::default().fg(Color::LightBlue)),
        Span::styled("'↑/↓' 选择节点 | ", Style::default().fg(Color::LightCyan)),
        Span::styled(
            "'鼠标滚动' 查看日志",
            Style::default().fg(Color::Rgb(200, 234, 213)),
        ),
    ])];

    let status_bar = Paragraph::new(status_text)
        .block(Block::default().borders(Borders::ALL))
        .style(Style::default().bg(Color::Black));

    f.render_widget(status_bar, main_chunks[1]);
}
