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

/// 将总秒数转换为智能的“天/时/分/秒”格式
fn format_duration(total_seconds: f32) -> String {
    // 处理负数或极小的值
    if total_seconds < 0.0 {
        return "0s".to_string();
    }

    // 取整进行计算，保留浮点数用于最后的秒级显示
    let total_secs_int = total_seconds as u64;

    let days = total_secs_int / 86400;
    let hours = (total_secs_int % 86400) / 3600;
    let minutes = (total_secs_int % 3600) / 60;
    // 最后的秒数保留两位小数，展示运行的精确感
    let seconds = total_seconds % 60.0;

    // 智能拼接字符串
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

/// Markdown 语法与日志高亮解析器
fn highlight_log_line(line: &str) -> Line<'_> {
    // 1. 解析 Markdown 标题 (### ) -> 独占一行，直接返回 (标题本身够大了，不需要内部加粗)
    if line.starts_with("### ") || line.starts_with("## ") || line.starts_with("# ") {
        return Line::from(Span::styled(line, Style::default().fg(Color::Cyan).bold()));
    }

    let mut spans = Vec::new();
    let mut remaining = line;

    // ✨ 核心修复 1：将 base_style 的定义提前到最上方，统管全局
    let mut base_style = Style::default().fg(Color::White);

    // ✨ 核心修复 2：解析引用区块 (> ) -> 剥离前缀，修改全局基准色，进入流水线往下走！
    if let Some(rest) = remaining.strip_prefix("> ") {
        base_style = Style::default().fg(Color::Rgb(171, 215, 223)).italic(); // 基准样式变为：浅蓝色 + 斜体
        spans.push(Span::styled("> ", base_style));
        remaining = rest;
    }

    // 3.1 时间戳提取
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

    // 3.2 动态基准色切换
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

    // 3.3 列表项拦截
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

    // 4. 终极内联加粗解析 (**关键词**)
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
                // 普通部分：使用动态传递下来的基准色
                spans.push(Span::styled(*part, base_style));
            }
        }
    } else if !remaining.is_empty() {
        spans.push(Span::styled(remaining, base_style));
    }

    Line::from(spans)
}

// 🌟 UI 状态机（封装得更加专业）
pub struct AppState {
    pub logs: Vec<String>,
    pub node_status: HashMap<String, String>,
    // 存储每个节点的详细输出结果，用于点击查看
    pub node_results: HashMap<String, String>,
    // 存储节点的原始 YAML 配置
    pub node_configs: HashMap<String, String>,
    // 用于控制左侧节点列表的滚动与选中
    pub node_list_state: ListState,
    // 用于控制右侧日志的上下滚动
    pub log_scroll_offset: u16,
    // 性能监控：记录工作流启动时间
    pub start_time: Instant,
    // 性能监控：当前活跃的 CPU 异步任务数
    pub running_tasks: usize,
    // 持久化日志文件句柄
    pub log_file: File,
    // 保存从引擎拿到的标准显示顺序
    pub display_order: Vec<String>,
    // 记录 spinner 的动画帧
    pub spinner_tick: usize,
    pub auto_scroll: bool,
    // 人工审批的挂起状态记录
    pub awaiting_approval: Option<String>,
}

impl AppState {
    pub fn new(
        display_order: Vec<String>,
        timestamp: &str,
        node_configs: HashMap<String, String>,
    ) -> Self {
        // ✨ 定义统一的日志目录
        let log_dir = "outputs/debug_logs";
        fs::create_dir_all(log_dir).ok();

        // ✨ 组合出带有时间戳的文件名
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

        // ✨ 初始化时，为所有的节点预先在 node_status 中占个位置，状态设为“等待调度”
        for id in &state.display_order {
            state
                .node_status
                .insert(id.clone(), "💤 等待调度".to_string());
        }

        state.node_list_state.select(Some(0));
        // 使用我们即将编写的新方法写入第一条初始化日志
        state.add_log(
            "[系统] 初始化完成，按 'q' 退出，'↑/↓' 选择节点，'鼠标滚动' 滚屏...".to_string(),
        );
        state
    }

    // ✨ 核心升级：统一日志写入方法（双写机制）
    pub fn add_log(&mut self, msg: String) {
        // 1. 写入内存，用于 TUI 屏幕渲染
        self.logs.push(msg.clone());
        if self.auto_scroll {
            // 只在用户没有往上翻看记录时，才自动滚到底部
            self.log_scroll_offset = 0;
        }

        // 2. 写入本地磁盘，用于永久追溯
        let now = Local::now().format("%Y-%m-%d %H:%M:%S");
        // 使用 writeln! 宏直接写入文件并换行
        let _ = writeln!(self.log_file, "[{}] {}", now, msg);
    }

    // 键盘交互：选中上一个节点
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

    // 键盘交互：选中下一个节点
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

    // 日志滚动
    pub fn scroll_logs_up(&mut self) {
        // 向上滚动，也就是看更旧的日志，意味着 offset 增加（离底部更远）
        // 我们需要改变 offset 的语义，把它当做 "距离底部的行数" 会更简单
        self.log_scroll_offset = self.log_scroll_offset.saturating_add(2);
        self.auto_scroll = false;
    }

    pub fn scroll_logs_down(&mut self) {
        // 向下滚动，也就是看更新的日志，意味着 offset 减少
        self.log_scroll_offset = self.log_scroll_offset.saturating_sub(2);

        if self.log_scroll_offset == 0 {
            self.auto_scroll = true; // 滚回最底下了，恢复自动滚动
        }
    }

    pub fn get_spinner_frame(&self) -> &str {
        let frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
        frames[self.spinner_tick % frames.len()]
    }
}

// 🎨 核心绘制逻辑（被 main.rs 调用）
pub fn draw_ui(f: &mut Frame, app_state: &mut AppState) {
    // 1. 全局垂直切分：主视图 (占据绝大部分) + 底部状态栏 (固定 3 行)
    let main_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(10), Constraint::Length(3)].as_ref())
        .split(f.size());

    // 2. 主视图水平切分：左侧节点树 (自适应) + 右侧日志区 (剩余全部)
    let top_chunks = Layout::default()
        .direction(Direction::Horizontal)
        // 🚀 核心修复：左侧严格固定 45 列，右侧像水一样贪婪填满所有剩余空间！绝不留白！
        .constraints([Constraint::Length(45), Constraint::Min(0)].as_ref())
        .split(main_chunks[0]);

    // ✨ 替换为：直接使用引擎计算好的拓扑顺序！
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

    // ✨ 2. 遍历实际的执行节点
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

    // ✨ 3. 决定右侧面板显示什么内容 (核心联动逻辑)
    let selected_idx = app_state.node_list_state.selected().unwrap_or(0);

    let log_text = if selected_idx == 0 {
        // [模式 A] 选中第 0 项（伪节点），显示全局所有日志流
        app_state.logs.join("\n\n")
    } else {
        // [模式 B] 选中了具体节点，提取它的私有详细数据
        let real_node_id = &app_state.display_order[selected_idx - 1]; // 减 1 抵消掉全局伪节点

        // 提取并美化该节点的 YAML 配置（利用你之前写的高亮逻辑，加上 > 前缀让它变成浅蓝色的极客风格！）
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

    // ✨ 遍历所有文本行，将其转换为带色彩的 Markdown 结构
    let mut colored_lines = Vec::new();
    for line in log_text.lines() {
        if line.is_empty() {
            colored_lines.push(Line::default()); // 处理空行
        } else {
            colored_lines.push(highlight_log_line(line)); // 调用刚才写好的高亮函数
        }
    }

    // ✨ 1. 获取面板可用的内部宽度（减去左右 Borders）
    let inner_width = top_chunks[1].width.saturating_sub(2);

    // ✨ 2. 编写一个“终端自动换行模拟器”，精准计算视觉行数
    let mut real_total_lines = 0;

    // 增加安全校验：防止终端窗口被缩到极小导致除零崩溃
    if inner_width > 0 {
        for line in log_text.lines() {
            let line_width = line.width() as u16;

            // 如果这一行本来就很短，完美放下，那就是 1 视觉行（也完美兼容 \n\n 产生的空行）
            if line_width <= inner_width {
                real_total_lines += 1;
                continue;
            }

            // 🚀 如果超长，我们模拟 Ratatui 的单词截断 (Word Wrap) 算法！
            let mut current_line_width = 0;

            for word in line.split(' ') {
                let word_width = word.width() as u16;

                // 判断是否需要加空格（如果是新的一行的第一个单词，前面不加空格）
                let space_width = if current_line_width > 0 { 1 } else { 0 };

                if current_line_width + space_width + word_width > inner_width {
                    // 放不下了，发生换行！
                    if current_line_width > 0 {
                        real_total_lines += 1; // 结算上一行
                    }

                    if word_width > inner_width {
                        // 遇到极端情况（如连续的中文、超长无空格的 URL），单词本身比屏幕还宽，只能暴力切断
                        real_total_lines += word_width.div_ceil(inner_width) - 1;
                        current_line_width = word_width % inner_width;
                    } else {
                        // 正常的英文单词，另起一行放下
                        current_line_width = word_width;
                    }
                } else {
                    // 还能放下，继续往当前行塞
                    current_line_width += space_width + word_width;
                }
            }
            // 结算最后遗留的字符
            if current_line_width > 0 {
                real_total_lines += 1;
            }
        }
    }

    real_total_lines += 1;

    // ✨ 3. 获取当前日志面板的实际内部高度
    let viewport_height = top_chunks[1].height.saturating_sub(2);

    // ✨ 4. 基于真实的视觉行数计算触底滚动量
    let max_scroll = real_total_lines.saturating_sub(viewport_height);

    let scroll_y = if app_state.auto_scroll {
        max_scroll // 自动滚动时，定位到最底部
    } else {
        // 手动滚动时，从最底部往回减去 offset
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

    // 绘制底部：系统运行状态栏 (酷炫的极客面板)
    let elapsed = app_state.start_time.elapsed().as_secs_f32();

    // ✨ 调用我们刚才写好的格式化函数
    let formatted_time = format_duration(elapsed);

    let status_text = vec![Line::from(vec![
        Span::styled(
            // ✨ 替换掉原来的 {:.2}s，使用智能字符串
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
