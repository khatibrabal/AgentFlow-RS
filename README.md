# 🚀 AgentFlow-RS——基于 TUI 与 GUI 的大模型异步工作流 (DAG) 引擎与多模态 Agent 可视化设计与编排系统

AgentFlow-RS 是一个完全使用 Rust 编写的高性能、强类型的 AI 智能体工作流引擎。系统立足于“配置即代码”的声明式思想，构建了严谨的**有向无环图 (DAG)** 调度内核，并横向扩展了 **GUI 可视化设计态**、**TUI 极客运行态** 与 **Headless Web 微服务** 三大形态，助力开发者快速构建具有 ReAct 自主思考、本地 RAG 检索与多模态文件解析能力的前沿 AI 原生应用。

## 1. ✨ 核心特性

- ⚡ **无锁化异步并发调度**：基于入度表与 Kahn 拓扑排序算法，结合 `tokio` 与 `Arc<RwLock<T>>`，实现图节点的安全、最大化并发执行。
- 🎨 **原生 GUI 可视化编排**：基于 `eframe/egui` 打造的设计器，支持节点拖拽连线，节点属性设置，一键生成标准 YAML 配置文件。并支持通过 YAML 文件逆向还原 GUI 画板组件进行编辑。
- 💻 **TUI 极客终端监控**：基于 `ratatui` 构建的状态机视图，支持节点选择查看与运行状态的实时动画（Spinner）、Markdown 日志流式彩色高亮实时显示与鼠标键盘交互。
- 🌐 **Headless API 微服务**：内置基于 `axum` 的轻量级 Web 服务器，支持通过 HTTP POST 请求异步触发工作流并收集结果。
- **📊 静态拓扑分析与可视化：** 内置 Dry-Run 静态分析机制，支持将复杂的 DAG 工作流内存模型直接降维编译为标准的 Graphviz (`.dot`) 与 Mermaid (`.md`) 图表。
- 📄 **多模态解析与本地 RAG**：无缝支持 PDF, Word, 图片(OCR) 和音频(Whisper) 文件的降维解析，并内置基于 FastEmbed 的离线向量检索。
- 🧠 **ReAct 自主代理引擎**：内置标准 ReAct 范式，大模型可根据目标自主反思、规划，并按需调用爬虫、终端、文件 IO 等本地工具。

## 2. 🛠️ 环境依赖与前置要求

在编译和运行本项目之前，请确保你的系统满足以下环境要求：

### 2.1 基础环境

- **操作系统**：Windows 10/11 (由于 GUI 字体加载与 Shell 节点针对 Windows 进行了默认优化)。
- **Rust 工具链**：请确保安装了较新的 Rust 版本（Edition 2021 兼容，建议 `1.84.0` 或更高版本）。

```bash
rustup update
```

### 2.2 本地模型文件结构

项目根目录的 `models` 文件夹配置了以下模型文件用于音频识别与 RAG 检索，程序执行时会自动加载：

```
agent_flow_rs/
├── models/
|   ├── models--Xenova--bge-base-en-v1.5   # FastEmbed 离线依赖：BGE 文本向量化嵌入模型 (RAG)
|   ├── tesseract                          # Tesseract OCR 用于解析图片节点
|   ├── ffmpeg.exe                         # FFmpeg 用于音频格式预处理与重采样
│   ├── whisper.exe                        # Whisper.cpp 预编译引擎
│   └── ggml-base.bin                      # Whisper 模型权重文件
```

### 2.3 环境变量配置

在项目根目录下创建 `.env` 文件，并填入以下 API 凭证：

```yaml
# 必需：用于大模型推理节点的 DeepSeek API 密钥
DEEPSEEK_API_KEY=sk-xxxxxx

# 可选：用于 Agentic 智能联网搜索节点的 Exa AI API 密钥
EXA_API_KEY=sk-xxxxxx
```

## 📦 3. 编译与构建指令

本项目完全依托 Cargo 进行自动化依赖下载与构建。

### 3.1 克隆代码并进入目录

```bash
git clone <your_repository_url>
cd agent_flow_rs
```

### 3.2 下载与配置依赖模型

请前往 [Releases 页面](https://github.com/khatibrabal/AgentFlow-RS/releases) 下载 `models.zip` 模型压缩包，解压后将其放入项目根目录中。

### 3.3 静态代码审查与格式化（开发规范）

```bash
cargo fmt
cargo clippy -- -D warnings
```

### 3.4 执行单元测试与并发调度测试

```bash
cargo test
```

### 3.5 编译 Release 生产版本

```bash
cargo build --release
```

## 🚀 4. 使用方法

系统通过基于 `clap` 框架的强类型命令行接口（CLI）提供多态运行模式。

### 模式 A：图形化可视化设计态 (GUI Mode)
- 启动基于 `egui` 的原生拖拽式节点编辑器，用于零代码编排工作流：

```bash
cargo run -- --gui
```
- 通过加载 YAML 文件逆向打开并还原画板组件进行编辑：

```bash
cargo run -- --gui --config workflow.yaml
```

* **操作指南：** 在右侧菜单右键添加算力节点，编辑节点属性，拖拽 `Flow` 端口进行连线。可编辑文件名，并点击顶部“生成并部署”按钮，系统将自动逆向生成对应 YAML 配置文件。

### 模式 B：终端高帧率运行态 (TUI Mode) - **默认模式**
- 解析 YAML 配置，在终端中以无锁异步并发模式执行任务，提供实时的 DAG 拓扑监控与日志滚动。

```bash
# 默认读取根目录的 workflow.yaml
cargo run

# 指定特定的配置文件并开启底层 Debug 模式（将 HTTP 原始报文落盘）
cargo run -- -c custom_workflow.yaml --debug
```
* **快捷键:** `↑/↓` 或 `W/S` 切换左侧节点探查器，`鼠标滚轮`  滚动右侧日志，`q` 或 `ESC` 键安全退出并释放终端。
* 日志或 debug 记录将自动保存至 `outputs/debug_logs/` 目录下。

### 模式 C：RESTful Web 微服务态 (Server Mode)
- 以 Headless（无头守护进程）模式启动，通过 HTTP 协议暴露底层执行引擎。

```bash
# 启动守护进程，默认挂载 8080 端口
cargo run --release -- --server

# 指定端口启动
cargo run --release -- --server --port 9090
```

* **接口调用示例 (cURL)：**
  
  发送 `POST` 请求，系统将为每次请求动态拉起一个隔离的 DAG 实例。
  
  **返回值：** 包含状态码、完整执行日志流以及最终结果的 JSON 报文。
```bash
curl -X POST http://127.0.0.1:8080/api/v1/workflow/run \
     -H "Content-Type: application/json" \
     -d '{"initial_input": "请帮我搜索关于 Rust 并发编程的最新进展", "config_path"："workflow.yaml"}'
```

​       发送 `GET` 请求，实时拉取 DAG 拓扑图。

```bash
curl -X GET "http://127.0.0.1:8080/api/v1/workflow/topology?config_path=workflow.yaml"
```

### 模式 D：静态分析与拓扑导出态 (Static Analysis)
- 不实际执行任何重型节点逻辑，仅在内存中进行 DAG 环路检测与拓扑排序，并将其编译导出为可视化 Graphviz (.dot) 图表和 Mermaid 图表和代码。默认读取根目录的 workflow.yaml：

```bash
# 导出为 Graphviz 专用的 .dot 文件
cargo run --release -- --export-dot my_arch

# 导出为支持在 Markdown/Notion 中直接预览的 Mermaid 格式
cargo run --release -- --export-mermaid my_arch
```
- 从特定的 YAML 文件生成图表代码：

```bash
# 从特定的 test.yml 生成 Graphviz (.dot) 图表
cargo run -- -c test.yml --export-dot test_graph

# 从特定的 my_flow.yaml 生成 Mermaid 图表
cargo run -- --config my_flow.yaml --export-mermaid flow_diagram
```

- 产出文件将自动保存至 `outputs/visualizations/` 目录下。

## 📜 5. 声明式 YAML 编排规范

本系统贯彻“配置即代码 (Configuration as Code)”原则。工作流由 `nodes` (节点定义) 与 `edges` (数据流向) 两部分构成。

- **基础编排示例 (`workflow.yaml`):**

```yaml
nodes:
  - id: "InputText"
    node_type: "Text"
    text: "https://news.ycombinator.com"
  
  - id: "WebCrawler"
    node_type: "Spider"
    max_pages: 10

  - id: "AI_Analyst"
    node_type: "DeepSeek"
    prompt: "请提炼爬虫获取的网页正文，总结出前3个热门话题。"

edges:
  - from: "InputText"
    to: "WebCrawler"
  - from: "WebCrawler"
    to: "AI_Analyst"
```

* **条件路由支持:** 在 `edges` 中可附加 `condition: "true"` 属性，配合 `RouterNode` 实现智能分支跳过机制。

## 🧩 6. 内置算力节点生态

系统底层实现了多态计算节点，覆盖了主流 Agent 应用的各类需求。在 YAML 或 GUI 中只需指定对应的 `node_type` 即可调用：

| **节点类型 (node_type)** | **分类** | **功能描述**                                                 |
| ------------------------ | -------- | ------------------------------------------------------------ |
| **Text**                 | 📥 IO     | 用户文本输入                                                 |
| **FileRead**             | 📥 IO     | 本地文本文档读取                                             |
| **FileWrite**            | 📥 IO     | 附带时间戳隔离的本地结果持久化。<br />产出文件将自动保存至 `outputs/reports/` 目录下。 |
| **MultiModalParse**      | 📥 IO     | 智能解析 PDF, DOCX 等文件, 图片(OCR) 和 音频(语音转文字)            |
| **RegexMatch**           | 🌊 处理   | 文本正则清洗捕获                                             |
| **Router**               | 🌊 处理   | 条件分支路由(控制流)                                         |
| **LocalRAG**             | 🧠 AI     | 基于 FastEmbed 的离线文本滑窗分块与余弦相似度检索            |
| **DeepSeek**             | 🧠 AI     | 标准化的大语言模型分析与文本总结节点                         |
| **ReActAgent**           | 🤖 代理   | 自主智能体中枢，根据 Prompt 自发调用爬虫/Shell等工具         |
| **Spider**               | 🛠️ 工具   | 并发无栈协程网页爬虫，内置 DOM 降噪清洗                      |
| **WebSearch**            | 🛠️ 工具   | 接入 Exa AI，支持意图识别的高维语义特征网页检索              |
| **Shell**                | 🛠️ 工具   | 底层系统命令终端执行器（支持 Windows `cmd` 与 Unix `sh`）    |
| **Approval**             | ✋ 控制   | 挂起执行图，请求 TUI 前端拦截（按 `Y/N`）人工审批            |

## 📁 7. 工程目录结构说明

```text
agent_flow_rs/
├── Cargo.toml                  # 核心依赖与版本清单
├── .env                        # 全局安全环境变量 (API Keys)
├── inputs/                     # 多模态文件与数据的安全读取沙盒
├── outputs/                    # 系统的交付产物存放区
│   ├── debug_logs/             # 旁路诊断与网络报文追踪日志
│   ├── reports/                # AI 最终生成的结构化报告
│   └── visualizations/         # 导出的架构拓扑图
├── models/                     # 本地绿色免安装离线模型
└── src/                        # 核心业务代码
    ├── main.rs                 # 命令行入口、模式路由与生命周期管理
    ├── tui.rs                  # 运行态控制台 (Ratatui 渲染引擎)
    ├── gui.rs                  # 设计态编辑器 (Egui 图形化画布)
    ├── server.rs               # 微服务网关 (Axum RESTful 接口)
    ├── executor.rs             # DAG 调度、死锁规避与多路复用总线
    ├── graph.rs                # 拓扑解析与 DFS 三色环路检测算法
    ├── node.rs                 # ExecutableNode 泛型接口与具体实体
    └── error.rs                # 领域驱动错误枚举 (Thiserror)
```

## 🛡️ 8. 错误处理与架构设计

1. **DAG 环路免疫**：系统在 `GraphBuilder` 构建期采用深度优先搜索（DFS）进行强验证，任何 `A -> B -> A` 的死循环配置都会在系统启动前被直接拦截。
2. **读写锁隔离**：执行态采用 `Arc<RwLock<HashMap>>` 共享上下文，上游节点产出时加写锁，下游读取时加读锁，消灭数据竞态（Data Race）。
3. **安全跨界降维**：GUI 画布采用反向 AST 工程，有效剥离视觉坐标信息，将内存状态安全降维为扁平化 YAML，实现“设计”与“运行”的解耦。

## ⚖️ 9. 许可证与开发规范

* 本项目严格通过了 `cargo clippy -- -D warnings` 零容忍静态审查。
* 多线程锁控制遵循极小化粒度，确保了框架的吞吐性能。
* 在生产环境中，推荐使用 `cargo build --release` 以获得最佳的大模型网络解析与并发调度的性能体验。

*Author: 杨嘉仪 | Nankai University, School of Software | 2026*
