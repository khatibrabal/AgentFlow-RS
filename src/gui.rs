//! # 工作流可视化设计器模块 (GUI Editor)
//!
//! 本模块基于 `eframe` 和 `egui_node_graph2` 框架，提供了一个图形化的有向无环图 (DAG) 工作流设计界面。
//! 核心功能涵盖：节点拖拽编排、组件参数的动态配置、拓扑结构的实时渲染，
//! 以及在 GUI 内存模型与标准引擎 YAML 配置文件之间的双向解析与持久化。

// src/gui.rs
use eframe::egui;
use egui_node_graph2::*;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;
use std::fs;
use std::time::Instant;

/// 节点连线与引脚的传输数据类型。
///
/// 在可视化图中，引脚分为两大类：用于图计算拓扑的数据流连线，以及用于界面展示的静态配置项参数。
#[derive(PartialEq, Eq, Deserialize, Serialize)]
pub enum MyDataType {
    /// 表示节点之间的动态数据流动，仅用于连接引脚。
    Flow,
    /// 表示节点自身的静态属性或配置参数，通常伴随输入控件。
    Param,
}

/// 节点引脚绑定的默认数据值枚举。
///
/// 涵盖了当前工作流引擎支持的基础数据结构，主要用于在 UI 中渲染对应的输入与显示控件。
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum MyValueType {
    /// 纯文本或多行字符串数据。
    Text(String),
    /// 无符号整型数值数据。
    Number(usize),
}

impl Default for MyValueType {
    fn default() -> Self {
        MyValueType::Text(String::new())
    }
}

/// 节点的内部数据包装载体。
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MyNodeData {
    /// 记录当前节点所对应的工作流组件模板。
    pub template: MyNodeTemplate,
}

/// 引擎内置的全量节点模板注册表。
///
/// 包含工作流底层所有支持的具体节点类型。每次向底层调度引擎添加新节点时，
/// 必须在此处同步注册对应的模板，以支持 GUI 界面的拖拽与渲染。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub enum MyNodeTemplate {
    Text,
    FileRead,
    FileWrite,
    RegexMatch,
    Spider,
    MultiModalParse,
    DeepSeek,
    WebSearch,
    LocalRAG,
    Shell,
    Approval,
    Router,
    ReActAgent,
}

/// 节点内部的自定义响应事件类型。
/// 当前实现暂未定义特殊交互事件，预留作为扩展接口。
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum MyResponse {}

/// 整个图编辑器的全局共享状态。
#[derive(Default, Serialize, Deserialize)]
pub struct MyGraphState {}

impl DataTypeTrait<MyGraphState> for MyDataType {
    fn data_type_color(&self, _user_state: &mut MyGraphState) -> egui::Color32 {
        match self {
            MyDataType::Flow => egui::Color32::from_rgb(0, 255, 127),
            MyDataType::Param => egui::Color32::from_rgb(100, 100, 100),
        }
    }
    fn name(&self) -> Cow<'_, str> {
        match self {
            MyDataType::Flow => Cow::Borrowed("数据流"),
            MyDataType::Param => Cow::Borrowed("配置项"),
        }
    }
}

impl NodeTemplateTrait for MyNodeTemplate {
    type NodeData = MyNodeData;
    type DataType = MyDataType;
    type ValueType = MyValueType;
    type UserState = MyGraphState;
    type CategoryType = &'static str;

    fn node_finder_label(&self, _user_state: &mut Self::UserState) -> Cow<'_, str> {
        // UI 渲染层保留 Emoji 符号以提升辨识度
        Cow::Borrowed(match self {
            MyNodeTemplate::Text => "纯文本输入 📝",
            MyNodeTemplate::FileRead => "读取文件 📂",
            MyNodeTemplate::FileWrite => "写入文件 💾",
            MyNodeTemplate::RegexMatch => "正则清洗 ✂️",
            MyNodeTemplate::Spider => "多线程爬虫 🕷️",
            MyNodeTemplate::MultiModalParse => "多模态解析 📄",
            MyNodeTemplate::DeepSeek => "AI Agent 中枢 🧠",
            MyNodeTemplate::WebSearch => "Agentic Web 搜索 🔍",
            MyNodeTemplate::LocalRAG => "本地向量检索 📚",
            MyNodeTemplate::Shell => "终端命令 💻",
            MyNodeTemplate::Approval => "人工审批 ✋",
            MyNodeTemplate::Router => "条件分支判断 🔀",
            MyNodeTemplate::ReActAgent => "ReAct 自主代理 🤖",
        })
    }

    fn node_finder_categories(&self, _user_state: &mut Self::UserState) -> Vec<&'static str> {
        match self {
            MyNodeTemplate::FileRead
            | MyNodeTemplate::MultiModalParse
            | MyNodeTemplate::Spider
            | MyNodeTemplate::WebSearch => {
                vec!["📥 数据源 (Inputs)"]
            }
            MyNodeTemplate::DeepSeek | MyNodeTemplate::LocalRAG | MyNodeTemplate::ReActAgent => {
                vec!["🧠 AI 与检索 (AI & Search)"]
            }
            MyNodeTemplate::Text | MyNodeTemplate::RegexMatch | MyNodeTemplate::Router => {
                vec!["🌊 数据处理 (Processing)"]
            }
            MyNodeTemplate::FileWrite | MyNodeTemplate::Shell | MyNodeTemplate::Approval => {
                vec!["📤 输出与系统 (Outputs)"]
            }
        }
    }

    fn node_graph_label(&self, user_state: &mut Self::UserState) -> String {
        self.node_finder_label(user_state).into_owned()
    }

    fn user_data(&self, _user_state: &mut Self::UserState) -> Self::NodeData {
        MyNodeData { template: *self }
    }

    fn build_node(
        &self,
        graph: &mut Graph<Self::NodeData, Self::DataType, Self::ValueType>,
        _user_state: &mut Self::UserState,
        node_id: NodeId,
    ) {
        macro_rules! add_in {
            () => {
                graph.add_input_param(
                    node_id,
                    "In".into(),
                    MyDataType::Flow,
                    MyValueType::Text("".into()),
                    InputParamKind::ConnectionOnly,
                    true,
                );
            };
        }
        macro_rules! add_out {
            () => {
                graph.add_output_param(node_id, "Out".into(), MyDataType::Flow);
            };
        }
        macro_rules! add_text {
            ($name:expr, $default:expr) => {
                graph.add_input_param(
                    node_id,
                    $name.into(),
                    MyDataType::Param,
                    MyValueType::Text($default.to_string()),
                    InputParamKind::ConstantOnly,
                    true,
                );
            };
        }
        macro_rules! add_num {
            ($name:expr, $default:expr) => {
                graph.add_input_param(
                    node_id,
                    $name.into(),
                    MyDataType::Param,
                    MyValueType::Number($default),
                    InputParamKind::ConstantOnly,
                    true,
                );
            };
        }

        // 统一在节点顶部挂载唯一标识符 (ID) 控件，默认使用枚举名称
        // 用户可通过 UI 点击该属性进行重命名
        add_text!("ID", format!("{:?}", self));

        match self {
            MyNodeTemplate::Text => {
                add_text!("text", "你好，AgentFlow！");
                add_out!();
            }
            MyNodeTemplate::FileRead => {
                add_text!("file_path", "input.txt");
                add_out!();
            }
            MyNodeTemplate::FileWrite => {
                add_in!();
                add_text!("file_path", "output.md");
            }
            MyNodeTemplate::RegexMatch => {
                add_in!();
                add_text!("pattern", r"Email:\s*([a-zA-Z0-9@.]+)");
                add_out!();
            }
            MyNodeTemplate::Spider => {
                add_in!();
                add_num!("max_pages", 3);
                add_text!("link_selector", ".titleline > a");
                add_out!();
            }
            MyNodeTemplate::MultiModalParse => {
                add_text!("file_path", "document.pdf");
                add_out!();
            }
            MyNodeTemplate::DeepSeek => {
                add_in!();
                add_text!("prompt", "你是一个AI助手，请总结下文：");
                add_out!();
            }
            MyNodeTemplate::WebSearch => {
                add_in!();
                add_text!("query", "");
                add_num!("num_results", 5);
                add_out!();
            }
            MyNodeTemplate::LocalRAG => {
                add_in!();
                add_text!("query", "核心观点是什么？");
                add_num!("top_k", 3);
                add_out!();
            }
            MyNodeTemplate::Shell => {
                add_in!();
                add_text!("command", "echo AgentFlow 启动!");
                add_out!();
            }
            MyNodeTemplate::Approval => {
                add_in!();
                add_text!("message", "⚠️ 危险操作，请确认是否继续？(y/n)");
                add_out!();
            }
            MyNodeTemplate::Router => {
                add_in!();
                add_text!("keyword", "ERROR");
                graph.add_output_param(node_id, "√ 包含 (True)".into(), MyDataType::Flow);
                graph.add_output_param(node_id, "× 不包含 (False)".into(), MyDataType::Flow);
            }
            MyNodeTemplate::ReActAgent => {
                add_in!();
                add_num!("max_steps", 5);
                add_out!();
            }
        }
    }
}

/// 负责将内存参数数据模型渲染为界面对应的可视交互控件。
impl WidgetValueTrait for MyValueType {
    type Response = MyResponse;
    type UserState = MyGraphState;
    type NodeData = MyNodeData;

    fn value_widget(
        &mut self,
        param_name: &str,
        _node_id: NodeId,
        ui: &mut egui::Ui,
        _user_state: &mut Self::UserState,
        _node_data: &Self::NodeData,
    ) -> Vec<Self::Response> {
        ui.horizontal(|ui| {
            let display_name = if param_name == "ID" {
                "ID"
            } else {
                param_name
            };
            ui.add(egui::Label::new(display_name).truncate());

            if param_name == "In" || param_name.trim().is_empty() {
                return;
            }

            match self {
                MyValueType::Text(value) => {
                    let text_width = ui.fonts(|f| {
                        f.layout_no_wrap(
                            value.clone(),
                            egui::FontId::proportional(14.0),
                            egui::Color32::TRANSPARENT,
                        )
                            .rect
                            .width()
                    });

                    let dynamic_width = text_width.clamp(150.0, 400.0) + 20.0;

                    let rows = if param_name == "prompt"
                        || param_name == "text"
                        || param_name == "command"
                    {
                        3
                    } else {
                        1
                    };

                    ui.add(
                        egui::TextEdit::multiline(value)
                            .desired_width(dynamic_width)
                            .desired_rows(rows),
                    );
                }
                MyValueType::Number(value) => {
                    ui.add(egui::DragValue::new(value).speed(1.0));
                }
            }
        });
        Vec::new()
    }
}

impl UserResponseTrait for MyResponse {}
impl NodeDataTrait for MyNodeData {
    type Response = MyResponse;
    type UserState = MyGraphState;
    type DataType = MyDataType;
    type ValueType = MyValueType;

    fn bottom_ui(
        &self,
        _ui: &mut egui::Ui,
        _node_id: NodeId,
        _graph: &Graph<Self, Self::DataType, Self::ValueType>,
        _user_state: &mut Self::UserState,
    ) -> Vec<NodeResponse<Self::Response, Self>> {
        Vec::new()
    }
}

type MyGraph = Graph<MyNodeData, MyDataType, MyValueType>;
type MyEditorState =
GraphEditorState<MyNodeData, MyDataType, MyValueType, MyNodeTemplate, MyGraphState>;

/// GUI 引擎反序列化专用的节点配置视图。
///
/// 相比于底层执行器，此视图结构采用纯量扁平化定义，以便与界面的 UI 控件实现一对一绑定解析。
#[derive(Deserialize, Debug)]
struct GuiNodeConfig {
    id: String,
    node_type: String,
    prompt: Option<String>,
    query: Option<String>,
    file_path: Option<String>,
    pattern: Option<String>,
    max_pages: Option<usize>,
    text: Option<String>,
    link_selector: Option<String>,
    num_results: Option<usize>,
    top_k: Option<usize>,
    command: Option<String>,
    keyword: Option<String>,
    message: Option<String>,
}

/// GUI 引擎反序列化专用的有向边配置视图。
#[derive(Deserialize, Debug)]
struct GuiEdgeConfig {
    from: String,
    to: String,
    condition: Option<String>,
}

/// GUI 工作流反序列化专用的根配置视图。
#[derive(Deserialize, Debug)]
struct GuiWorkflowConfig {
    nodes: Vec<GuiNodeConfig>,
    edges: Vec<GuiEdgeConfig>,
}

/// 图形化节点编辑器应用程序主结构。
///
/// 实现了 `eframe::App` 契约，管理节点图编辑器实例、自动保存状态与文件句柄。
pub struct NodeGraphApp {
    state: MyEditorState,
    last_saved: Option<Instant>,
    save_filename: String,
}

impl NodeGraphApp {
    /// 构造并初始化图编辑器应用程序实例。
    ///
    /// # Arguments
    ///
    /// * `config_path` - 默认加载与保存的目标 YAML 配置文件路径。如果文件存在，则尝试载入。
    pub fn new(config_path: &str) -> Self {
        let mut state = MyEditorState::new(1.0);
        let mut save_filename = "workflow.yaml".to_string();

        if std::path::Path::new(config_path).exists() {
            if load_graph_from_yaml(&mut state, config_path).is_ok() {
                save_filename = config_path.to_string();
                println!("成功在设计器中载入工作流: {}", config_path);
            } else {
                println!(
                    "警告: 无法解析工作流文件 {}，将创建一个新画布。",
                    config_path
                );
            }
        }

        Self {
            state,
            last_saved: None,
            save_filename,
        }
    }
}

impl eframe::App for NodeGraphApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {

        // 按下 Esc 键或 Ctrl+Q 优雅关闭窗口
        if ctx.input(|i| i.key_pressed(egui::Key::Escape) || (i.modifiers.ctrl && i.key_pressed(egui::Key::Q))) {
            ctx.send_viewport_cmd(egui::ViewportCommand::Close);
        }

        egui::TopBottomPanel::top("top").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.heading("🚀 AgentFlow-RS | 可视化设计态 (Design-Time)");
                ui.add_space(20.0);

                ui.label("保存为:");
                ui.add(egui::TextEdit::singleline(&mut self.save_filename).desired_width(180.0));

                if ui.button("💾 生成并部署").clicked() {
                    let mut final_name = self.save_filename.trim().to_string();
                    if final_name.is_empty() {
                        final_name = "workflow.yaml".to_string();
                    } else if !final_name.ends_with(".yaml") && !final_name.ends_with(".yml") {
                        final_name.push_str(".yaml");
                    }
                    self.save_filename = final_name.clone();

                    export_graph_to_yaml(&self.state.graph, &final_name);
                    self.last_saved = Some(Instant::now());
                }

                if let Some(save_time) = self.last_saved
                    && save_time.elapsed().as_secs() < 3 {
                        ui.label(
                            egui::RichText::new(format!(
                                "✅ 已成功导出至 {}！",
                                self.save_filename
                            ))
                                .color(egui::Color32::GREEN),
                        );
                    }
            });
        });

        let graph_response = egui::CentralPanel::default()
            .show(ctx, |ui| {
                self.state.draw_graph_editor(
                    ui,
                    AllMyNodeTemplates,
                    &mut MyGraphState::default(),
                    Vec::default(),
                )
            })
            .inner;

        // 处理底层节点路由事件
        for node_response in graph_response.node_responses {
            if let NodeResponse::User(_) = node_response {}
        }
    }
}

/// 支持在 UI 中枚举的可用节点集合。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct AllMyNodeTemplates;
impl NodeTemplateIter for AllMyNodeTemplates {
    type Item = MyNodeTemplate;
    fn all_kinds(&self) -> Vec<Self::Item> {
        vec![
            MyNodeTemplate::Text,
            MyNodeTemplate::FileRead,
            MyNodeTemplate::FileWrite,
            MyNodeTemplate::RegexMatch,
            MyNodeTemplate::Spider,
            MyNodeTemplate::MultiModalParse,
            MyNodeTemplate::DeepSeek,
            MyNodeTemplate::WebSearch,
            MyNodeTemplate::LocalRAG,
            MyNodeTemplate::Shell,
            MyNodeTemplate::Approval,
            MyNodeTemplate::Router,
            MyNodeTemplate::ReActAgent,
        ]
    }
}

/// 读取并解析本地 YAML 配置，将其还原至当前编辑器画布模型中。
///
/// 该函数会遍历 YAML 中定义的节点配置，在画布中实例化对应模板，并恢复其预存的参数配置。
/// 随后根据边的定义恢复画布中的拓扑连线关系。
///
/// # Arguments
///
/// * `state` - UI 库全局编辑器状态结构体引用。
/// * `path` - 目标读取的 YAML 配置文件路径。
///
/// # Errors
///
/// 若文件读取异常或反序列化结构映射失败，返回 `anyhow::Error`。
fn load_graph_from_yaml(state: &mut MyEditorState, path: &str) -> Result<(), anyhow::Error> {
    let content = fs::read_to_string(path)?;
    let config: GuiWorkflowConfig = serde_yaml::from_str(&content)?;

    let mut id_map = HashMap::new();
    let mut x_offset = 0.0;
    let mut y_offset = 0.0;

    // 阶段一：重构所有配置节点实例
    for node_conf in &config.nodes {
        let template = match node_conf.node_type.as_str() {
            "Text" => MyNodeTemplate::Text,
            "FileRead" => MyNodeTemplate::FileRead,
            "FileWrite" => MyNodeTemplate::FileWrite,
            "RegexMatch" => MyNodeTemplate::RegexMatch,
            "Spider" => MyNodeTemplate::Spider,
            "MultiModalParse" => MyNodeTemplate::MultiModalParse,
            "DeepSeek" => MyNodeTemplate::DeepSeek,
            "WebSearch" => MyNodeTemplate::WebSearch,
            "LocalRAG" => MyNodeTemplate::LocalRAG,
            "Shell" => MyNodeTemplate::Shell,
            "Approval" => MyNodeTemplate::Approval,
            "Router" => MyNodeTemplate::Router,
            "ReActAgent" => MyNodeTemplate::ReActAgent,
            _ => continue, // 忽略无法映射的未知节点
        };

        // 注入目标节点并构建参数插槽
        let node_id = state.graph.add_node(
            node_conf.id.clone(),
            MyNodeData { template },
            |graph, node_id| template.build_node(graph, &mut MyGraphState::default(), node_id),
        );

        state.node_order.push(node_id);

        // 初始化屏幕空间排布坐标
        state.node_positions.insert(node_id, egui::pos2(x_offset, y_offset));

        x_offset += 350.0;
        if x_offset > 1200.0 {
            x_offset = 0.0;
            y_offset += 250.0;
        }

        id_map.insert(node_conf.id.clone(), node_id);

        let inputs_to_process: Vec<(String, InputId)> = state.graph.nodes[node_id]
            .inputs
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();

        // 恢复所有输入型配置项的具体数值
        for (param_name, param_id) in inputs_to_process {
            if param_name == "In" {
                continue;
            }
            if let Some(input_param) = state.graph.inputs.get_mut(param_id) {
                match &mut input_param.value {
                    MyValueType::Text(val) => {
                        let new_val = match param_name.as_str() {
                            // 将 YAML 中的系统 ID 映射回编辑框
                            "ID" => Some(node_conf.id.clone()),
                            "prompt" => node_conf.prompt.clone(),
                            "text" => node_conf.text.clone(),
                            "file_path" => node_conf.file_path.clone(),
                            "pattern" => node_conf.pattern.clone(),
                            "link_selector" => node_conf.link_selector.clone(),
                            "query" => node_conf.query.clone(),
                            "command" => node_conf.command.clone(),
                            "keyword" => node_conf.keyword.clone(),
                            "message" => node_conf.message.clone(),
                            _ => None,
                        };
                        if let Some(v) = new_val {
                            *val = v;
                        }
                    }
                    MyValueType::Number(val) => {
                        let new_val = match param_name.as_str() {
                            "max_pages" => node_conf.max_pages,
                            "num_results" => node_conf.num_results,
                            "top_k" => node_conf.top_k,
                            _ => None,
                        };
                        if let Some(v) = new_val {
                            *val = v;
                        }
                    }
                }
            }
        }
    }

    // 阶段二：重连所有路由有向边
    for edge_conf in &config.edges {
        if let (Some(&from_id), Some(&to_id)) =
            (id_map.get(&edge_conf.from), id_map.get(&edge_conf.to))
        {
            let mut out_id = None;
            if let Some(source_node) = state.graph.nodes.get(from_id) {
                for (name, id) in &source_node.outputs {
                    if let Some(cond) = &edge_conf.condition {
                        if cond == "true" && name == "√ 包含 (True)" {
                            out_id = Some(*id);
                            break;
                        }
                        if cond == "false" && name == "× 不包含 (False)" {
                            out_id = Some(*id);
                            break;
                        }
                    } else if name == "Out" {
                        out_id = Some(*id);
                        break;
                    }
                }
            }

            let mut in_id = None;
            if let Some(target_node) = state.graph.nodes.get(to_id) {
                for (name, id) in &target_node.inputs {
                    if name == "In" {
                        in_id = Some(*id);
                        break;
                    }
                }
            }

            if let (Some(o), Some(i)) = (out_id, in_id) {
                state.graph.add_connection(o, i, Default::default());
            }
        }
    }

    Ok(())
}

/// 导出逻辑：将图形编辑器当前内存模型序列化为标准工作流 YAML 配置。
///
/// # Arguments
///
/// * `graph` - 编辑器维护的 DAG 图形化对象容器。
/// * `filename` - 目标输出的磁盘文件路径。
///
/// # Panics
///
/// 若当前系统缺乏对目标路径的写入权限，将引发运行时恐慌。
fn export_graph_to_yaml(graph: &MyGraph, filename: &str) {
    let mut yaml_out = String::new();
    yaml_out.push_str("nodes:\n");

    let mut id_map = HashMap::new();

    for (counter, (node_id, node)) in graph.nodes.iter().enumerate() {
        // 构建默认的命名空间以防止冲突
        let mut target_id = format!("{:?}_{}", node.user_data.template, counter);

        // 如果检测到用户在面板的 ID 输入框覆写了名称，则应用用户设定的值
        for (param_name, param_id) in &node.inputs {
            if param_name == "ID"
                && let MyValueType::Text(val) = &graph.inputs.get(*param_id).unwrap().value
                && !val.trim().is_empty() {
                    target_id = val.trim().to_string();
                }
        }

        id_map.insert(node_id, target_id.clone());

        yaml_out.push_str(&format!("  - id: \"{}\"\n", target_id));
        yaml_out.push_str(&format!(
            "    node_type: \"{:?}\"\n",
            node.user_data.template
        ));

        for (param_name, param_id) in &node.inputs {
            // 输出配置时剥离内部连接引脚和虚构的 ID 字段
            if param_name == "In" || param_name == "ID" || param_name.trim().is_empty() {
                continue;
            }

            let input_param = graph.inputs.get(*param_id).unwrap();
            match &input_param.value {
                MyValueType::Text(val) => {
                    if !val.trim().is_empty() {
                        if val.contains('\n') {
                            yaml_out.push_str(&format!("    {}: |\n", param_name));
                            for line in val.lines() {
                                yaml_out.push_str(&format!("      {}\n", line));
                            }
                        } else {
                            let safe_val = val.replace('\"', "\\\"");
                            yaml_out.push_str(&format!("    {}: \"{}\"\n", param_name, safe_val));
                        }
                    }
                }
                MyValueType::Number(val) => {
                    yaml_out.push_str(&format!("    {}: {}\n", param_name, val));
                }
            }
        }

        yaml_out.push('\n');
    }

    yaml_out.push_str("edges:\n");

    for (input_id, outputs) in graph.connections.iter() {
        for &output_id in outputs {
            let output_pin = graph.outputs.get(output_id).unwrap();
            let input_pin = graph.inputs.get(input_id).unwrap();

            let from_node_id = output_pin.node;
            let to_node_id = input_pin.node;

            let from_str = id_map.get(&from_node_id).unwrap();
            let to_str = id_map.get(&to_node_id).unwrap();

            let from_node = graph.nodes.get(from_node_id).unwrap();
            let mut port_name = "";
            for (name, id) in &from_node.outputs {
                if *id == output_id {
                    port_name = name;
                    break;
                }
            }

            yaml_out.push_str(&format!("  - from: \"{}\"\n", from_str));
            yaml_out.push_str(&format!("    to: \"{}\"\n", to_str));

            // 对齐条件控制网关的逻辑映射参数
            if port_name == "√ 包含 (True)" {
                yaml_out.push_str("    condition: \"true\"\n");
            } else if port_name == "× 不包含 (False)" {
                yaml_out.push_str("    condition: \"false\"\n");
            }
        }

        yaml_out.push('\n');
    }

    fs::write(filename, yaml_out).unwrap_or_else(|_| panic!("无法写入 {}", filename));
    println!("✅ 成功从可视化画布生成 {} !", filename);
}

/// 图形化编辑器前端环境启动入口。
///
/// 内部封装了视窗布局、系统字体跨平台挂载等操作。该函数将阻塞调用方所在线程。
///
/// # Arguments
///
/// * `config_path` - GUI 启动时默认尝试加载的工作流配置路径。
///
/// # Errors
///
/// 当系统图形上下文无法初始化时抛出 `eframe::Error` 异常。
pub fn start_gui(config_path: &str) -> Result<(), eframe::Error> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([1100.0, 750.0]),
        ..Default::default()
    };

    let path_clone = config_path.to_string();

    eframe::run_native(
        "AgentFlow-RS | 可视化设计器",
        options,
        Box::new(move |cc| {
            let mut fonts = egui::FontDefinitions::default();

            // 为 Windows 系统加载默认的宽字符/中文字体库与表情回退机制
            if let Ok(font_data) = fs::read("C:\\Windows\\Fonts\\msyh.ttc") {
                fonts
                    .font_data
                    .insert("msyh".to_owned(), egui::FontData::from_owned(font_data));
                fonts
                    .families
                    .get_mut(&egui::FontFamily::Proportional)
                    .unwrap()
                    .insert(0, "msyh".to_owned());
                fonts
                    .families
                    .get_mut(&egui::FontFamily::Monospace)
                    .unwrap()
                    .insert(0, "msyh".to_owned());
            }

            if let Ok(font_data) = fs::read("C:\\Windows\\Fonts\\seguiemj.ttf") {
                fonts.font_data.insert(
                    "windows_emoji".to_owned(),
                    egui::FontData::from_owned(font_data),
                );
                fonts
                    .families
                    .get_mut(&egui::FontFamily::Proportional)
                    .unwrap()
                    .push("windows_emoji".to_owned());
                fonts
                    .families
                    .get_mut(&egui::FontFamily::Monospace)
                    .unwrap()
                    .push("windows_emoji".to_owned());
            }
            cc.egui_ctx.set_fonts(fonts);

            let mut style = (*cc.egui_ctx.style()).clone();
            style.spacing.menu_margin = egui::Margin::same(10.0);
            style.spacing.interact_size = egui::vec2(250.0, 24.0);
            cc.egui_ctx.set_style(style);

            Ok(Box::new(NodeGraphApp::new(&path_clone)))
        }),
    )
}
