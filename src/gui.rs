// src/gui.rs
use eframe::egui;
use egui_node_graph2::*;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;
use std::fs;
use std::time::Instant;

// 1. 定义节点间流动的数据类型
#[derive(PartialEq, Eq, Deserialize, Serialize)]
pub enum MyDataType {
    Flow,
    Param,
}

// 2. 定义插槽的默认值类型
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum MyValueType {
    Text(String),
    Number(usize),
}

impl Default for MyValueType {
    fn default() -> Self {
        MyValueType::Text(String::new())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MyNodeData {
    pub template: MyNodeTemplate,
}

// 3. ✨ 囊括引擎中所有的节点模板
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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum MyResponse {}

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
                add_num!("max_pages", 5); // 借用 max_pages 作为思考上限
                add_out!();
            }
        }
    }
}

// ✨ 在面板上渲染对应的输入框组件
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
            ui.add(egui::Label::new(param_name).truncate());

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

// 🌟 GUI 解析专用的精简版 YAML 数据结构
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

#[derive(Deserialize, Debug)]
struct GuiEdgeConfig {
    from: String,
    to: String,
    condition: Option<String>,
}

#[derive(Deserialize, Debug)]
struct GuiWorkflowConfig {
    nodes: Vec<GuiNodeConfig>,
    edges: Vec<GuiEdgeConfig>,
}

pub struct NodeGraphApp {
    state: MyEditorState,
    last_saved: Option<Instant>,
    save_filename: String,
}

impl NodeGraphApp {
    pub fn new(config_path: &str) -> Self {
        let mut state = MyEditorState::new(1.0);
        let mut save_filename = "workflow.yaml".to_string();

        // 🚀 核心逻辑：如果指定了文件并且文件存在，执行反序列化！
        if std::path::Path::new(config_path).exists() {
            if load_graph_from_yaml(&mut state, config_path).is_ok() {
                save_filename = config_path.to_string();
                println!("✅ 成功在设计器中载入工作流: {}", config_path);
            } else {
                println!(
                    "⚠️ 警告: 无法解析工作流文件 {}，将创建一个新画布。",
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

        for node_response in graph_response.node_responses {
            if let NodeResponse::User(_) = node_response {}
        }
    }
}

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

// 🚀 反向工程：将 YAML 解析为内存画布模型！
fn load_graph_from_yaml(state: &mut MyEditorState, path: &str) -> Result<(), anyhow::Error> {
    let content = fs::read_to_string(path)?;
    let config: GuiWorkflowConfig = serde_yaml::from_str(&content)?;

    let mut id_map = HashMap::new();
    let mut x_offset = 0.0;
    let mut y_offset = 0.0;

    // 1. 还原所有节点
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
            _ => continue, // 忽略未知节点
        };

        // 将节点加入画布
        let node_id = state.graph.add_node(
            node_conf.id.clone(),
            MyNodeData { template },
            |graph, node_id| template.build_node(graph, &mut MyGraphState::default(), node_id),
        );

        // ✨ 关键修复：手动同步 GUI 的节点渲染层级顺序，防止底层断言失败 (Panic 101)
        state.node_order.push(node_id);

        // 简单的自动网格排版算法
        state.node_positions.insert(node_id, egui::pos2(x_offset, y_offset));

        x_offset += 350.0;
        if x_offset > 1200.0 {
            x_offset = 0.0;
            y_offset += 250.0;
        }

        id_map.insert(node_conf.id.clone(), node_id);

        // 还原参数
        let inputs_to_process: Vec<(String, InputId)> = state.graph.nodes[node_id]
            .inputs
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();

        for (param_name, param_id) in inputs_to_process {
            if param_name == "In" {
                continue;
            }
            if let Some(input_param) = state.graph.inputs.get_mut(param_id) {
                match &mut input_param.value {
                    MyValueType::Text(val) => {
                        let new_val = match param_name.as_str() {
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

    // 2. 还原所有连线
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
                // 尝试传入默认的泛型或空数据。很多库默认连线数据是 () 或是你的 DataType
                state.graph.add_connection(o, i, Default::default());
            }
        }
    }

    Ok(())
}

fn export_graph_to_yaml(graph: &MyGraph, filename: &str) {
    let mut yaml_out = String::new();
    yaml_out.push_str("nodes:\n");

    let mut id_map = HashMap::new();

    for (counter, (node_id, node)) in graph.nodes.iter().enumerate() {
        let str_id = format!("{:?}_{}", node.user_data.template, counter);
        id_map.insert(node_id, str_id.clone());

        yaml_out.push_str(&format!("  - id: \"{}\"\n", str_id));
        yaml_out.push_str(&format!(
            "    node_type: \"{:?}\"\n",
            node.user_data.template
        ));

        for (param_name, param_id) in &node.inputs {
            if param_name == "In" || param_name.trim().is_empty() {
                continue;
            }

            let input_param = graph.inputs.get(*param_id).unwrap();
            match &input_param.value {
                MyValueType::Text(val) => {
                    if !val.trim().is_empty() {
                        // 如果文本中包含真实的换行符，使用 YAML 的块标量语法 (Block Scalar `|`)
                        if val.contains('\n') {
                            yaml_out.push_str(&format!("    {}: |\n", param_name));
                            // 给每一行文本加上 6 个空格的缩进
                            for line in val.lines() {
                                yaml_out.push_str(&format!("      {}\n", line));
                            }
                        } else {
                            // 单行文本，保持原来的引号包裹
                            let safe_val = val.replace("\"", "\\\"");
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

            if port_name == "√ 包含 (True)" {
                yaml_out.push_str("    condition: \"true\"\n");
            } else if port_name == "× 不包含 (False)" {
                yaml_out.push_str("    condition: \"false\"\n");
            }
        }

        yaml_out.push('\n');
    }

    fs::write(filename, yaml_out).unwrap_or_else(|_| panic!("无法写入 {}", filename));
    println!("🎉 成功从可视化画布生成 {} !", filename);
}

// ✨ 修改入口签名，接收配置路径
pub fn start_gui(config_path: &str) -> Result<(), eframe::Error> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([1100.0, 750.0]),
        ..Default::default()
    };

    // 因为 start_gui 要求闭包具备 'static 生命周期，所以先 clone 一份 string
    let path_clone = config_path.to_string();

    eframe::run_native(
        "AgentFlow-RS | 可视化设计器",
        options,
        Box::new(move |cc| {
            let mut fonts = egui::FontDefinitions::default();

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

            // ✨ 将闭包捕获的路径传给 App 的初始化方法
            Ok(Box::new(NodeGraphApp::new(&path_clone)))
        }),
    )
}
