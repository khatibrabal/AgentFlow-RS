// src/node.rs
use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::Local;
use dotext::{Docx, MsDoc};
use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
use readability::extractor;
use rusty_tesseract::{Args, Image};
use scraper::{Html, Selector};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::task::JoinSet;
use url::Url;

/// 所有工作流计算节点必须实现的泛型契约接口。
///
/// 继承了 `Send + Sync` 以保证其能在多核异步调度器 (`tokio::spawn`)
/// 的线程边界之间安全传递和共享。所有的计算、网络请求和 IO 操作均在此执行。
#[async_trait]
pub trait ExecutableNode: Send + Sync {
    /// 获取该节点在 DAG 图中的全局唯一 ID。
    fn id(&self) -> &str;
    /// 获取该节点的人类可读展示名称（推荐包含 Emoji 以适配 TUI 渲染）。
    fn name(&self) -> &str;
    /// 触发节点的实际运算逻辑。
    ///
    /// # Arguments
    /// * `input` - 由底层调度器自动组装的上游依赖数据总和（文本流）
    /// * `debug` - 是否开启底层调试模式，开启后将执行明细写入日志文件
    async fn execute(&self, input: &str, debug: bool, timestamp: &str) -> Result<String>;

    /// ✨ 核心升级：带有实时日志推流能力的执行接口
    /// 默认实现会自动降级调用老版的 execute，这样你就不需要去修改另外那十几个普通节点！
    async fn execute_with_stream(
        &self,
        input: &str,
        debug: bool,
        timestamp: &str,
        _log_tx: tokio::sync::mpsc::Sender<String>,
    ) -> Result<String> {
        self.execute(input, debug, timestamp).await
    }
}

/// 负责从本地文件系统加载纯文本数据的 IO 节点。
///
/// 此节点通常作为工作流的起点（入度为 0），用于将本地知识库、
/// 配置文件或长文本数据注入到 DAG 数据流中。
///
/// # Example
/// ```yaml
/// - id: "LoadConfig"
///   node_type: "FileRead"
///   file_path: "./local_knowledge.md"
/// ```
pub struct FileReadNode {
    pub id: String,
    pub file_path: String,
}

#[async_trait]
impl ExecutableNode for FileReadNode {
    fn id(&self) -> &str {
        &self.id
    }
    fn name(&self) -> &str {
        "📂 读取文件"
    }
    async fn execute(&self, _input: &str, _debug: bool, _timestamp: &str) -> Result<String> {
        let content = fs::read_to_string(&self.file_path)
            .with_context(|| format!("无法读取本地文件: {}", self.file_path))?;
        Ok(content)
    }
}

/// 负责将 DAG 数据流持久化到本地文件系统的 IO 节点。
///
/// 此节点通常作为工作流的终点（出度为 0），用于保存大模型生成的报告、
/// 爬虫抓取的数据或系统执行的最终状态。
///
/// # Example
/// ```yaml
/// - id: "SaveReport"
///   node_type: "FileWrite"
///   file_path: "./output/final_report.md"
/// ```
pub struct FileWriteNode {
    pub id: String,
    pub original_path: String,
    pub timestamp: String,
}
#[async_trait]
impl ExecutableNode for FileWriteNode {
    fn id(&self) -> &str {
        &self.id
    }
    fn name(&self) -> &str {
        "💾 写入文件"
    }

    async fn execute(&self, input: &str, _debug: bool, _timestamp: &str) -> Result<String> {
        // ✨ 智能重命名逻辑：解析用户的文件名并插入时间戳
        let path = Path::new(&self.original_path);
        let stem = path.file_stem().unwrap_or_default().to_string_lossy();
        let ext = path.extension().unwrap_or_default().to_string_lossy();

        let final_ext = if ext.is_empty() {
            String::new()
        } else {
            format!(".{}", ext)
        };

        // 组装最终路径：outputs/reports/文件名_时间戳.扩展名
        let final_filename = format!("{}_{}{}", stem, self.timestamp, final_ext);
        let final_path = format!("outputs/reports/{}", final_filename);

        // 自动创建 reports 文件夹
        fs::create_dir_all("outputs/reports").ok();

        fs::write(&final_path, input).with_context(|| format!("无法写入文件: {}", final_path))?;

        Ok(format!("成功将结果保存至: {}", final_path))
    }
}

/// 基于 DeepSeek V4 架构的智能 Agent 分析中枢。
///
/// 作为 Agentic RAG 的核心推理单元，负责接收系统注入的 Prompt
/// 以及检索节点提供的海量上游参考资料，进行深度逻辑融合与产出。
///
/// # Example
/// ```yaml
/// - id: "Analyst"
///   node_type: "DeepSeek"
///   prompt: "你是一个高级架构师，请总结上文内容并输出架构方案。"
/// ```
pub struct DeepSeekNode {
    pub id: String,
    pub prompt: String,
}
#[async_trait]
impl ExecutableNode for DeepSeekNode {
    fn id(&self) -> &str {
        &self.id
    }
    fn name(&self) -> &str {
        "🧠 AI Agent"
    }

    async fn execute(&self, input: &str, debug: bool, timestamp: &str) -> Result<String> {
        let api_key = std::env::var("DEEPSEEK_API_KEY")
            .unwrap_or_else(|_| "sk-your-api-key-here".to_string());

        if api_key == "sk-your-api-key-here" {
            anyhow::bail!("请在根目录的 .env 文件中配置真实的 DEEPSEEK_API_KEY");
        }

        let client = reqwest::Client::new();
        let payload = serde_json::json!({
            "model": "deepseek-chat",
            "messages": [
                {"role": "system", "content": &self.prompt},
                {"role": "user", "content": input}
            ],
            "temperature": 0.7
        });

        let response = client
            .post("https://api.deepseek.com/chat/completions")
            .header("Authorization", format!("Bearer {}", api_key))
            .header("Accept", "application/json")
            .json(&payload)
            .send()
            .await
            .context("大模型网络请求发送失败")?;

        let status = response.status();
        let raw_text = response
            .text()
            .await
            .unwrap_or_else(|_| "无法读取实体".to_string());

        if debug {
            // 打开或创建本地日志文件，追加写入
            fs::create_dir_all("outputs/debug_logs").ok();
            let log_path = format!("outputs/debug_logs/{}_debug.log", timestamp);

            if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(&log_path) {
                let now = Local::now().format("%Y-%m-%d %H:%M:%S");
                let log_entry = format!(
                    "[{}] Node: {} | HTTP Status: {}\nRaw JSON payload:\n{}\n----------------------------------------\n",
                    now, self.id, status, raw_text
                );
                let _ = file.write_all(log_entry.as_bytes()); // 偷偷落盘
            }
        }

        if !status.is_success() {
            anyhow::bail!("API 拒绝服务! 状态: {} \n返回内容: {}", status, raw_text);
        }

        // 把文本重新转回 JSON 对象
        let resp_json: serde_json::Value =
            serde_json::from_str(&raw_text).context("解析响应失败")?;

        if let Some(content) = resp_json["choices"][0]["message"]["content"].as_str() {
            Ok(content.to_string())
        } else {
            anyhow::bail!("API 返回成功，但格式异常：{}", resp_json)
        }
    }
}

/// 基于正则表达式的高性能文本清洗节点。
///
/// 利用 `regex` 库提取上游乱码文本中的关键信息。
/// 必须包含至少一个捕获组 `()`，提取出的内容将作为输出向下游传递。
///
/// # Example
/// ```yaml
/// - id: "ExtractEmail"
///   node_type: "RegexMatch"
///   pattern: "Email:\\s*([a-zA-Z0-9@.]+)"
/// ```
pub struct RegexMatchNode {
    pub id: String,
    pub pattern: String,
}

#[async_trait]
impl ExecutableNode for RegexMatchNode {
    fn id(&self) -> &str {
        &self.id
    }
    fn name(&self) -> &str {
        "✂️ 正则清洗"
    }

    async fn execute(&self, input: &str, _debug: bool, _timestamp: &str) -> Result<String> {
        let re = regex::Regex::new(&self.pattern).context("正则表达式编译失败，请检查语法")?;

        // 尝试捕获第一个匹配组，如果没有匹配组则返回整个匹配字符串
        if let Some(captures) = re.captures(input) {
            let matched_text = captures
                .get(1)
                .map_or_else(|| captures.get(0).unwrap().as_str(), |m| m.as_str());
            Ok(matched_text.to_string())
        } else {
            anyhow::bail!("未能从输入文本中匹配到正则: {}", self.pattern)
        }
    }
}

/// 危险但强大的底层系统命令执行节点。
///
/// 独立配置要执行的命令字符串，忽略上游输入。
/// 跨平台支持：Windows 自动路由至 `cmd`，Unix 系统路由至 `sh`。
///
/// # Example
/// ```yaml
/// - id: "RunScript"
///   node_type: "Shell"
///   command: "echo Hello World"
/// ```
pub struct ShellNode {
    pub id: String,
    pub command: String, // ✨ 新增：节点自带的命令参数
}

#[async_trait]
impl ExecutableNode for ShellNode {
    fn id(&self) -> &str {
        &self.id
    }
    fn name(&self) -> &str {
        "💻 执行命令"
    }

    async fn execute(&self, input: &str, _debug: bool, _timestamp: &str) -> Result<String> {
        let shell = if cfg!(target_os = "windows") {
            "cmd"
        } else {
            "sh"
        };
        let arg = if cfg!(target_os = "windows") {
            "/C"
        } else {
            "-c"
        };

        // ✨ 微调：如果自身 command 为空，就把大模型传入的 input 当作命令！
        let target_cmd = if self.command.trim().is_empty() {
            input.trim()
        } else {
            &self.command
        };

        if target_cmd.is_empty() {
            anyhow::bail!("终端命令不能为空！");
        }

        let output = std::process::Command::new(shell)
            .arg(arg)
            .arg(target_cmd) // ✨ 核心修改：执行自身配置的命令，而不是上游传入的 input
            .output()
            .context("Shell 命令启动失败")?;

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();

        if output.status.success() {
            Ok(if stdout.trim().is_empty() {
                "命令执行成功 (无输出)".to_string()
            } else {
                stdout
            })
        } else {
            anyhow::bail!("命令执行失败!\n错误输出: {}", stderr)
        }
    }
}

/// 并发多线程异步爬虫
///
/// 利用 `tokio::task::JoinSet` 构建微型无栈协程池，突破 I/O 阻塞瓶颈。
/// 核心内置了 Mozilla `readability` 算法引擎，实现免规则的工业级 DOM 清洗与无用节点剔除。
///
/// # Example
/// ```yaml
/// - id: "CrawlNews"
///   node_type: "Spider"
///   max_pages: 5
///   link_selector: ".titleline > a"
/// ```
pub struct SpiderNode {
    pub id: String,
    pub max_pages: usize,
    pub link_selector: Option<String>,
}

#[async_trait]
impl ExecutableNode for SpiderNode {
    fn id(&self) -> &str {
        &self.id
    }
    fn name(&self) -> &str {
        "🕷️ 多线程爬虫"
    }

    async fn execute(&self, input: &str, debug: bool, timestamp: &str) -> Result<String> {
        let root_url = input.trim();
        if !(root_url.starts_with("http://") || root_url.starts_with("https://")) {
            anyhow::bail!("爬虫需要一个合法的 URL 作为输入: {}", root_url);
        }

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            .build()?;

        let root_resp = client
            .get(root_url)
            .send()
            .await
            .with_context(|| format!("爬虫无法访问根网址: {}", root_url))?;
        let root_html = root_resp.text().await?;

        let mut target_urls = Vec::new();
        {
            let document = Html::parse_document(&root_html);

            let selector_str = self.link_selector.as_deref().unwrap_or("a[href]");

            let selector = Selector::parse(selector_str)
                .map_err(|e| anyhow::anyhow!("CSS 选择器 '{}' 语法错误: {:?}", selector_str, e))?;

            for element in document.select(&selector) {
                if let Some(href) = element.value().attr("href") {
                    let full_url = if href.starts_with("http") {
                        href.to_string()
                    } else {
                        if href.starts_with('/') {
                            if let Ok(base) = Url::parse(root_url) {
                                format!(
                                    "{}://{}{}",
                                    base.scheme(),
                                    base.host_str().unwrap_or(""),
                                    href
                                )
                            } else {
                                continue;
                            }
                        } else if href.starts_with("item?id=") {
                            format!("https://news.ycombinator.com/{}", href)
                        } else {
                            continue;
                        }
                    };

                    if !target_urls.contains(&full_url) {
                        target_urls.push(full_url);
                    }
                }
                if target_urls.len() >= self.max_pages {
                    break;
                }
            }
        }

        if target_urls.is_empty() {
            anyhow::bail!(
                "未能从 {} 解析出任何链接。请检查 CSS 选择器 '{}' 是否匹配了该网页的结构。",
                root_url,
                self.link_selector.as_deref().unwrap_or("a[href]")
            );
        }

        let mut crawl_logs = String::from("📡 目标抓取列表:\n");

        for url in &target_urls {
            let log_line = format!("  -> 🎯 [正在爬取]: {}", url);
            crawl_logs.push_str(&log_line);
            crawl_logs.push('\n');

            if debug {
                fs::create_dir_all("outputs/debug_logs").ok();
                let log_path = format!("outputs/debug_logs/{}_debug.log", timestamp); // 使用传进来的时间戳

                if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(&log_path) {
                    let now = Local::now().format("%Y-%m-%d %H:%M:%S");
                    let _ = writeln!(file, "[{}] 🕷️ SpiderNode [{}] {}", now, self.id, log_line);
                }
            }
        }
        crawl_logs.push_str("\n========================\n\n");

        // 3. 多线程并发抓取 (✨ 核心升级：传入索引 i)
        let mut join_set = JoinSet::new();
        for (i, url) in target_urls.into_iter().enumerate() {
            let client_clone = client.clone();
            join_set.spawn(async move {
                match client_clone.get(&url).send().await {
                    Ok(resp) => (i, url, resp.text().await.unwrap_or_default()),
                    Err(_) => (i, url, String::new()),
                }
            });
        }

        // 4. 收集并发抓取的结果 (✨ 核心升级：先收集到数组里，不要直接拼字符串)
        let mut parsed_articles: Vec<(usize, String)> = Vec::new();
        let mut success_count = 0;

        while let Some(res) = join_set.join_next().await {
            // 解构出传回来的索引 i
            if let Ok((i, url, page_text)) = res
                && !page_text.is_empty()
                && let Ok(parsed_url) = Url::parse(&url)
            {
                let mut cursor = std::io::Cursor::new(page_text.into_bytes());

                if let Ok(product) = extractor::extract(&mut cursor, &parsed_url) {
                    let clean_text = product.text;

                    if clean_text.trim().len() > 50 {
                        let truncated = if clean_text.len() > 2000 {
                            &clean_text[0..2000]
                        } else {
                            &clean_text
                        };

                        // 把单篇文章拼好
                        let article = format!(
                            "==== 标题: {} ====\n{}\n\n---\n\n",
                            // i + 1, // 显示真实的网站排名 (索引 + 1)
                            product.title,
                            truncated
                        );

                        // 存入数组，等待排序
                        parsed_articles.push((i, article));
                        success_count += 1;
                    }
                }
            }
        }

        if success_count == 0 {
            anyhow::bail!("抓取了链接，但提取正文失败。目标网页可能存在强烈的反爬虫拦截。");
        }

        // 🌟 按照索引 i 进行升序排序
        parsed_articles.sort_by_key(|a| a.0);

        // 排序完成后，把所有文章的文本提取出来，合并成最终的大字符串
        let mut combined_text = String::new();
        for (_, text) in parsed_articles {
            combined_text.push_str(&text);
        }

        Ok(format!(
            "爬虫成功并发抓取了 {} 个页面并提取了核心正文。\n{}\n合并内容如下:\n{}",
            success_count, crawl_logs, combined_text
        ))
    }
}

/// 纯文本注入节点，用于模拟用户输入或常量定义。
///
/// 忽略一切上游输入，绝对忠诚地向下游输出配置好的静态字符串。
///
/// # Example
/// ```yaml
/// - id: "UserPrompt"
///   node_type: "Text"
///   text: "请帮我查一下量子计算的最新进展。"
/// ```
pub struct TextNode {
    pub id: String,
    pub text: String,
}

#[async_trait]
impl ExecutableNode for TextNode {
    fn id(&self) -> &str {
        &self.id
    }
    fn name(&self) -> &str {
        "📝 文本输入"
    }

    async fn execute(&self, _input: &str, _debug: bool, _timestamp: &str) -> Result<String> {
        Ok(self.text.clone())
    }
}

/// 神经网络语义搜索引擎中枢 (接入 Exa AI)。
///
/// 完全超越了传统 TF-IDF 关键字匹配的限制。支持基于意图的高维张量检索。
/// 内置了 Agentic 的自我反思能力：若用户未指定搜索词，它将调用
/// 前置的大模型将模糊意图动态翻译为专业的特征检索 Query。
///
/// # Example
/// ```yaml
/// - id: "WebSearcher"
///   node_type: "WebSearch"
///   num_results: 5
/// ```
pub struct WebSearchNode {
    pub id: String,
    pub query: Option<String>,
    pub num_results: usize,
    pub raw_mode: bool,
}

#[async_trait]
impl ExecutableNode for WebSearchNode {
    fn id(&self) -> &str {
        &self.id
    }
    fn name(&self) -> &str {
        "🌐 Agentic 智能搜索"
    }

    async fn execute(&self, input: &str, debug: bool, timestamp: &str) -> Result<String> {
        // 1. 安全读取双 API Key
        let exa_api_key =
            std::env::var("EXA_API_KEY").unwrap_or_else(|_| "sk-your-exa-api-key-here".to_string());
        let ds_api_key = std::env::var("DEEPSEEK_API_KEY")
            .unwrap_or_else(|_| "sk-your-api-key-here".to_string());

        if exa_api_key.starts_with("sk-your") || ds_api_key.starts_with("sk-your") {
            anyhow::bail!("请确保在 .env 文件中配置了真实的 EXA_API_KEY 和 DEEPSEEK_API_KEY");
        }

        let client = reqwest::Client::new();

        // 🧠 阶段一：大模型思考阶段 (Query Generation)
        let search_query = if let Some(q) = &self.query {
            q.clone()
        } else if self.raw_mode {
            input.trim().to_string()
        } else {
            if debug {
                fs::create_dir_all("outputs/debug_logs").ok();
                let log_path = format!("outputs/debug_logs/{}_debug.log", timestamp); // 使用传进来的时间戳

                if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(&log_path) {
                    let _ = writeln!(
                        file,
                        "[{}] 🧠 WebSearchNode 正在启动大模型思考搜索词...",
                        Local::now().format("%Y-%m-%d %H:%M:%S")
                    );
                }
            }

            let ds_payload = serde_json::json!({
                "model": "deepseek-v4-pro",
                "messages": [
                    {
                        "role": "system",
                        "content": "你是一个专业的学术与情报检索中枢。请将用户的自然语言提问，提取并翻译成最精准的英文检索引擎 Query。要求：只输出这句纯英文 Query，绝对不要包含任何多余的解释、标点符号或前缀。"
                    },
                    {"role": "user", "content": input}
                ],
                "thinking": {"type": "disabled"},
                "temperature": 0.3 // 调低温度，保证搜索词的精准性和稳定性
            });

            let ds_response = client
                .post("https://api.deepseek.com/chat/completions")
                .header("Authorization", format!("Bearer {}", ds_api_key))
                .header("Accept", "application/json")
                .json(&ds_payload)
                .send()
                .await
                .context("在生成搜索词时，DeepSeek 网络请求失败")?;

            let raw_ds_text = ds_response.text().await.unwrap_or_default();
            let ds_json: serde_json::Value =
                serde_json::from_str(&raw_ds_text).context("解析 DeepSeek 响应失败")?;

            let generated_query = ds_json["choices"][0]["message"]["content"]
                .as_str()
                .context("无法从大模型提取生成的 Query")?
                .trim()
                .to_string();

            if generated_query.is_empty() {
                anyhow::bail!("大模型生成的搜索词为空！");
            }

            generated_query
        };

        // 🌐 阶段二：全网检索阶段 (Exa Search)
        let exa_payload = serde_json::json!({
            "query": search_query,
            "type": "auto",
            "numResults": self.num_results,
            "contents": {
                "highlights": true
            }
        });

        let exa_response = client
            .post("https://api.exa.ai/search")
            .header("x-api-key", exa_api_key)
            .header("Content-Type", "application/json")
            .json(&exa_payload)
            .send()
            .await
            .context("Exa Search API 网络请求失败")?;

        let status = exa_response.status();
        let raw_text = exa_response
            .text()
            .await
            .unwrap_or_else(|_| "无法读取实体".to_string());

        if debug {
            fs::create_dir_all("outputs/debug_logs").ok();
            let log_path = format!("outputs/debug_logs/{}_debug.log", timestamp); // 使用传进来的时间戳

            if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(&log_path) {
                let now = Local::now().format("%Y-%m-%d %H:%M:%S");
                let log_entry = format!(
                    "[{}] 🌐 WebSearchNode [{}]\n🎯 最终使用 Query: {}\nHTTP Status: {}\nResponse:\n{}\n-------------------\n",
                    now, self.id, search_query, status, raw_text
                );
                let _ = file.write_all(log_entry.as_bytes());
            }
        }

        if !status.is_success() {
            anyhow::bail!(
                "Exa API 拒绝服务! 状态: {} \n返回内容: {}",
                status,
                raw_text
            );
        }

        let resp_json: serde_json::Value =
            serde_json::from_str(&raw_text).context("解析 Exa 响应 JSON 失败")?;
        let results = resp_json["results"]
            .as_array()
            .context("Exa 返回格式异常，找不到 results 数组")?;

        if results.is_empty() {
            return Ok(format!(
                "🔍 针对 '{}' 搜索完毕，但未能找到任何相关结果。",
                search_query
            ));
        }

        // 📝 阶段三：组装高维战报
        let mut combined_report = format!(
            "\n🌐 [搜索节点] 当前使用的检索关键词: `{}`\n🔍 共为您检索到 {} 条高价值情报：\n\n",
            search_query,
            results.len()
        );

        for (i, res) in results.iter().enumerate() {
            let title = res["title"].as_str().unwrap_or("无标题");
            // let url = res["url"].as_str().unwrap_or("无URL");

            combined_report.push_str(&format!("### {}. {}\n", i + 1, title));

            if let Some(highlights) = res["highlights"].as_array() {
                let highlight_texts: Vec<&str> =
                    highlights.iter().filter_map(|h| h.as_str()).collect();

                if !highlight_texts.is_empty() {
                    combined_report.push_str("**关键摘录:**\n");
                    for text in highlight_texts {
                        combined_report.push_str(&format!("{}\n", text.replace('\n', " ")));
                    }
                } else {
                    combined_report.push_str("(未能提取到关键摘要)\n");
                }
            }
            combined_report.push_str("\n---\n\n");
        }

        Ok(combined_report)
    }
}

/// 多模态文件解析节点
///
/// 能够智能识别文件后缀，自动调用对应的底层解析引擎 (PDF提取、Word解析、OCR识别)，
/// 将多模态文件统一降维成大模型可读的纯文本流。
pub struct MultiModalParseNode {
    pub id: String,
    pub file_path: String,
}

impl MultiModalParseNode {
    /// 专用的 PDF 解析子程序
    fn parse_pdf(path: &Path) -> Result<String> {
        // ✨ 核心防御机制：为系统标准输出和标准错误戴上“口罩”。
        // 在这两个变量离开作用域（Drop）之前，第三方库的所有 println! 和 eprintln!
        // 都会被强行导入黑洞，绝不让它们撕裂我们的 TUI 界面！
        let _gag_out = gag::Gag::stdout().ok();

        let content = pdf_extract::extract_text(path)?;
        Ok(content.replace("-\n", "").replace("\n\n\n", "\n\n"))
    }

    /// 专用的 Word (.docx) 解析子程序
    fn parse_docx(path: &Path) -> Result<String> {
        let mut docx =
            Docx::open(path).map_err(|e| anyhow::anyhow!("打开 DOCX 文件失败: {:?}", e))?;
        let mut content = String::new();
        docx.read_to_string(&mut content)
            .map_err(|e| anyhow::anyhow!("读取 DOCX 内容失败: {:?}", e))?;
        Ok(content)
    }

    /// 专用的图像 OCR 解析子程序 (处理 PNG/JPG 等)
    fn parse_image(path: &Path) -> Result<String> {
        // ✨ 极客升级：优先尝试调用本地免安装版的 Tesseract (与 Whisper 解耦方案完全一致！)
        let local_exe = if cfg!(target_os = "windows") {
            "models/tesseract/tesseract.exe"
        } else {
            "models/tesseract/tesseract"
        };
        let local_tessdata = "models/tesseract/tessdata";

        // 1. 如果用户把 tesseract.exe 放到了 models 目录下，直接走底层子进程解耦模式
        if Path::new(local_exe).exists() {
            let mut cmd = std::process::Command::new(local_exe);
            cmd.arg(path.to_str().unwrap())
                .arg("stdout") // 告诉引擎把识别结果输出到标准流，而不是写成文件
                .arg("-l").arg("chi_sim+eng");

            // 如果存在专用的语言包文件夹，强制挂载，实现 100% 离线与路径隔离
            if Path::new(local_tessdata).exists() {
                cmd.arg("--tessdata-dir").arg(local_tessdata);
            }

            let output = cmd.output().context("启动本地 tesseract 子进程失败！")?;
            if output.status.success() {
                return Ok(String::from_utf8_lossy(&output.stdout).trim().to_string());
            } else {
                let err = String::from_utf8_lossy(&output.stderr);
                anyhow::bail!("本地 Tesseract OCR 推理失败: {}", err);
            }
        }

        // 载入本地图片
        let img = Image::from_path(path).map_err(|e| anyhow::anyhow!("无法加载图片: {:?}", e))?;

        // 配置 Tesseract 参数：指定中英文混合识别 (chi_sim+eng)
        let args = Args {
            lang: "chi_sim+eng".to_string(),
            ..Default::default()
        };

        // 执行光学字符识别
        let content = rusty_tesseract::image_to_string(&img, &args).map_err(|e| {
            anyhow::anyhow!(
                "OCR 识别失败，请检查是否在系统安装了 tesseract，或确认 models/tesseract 下有正确的语言包: {:?}",
                e
            )
        })?;

        Ok(content)
    }

    /// 🌟 专用的本地音频 AI 推理子程序 (子进程解耦版)
    fn parse_audio(path: &Path) -> Result<String> {
        // 1. 检查必备的本地文件
        let model_path = "models/ggml-base.bin";
        let whisper_exe = "models/whisper.exe";

        if !Path::new(model_path).exists() {
            anyhow::bail!("找不到模型文件: {}。请确认文件存在。", model_path);
        }
        if !Path::new(whisper_exe).exists() {
            anyhow::bail!(
                "找不到 Whisper 引擎: {}。请下载预编译的 whisper.exe 放入 tools 目录。",
                whisper_exe
            );
        }

        // ✨ 优先寻找本地免安装版的 FFmpeg
        let local_ffmpeg = if cfg!(target_os = "windows") {
            "models/ffmpeg.exe"
        } else {
            "models/ffmpeg"
        };

        let ffmpeg_cmd = if Path::new(local_ffmpeg).exists() {
            local_ffmpeg
        } else {
            "ffmpeg" // 智能降级：如果本地没带，尝试调用系统全局环境变量里的 ffmpeg
        };

        // 2. 利用 FFmpeg 将输入音频重采样为 16kHz WAV
        let temp_wav = format!("outputs/temp_{}.wav", Local::now().format("%H%M%S"));
        fs::create_dir_all("outputs").ok();

        let status = std::process::Command::new(ffmpeg_cmd)
            .args([
                "-i",
                path.to_str().unwrap(),
                "-ar",
                "16000",
                "-ac",
                "1",
                "-c:a",
                "pcm_s16le",
                "-y",
                &temp_wav,
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .context(format!("启动 {} 失败！请确保 models/ffmpeg 目录下存在免安装版，或系统已配置 ffmpeg 环境变量。", ffmpeg_cmd))?;

        if !status.success() {
            anyhow::bail!("音频预处理重采样失败！");
        }

        // 3. 🚀 核心魔法：使用 Rust 的 Command 唤起本地 C++ 编译好的 exe！
        // 命令行等价于: whisper.exe -m ggml-base.bin -f temp.wav -nt
        let output = std::process::Command::new(whisper_exe)
            .args([
                "-m", model_path, // 指定模型
                "-f", &temp_wav, // 指定输入的 wav 文件
                "-nt",     // 不输出时间戳，只输出纯文本 (no timestamps)
                "-l", "auto", // 自动检测语言
            ])
            .output()
            .context("启动 whisper.exe 子进程失败！")?;

        // 4. 清理临时 WAV 文件
        let _ = fs::remove_file(&temp_wav);

        // 5. 捕获子进程的输出
        if output.status.success() {
            // whisper.cpp 默认把识别的文字输出在 stdout
            let text = String::from_utf8_lossy(&output.stdout).to_string();
            // 去除可能出现的前后空白字符
            Ok(text.trim().to_string())
        } else {
            let err = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("Whisper 本地推理失败: {}", err)
        }
    }
}

#[async_trait]
impl ExecutableNode for MultiModalParseNode {
    fn id(&self) -> &str {
        &self.id
    }
    fn name(&self) -> &str {
        "📄 多模态解析器"
    }

    async fn execute(&self, input: &str, _debug: bool, _timestamp: &str) -> Result<String> {
        // ✨ 微调：如果配置的文件路径为空，就尝试去读取大模型传进来的 input 路径
        let target_path = if self.file_path.trim().is_empty() {
            input.trim()
        } else {
            &self.file_path
        };

        let path = Path::new(target_path); // 用 target_path 替代 self.file_path
        if !path.exists() {
            anyhow::bail!("找不到指定的文件: {}", target_path);
        }

        let ext = path
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_lowercase();

        // ✨ 核心路由：增加音频后缀的支持！
        let content = match ext.as_str() {
            "pdf" => Self::parse_pdf(path).context("PDF 解析分支崩溃")?,
            "docx" => Self::parse_docx(path).context("Word 解析分支崩溃")?,
            "png" | "jpg" | "jpeg" | "bmp" => {
                Self::parse_image(path).context("OCR 解析分支崩溃")?
            }
            // 🎙️ 新增的音频推理分支！
            "mp3" | "wav" | "m4a" | "flac" | "ogg" => {
                Self::parse_audio(path).context("本地音频 AI 推理分支崩溃")?
            }
            "txt" | "md" | "json" | "csv" => fs::read_to_string(path).context("纯文本读取崩溃")?,
            _ => anyhow::bail!("不支持的多模态文件格式: .{}", ext),
        };

        if content.trim().is_empty() {
            anyhow::bail!("文件解析成功，但未能提取到任何有效文本。");
        }

        let final_output = format!(
            "=== [来源文件: {}] ===\n\n{}",
            path.display(),
            content.trim()
        );

        Ok(final_output)
    }
}

/// 本地内存级向量检索引擎 (Local RAG)
///
/// 接收上游极其冗长的文本（如整篇 PDF），在内存中对其进行滑窗分块 (Chunking)，
/// 使用 fastembed 提取多维特征张量，最后利用余弦相似度算法召回与 Query 最匹配的片段。
pub struct LocalVectorSearchNode {
    pub id: String,
    pub query: String,
    pub top_k: usize,
}

#[async_trait]
impl ExecutableNode for LocalVectorSearchNode {
    fn id(&self) -> &str {
        &self.id
    }
    fn name(&self) -> &str {
        "🧠 本地向量检索"
    }

    async fn execute(&self, input: &str, _debug: bool, _timestamp: &str) -> Result<String> {
        if input.trim().is_empty() {
            anyhow::bail!("上游节点没有传来任何文本，无法进行向量检索。");
        }

        // 1. 文本滑窗分块 (Chunking) - 每块大约 500 个字符
        let chunk_size = 500;
        let chunks: Vec<String> = input
            .chars()
            .collect::<Vec<char>>()
            .chunks(chunk_size)
            .map(|c| c.iter().collect::<String>())
            .collect();

        // 2. 初始化本地嵌入模型 (FastEmbed v5 的新版 Builder 语法)
        // let mut model = TextEmbedding::try_new(
        //     InitOptions::new(EmbeddingModel::BGEBaseENV15).with_show_download_progress(true),
        // )
        // .context("初始化本地 Embedding 模型失败")?;

        let cache_dir = std::path::PathBuf::from("models");

        let mut model = TextEmbedding::try_new(
            InitOptions::new(EmbeddingModel::BGEBaseENV15)
                .with_show_download_progress(true)
                .with_cache_dir(cache_dir)
        )
            .context("初始化本地 Embedding 模型失败（请确认模型文件已正确放置在 models 目录下）")?;

        // 3. 将所有文档块转化为特征向量
        let document_embeddings = model
            .embed(chunks.clone(), None)
            .context("文档块向量化计算失败")?;

        // 4. 将用户的查询词转化为特征向量
        let query_embedding = model
            .embed(vec![self.query.clone()], None)
            .context("Query 向量化计算失败")?
            .pop()
            .unwrap();

        // 5. Rust 余弦相似度计算 (Cosine Similarity)
        let mut scored_chunks: Vec<(usize, f32)> = document_embeddings
            .iter()
            .enumerate()
            .map(|(i, doc_emb)| {
                let dot_product: f32 = query_embedding
                    .iter()
                    .zip(doc_emb.iter())
                    .map(|(a, b)| a * b)
                    .sum();
                let norm_q: f32 = query_embedding.iter().map(|a| a * a).sum::<f32>().sqrt();
                let norm_d: f32 = doc_emb.iter().map(|b| b * b).sum::<f32>().sqrt();
                let similarity = if norm_q == 0.0 || norm_d == 0.0 {
                    0.0
                } else {
                    dot_product / (norm_q * norm_d)
                };
                (i, similarity)
            })
            .collect();

        // 6. 按相似度从高到低降序排列
        scored_chunks.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

        // 7. 提取 Top K 作为高维上下文 (将 rank 改为 _rank)
        let mut final_context = format!("基于本地知识库，针对 `{}` 的召回结果：\n\n", self.query);
        for (idx, score) in scored_chunks.into_iter().take(self.top_k) {
            final_context.push_str(&format!(
                "--- [匹配度: {:.2}%] ---\n{}\n\n",
                score * 100.0,
                chunks[idx].trim()
            ));
        }

        Ok(final_context)
    }
}

pub struct ApprovalNode {
    pub id: String,
    #[allow(dead_code)]
    pub message: String,
    // 引擎在构建时把全局广播的接收端分发给它
    pub rx: broadcast::Receiver<char>,
}

#[async_trait]
impl ExecutableNode for ApprovalNode {
    fn id(&self) -> &str {
        &self.id
    }
    fn name(&self) -> &str {
        "✋ 人工审批"
    }

    async fn execute(&self, _input: &str, _debug: bool, _timestamp: &str) -> Result<String> {
        // 1. (这里假设我们能通过某种方式发送 ExecutionEvent::RequireApproval 给 TUI)

        // 2. 挂起当前的 Tokio 异步任务，死等前端的键盘广播！
        let mut rx = self.rx.resubscribe();
        loop {
            if let Ok(key) = rx.recv().await {
                if key == 'y' {
                    return Ok("审批通过：允许执行危险操作".to_string());
                } else if key == 'n' {
                    anyhow::bail!("人工驳回了该操作！流程终止。");
                }
            }
        }
    }
}

/// 智能条件分支路由节点。
///
/// 检查上游传入的文本是否包含特定的 `keyword`。
/// 它自身并不处理跳过逻辑，而是输出特定的魔术字符串给调度器。
pub struct RouterNode {
    pub id: String,
    pub keyword: String,
}

#[async_trait]
impl ExecutableNode for RouterNode {
    fn id(&self) -> &str {
        &self.id
    }
    fn name(&self) -> &str {
        "🔀 条件分支判断"
    }

    async fn execute(&self, input: &str, _debug: bool, _timestamp: &str) -> Result<String> {
        // ✨ 根据是否包含关键字，输出供底层调度器识别的信号
        if input.contains(&self.keyword) {
            Ok("__CONDITION_TRUE__".to_string())
        } else {
            Ok("__CONDITION_FALSE__".to_string())
        }
    }
}

/// 强制大模型输出的标准 JSON 结构
#[derive(Deserialize, Debug)]
pub struct ReActAction {
    pub thought: String,
    pub action: String,
    pub action_input: String,
}

/// 🌟 把现有的 ExecutableNode 包装成大模型能懂的工具
pub struct NodeToolWrapper {
    pub description: String,
    pub node: Arc<dyn ExecutableNode>,
}

/// 真正的自主思考节点 (ReAct 范式)
pub struct ReActAgentNode {
    pub id: String,
    pub max_steps: usize,
    pub registered_tools: HashMap<String, NodeToolWrapper>,
}

impl ReActAgentNode {
    /// 自动将所有包裹好的节点，组装成给大模型看的 Prompt 说明
    fn generate_tools_prompt(&self) -> String {
        let mut desc = String::from("你拥有的工具列表如下：\n");
        for (i, (tool_name, wrapper)) in (1..).zip(self.registered_tools.iter()) {
            desc.push_str(&format!(
                "{}. `{}`: {}\n",
                i, tool_name, wrapper.description
            ));
        }
        desc
    }

    /// 根据大模型的决策，动态调用内部的 ExecutableNode
    async fn execute_tool(
        &self,
        action_name: &str,
        action_input: &str,
        debug: bool,
        timestamp: &str,
        log_tx: tokio::sync::mpsc::Sender<String>,
    ) -> String {
        if let Some(wrapper) = self.registered_tools.get(action_name) {
            // ✨ 改为调用 execute_with_stream，把水管接力传给底层的 WebSearch / Spider 工具
            match wrapper
                .node
                .execute_with_stream(action_input, debug, timestamp, log_tx)
                .await
            {
                Ok(result) => result,
                Err(e) => format!("工具执行崩溃: {}", e),
            }
        } else {
            format!(
                "Error: 找不到名为 '{}' 的工具，请检查工具名称是否拼写正确。",
                action_name
            )
        }
    }
}

#[async_trait]
impl ExecutableNode for ReActAgentNode {
    fn id(&self) -> &str {
        &self.id
    }
    fn name(&self) -> &str {
        "🤖 ReAct 自主代理"
    }

    // 屏蔽掉老接口
    async fn execute(&self, _input: &str, _debug: bool, _timestamp: &str) -> Result<String> {
        anyhow::bail!("ReActAgentNode 现在必须通过 execute_with_stream 动态调度");
    }

    // ✨ 启用全新的推流接口
    async fn execute_with_stream(
        &self,
        input: &str,
        debug: bool,
        timestamp: &str,
        log_tx: tokio::sync::mpsc::Sender<String>,
    ) -> Result<String> {
        let api_key = std::env::var("DEEPSEEK_API_KEY").unwrap_or_default();
        let client = reqwest::Client::new();
        let tools_prompt = self.generate_tools_prompt();
        let system_prompt = format!(
            r#"
你是一个在本地运行的高级 AI Agent。你可以通过输出 JSON 来调用工具。
{}
额外指令：当你得出最终结论时，请调用工具 `finish`，action_input 就是返回给用户的最终答案。

你必须且只能输出严格的 JSON 格式，不要有任何多余的 Markdown 标记：
{{
  "thought": "你的思考过程",
  "action": "工具名称",
  "action_input": "传给工具的参数"
}}
"#,
            tools_prompt
        );

        let mut messages = vec![
            serde_json::json!({"role": "system", "content": system_prompt}),
            serde_json::json!({"role": "user", "content": input}),
        ];

        let mut final_answer = String::new();

        // 🚀 实弹发射：直接推流给前端，不再攒着！
        let _ = log_tx
            .send("=== 🤖 ReAct Agent 开始思考 ===".to_string())
            .await;

        for step in 1..=self.max_steps {
            let _ = log_tx.send(format!("\n🔄 [第 {} 轮思考中...]", step)).await;

            let payload = serde_json::json!({
                "model": "deepseek-chat",
                "messages": messages,
                "temperature": 0.1
            });

            let response = client
                .post("https://api.deepseek.com/chat/completions")
                .header("Authorization", format!("Bearer {}", api_key))
                .json(&payload)
                .send()
                .await?;

            let resp_json: serde_json::Value = response.json().await?;
            let llm_reply = resp_json["choices"][0]["message"]["content"]
                .as_str()
                .unwrap_or("")
                .trim();
            messages.push(serde_json::json!({"role": "assistant", "content": llm_reply}));

            let clean_json = llm_reply
                .trim_start_matches("```json")
                .trim_start_matches("```")
                .trim_end_matches("```")
                .trim();

            let action_req: ReActAction = match serde_json::from_str(clean_json) {
                Ok(req) => req,
                Err(e) => {
                    let error_msg = format!(
                        "Observation: JSON 解析失败 ({}). 请严格按照要求的纯 JSON 格式重新输出。",
                        e
                    );
                    messages.push(serde_json::json!({"role": "user", "content": error_msg}));
                    let _ = log_tx
                        .send("❌ 模型输出了不合法的 JSON，触发自修复机制。".to_string())
                        .await;
                    continue;
                }
            };

            // 🚀 实弹发射：每一轮的思考痕迹实时推流
            let _ = log_tx
                .send(format!("🧠 思考: {}", action_req.thought))
                .await;
            let _ = log_tx
                .send(format!(
                    "🛠️ 动作: {} -> `{}`",
                    action_req.action, action_req.action_input
                ))
                .await;

            if action_req.action == "finish" {
                final_answer = action_req.action_input;
                let _ = log_tx.send("✅ 代理已得出最终结论！".to_string()).await;
                break;
            }

            // 执行下级工具，记得把水管 log_tx.clone() 一起塞给它！
            let observation = self
                .execute_tool(
                    &action_req.action,
                    &action_req.action_input,
                    debug,
                    timestamp,
                    log_tx.clone(),
                )
                .await;

            let obs_truncated = if observation.len() > 1000 {
                format!("{}...", &observation[..1000])
            } else {
                observation.clone()
            };

            let _ = log_tx.send(format!("✨ 观察结果: {}", obs_truncated)).await;

            messages.push(serde_json::json!({"role": "user", "content": format!("Observation: {}", obs_truncated)}));
        }

        if final_answer.is_empty() {
            anyhow::bail!(
                "ReAct Agent 达到了最大思考步数 ({})，但未能得出结论。",
                self.max_steps
            );
        }

        // 🎯 核心护城河：函数返回值 ONLY 暴露 final_answer，绝对阻断搜索词向外泄露！
        Ok(final_answer)
    }
}

// 🧪 单元测试层
#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[tokio::test]
    async fn test_text_node_output() {
        let node = TextNode {
            id: "test_text".to_string(),
            text: "Hello AgentFlow!".to_string(),
        };

        // ✨ 修复：传入占位的时间戳参数
        let result = node
            .execute("来自上游的噪音干扰", false, "20260619_test")
            .await
            .unwrap();
        assert_eq!(result, "Hello AgentFlow!", "文本节点输出内容不匹配");
    }

    #[tokio::test]
    async fn test_regex_match_node() {
        let node = RegexMatchNode {
            id: "test_regex".to_string(),
            pattern: r"Email: ([a-zA-Z0-9@.]+)".to_string(),
        };

        // 1. 测试能成功匹配的情况 (✨ 修复：传入时间戳)
        let valid_input = "联系人信息提取：Email: agent@rust.com 请尽快联系。";
        let valid_result = node
            .execute(valid_input, false, "20260619_test")
            .await
            .unwrap();
        assert_eq!(
            valid_result, "agent@rust.com",
            "正则表达式未能正确捕获捕获组(Group 1)"
        );

        // 2. 测试匹配失败的情况（应该抛出 Err）
        let invalid_input = "这里面根本没有邮箱地址。";
        let invalid_result = node.execute(invalid_input, false, "20260619_test").await;
        assert!(
            invalid_result.is_err(),
            "正则匹配不到时应该立刻返回错误中断工作流"
        );
    }

    #[tokio::test]
    async fn test_file_io_nodes_pipeline() {
        let test_timestamp = "test_12345";

        let write_node = FileWriteNode {
            id: "writer".to_string(),
            original_path: "agent_test.txt".to_string(),
            timestamp: test_timestamp.to_string(),
        };

        // 根据 FileWriteNode 的重命名逻辑，推算出它实际写入的路径
        let expected_real_path = format!("outputs/reports/agent_test_{}.txt", test_timestamp);

        let read_node = FileReadNode {
            id: "reader".to_string(),
            file_path: expected_real_path.clone(),
        };

        let mock_data = "这是一段由工作流引擎自动生成的前沿科研数据。";

        // 2. 测试写入节点
        let write_result = write_node.execute(mock_data, false, test_timestamp).await;
        assert!(write_result.is_ok(), "文件写入节点执行失败");

        // 3. 测试读取节点
        let read_result = read_node.execute("", false, test_timestamp).await.unwrap();
        assert_eq!(
            read_result, mock_data,
            "读取出的数据与写入的数据产生了哈希或文本损坏！"
        );

        // 4. 清理案发现场（极客的修养：删除测试产生的真实文件）
        let cleanup_result = fs::remove_file(&expected_real_path);
        assert!(cleanup_result.is_ok(), "清理临时测试文件失败");
    }
}
