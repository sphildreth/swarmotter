// SPDX-License-Identifier: Apache-2.0

//! `swarmotterctl`: API-backed command-line client for SwarmOtter.
//!
//! The client is deliberately a control-plane consumer only. It does not open
//! torrent data-plane sockets or read daemon-private storage.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use clap::{Parser, Subcommand};
use http_body_util::{BodyExt, Full};
use hyper::header::{AUTHORIZATION, CONNECTION, CONTENT_TYPE, HOST, USER_AGENT};
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use serde::Serialize;
use serde_json::{json, Value};
use thiserror::Error;
use url::Url;

const JSON_FORMAT_VERSION: u8 = 1;
const DEFAULT_URL: &str = "http://127.0.0.1:9091";
const DEFAULT_TIMEOUT_SECONDS: u64 = 30;
const MAX_RESPONSE_BYTES: usize = 128 * 1024 * 1024;

#[derive(Parser, Debug)]
#[command(
    name = "swarmotterctl",
    version,
    about = "Control a running SwarmOtter daemon through its native API"
)]
struct Cli {
    /// API base URL. Defaults to SWARMOTTER_API_URL or http://127.0.0.1:9091.
    #[arg(long, global = true)]
    url: Option<String>,
    /// File containing the API token. Defaults to SWARMOTTER_API_TOKEN_FILE.
    #[arg(long, global = true)]
    token_file: Option<PathBuf>,
    /// Request timeout in seconds.
    #[arg(long, global = true)]
    timeout_seconds: Option<u64>,
    /// Emit one versioned JSON document on stdout.
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Add a magnet or .torrent file.
    Add {
        #[command(subcommand)]
        source: AddSource,
    },
    /// List torrents using server-side filtering, sorting, and pagination.
    List {
        #[arg(long)]
        q: Option<String>,
        /// Filter by state. Repeat or use a comma-separated value.
        #[arg(long)]
        state: Vec<String>,
        #[arg(long)]
        health: Vec<String>,
        #[arg(long)]
        label: Vec<String>,
        #[arg(long)]
        sort: Option<String>,
        #[arg(long)]
        direction: Option<String>,
        #[arg(long, default_value_t = 1)]
        page: usize,
        #[arg(long, default_value_t = 200)]
        per_page: usize,
        /// Fetch every server page. Conflicts with --page.
        #[arg(long, conflicts_with = "page")]
        all: bool,
    },
    /// Show one torrent's details.
    Show { hash: String },
    /// Show global health, or one torrent's health when a hash is supplied.
    Health { hash: Option<String> },
    /// Pause one torrent.
    Pause { hash: String },
    /// Resume one torrent.
    Resume { hash: String },
    /// Pause all download and upload activity (master traffic switch).
    PauseAll,
    /// Clear the master traffic switch and resume queue/seeding policy.
    ResumeAll,
    /// Start one torrent now, bypassing queue admission.
    Start { hash: String },
    /// Stop one torrent.
    Stop { hash: String },
    /// Remove one torrent.
    Remove {
        hash: String,
        #[arg(long)]
        delete_data: bool,
    },
    /// Force a recheck.
    Recheck { hash: String },
    /// Reannounce to configured trackers.
    Reannounce { hash: String },
    /// Set per-torrent bandwidth limits in bytes/sec (0 = unlimited).
    Limits {
        hash: String,
        #[arg(long)]
        download: Option<u64>,
        #[arg(long)]
        upload: Option<u64>,
    },
}

#[derive(Subcommand, Debug)]
enum AddSource {
    /// Add a magnet URI.
    Magnet {
        magnet: String,
        #[arg(long)]
        dir: Option<String>,
        #[arg(long)]
        profile: Option<String>,
        #[arg(long = "label")]
        labels: Vec<String>,
        #[arg(long)]
        paused: bool,
        #[arg(long)]
        preview: bool,
    },
    /// Add a .torrent file.
    File {
        path: PathBuf,
        #[arg(long)]
        profile: Option<String>,
        #[arg(long = "label")]
        labels: Vec<String>,
        #[arg(long)]
        paused: bool,
        #[arg(long)]
        preview: bool,
    },
}

#[derive(Debug, Error)]
enum CliError {
    #[error("{0}")]
    Usage(String),
    #[error("configuration error: {0}")]
    Config(String),
    #[error("connection failed: {0}")]
    Connection(String),
    #[error("authentication failed: {0}")]
    Auth(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("validation failed: {0}")]
    Validation(String),
    #[error("daemon API error: {0}")]
    Api(String),
    #[error("{0}")]
    Io(String),
}

impl CliError {
    fn exit_code(&self) -> i32 {
        match self {
            Self::Usage(_) => 2,
            Self::Config(_) => 2,
            Self::Auth(_) => 3,
            Self::NotFound(_) => 4,
            Self::Connection(_) => 5,
            Self::Validation(_) => 6,
            Self::Api(_) | Self::Io(_) => 1,
        }
    }

    fn code(&self) -> &'static str {
        match self {
            Self::Usage(_) => "usage",
            Self::Config(_) => "configuration",
            Self::Connection(_) => "connection_failed",
            Self::Auth(_) => "authentication_failed",
            Self::NotFound(_) => "not_found",
            Self::Validation(_) => "validation_failed",
            Self::Api(_) => "api_error",
            Self::Io(_) => "io_error",
        }
    }
}

#[derive(Serialize)]
struct JsonOutput<T> {
    format_version: u8,
    command: String,
    success: bool,
    data: Option<T>,
    error: Option<JsonError>,
}

#[derive(Serialize)]
struct JsonError {
    code: String,
    message: String,
    status: Option<u16>,
}

struct CommandOutput {
    command: &'static str,
    data: Value,
    human: String,
}

struct ApiClient {
    base: Url,
    token: Option<String>,
    timeout: Duration,
}

impl ApiClient {
    fn new(base: Url, token: Option<String>, timeout: Duration) -> Self {
        Self {
            base,
            token,
            timeout,
        }
    }

    async fn get(&self, path: &str) -> Result<Value, CliError> {
        self.request_json(Method::GET, path, None, None).await
    }

    async fn put(&self, path: &str, body: Option<Value>) -> Result<Value, CliError> {
        let bytes = body
            .map(|value| serde_json::to_vec(&value))
            .transpose()
            .map_err(|error| CliError::Usage(format!("encode request JSON: {error}")))?;
        self.request_json(Method::PUT, path, bytes, Some("application/json"))
            .await
    }

    async fn post(&self, path: &str, body: Option<Value>) -> Result<Value, CliError> {
        let bytes = body
            .map(|value| serde_json::to_vec(&value))
            .transpose()
            .map_err(|error| CliError::Usage(format!("encode request JSON: {error}")))?;
        self.request_json(Method::POST, path, bytes, Some("application/json"))
            .await
    }

    async fn delete(&self, path: &str) -> Result<Value, CliError> {
        self.request_json(Method::DELETE, path, None, None).await
    }

    async fn post_bytes(
        &self,
        path: &str,
        bytes: Vec<u8>,
        content_type: &str,
    ) -> Result<Value, CliError> {
        self.request_json(Method::POST, path, Some(bytes), Some(content_type))
            .await
    }

    async fn request_json(
        &self,
        method: Method,
        path: &str,
        body: Option<Vec<u8>>,
        content_type: Option<&str>,
    ) -> Result<Value, CliError> {
        let (status, body) = self.request(method, path, body, content_type).await?;
        let parsed = serde_json::from_slice::<Value>(&body).ok();
        if status.is_success() {
            if let Some(value) = parsed {
                if value.get("success").and_then(Value::as_bool) == Some(true) {
                    return Ok(value.get("data").cloned().unwrap_or(Value::Null));
                }
                return Ok(value);
            }
            return Err(CliError::Api(
                "daemon returned a successful non-JSON response".into(),
            ));
        }
        let (code, message) = parsed
            .as_ref()
            .and_then(|value| value.get("error"))
            .map(|error| {
                (
                    error
                        .get("code")
                        .and_then(Value::as_str)
                        .unwrap_or("api_error"),
                    error
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("daemon API request failed"),
                )
            })
            .unwrap_or_else(|| ("api_error", "daemon API request failed"));
        Err(error_for_status(status, code, message))
    }

    async fn request(
        &self,
        method: Method,
        path: &str,
        body: Option<Vec<u8>>,
        content_type: Option<&str>,
    ) -> Result<(StatusCode, Vec<u8>), CliError> {
        let url = self
            .base
            .join(path.trim_start_matches('/'))
            .map_err(|error| CliError::Config(format!("invalid API URL/path: {error}")))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(CliError::Config("API URL must use http or https".into()));
        }
        let host = url
            .host_str()
            .ok_or_else(|| CliError::Config("API URL has no host".into()))?
            .to_string();
        let port = url
            .port_or_known_default()
            .ok_or_else(|| CliError::Config("API URL has no known TCP port".into()))?;
        let authority = host_authority(&url)?;
        let target = match url.query() {
            Some(query) => format!("{}?{}", url.path(), query),
            None => url.path().to_string(),
        };
        let body_len = body.as_ref().map_or(0, Vec::len);
        let body = body.unwrap_or_default();
        let mut builder = Request::builder()
            .method(method)
            .uri(target)
            .header(HOST, authority)
            .header(CONNECTION, "close")
            .header(
                USER_AGENT,
                concat!("swarmotterctl/", env!("CARGO_PKG_VERSION")),
            )
            .header("accept", "application/json");
        if let Some(value) = self.token.as_deref().filter(|value| !value.is_empty()) {
            builder = builder.header(AUTHORIZATION, format!("Bearer {value}"));
        }
        if let Some(value) = content_type {
            builder = builder.header(CONTENT_TYPE, value);
        }
        builder = builder.header("content-length", body_len.to_string());
        let request = builder
            .body(Full::new(Bytes::from(body)))
            .map_err(|error| CliError::Connection(format!("build request: {error}")))?;

        let request_future = async {
            let stream = connect_stream(&host, port).await?;
            let response = if url.scheme() == "https" {
                let config = tls_config()?;
                let connector = tokio_rustls::TlsConnector::from(config);
                let server_name =
                    rustls::pki_types::ServerName::try_from(host.clone()).map_err(|error| {
                        CliError::Connection(format!("invalid TLS host {host}: {error}"))
                    })?;
                let stream = connector
                    .connect(server_name, stream)
                    .await
                    .map_err(|error| CliError::Connection(format!("TLS handshake: {error}")))?;
                send_http1(stream, request).await?
            } else {
                send_http1(stream, request).await?
            };
            Ok::<_, CliError>(response)
        };
        tokio::time::timeout(self.timeout, request_future)
            .await
            .map_err(|_| CliError::Connection(format!("request to {url} timed out")))?
    }
}

async fn connect_stream(host: &str, port: u16) -> Result<tokio::net::TcpStream, CliError> {
    let address = if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        std::net::SocketAddr::new(ip, port)
    } else {
        let mut addresses = tokio::net::lookup_host((host, port))
            .await
            .map_err(|error| CliError::Connection(format!("resolve {host}:{port}: {error}")))?;
        addresses
            .next()
            .ok_or_else(|| CliError::Connection(format!("resolve {host}:{port}: no addresses")))?
    };
    tokio::net::TcpStream::connect(address)
        .await
        .map_err(|error| CliError::Connection(format!("connect {address}: {error}")))
}

async fn send_http1<S>(
    stream: S,
    request: Request<Full<Bytes>>,
) -> Result<(StatusCode, Vec<u8>), CliError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .map_err(|error| CliError::Connection(format!("HTTP/1 handshake: {error}")))?;
    let driver = tokio::spawn(connection);
    let response = sender
        .send_request(request)
        .await
        .map_err(|error| CliError::Connection(format!("HTTP request: {error}")))?;
    drop(sender);
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .map_err(|error| CliError::Connection(format!("read HTTP response: {error}")))?
        .to_bytes();
    driver.abort();
    if body.len() > MAX_RESPONSE_BYTES {
        return Err(CliError::Api(format!(
            "response exceeded {MAX_RESPONSE_BYTES} bytes"
        )));
    }
    Ok((status, body.to_vec()))
}

fn tls_config() -> Result<Arc<rustls::ClientConfig>, CliError> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    Ok(Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ))
}

fn host_authority(url: &Url) -> Result<String, CliError> {
    let host = match url
        .host()
        .ok_or_else(|| CliError::Config("API URL has no host".into()))?
    {
        url::Host::Domain(domain) => domain.to_string(),
        url::Host::Ipv4(address) => address.to_string(),
        url::Host::Ipv6(address) => format!("[{address}]"),
    };
    let default_port = match url.scheme() {
        "http" => 80,
        "https" => 443,
        other => {
            return Err(CliError::Config(format!(
                "unsupported API URL scheme {other}"
            )))
        }
    };
    Ok(match url.port() {
        Some(port) if port != default_port => format!("{host}:{port}"),
        _ => host,
    })
}

fn error_for_status(status: StatusCode, code: &str, message: &str) -> CliError {
    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        CliError::Auth(message.to_string())
    } else if status == StatusCode::NOT_FOUND {
        CliError::NotFound(message.to_string())
    } else if status.is_client_error() {
        CliError::Validation(format!("{code}: {message}"))
    } else {
        CliError::Api(format!("HTTP {} {code}: {message}", status.as_u16()))
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let json_mode = cli.json;
    let command_name = command_name(&cli.command).to_string();
    let result = execute(cli).await;
    match result {
        Ok(output) => {
            if json_mode {
                print_json(JsonOutput {
                    format_version: JSON_FORMAT_VERSION,
                    command: output.command.to_string(),
                    success: true,
                    data: Some(output.data),
                    error: None,
                });
            } else {
                println!("{}", output.human);
            }
        }
        Err(error) => {
            if json_mode {
                print_json(JsonOutput::<Value> {
                    format_version: JSON_FORMAT_VERSION,
                    command: command_name,
                    success: false,
                    data: None,
                    error: Some(JsonError {
                        code: error.code().into(),
                        message: error.to_string(),
                        status: None,
                    }),
                });
            } else {
                eprintln!("swarmotterctl: {error}");
            }
            std::process::exit(error.exit_code());
        }
    }
}

fn print_json<T: Serialize>(value: JsonOutput<T>) {
    match serde_json::to_string_pretty(&value) {
        Ok(text) => println!("{text}"),
        Err(error) => eprintln!("swarmotterctl: serialize JSON output: {error}"),
    }
}

fn command_name(command: &Command) -> &'static str {
    match command {
        Command::Add { .. } => "add",
        Command::List { .. } => "list",
        Command::Show { .. } => "show",
        Command::Health { .. } => "health",
        Command::Pause { .. } => "pause",
        Command::Resume { .. } => "resume",
        Command::PauseAll => "pause-all",
        Command::ResumeAll => "resume-all",
        Command::Start { .. } => "start",
        Command::Stop { .. } => "stop",
        Command::Remove { .. } => "remove",
        Command::Recheck { .. } => "recheck",
        Command::Reannounce { .. } => "reannounce",
        Command::Limits { .. } => "limits",
    }
}

async fn execute(cli: Cli) -> Result<CommandOutput, CliError> {
    let base = resolve_base_url(cli.url.as_deref())?;
    let token = resolve_token(cli.token_file.as_deref())?;
    let timeout = cli.timeout_seconds.unwrap_or_else(|| {
        std::env::var("SWARMOTTER_API_TIMEOUT_SECONDS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_TIMEOUT_SECONDS)
    });
    if timeout == 0 {
        return Err(CliError::Config(
            "timeout must be greater than zero seconds".into(),
        ));
    }
    let client = ApiClient::new(base, token, Duration::from_secs(timeout));
    match cli.command {
        Command::Add { source } => add(&client, source).await,
        Command::List {
            q,
            state,
            health,
            label,
            sort,
            direction,
            page,
            per_page,
            all,
        } => {
            list(
                &client, q, state, health, label, sort, direction, page, per_page, all,
            )
            .await
        }
        Command::Show { hash } => show(&client, &hash).await,
        Command::Health { hash } => health(&client, hash.as_deref()).await,
        Command::Pause { hash } => action(&client, &hash, "pause", "paused").await,
        Command::Resume { hash } => action(&client, &hash, "resume", "resumed").await,
        Command::PauseAll => session(&client, true).await,
        Command::ResumeAll => session(&client, false).await,
        Command::Start { hash } => action(&client, &hash, "start", "started").await,
        Command::Stop { hash } => action(&client, &hash, "stop", "stopped").await,
        Command::Remove { hash, delete_data } => remove(&client, &hash, delete_data).await,
        Command::Recheck { hash } => action(&client, &hash, "recheck", "recheck requested").await,
        Command::Reannounce { hash } => {
            action(&client, &hash, "reannounce", "reannounce requested").await
        }
        Command::Limits {
            hash,
            download,
            upload,
        } => limits(&client, &hash, download, upload).await,
    }
}

fn resolve_base_url(value: Option<&str>) -> Result<Url, CliError> {
    let raw = value
        .map(str::to_string)
        .or_else(|| std::env::var("SWARMOTTER_API_URL").ok())
        .unwrap_or_else(|| DEFAULT_URL.to_string());
    let mut url = Url::parse(&raw)
        .map_err(|error| CliError::Config(format!("invalid API URL {raw:?}: {error}")))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(CliError::Config("API URL must use http or https".into()));
    }
    if url.host().is_none() {
        return Err(CliError::Config("API URL has no host".into()));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(CliError::Config(
            "API URL must not contain user information; use SWARMOTTER_API_TOKEN or --token-file"
                .into(),
        ));
    }
    if !url.path().is_empty() && url.path() != "/" {
        url.set_path("/");
    }
    url.set_query(None);
    url.set_fragment(None);
    Ok(url)
}

fn resolve_token(token_file: Option<&Path>) -> Result<Option<String>, CliError> {
    if let Some(path) = token_file {
        let value = std::fs::read_to_string(path).map_err(|error| {
            CliError::Config(format!("read token file {}: {error}", path.display()))
        })?;
        let value = value.trim().to_string();
        if value.is_empty() {
            return Err(CliError::Config(format!(
                "token file {} is empty",
                path.display()
            )));
        }
        return Ok(Some(value));
    }
    if let Ok(value) = std::env::var("SWARMOTTER_API_TOKEN") {
        let value = value.trim().to_string();
        if !value.is_empty() {
            return Ok(Some(value));
        }
    }
    if let Ok(path) = std::env::var("SWARMOTTER_API_TOKEN_FILE") {
        return resolve_token(Some(Path::new(&path)));
    }
    Ok(None)
}

async fn add(client: &ApiClient, source: AddSource) -> Result<CommandOutput, CliError> {
    match source {
        AddSource::Magnet {
            magnet,
            dir,
            profile,
            labels,
            paused,
            preview,
        } => {
            let body = json!({
                "magnet": magnet,
                "download_dir": dir,
                "profile": profile,
                "labels": (!labels.is_empty()).then_some(labels),
                "paused": paused,
                "preview": preview,
            });
            let data = client.post("/api/v1/torrents/magnet", Some(body)).await?;
            let hash = data.as_str().unwrap_or_default().to_string();
            Ok(CommandOutput {
                command: "add",
                data: json!({ "info_hash": hash }),
                human: hash,
            })
        }
        AddSource::File {
            path,
            profile,
            labels,
            paused,
            preview,
        } => {
            let bytes = std::fs::read(&path).map_err(|error| {
                CliError::Io(format!("read torrent file {}: {error}", path.display()))
            })?;
            let mut query = Vec::new();
            if let Some(profile) = profile {
                query.push(format!("profile={}", percent_encode(&profile)));
            }
            if !labels.is_empty() {
                query.push(format!("labels={}", percent_encode(&labels.join(","))));
            }
            if paused {
                query.push("paused=true".into());
            }
            if preview {
                query.push("preview=true".into());
            }
            let path = if query.is_empty() {
                "/api/v1/torrents/file".to_string()
            } else {
                format!("/api/v1/torrents/file?{}", query.join("&"))
            };
            let data = client
                .post_bytes(&path, bytes, "application/x-bittorrent")
                .await?;
            let hash = data.as_str().unwrap_or_default().to_string();
            Ok(CommandOutput {
                command: "add",
                data: json!({ "info_hash": hash }),
                human: hash,
            })
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn list(
    client: &ApiClient,
    q: Option<String>,
    state: Vec<String>,
    health: Vec<String>,
    label: Vec<String>,
    sort: Option<String>,
    direction: Option<String>,
    page: usize,
    per_page: usize,
    all: bool,
) -> Result<CommandOutput, CliError> {
    if per_page == 0 || per_page > 500 {
        return Err(CliError::Usage(
            "--per-page must be between 1 and 500".into(),
        ));
    }
    if page == 0 {
        return Err(CliError::Usage("--page must be at least 1".into()));
    }
    let build_path = |page: usize, per_page: usize| {
        let mut query = vec![format!("page={page}"), format!("per_page={per_page}")];
        if let Some(q) = q.as_deref() {
            query.push(format!("q={}", percent_encode(q)));
        }
        for (name, values) in [("state", &state), ("health", &health), ("label", &label)] {
            if !values.is_empty() {
                query.push(format!("{name}={}", percent_encode(&values.join(","))));
            }
        }
        if let Some(sort) = sort.as_deref() {
            query.push(format!("sort={}", percent_encode(sort)));
        }
        if let Some(direction) = direction.as_deref() {
            query.push(format!("dir={}", percent_encode(direction)));
        }
        format!("/api/v1/torrents/query?{}", query.join("&"))
    };
    let mut data = client.get(&build_path(page, per_page)).await?;
    if all {
        let mut rows = data
            .get("rows")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let page_count = data
            .get("page_count")
            .and_then(Value::as_u64)
            .unwrap_or(1)
            .max(1);
        for next_page in 2..=page_count {
            let page_data = client
                .get(&build_path(next_page as usize, per_page))
                .await?;
            if let Some(next_rows) = page_data.get("rows").and_then(Value::as_array) {
                rows.extend(next_rows.iter().cloned());
            }
        }
        if let Some(object) = data.as_object_mut() {
            object.insert("rows".into(), Value::Array(rows));
            object.insert("page".into(), json!(1));
            object.insert("per_page".into(), json!(per_page));
            object.insert("page_count".into(), json!(1));
        }
    }
    let human = render_list(&data);
    Ok(CommandOutput {
        command: "list",
        data,
        human,
    })
}

fn render_list(data: &Value) -> String {
    let rows = data
        .get("rows")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if rows.is_empty() {
        return "No torrents.".into();
    }
    let mut table = vec![vec![
        "HASH".to_string(),
        "NAME".to_string(),
        "STATE".to_string(),
        "PROGRESS".to_string(),
        "DOWN".to_string(),
        "UP".to_string(),
        "PEERS".to_string(),
        "RATIO".to_string(),
        "HEALTH".to_string(),
    ]];
    for row in rows {
        table.push(vec![
            text(&row, "info_hash"),
            text(&row, "name"),
            text(&row, "state"),
            progress(&row),
            rate(&row, "rate_down"),
            rate(&row, "rate_up"),
            text(&row, "active_peer_workers"),
            number(&row, "ratio"),
            row.get("health")
                .and_then(|health| health.get("label"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
        ]);
    }
    render_table(&table)
}

async fn show(client: &ApiClient, hash: &str) -> Result<CommandOutput, CliError> {
    let data = client
        .get(&format!("/api/v1/torrents/{}", percent_encode(hash)))
        .await?;
    let human = render_details(&data);
    Ok(CommandOutput {
        command: "show",
        data,
        human,
    })
}

fn render_details(row: &Value) -> String {
    let identity = row.get("identity").cloned().unwrap_or(Value::Null);
    let lines = [
        ("Hash", text(row, "info_hash")),
        ("Name", text(row, "name")),
        ("State", text(row, "state")),
        ("Identity", identity_summary(&identity)),
        (
            "Size",
            format!(
                "{} / {}",
                bytes(row, "bytes_completed"),
                bytes(row, "total_length")
            ),
        ),
        ("Progress", progress(row)),
        ("Downloaded", bytes(row, "downloaded")),
        ("Uploaded", bytes(row, "uploaded")),
        ("Ratio", number(row, "ratio")),
        ("Down rate", rate(row, "rate_down")),
        ("Up rate", rate(row, "rate_up")),
        (
            "Peers",
            format!(
                "{} active / {} known",
                text(row, "active_peer_workers"),
                text(row, "known_peers")
            ),
        ),
        (
            "Health",
            row.get("health")
                .and_then(|v| v.get("label"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
        ),
        ("Error", text(row, "error")),
        (
            "Labels",
            row.get("labels").map(format_array).unwrap_or_default(),
        ),
        ("Download dir", text(row, "download_dir")),
        ("Queue position", text(row, "queue_position")),
    ];
    lines
        .iter()
        .map(|(key, value)| format!("{key}: {value}"))
        .collect::<Vec<_>>()
        .join("\n")
}

async fn health(client: &ApiClient, hash: Option<&str>) -> Result<CommandOutput, CliError> {
    let (data, human) = match hash {
        Some(hash) => {
            let data = client
                .get(&format!("/api/v1/torrents/{}", percent_encode(hash)))
                .await?;
            let health = data.get("health").cloned().unwrap_or(Value::Null);
            let human = format!(
                "{}: {} ({}/100)\n{}",
                text(&data, "name"),
                health
                    .get("label")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown"),
                health.get("score").and_then(Value::as_i64).unwrap_or(0),
                health.get("reasons").map(format_array).unwrap_or_default()
            );
            (health, human)
        }
        None => {
            let data = client.get("/api/v1/health").await?;
            let human = serde_json::to_string_pretty(&data).unwrap_or_default();
            (data, human)
        }
    };
    Ok(CommandOutput {
        command: "health",
        data,
        human,
    })
}

async fn action(
    client: &ApiClient,
    hash: &str,
    action: &'static str,
    human: &'static str,
) -> Result<CommandOutput, CliError> {
    client
        .post(
            &format!("/api/v1/torrents/{}/{}", percent_encode(hash), action),
            None,
        )
        .await?;
    Ok(CommandOutput {
        command: action,
        data: json!({ "info_hash": hash, "action": action }),
        human: format!("{human}: {hash}"),
    })
}

async fn session(client: &ApiClient, paused: bool) -> Result<CommandOutput, CliError> {
    let data = client
        .put("/api/v1/session", Some(json!({ "paused": paused })))
        .await?;
    Ok(CommandOutput {
        command: if paused { "pause-all" } else { "resume-all" },
        data,
        human: if paused {
            "all download and upload activity paused".to_string()
        } else {
            "master traffic switch cleared".to_string()
        },
    })
}

async fn remove(
    client: &ApiClient,
    hash: &str,
    delete_data: bool,
) -> Result<CommandOutput, CliError> {
    client
        .delete(&format!(
            "/api/v1/torrents/{}?delete_data={delete_data}",
            percent_encode(hash)
        ))
        .await?;
    Ok(CommandOutput {
        command: "remove",
        data: json!({ "info_hash": hash, "delete_data": delete_data }),
        human: format!("removed: {hash}"),
    })
}

async fn limits(
    client: &ApiClient,
    hash: &str,
    download: Option<u64>,
    upload: Option<u64>,
) -> Result<CommandOutput, CliError> {
    if download.is_none() && upload.is_none() {
        return Err(CliError::Usage(
            "at least one of --download or --upload is required".into(),
        ));
    }
    client
        .post(
            &format!("/api/v1/torrents/{}/limits", percent_encode(hash)),
            Some(json!({
                "download_limit": download.unwrap_or(0),
                "upload_limit": upload.unwrap_or(0),
            })),
        )
        .await?;
    Ok(CommandOutput {
        command: "limits",
        data: json!({
            "info_hash": hash,
            "download_limit": download.unwrap_or(0),
            "upload_limit": upload.unwrap_or(0),
        }),
        human: format!(
            "limits set: {hash} down={} up={}",
            download.unwrap_or(0),
            upload.unwrap_or(0)
        ),
    })
}

fn text(value: &Value, key: &str) -> String {
    value
        .get(key)
        .map(|value| match value {
            Value::Null => String::new(),
            Value::String(value) => value.clone(),
            other => other.to_string(),
        })
        .unwrap_or_default()
}

fn number(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_f64)
        .map(|value| format!("{value:.2}"))
        .unwrap_or_default()
}

fn bytes(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_u64)
        .map(format_bytes)
        .unwrap_or_else(|| "0 B".into())
}

fn rate(value: &Value, key: &str) -> String {
    let value = value.get(key).and_then(Value::as_u64).unwrap_or(0);
    format!("{}/s", format_bytes(value))
}

fn progress(value: &Value) -> String {
    let total = value
        .get("total_length")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let completed = value
        .get("bytes_completed")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    if total == 0 {
        "0.0%".into()
    } else {
        format!("{:.1}%", completed as f64 / total as f64 * 100.0)
    }
}

fn format_bytes(value: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = value as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} {}", value as u64, UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn identity_summary(identity: &Value) -> String {
    let kind = identity
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let v1 = identity.get("v1").and_then(Value::as_str).unwrap_or("");
    let v2 = identity.get("v2").and_then(Value::as_str).unwrap_or("");
    match kind {
        "hybrid" => format!("hybrid v1={v1} v2={v2}"),
        "v2" => format!("v2 {v2}"),
        "v1" => format!("v1 {v1}"),
        other => other.to_string(),
    }
}

fn format_array(value: &Value) -> String {
    value
        .as_array()
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_default()
}

fn render_table(rows: &[Vec<String>]) -> String {
    let widths = (0..rows[0].len())
        .map(|index| {
            rows.iter()
                .map(|row| row[index].chars().count())
                .max()
                .unwrap_or(0)
        })
        .collect::<Vec<_>>();
    rows.iter()
        .map(|row| {
            row.iter()
                .enumerate()
                .map(|(index, value)| {
                    let truncated = truncate(value, 48);
                    format!("{truncated:<width$}", width = widths[index])
                })
                .collect::<Vec<_>>()
                .join("  ")
                .trim_end()
                .to_string()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn truncate(value: &str, limit: usize) -> String {
    if value.chars().count() <= limit {
        return value.to_string();
    }
    value
        .chars()
        .take(limit.saturating_sub(1))
        .collect::<String>()
        + "…"
}

fn percent_encode(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}
