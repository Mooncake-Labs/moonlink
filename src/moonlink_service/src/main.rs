use clap::Parser;
use moonlink_service::{start_with_config, Result, ServiceConfig};

/// Default REST API port
const DEFAULT_REST_PORT: u16 = 3030;
/// Default moonlink TCP API port.
const DEFAULT_TCP_PORT: u16 = 3031;
/// Default otel API port.
const DEFAULT_OTEL_PORT: u16 = 3435;

#[derive(Parser)]
#[command(name = "moonlink-service")]
#[command(about = "Moonlink data ingestion service")]
struct Cli {
    /// Base path for Moonlink data storage
    base_path: String,

    /// Port for REST API server (optional, defaults to 3030)
    #[arg(long, short = 'p')]
    rest_api_port: Option<u16>,
    /// Disable REST API server
    #[arg(long)]
    no_rest_api: bool,

    /// Port for moonlink standalone server (optional, defaults to 3031).
    #[arg(long)]
    tcp_port: Option<u16>,
    /// Disable standalone deployment.
    #[arg(long)]
    no_tcp_api: bool,

    /// Port for otel API server (optional, defaults to 3435).
    #[arg(long)]
    otel_port: Option<u16>,
    /// Disable standalone deployment.
    #[arg(long)]
    no_otel_api: bool,

    /// IP/port for data server.
    /// For example: http://34.19.1.175:8080.
    #[arg(long)]
    data_server_uri: Option<String>,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    // By default enables backtrace for better troubleshooting capability, no performance overhead, only takes effect at panic.
    std::env::set_var("RUST_BACKTRACE", "1");

    let cli = Cli::parse();
    let config = ServiceConfig {
        base_path: cli.base_path,
        data_server_uri: cli.data_server_uri,
        rest_api_port: if cli.no_rest_api {
            None
        } else {
            Some(cli.rest_api_port.unwrap_or(DEFAULT_REST_PORT))
        },
        tcp_port: if cli.no_tcp_api {
            None
        } else {
            Some(cli.tcp_port.unwrap_or(DEFAULT_TCP_PORT))
        },
        otel_api_port: if cli.no_otel_api {
            None
        } else {
            Some(cli.otel_port.unwrap_or(DEFAULT_OTEL_PORT))
        },
    };

    start_with_config(config).await
}
