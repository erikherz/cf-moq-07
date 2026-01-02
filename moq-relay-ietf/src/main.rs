use clap::Parser;

mod api;
mod consumer;
mod local;
mod producer;
mod relay;
mod remote;
mod session;
mod web;
mod websocket;

pub use api::*;
pub use consumer::*;
pub use local::*;
pub use producer::*;
pub use relay::*;
pub use remote::*;
pub use session::*;
pub use web::*;
pub use websocket::*;

use std::net;
use url::Url;

#[derive(Parser, Clone)]
pub struct Cli {
    /// Listen on this address for QUIC/WebTransport connections
    #[arg(long, default_value = "[::]:443")]
    pub bind: net::SocketAddr,

    /// The TLS configuration.
    #[command(flatten)]
    pub tls: moq_native_ietf::tls::Args,

    /// Forward all announces to the provided server for authentication/routing.
    /// If not provided, the relay accepts every unique announce.
    #[arg(long)]
    pub announce: Option<Url>,

    /// The URL of the moq-api server in order to run a cluster.
    /// Must be used in conjunction with --node to advertise the origin
    #[arg(long)]
    pub api: Option<Url>,

    /// The hostname that we advertise to other origins.
    /// The provided certificate must be valid for this address.
    #[arg(long)]
    pub node: Option<Url>,

    /// Enable development mode.
    /// This hosts a HTTPS web server via TCP to serve the fingerprint of the certificate.
    #[arg(long)]
    pub dev: bool,

    /// Listen on this address for WebSocket connections (Safari support).
    /// When provided, starts a WSS server for browsers without WebTransport support.
    #[arg(long)]
    pub ws_bind: Option<net::SocketAddr>,

    /// Disable TLS for WebSocket server (for use behind TLS-terminating proxies like Cloudflare).
    /// WARNING: Only use this when the WebSocket server is behind a trusted proxy that handles TLS.
    #[arg(long, default_value = "false")]
    pub ws_no_tls: bool,

    /// Run in WebSocket-only mode without the QUIC server.
    /// Use this for environments that don't support UDP (like Cloudflare Containers).
    #[arg(long, default_value = "false")]
    pub ws_only: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    // Disable tracing so we don't get a bunch of Quinn spam.
    let tracer = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::WARN)
        .finish();
    tracing::subscriber::set_global_default(tracer).unwrap();

    let cli = Cli::parse();
    let tls = cli.tls.load()?;

    if tls.server.is_none() {
        anyhow::bail!("missing TLS certificates");
    }

    // WebSocket-only mode for environments without UDP support (like Cloudflare Containers)
    if cli.ws_only {
        let ws_bind = cli.ws_bind.ok_or_else(|| {
            anyhow::anyhow!("--ws-bind is required when using --ws-only mode")
        })?;

        log::info!("Running in WebSocket-only mode (no QUIC)");

        let locals = Locals::new();

        // Set up upstream connection for forwarding announces (via HTTPS, not QUIC)
        // Note: In ws-only mode, we can't forward to QUIC relays directly
        // The container will just be a local relay without upstream forwarding

        let ws_server = WebSocketServer::new(WebSocketConfig {
            bind: ws_bind,
            tls: tls.clone(),
            no_tls: cli.ws_no_tls,
            locals,
            remotes: None, // No remote fetching in ws-only mode
            api: None,     // No cluster API in ws-only mode
            forward: None, // No announce forwarding in ws-only mode
            quic_addr: ws_bind, // Dummy address
        });

        return ws_server.run().await;
    }

    // Normal mode with QUIC server
    let relay = Relay::new(RelayConfig {
        tls: tls.clone(),
        bind: cli.bind,
        node: cli.node,
        api: cli.api,
        announce: cli.announce,
        ws_bind: cli.ws_bind,
        ws_no_tls: cli.ws_no_tls,
    })?;

    if cli.dev {
        // Create a web server too.
        // Currently this only contains the certificate fingerprint (for development only).
        let web = Web::new(WebConfig {
            bind: cli.bind,
            tls,
        });

        tokio::spawn(async move {
            web.run().await.expect("failed to run web server");
        });
    }

    relay.run().await
}
