use anyhow::Context;
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

/// Helper function to connect to an upstream relay via QUIC
async fn connect_upstream(
    upstream_url: &Url,
    tls: &moq_native_ietf::tls::Config,
    locals: Locals,
) -> anyhow::Result<()> {
    // Create a QUIC client-only endpoint (binds to ephemeral port)
    let quic = moq_native_ietf::quic::Endpoint::new(moq_native_ietf::quic::Config {
        bind: "[::]:0".parse().unwrap(),
        tls: tls.clone(),
    })?;

    // Connect to the upstream relay
    let upstream_session = quic
        .client
        .connect(upstream_url)
        .await
        .context("failed to connect to upstream relay")?;

    let (session, publisher, subscriber) =
        moq_transport::session::Session::connect(upstream_session)
            .await
            .context("failed to establish upstream MoQ session")?;

    log::info!("Connected to upstream relay: {}", upstream_url);

    // Create session for the upstream connection
    let upstream = Session {
        session,
        producer: Some(Producer::new(publisher, locals.clone(), None)),
        consumer: Some(Consumer::new(subscriber, locals.clone(), None, None)),
    };

    // Run the upstream session (blocks until it ends)
    upstream.run().await.map_err(|e| anyhow::anyhow!("upstream session error: {}", e))
}

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

    /// Upstream relay URL for forwarding announces and fetching streams.
    /// Use with --ws-only to bridge WebSocket clients to a QUIC relay.
    #[arg(long)]
    pub upstream: Option<Url>,
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

    // WebSocket-only mode for environments without UDP support (like Cloudflare Containers)
    if cli.ws_only {
        let ws_bind = cli.ws_bind.ok_or_else(|| {
            anyhow::anyhow!("--ws-bind is required when using --ws-only mode")
        })?;

        // In ws-only mode with no TLS, we don't need certificates
        if !cli.ws_no_tls && tls.server.is_none() {
            anyhow::bail!("TLS certificates required unless --ws-no-tls is specified");
        }

        log::info!("Running in WebSocket-only mode (no QUIC server)");

        let locals = Locals::new();

        // Start the WebSocket server first (so container is ready immediately)
        // Connect to upstream in the background
        let ws_server = WebSocketServer::new(WebSocketConfig {
            bind: ws_bind,
            tls: tls.clone(),
            no_tls: cli.ws_no_tls,
            locals: locals.clone(),
            remotes: None,
            api: None,
            forward: None, // We'll handle forwarding differently
            quic_addr: ws_bind, // Dummy address
        });

        // If upstream is provided, spawn a background task to connect to it
        if let Some(upstream_url) = cli.upstream.clone() {
            let upstream_tls = tls.clone();
            let upstream_locals = locals.clone();

            tokio::spawn(async move {
                log::info!("Connecting to upstream relay in background: {}", upstream_url);

                // Retry loop for upstream connection
                loop {
                    match connect_upstream(&upstream_url, &upstream_tls, upstream_locals.clone()).await {
                        Ok(_) => {
                            log::info!("Upstream connection closed, reconnecting...");
                        }
                        Err(e) => {
                            log::error!("Failed to connect to upstream relay: {}. Retrying in 5s...", e);
                        }
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            });
        } else {
            log::warn!("No --upstream specified, running in isolated mode (no stream forwarding)");
        }

        return ws_server.run().await;
    }

    // Normal mode requires TLS certificates for QUIC
    if tls.server.is_none() {
        anyhow::bail!("missing TLS certificates");
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
