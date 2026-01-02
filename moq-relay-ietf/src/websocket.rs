//! WebSocket server for Safari browser support.
//!
//! This module provides a WebSocket-based transport layer for browsers that don't
//! support WebTransport (like Safari). It uses the `web-transport-ws` polyfill
//! to provide MoQ protocol support over WebSockets.

use std::{net, sync::Arc};

use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

use crate::{Api, Consumer, Locals, Producer, RemotesConsumer, Session};

/// Configuration for the WebSocket server.
pub struct WebSocketConfig {
    /// Address to bind the WebSocket server to.
    pub bind: net::SocketAddr,
    /// TLS configuration for WSS.
    pub tls: moq_native_ietf::tls::Config,
    /// Disable TLS for WebSocket server (for use behind TLS-terminating proxies).
    pub no_tls: bool,
    /// Local track registry.
    pub locals: Locals,
    /// Remote origins consumer (for fetching from other relays).
    pub remotes: Option<RemotesConsumer>,
    /// API client for cluster coordination.
    pub api: Option<Api>,
    /// Optional producer to forward announces to.
    pub forward: Option<Producer>,
    /// The QUIC endpoint's local address (unused but kept for API consistency).
    pub quic_addr: net::SocketAddr,
}

/// Shared state for WebSocket handlers.
#[derive(Clone)]
struct WebSocketState {
    locals: Locals,
    remotes: Option<RemotesConsumer>,
    api: Option<Api>,
    forward: Option<Producer>,
}

/// WebSocket server for Safari support.
pub struct WebSocketServer {
    bind: net::SocketAddr,
    tls: Option<TlsAcceptor>,
    state: WebSocketState,
}

impl WebSocketServer {
    /// Create a new WebSocket server with the given configuration.
    pub fn new(config: WebSocketConfig) -> Self {
        let state = WebSocketState {
            locals: config.locals,
            remotes: config.remotes,
            api: config.api,
            forward: config.forward,
        };

        let tls = if config.no_tls {
            None
        } else {
            let mut tls_config = config.tls.server.expect("missing server TLS configuration");
            // Use HTTP/1.1 for WebSocket upgrades (HTTP/2 doesn't support WebSocket upgrade)
            tls_config.alpn_protocols = vec![b"http/1.1".to_vec()];
            Some(TlsAcceptor::from(Arc::new(tls_config)))
        };

        Self {
            bind: config.bind,
            tls,
            state,
        }
    }

    /// Run the WebSocket server.
    pub async fn run(self) -> anyhow::Result<()> {
        let listener = TcpListener::bind(self.bind).await?;

        if self.tls.is_some() {
            log::info!("WebSocket server listening on wss://{}/", self.bind);
        } else {
            log::info!("WebSocket server listening on ws://{}/ (no TLS - behind proxy)", self.bind);
        }

        loop {
            let (stream, addr) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    log::warn!("Failed to accept TCP connection: {:?}", e);
                    continue;
                }
            };

            let tls = self.tls.clone();
            let state = self.state.clone();

            tokio::spawn(async move {
                // Handle connection with or without TLS
                let ws_session = if let Some(tls) = tls {
                    // Perform TLS handshake
                    let tls_stream = match tls.accept(stream).await {
                        Ok(s) => s,
                        Err(e) => {
                            log::debug!("TLS handshake failed from {}: {:?}", addr, e);
                            return;
                        }
                    };

                    log::debug!("TLS connection established from {}", addr);

                    // Accept WebSocket connection with WebTransport protocol negotiation
                    match web_transport_ws::Session::accept(tls_stream).await {
                        Ok(s) => s,
                        Err(e) => {
                            log::warn!("WebSocket/WebTransport handshake failed from {}: {:?}", addr, e);
                            return;
                        }
                    }
                } else {
                    // No TLS - accept WebSocket directly over TCP (for Cloudflare Containers)
                    log::debug!("Accepting plain WebSocket connection from {}", addr);

                    match web_transport_ws::Session::accept(stream).await {
                        Ok(s) => s,
                        Err(e) => {
                            log::warn!("WebSocket handshake failed from {}: {:?}", addr, e);
                            return;
                        }
                    }
                };

                log::info!("WebSocket session established from {}", addr);

                // Wrap in our bridge's unified Session type
                let transport_session: web_transport::Session = ws_session.into();

                // Now accept the MoQ session over the WebSocket transport
                let (moq_session, publisher, subscriber) =
                    match moq_transport::session::Session::accept(transport_session).await {
                        Ok(session) => session,
                        Err(err) => {
                            log::warn!("Failed to accept MoQ session from {}: {}", addr, err);
                            return;
                        }
                    };

                log::info!("MoQ session established from {} (WebSocket)", addr);

                // Create the relay session handler
                let session = Session {
                    session: moq_session,
                    producer: publisher.map(|publisher| {
                        Producer::new(publisher, state.locals.clone(), state.remotes.clone())
                    }),
                    consumer: subscriber.map(|subscriber| {
                        Consumer::new(subscriber, state.locals.clone(), state.api.clone(), state.forward.clone())
                    }),
                };

                // Run the session
                if let Err(err) = session.run().await {
                    log::warn!("MoQ session from {} (WebSocket) ended with error: {}", addr, err);
                } else {
                    log::debug!("MoQ session from {} (WebSocket) ended gracefully", addr);
                }
            });
        }
    }
}
