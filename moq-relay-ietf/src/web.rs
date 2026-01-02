use std::{net, sync::Arc};

use axum::{extract::State, http::Method, response::IntoResponse, routing::get, Router};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tower_http::cors::{Any, CorsLayer};
use tower_service::Service;

pub struct WebConfig {
    pub bind: net::SocketAddr,
    pub tls: moq_native_ietf::tls::Config,
}

// Run a HTTP server using Axum
// TODO remove this when Chrome adds support for self-signed certificates using WebTransport
pub struct Web {
    bind: net::SocketAddr,
    tls: TlsAcceptor,
    fingerprint: String,
}

impl Web {
    pub fn new(config: WebConfig) -> Self {
        // Get the first certificate's fingerprint.
        // TODO serve all of them so we can support multiple signature algorithms.
        let fingerprint = config
            .tls
            .fingerprints
            .first()
            .expect("missing certificate")
            .clone();

        let mut tls_config = config.tls.server.expect("missing server configuration");
        tls_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        let tls = TlsAcceptor::from(Arc::new(tls_config));

        Self {
            bind: config.bind,
            tls,
            fingerprint,
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let app = Router::new()
            .route("/fingerprint", get(serve_fingerprint))
            .layer(
                CorsLayer::new()
                    .allow_origin(Any)
                    .allow_methods([Method::GET]),
            )
            .with_state(self.fingerprint);

        let listener = TcpListener::bind(self.bind).await?;
        log::info!("Dev web server listening on https://{}/fingerprint", self.bind);

        loop {
            let (stream, addr) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    log::warn!("Failed to accept TCP connection: {:?}", e);
                    continue;
                }
            };

            let tls = self.tls.clone();
            let app = app.clone();

            tokio::spawn(async move {
                // Perform TLS handshake
                let tls_stream = match tls.accept(stream).await {
                    Ok(s) => s,
                    Err(e) => {
                        log::debug!("TLS handshake failed from {}: {:?}", addr, e);
                        return;
                    }
                };

                // Serve HTTP over TLS
                let io = hyper_util::rt::TokioIo::new(tls_stream);
                let service = hyper::service::service_fn(move |req| {
                    let mut app = app.clone();
                    async move {
                        app.call(req).await
                    }
                });

                if let Err(e) = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, service)
                    .await
                {
                    log::debug!("HTTP connection error from {}: {:?}", addr, e);
                }
            });
        }
    }
}

async fn serve_fingerprint(State(fingerprint): State<String>) -> impl IntoResponse {
    fingerprint
}
