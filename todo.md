# Safari WebSocket Bridge - Project Status

## Goal
Enable Safari browser support for vivoh.earth streaming by creating a WebSocket-to-QUIC bridge running in Cloudflare Containers.

### Architecture
```
Chrome/Firefox → Direct WebTransport → relay.cloudflare.mediaoverquic.com
Safari Publish  → WebSocket → Container → QUIC → Cloudflare Relay
Safari Watch    → WebSocket ← Container ← QUIC ← Cloudflare Relay
```

The container acts as a WebSocket-to-QUIC bridge for Safari browsers that don't support WebTransport.

---

## Current Status: Container Crashes on Startup

The container builds successfully but crashes immediately after starting, before it can bind to port 4444.

### What Works
- Dockerfile builds successfully with moq-relay-ietf
- Container image pushes to Cloudflare registry
- Worker/Durable Object correctly routes `/moq` requests to container
- TLS skip fix is implemented and pushed to `safari-websocket-bridge` branch

### What's Broken
- Container starts but never listens on port 4444
- Worker logs show: "The container is not listening in the TCP address 10.0.0.1:4444"
- No container logs visible to diagnose the crash

---

## Key Files

### Cloudflare Container
- **Dockerfile**: `/Users/erikherz/Desktop/git/vivoh.earth/container/Dockerfile`
  - Builds from `rust:1.83-bookworm`
  - Clones `erikherz/cf-moq-07` branch `safari-websocket-bridge`
  - Runs: `moq-relay --ws-only --ws-bind 0.0.0.0:4444 --ws-no-tls`

### MoQ Relay (this repo)
- **Main entry**: `moq-relay-ietf/src/main.rs`
  - Contains `NoVerifier` struct to skip TLS certificate loading
  - `--ws-only` mode skips QUIC server, only runs WebSocket
  - `--ws-no-tls` disables TLS on WebSocket server (Cloudflare handles TLS at edge)

- **WebSocket server**: `moq-relay-ietf/src/websocket.rs`
  - Uses `web_transport_ws::Session::accept()` for WebSocket connections
  - Wraps connections in MoQ transport layer

### Worker (vivoh.earth repo)
- **Worker**: `/Users/erikherz/Desktop/git/vivoh.earth/src/worker/index.ts`
  - `MoqRelay` Durable Object handles container lifecycle
  - Routes WebSocket requests with `webtransport` protocol to container
  - Uses `ctx.container.start()` and `port.fetch()` to communicate

---

## Fixes Applied

### 1. TLS Loading Fix (Commit: 24dd070)
**Problem**: `rustls_native_certs::load_native_certs()` fails in Cloudflare Container environment.

**Solution**: Skip TLS loading entirely when running in `--ws-only --ws-no-tls` mode without upstream:
```rust
let tls = if cli.ws_no_tls && cli.upstream.is_none() {
    moq_native_ietf::tls::Config {
        client: rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(std::sync::Arc::new(NoVerifier))
            .with_no_client_auth(),
        server: None,
        fingerprints: vec![],
    }
} else {
    cli.tls.load()?
};
```

### 2. SessionError Conversion Fix (Commit: 94afb39)
Fixed error type conversion for upstream session errors.

### 3. WebSocket Server Startup Order (Commit: e43488b)
Start WebSocket server before attempting upstream connection.

---

## Debugging Attempts

1. **Added QUIC test with moq-pub** - Didn't help, container still crashes
2. **Simplified startup script** - Script syntax was broken, fixed with printf
3. **Removed startup script entirely** - Still crashes
4. **Verified GitHub code** - TLS skip fix is present on safari-websocket-bridge branch

---

## Next Steps to Try

1. **Run container locally with debug logging**
   ```bash
   docker run --rm -e RUST_LOG=debug vivoh-earth-moqrelay:2857e6f4 \
     --ws-only --ws-bind 0.0.0.0:4444 --ws-no-tls
   ```
   This will show what error causes the crash.

2. **Check if issue is in WebSocket server initialization**
   - The `web_transport_ws::Session::accept()` expects raw TCP streams
   - Cloudflare Containers use HTTP request forwarding via `port.fetch()`
   - This architectural mismatch may be the root cause

3. **Consider alternative: HTTP-based WebSocket server**
   - Use `axum` or `hyper` to handle HTTP requests
   - Manually upgrade to WebSocket
   - Then pass to `web_transport_ws`

4. **Test outbound QUIC connectivity**
   - Once container runs, verify it can connect to upstream relay
   - Use moq-pub to test: `moq-pub https://relay.cloudflare.mediaoverquic.com test`

---

## Repository Branches

- **cf-moq-07 (this repo)**
  - `main` - Original code
  - `safari-websocket-bridge` - Contains all WebSocket bridge fixes

- **vivoh.earth**
  - `main` - Contains worker code and Dockerfile

---

## Environment

- Cloudflare Containers (container-backed Durable Objects)
- Rust 1.83
- moq-relay-ietf v0.7.4
- web-transport-ws from moq-dev/web-transport branch main
