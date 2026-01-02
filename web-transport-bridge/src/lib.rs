//! WebTransport bridge with WebSocket support.
//!
//! This crate provides a unified Session type that supports both:
//! - QUIC-based WebTransport (Chrome, Edge, Firefox)
//! - WebSocket-based transport (Safari)
//!
//! It's designed as a drop-in replacement for `web-transport` v0.3.

use bytes::{Buf, BufMut, Bytes};

// Import traits to enable method calls on web_transport_ws types
use web_transport_trait::RecvStream as RecvStreamTrait;
use web_transport_trait::SendStream as SendStreamTrait;
use web_transport_trait::Session as SessionTrait;

/// A WebTransport Session that can be backed by either QUIC or WebSocket.
///
/// This enum provides a unified interface for both transport types,
/// allowing the same MoQ code to work with Chrome (WebTransport) and Safari (WebSocket).
#[derive(Clone)]
pub enum Session {
    /// QUIC-based WebTransport session (Chrome, Edge, Firefox)
    Quinn(web_transport_quinn::Session),
    /// WebSocket-based session (Safari)
    WebSocket(web_transport_ws::Session),
}

impl Session {
    /// Block until the peer creates a new unidirectional stream.
    pub async fn accept_uni(&mut self) -> Result<RecvStream, SessionError> {
        match self {
            Session::Quinn(s) => s.accept_uni().await.map(RecvStream::Quinn).map_err(SessionError::Quinn),
            Session::WebSocket(s) => s
                .accept_uni()
                .await
                .map(RecvStream::WebSocket)
                .map_err(|e| SessionError::WebSocket(format!("{:?}", e))),
        }
    }

    /// Block until the peer creates a new bidirectional stream.
    pub async fn accept_bi(&mut self) -> Result<(SendStream, RecvStream), SessionError> {
        match self {
            Session::Quinn(s) => s
                .accept_bi()
                .await
                .map(|(send, recv)| (SendStream::Quinn(send), RecvStream::Quinn(recv)))
                .map_err(SessionError::Quinn),
            Session::WebSocket(s) => s
                .accept_bi()
                .await
                .map(|(send, recv)| (SendStream::WebSocket(send), RecvStream::WebSocket(recv)))
                .map_err(|e| SessionError::WebSocket(format!("{:?}", e))),
        }
    }

    /// Open a new bidirectional stream.
    pub async fn open_bi(&mut self) -> Result<(SendStream, RecvStream), SessionError> {
        match self {
            Session::Quinn(s) => s
                .open_bi()
                .await
                .map(|(send, recv)| (SendStream::Quinn(send), RecvStream::Quinn(recv)))
                .map_err(SessionError::Quinn),
            Session::WebSocket(s) => s
                .open_bi()
                .await
                .map(|(send, recv)| (SendStream::WebSocket(send), RecvStream::WebSocket(recv)))
                .map_err(|e| SessionError::WebSocket(format!("{:?}", e))),
        }
    }

    /// Open a new unidirectional stream.
    pub async fn open_uni(&mut self) -> Result<SendStream, SessionError> {
        match self {
            Session::Quinn(s) => s.open_uni().await.map(SendStream::Quinn).map_err(SessionError::Quinn),
            Session::WebSocket(s) => s
                .open_uni()
                .await
                .map(SendStream::WebSocket)
                .map_err(|e| SessionError::WebSocket(format!("{:?}", e))),
        }
    }

    /// Send a datagram over the network.
    pub async fn send_datagram(&mut self, payload: Bytes) -> Result<(), SessionError> {
        match self {
            Session::Quinn(s) => s.send_datagram(payload).map_err(SessionError::Quinn),
            Session::WebSocket(_s) => {
                // WebSocket doesn't support datagrams natively
                // For MoQ, datagrams are optional - we can return an error or silently drop
                Err(SessionError::DatagramsNotSupported)
            }
        }
    }

    /// The maximum size of a datagram that can be sent.
    pub async fn max_datagram_size(&self) -> usize {
        match self {
            Session::Quinn(s) => s.max_datagram_size(),
            Session::WebSocket(_) => 0, // Datagrams not supported
        }
    }

    /// Receive a datagram over the network.
    pub async fn recv_datagram(&mut self) -> Result<Bytes, SessionError> {
        match self {
            Session::Quinn(s) => s.read_datagram().await.map_err(SessionError::Quinn),
            Session::WebSocket(_) => Err(SessionError::DatagramsNotSupported),
        }
    }

    /// Close the connection immediately with a code and reason.
    pub fn close(self, code: u32, reason: &str) {
        match self {
            Session::Quinn(s) => s.close(code, reason.as_bytes()),
            Session::WebSocket(s) => s.close(code, reason),
        }
    }

    /// Block until the connection is closed.
    pub async fn closed(&self) -> SessionError {
        match self {
            Session::Quinn(s) => SessionError::Quinn(s.closed().await),
            Session::WebSocket(s) => SessionError::WebSocket(format!("{:?}", s.closed().await)),
        }
    }
}

/// Convert a Quinn session into a unified Session.
impl From<web_transport_quinn::Session> for Session {
    fn from(session: web_transport_quinn::Session) -> Self {
        Session::Quinn(session)
    }
}

/// Convert a WebSocket session into a unified Session.
impl From<web_transport_ws::Session> for Session {
    fn from(session: web_transport_ws::Session) -> Self {
        Session::WebSocket(session)
    }
}

/// An outgoing stream of bytes to the peer.
pub enum SendStream {
    Quinn(web_transport_quinn::SendStream),
    WebSocket(web_transport_ws::SendStream),
}

impl SendStream {
    /// Write some of the buffer to the stream.
    pub async fn write(&mut self, buf: &[u8]) -> Result<usize, WriteError> {
        match self {
            SendStream::Quinn(s) => s.write(buf).await.map_err(WriteError::Quinn),
            SendStream::WebSocket(s) => s
                .write(buf)
                .await
                .map_err(|e| WriteError::WebSocket(format!("{:?}", e))),
        }
    }

    /// Write some of the given buffer to the stream.
    pub async fn write_buf<B: Buf>(&mut self, buf: &mut B) -> Result<usize, WriteError> {
        match self {
            SendStream::Quinn(s) => {
                let size = s.write(buf.chunk()).await.map_err(WriteError::Quinn)?;
                buf.advance(size);
                Ok(size)
            }
            SendStream::WebSocket(s) => {
                let size = s
                    .write(buf.chunk())
                    .await
                    .map_err(|e| WriteError::WebSocket(format!("{:?}", e)))?;
                buf.advance(size);
                Ok(size)
            }
        }
    }

    /// Write the entire chunk of bytes to the stream.
    pub async fn write_chunk(&mut self, buf: Bytes) -> Result<(), WriteError> {
        match self {
            SendStream::Quinn(s) => s.write_chunk(buf).await.map_err(WriteError::Quinn),
            SendStream::WebSocket(s) => s
                .write(&buf)
                .await
                .map(|_| ())
                .map_err(|e| WriteError::WebSocket(format!("{:?}", e))),
        }
    }

    /// Set the stream's priority.
    pub fn set_priority(&mut self, order: i32) {
        match self {
            SendStream::Quinn(s) => { s.set_priority(order).ok(); }
            SendStream::WebSocket(_) => { /* WebSocket doesn't support priorities */ }
        }
    }

    /// Send an immediate reset code, closing the stream.
    pub fn reset(self, code: u32) {
        match self {
            SendStream::Quinn(mut s) => { s.reset(code).ok(); }
            SendStream::WebSocket(mut s) => { s.reset(code); }
        }
    }
}

/// An incoming stream of bytes from the peer.
pub enum RecvStream {
    Quinn(web_transport_quinn::RecvStream),
    WebSocket(web_transport_ws::RecvStream),
}

impl RecvStream {
    /// Read some data into the provided buffer.
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>, ReadError> {
        match self {
            RecvStream::Quinn(s) => s.read(buf).await.map_err(ReadError::Quinn),
            RecvStream::WebSocket(s) => s
                .read(buf)
                .await
                .map_err(|e| ReadError::WebSocket(format!("{:?}", e))),
        }
    }

    /// Read some data into the provided buffer.
    pub async fn read_buf<B: BufMut>(&mut self, buf: &mut B) -> Result<bool, ReadError> {
        match self {
            RecvStream::Quinn(s) => {
                let dst = buf.chunk_mut();
                let dst = unsafe { &mut *(dst as *mut _ as *mut [u8]) };
                let size = match s.read(dst).await.map_err(ReadError::Quinn)? {
                    Some(size) => size,
                    None => return Ok(false),
                };
                unsafe { buf.advance_mut(size) };
                Ok(true)
            }
            RecvStream::WebSocket(s) => {
                let dst = buf.chunk_mut();
                let dst = unsafe { &mut *(dst as *mut _ as *mut [u8]) };
                let size = match s
                    .read(dst)
                    .await
                    .map_err(|e| ReadError::WebSocket(format!("{:?}", e)))?
                {
                    Some(size) => size,
                    None => return Ok(false),
                };
                unsafe { buf.advance_mut(size) };
                Ok(true)
            }
        }
    }

    /// Read the next chunk of data with the provided maximum size.
    pub async fn read_chunk(&mut self, max: usize) -> Result<Option<Bytes>, ReadError> {
        match self {
            RecvStream::Quinn(s) => s
                .read_chunk(max, true)
                .await
                .map(|opt| opt.map(|chunk| chunk.bytes))
                .map_err(ReadError::Quinn),
            RecvStream::WebSocket(s) => {
                // WebSocket streams don't have chunk-based reading, use regular read
                let mut buf = vec![0u8; max];
                match s
                    .read(&mut buf)
                    .await
                    .map_err(|e| ReadError::WebSocket(format!("{:?}", e)))?
                {
                    Some(size) => {
                        buf.truncate(size);
                        Ok(Some(Bytes::from(buf)))
                    }
                    None => Ok(None),
                }
            }
        }
    }

    /// Send a `STOP_SENDING` code.
    pub fn stop(self, code: u32) {
        match self {
            RecvStream::Quinn(mut s) => { s.stop(code).ok(); }
            RecvStream::WebSocket(mut s) => { s.stop(code); }
        }
    }
}

/// Session errors
#[derive(Clone, Debug, thiserror::Error)]
pub enum SessionError {
    #[error("quinn session error: {0}")]
    Quinn(#[from] web_transport_quinn::SessionError),

    #[error("websocket session error: {0}")]
    WebSocket(String),

    #[error("datagrams not supported over WebSocket")]
    DatagramsNotSupported,
}

/// Write errors
#[derive(Clone, Debug, thiserror::Error)]
pub enum WriteError {
    #[error("quinn write error: {0}")]
    Quinn(#[from] web_transport_quinn::WriteError),

    #[error("websocket write error: {0}")]
    WebSocket(String),
}

/// Read errors
#[derive(Clone, Debug, thiserror::Error)]
pub enum ReadError {
    #[error("quinn read error: {0}")]
    Quinn(#[from] web_transport_quinn::ReadError),

    #[error("websocket read error: {0}")]
    WebSocket(String),
}
