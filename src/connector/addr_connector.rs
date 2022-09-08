use std::{fmt, net::SocketAddr};

use volo_grpc::Status as GrpcStatus;

use crate::{connector::GrpcConnector, BoxFuture, CircuitBreakee, GrpcSendRequest};

#[derive(Clone)]
pub struct AddrConnector {
    inner: GrpcSendRequest,
    addr: SocketAddr,
}

#[derive(Default)]
struct Inner {
    _sender: Option<GrpcSendRequest>,
}

impl CircuitBreakee for AddrConnector {
    type Key = SocketAddr;

    fn key(&self) -> Self::Key {
        self.addr
    }
}

impl fmt::Debug for AddrConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AddrConnector")
            .field("addr", &self.addr)
            .finish()
    }
}

impl AddrConnector {
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn new(addr: SocketAddr) -> Self {
        AddrConnector {
            inner: GrpcSendRequest::new(addr),
            addr,
        }
    }
}

impl GrpcConnector for AddrConnector {
    type Conn = GrpcSendRequest;

    fn connection(&mut self) -> BoxFuture<Self::Conn, GrpcStatus> {
        let sender = self.inner.clone();
        Box::pin(async move { Ok(sender) })
    }

    fn reset(&mut self) -> BoxFuture<(), GrpcStatus> {
        Box::pin(async { Ok(()) })
    }
}
