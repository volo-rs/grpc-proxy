use std::net::SocketAddr;

use volo_grpc::status::Status as GrpcStatus;

use crate::{connector::GrpcConnector, BoxFuture, CircuitBreakee, GrpcSendRequest};

#[derive(Clone)]
pub struct OneToOneConnector {
    addr: SocketAddr,
}

impl CircuitBreakee for OneToOneConnector {
    type Key = SocketAddr;

    fn key(&self) -> Self::Key {
        self.addr
    }
}

impl OneToOneConnector {
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn new(addr: SocketAddr) -> Self {
        OneToOneConnector { addr }
    }
}

impl GrpcConnector for OneToOneConnector {
    type Conn = GrpcSendRequest;

    fn connection(&mut self) -> BoxFuture<Self::Conn, GrpcStatus> {
        let addr = self.addr();
        Box::pin(async move {
            let new_sender = GrpcSendRequest::new(addr);
            Ok(new_sender)
        })
    }

    fn reset(&mut self) -> BoxFuture<(), GrpcStatus> {
        Box::pin(async move { Ok(()) })
    }
}
