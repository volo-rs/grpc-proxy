pub mod addr_connector;
pub mod circuit_breaker;
mod one_to_one_connector;

pub use circuit_breaker::{
    CircuitBreakableConnector, CircuitBreakableConnectorBuilder, CircuitBreakee, CircuitBreaker,
};
pub use one_to_one_connector::OneToOneConnector;
use volo_grpc::Status as GrpcStatus;

use crate::{BoxFuture, ConnExtra};

pub trait GrpcConnector: Send + Sync + 'static {
    type Conn: Send + 'static;
    fn connection(&mut self) -> BoxFuture<Self::Conn, GrpcStatus>;
    fn reset(&mut self) -> BoxFuture<(), GrpcStatus>;
    fn connection_with_extra(&mut self) -> BoxFuture<(Self::Conn, ConnExtra), GrpcStatus> {
        let f = self.connection();
        Box::pin(async move {
            let extra = ConnExtra::default();
            Ok((f.await?, extra))
        })
    }
}
