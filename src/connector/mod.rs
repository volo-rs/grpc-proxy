pub mod circuit_breaker;
mod one_to_one_connector;

pub use circuit_breaker::{
    CircuitBreakableConnector, CircuitBreakableConnectorBuilder, CircuitBreakee, CircuitBreaker,
};
use motore::BoxError;
pub use one_to_one_connector::OneToOneConnector;

use crate::{BoxFuture, ConnExtra};

pub trait GrpcConnector: Send + Sync + 'static {
    type Conn: Send + 'static;
    fn connection(&mut self) -> BoxFuture<Self::Conn, BoxError>;
    fn reset(&mut self) -> BoxFuture<(), BoxError>;
    fn connection_with_extra(&mut self) -> BoxFuture<(Self::Conn, ConnExtra), BoxError> {
        let f = self.connection();
        Box::pin(async move {
            let extra = ConnExtra::default();
            Ok((f.await?, extra))
        })
    }
}
