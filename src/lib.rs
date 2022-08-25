pub mod connector;

use std::{net::SocketAddr, time::Duration};

pub use connector::*;
use http::{uri::Authority, Request, Response, Uri};
use metainfo::TypeMap;

pub type BoxFuture<T, E> = futures::future::BoxFuture<'static, Result<T, E>>;
pub type ConnExtra = TypeMap;

#[derive(Clone)]
pub struct GrpcSendRequest {
    auth: Authority,
    // http2 only should be set when initializing the connection.
    pub client: hyper::Client<hyper::client::HttpConnector, hyper::Body>,
}

impl GrpcSendRequest {
    pub fn new(addr: SocketAddr) -> Self {
        let five_mins = Duration::from_secs(60 * 5);
        let mut http = hyper::client::connect::HttpConnector::new();
        http.enforce_http(false);
        http.set_nodelay(true);
        http.set_keepalive(Some(five_mins));
        let client = hyper::Client::builder()
            .http2_keep_alive_interval(five_mins)
            .http2_only(true)
            .http2_keep_alive_while_idle(true)
            .pool_idle_timeout(Duration::from_secs(30))
            .build(http);
        GrpcSendRequest {
            auth: addr.to_string().parse().unwrap(),
            client,
        }
    }

    pub async fn send_request(
        &self,
        mut req: Request<hyper::Body>,
    ) -> hyper::Result<Response<hyper::Body>> {
        // we need to do a hack here to tell hyper to use the authority of our addr
        let mut parts = req.uri_mut().clone().into_parts();
        parts.authority = Some(self.auth.clone());
        let uri = Uri::from_parts(parts).unwrap();
        *req.uri_mut() = uri;
        self.client.request(req).await
    }
}
