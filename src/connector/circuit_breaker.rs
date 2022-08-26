use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    convert::Infallible,
    fmt,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::{atomic::AtomicBool, Arc},
    time::{Duration, Instant},
};

use http::Response;
use hyper::Body;
use index_list::{Index, IndexList};
use motore::BoxError;
use tokio::sync::Mutex;

use crate::{connector::GrpcConnector, BoxFuture, ConnExtra};

const DEFAULT_WINDOW_SIZE: usize = 1000;
const DEFAULT_ERROR_RATE_THRESHOLD: f32 = 0.6;
const DEFAULT_HALF_OPEN_THRESHOLD: usize = 100;
const DEFAULT_COOLDOWN_DURATION_SEC: u64 = 100;

pub type InspectedFuture =
    Pin<Box<dyn Future<Output = Result<Response<Body>, BoxError>> + Send + 'static>>;
pub type DynCondition<C> =
    Arc<dyn Fn(Handler<C>, InspectedFuture) -> InspectedFuture + Send + Sync + 'static>;

pub trait CircuitBreakee: GrpcConnector {
    type Key: Clone + PartialOrd + Ord + Send + Sync + std::fmt::Debug + 'static;

    fn key(&self) -> Self::Key;
}

pub trait CircuitBreaker: GrpcConnector {
    type Breakee: CircuitBreakee;

    fn submit_candidates(
        &self,
        candidates: impl IntoIterator<Item = (<Self::Breakee as CircuitBreakee>::Key, Self::Breakee)>
            + Send
            + 'static,
    ) -> BoxFuture<(), Infallible>;
}

pub struct CircuitBreakableConnector<S: CircuitBreakee> {
    inner: Arc<Mutex<CircuitBreakerInner<S>>>,
}

impl<S: CircuitBreakee> Clone for CircuitBreakableConnector<S> {
    fn clone(&self) -> Self {
        CircuitBreakableConnector {
            inner: self.inner.clone(),
        }
    }
}

impl<S: CircuitBreakee> CircuitBreaker for CircuitBreakableConnector<S> {
    type Breakee = S;

    fn submit_candidates(
        &self,
        candidates: impl IntoIterator<Item = (S::Key, S)> + Send + 'static,
    ) -> BoxFuture<(), Infallible> {
        let f = self.update_candidates(candidates);
        Box::pin(async move {
            f.await;
            Ok(())
        })
    }
}

impl<S> GrpcConnector for CircuitBreakableConnector<S>
where
    S: CircuitBreakee + GrpcConnector + Send + Sync + 'static,
    S::Key: Send + Sync + std::fmt::Debug + 'static,
{
    type Conn = S::Conn;

    fn connection(&mut self) -> BoxFuture<Self::Conn, BoxError> {
        let f = self.create_connection();
        Box::pin(async move {
            let (r, _) = f.await?;
            Ok(r)
        })
    }

    fn connection_with_extra(&mut self) -> BoxFuture<(Self::Conn, ConnExtra), BoxError> {
        let mut extra = ConnExtra::default();
        let f = self.create_connection();
        Box::pin(async move {
            let (r, h) = f.await?;
            extra.insert(h.key.clone());
            extra.insert(h);
            Ok((r, extra))
        })
    }

    fn reset(&mut self) -> BoxFuture<(), BoxError> {
        tracing::trace!("[VOLO] reset connector");
        let inner = self.inner.clone();
        Box::pin(async move {
            inner.lock().await.reset().await?;
            Ok(())
        })
    }
}

struct Config {
    window_size: usize,
    error_rate_threshold: f32,
    half_open_threshold: usize,
    cooldown_duration: Duration,
}

impl Config {
    fn made_intuitive(mut self) -> Self {
        self.window_size = std::cmp::max(self.window_size, self.half_open_threshold);
        self
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            window_size: DEFAULT_WINDOW_SIZE,
            error_rate_threshold: DEFAULT_ERROR_RATE_THRESHOLD,
            half_open_threshold: DEFAULT_HALF_OPEN_THRESHOLD,
            cooldown_duration: Duration::from_secs(DEFAULT_COOLDOWN_DURATION_SEC),
        }
    }
}

pub struct CircuitBreakableConnectorBuilder<S> {
    config: Config,
    phantom: PhantomData<S>,
}

impl<S> CircuitBreakableConnectorBuilder<S> {
    pub fn window_size(mut self, window_size: usize) -> Self {
        self.config.window_size = window_size;
        self
    }

    pub fn error_rate_threshold(mut self, error_rate_threshold: f32) -> Self {
        self.config.error_rate_threshold = error_rate_threshold;
        self
    }

    pub fn half_open_threshold(mut self, half_open_threshold: usize) -> Self {
        self.config.half_open_threshold = half_open_threshold;
        self
    }

    pub fn cooldown_duration(mut self, cooldown_duration: Duration) -> Self {
        self.config.cooldown_duration = cooldown_duration;
        self
    }

    pub fn build(self, services: impl IntoIterator<Item = S>) -> CircuitBreakableConnector<S>
    where
        S: CircuitBreakee,
    {
        CircuitBreakableConnector::new_with_config(services, self.config.made_intuitive())
    }
}

impl<S> CircuitBreakableConnector<S>
where
    S: CircuitBreakee + GrpcConnector + Send + 'static,
    S::Key: Clone + Sync + Send + 'static,
{
    pub fn create_connection(&self) -> BoxFuture<(S::Conn, Handler<S>), BoxError> {
        let inner = self.inner.clone();
        Box::pin(async move {
            let mut m = inner.lock().await;
            match m.create_connection().await {
                Ok((resp, key)) => {
                    drop(m);
                    Ok((resp, Handler::new(inner, key)))
                }
                Err(err) => {
                    let key = m.current_key();
                    drop(m);
                    if let Some(key) = key {
                        let h = Handler::new(inner, key);
                        h.error().await;
                    }
                    Err(err)
                }
            }
        })
    }

    pub fn update_candidates(
        &self,
        candidates: impl IntoIterator<Item = (S::Key, S)> + Send + 'static,
    ) -> impl Future<Output = ()> + 'static {
        let inner = self.inner.clone();
        async move {
            inner.lock().await.update_candidates(candidates);
        }
    }
}

impl<S: CircuitBreakee> CircuitBreakableConnector<S> {
    pub fn builder() -> CircuitBreakableConnectorBuilder<S> {
        CircuitBreakableConnectorBuilder {
            config: Config::default(),
            phantom: PhantomData,
        }
    }

    pub fn new(services: impl IntoIterator<Item = S>) -> CircuitBreakableConnector<S>
    where
        S: CircuitBreakee + GrpcConnector,
    {
        CircuitBreakableConnector::new_with_config(services, Config::default())
    }

    fn new_with_config(services: impl IntoIterator<Item = S>, config: Config) -> Self {
        let mut closed_list = IndexList::new();
        let mut map = BTreeMap::new();
        for service in services {
            let key = service.key();
            let idx = closed_list.insert_first(service);
            map.insert(
                key,
                InstanceState {
                    cursor: Cursor::Closed(idx),
                    dashboard: Dashboard::new(config.window_size),
                },
            );
        }
        let current = closed_list.first_index();
        let inner = CircuitBreakerInner {
            closed_list,
            open_list: IndexList::new(),
            half_open_list: IndexList::new(),
            map,
            cooldown_stopwatch: BTreeMap::new(),
            current: Cursor::Closed(current),
            config,
        };
        CircuitBreakableConnector {
            inner: Arc::new(Mutex::new(inner)),
        }
    }
}

pub struct Handler<S: CircuitBreakee> {
    inner: Arc<Mutex<CircuitBreakerInner<S>>>,
    key: S::Key,
    disposed: Arc<AtomicBool>,
}

impl<S: CircuitBreakee> Handler<S> {
    fn new(inner: Arc<Mutex<CircuitBreakerInner<S>>>, key: S::Key) -> Self {
        Handler {
            inner,
            key,
            disposed: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl<S: CircuitBreakee> Clone for Handler<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            key: self.key.clone(),
            disposed: self.disposed.clone(),
        }
    }
}

impl<S: CircuitBreakee + GrpcConnector> Handler<S> {
    pub async fn success(&self) {
        self.notify_result(true).await;
    }

    pub async fn error(&self) {
        self.notify_result(false).await;
    }

    pub async fn notify_result(&self, success: bool) {
        if !self
            .disposed
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            self.inner.lock().await.check(&self.key, success);
        }
    }

    pub fn key(&self) -> S::Key {
        self.key.clone()
    }
}

struct CircuitBreakerInner<S: CircuitBreakee> {
    closed_list: IndexList<S>,
    open_list: IndexList<S>,
    half_open_list: IndexList<S>,
    map: BTreeMap<S::Key, InstanceState>,
    cooldown_stopwatch: BTreeMap<Instant, S::Key>,
    current: Cursor,
    config: Config,
}

struct InstanceState {
    cursor: Cursor,
    dashboard: Dashboard,
}

impl<S: GrpcConnector + CircuitBreakee> CircuitBreakerInner<S> {
    async fn reset(&mut self) -> Result<(), BoxError> {
        self.cooldown_stopwatch.clear();
        self.map.clear();
        self.closed_list.append(&mut self.half_open_list);
        self.closed_list.append(&mut self.open_list);
        let mut new_list = IndexList::new();
        for mut s in self.closed_list.drain_iter() {
            s.reset().await?;
            let key = s.key();
            let idx = new_list.insert_last(s);
            self.map.insert(
                key,
                InstanceState {
                    cursor: Cursor::Closed(idx),
                    dashboard: Dashboard::new(self.config.window_size),
                },
            );
        }
        let _ = std::mem::replace(&mut self.closed_list, new_list);
        self.current = Cursor::Closed(self.closed_list.first_index());
        Ok(())
    }
}

impl<S: CircuitBreakee> CircuitBreakerInner<S> {
    fn current_key(&self) -> Option<S::Key> {
        Some(
            match self.current {
                Cursor::Closed(idx) => self.closed_list.get(idx),
                Cursor::Open(idx) => self.open_list.get(idx),
                Cursor::HalfOpen(idx) => self.half_open_list.get(idx),
            }?
            .key(),
        )
    }

    fn next_cursor(&self, cursor: Cursor) -> Cursor {
        match cursor {
            Cursor::Closed(idx) => {
                let next = self.closed_list.next_index(idx);
                if next.is_some() {
                    return Cursor::Closed(next);
                }
                let next = self.half_open_list.first_index();
                if next.is_some() {
                    return Cursor::HalfOpen(next);
                }
                // must exists at this point
                Cursor::Closed(self.closed_list.first_index())
            }
            Cursor::HalfOpen(idx) => {
                let next = self.half_open_list.next_index(idx);
                if next.is_some() {
                    return Cursor::HalfOpen(next);
                }
                let next = self.closed_list.first_index();
                if next.is_some() {
                    return Cursor::Closed(next);
                }
                Cursor::HalfOpen(self.half_open_list.first_index())
            }
            Cursor::Open(_) => unreachable!(),
        }
    }
}

impl<S> CircuitBreakerInner<S>
where
    S: CircuitBreakee + GrpcConnector,
{
    async fn create_connection(&mut self) -> Result<(S::Conn, S::Key), BoxError> {
        self.check_cooled().await;
        match self.current {
            Cursor::Closed(idx) => {
                let service = self.closed_list.get_mut(idx).ok_or_else(|| {
                    "CircuitBreaker: closed list is empty, but current cursor is closed".to_string()
                })?;
                let key = service.key();
                let conn = service.connection().await?;
                self.current = self.next_cursor(self.current);
                Ok((conn, key))
            }
            Cursor::HalfOpen(idx) => {
                let service = self.half_open_list.get_mut(idx).ok_or_else(|| {
                    "CircuitBreaker: half open list is empty, but current cursor is half open"
                        .to_string()
                })?;
                let key = service.key();
                let conn = service.connection().await?;
                self.current = self.next_cursor(self.current);
                Ok((conn, key))
            }
            Cursor::Open(_) => unreachable!(),
        }
    }
}

impl<S> CircuitBreakerInner<S>
where
    S: CircuitBreakee + GrpcConnector,
{
    fn update_candidates(&mut self, candidates: impl IntoIterator<Item = (S::Key, S)>) {
        let mut key_set = BTreeSet::new();
        for (key, candidate) in candidates {
            // TODO: key may be duplicated, reuseport may be used
            key_set.insert(key.clone());
            if self.map.contains_key(&key) {
                continue;
            }
            let idx = self.closed_list.insert_last(candidate);
            let state = InstanceState {
                cursor: Cursor::Closed(idx),
                dashboard: Dashboard::new(self.config.window_size),
            };
            self.map.insert(key, state);
        }
        let all_keys: BTreeSet<_> = self.map.keys().cloned().collect();
        let to_remove: Vec<_> = all_keys.difference(&key_set).collect();
        if !to_remove.is_empty() {
            tracing::trace!(to_remove=?to_remove, "[VOLO] remove connectors");
            for key in to_remove {
                if let Some(state) = self.map.remove(key) {
                    self.remove(state.cursor);
                }
            }
        }
        self.check_and_correct_current_cursor();
    }

    fn remove(&mut self, cursor: Cursor) {
        match cursor {
            Cursor::Closed(idx) => {
                if self.closed_list.get(idx).is_some() {
                    self.closed_list.remove(idx);
                }
            }
            Cursor::HalfOpen(idx) => {
                if self.half_open_list.get(idx).is_some() {
                    self.half_open_list.remove(idx);
                }
            }
            Cursor::Open(idx) => {
                if self.open_list.get(idx).is_some() {
                    self.open_list.remove(idx);
                }
            }
        };
    }

    fn no_current(&self) -> bool {
        match self.current {
            Cursor::Closed(idx) => self.closed_list.get(idx).is_none(),
            Cursor::HalfOpen(idx) => self.half_open_list.get(idx).is_none(),
            Cursor::Open(_) => true,
        }
    }

    fn check(&mut self, key: &S::Key, success: bool) {
        let state = self.map.get_mut(key);
        // the key has been removed, ignore it
        if state.is_none() {
            tracing::trace!(
                "[VOLO] circuitbreaker key {:?} has been removed, ignore it",
                key
            );
            return;
        }
        let state = state.unwrap();
        if success {
            state.dashboard.success();
        } else {
            state.dashboard.error();
        }

        self.drive_transit(key);
    }

    fn drive_transit(&mut self, key: &S::Key) {
        let next = self.next_cursor(self.current);
        let state = self.map.get_mut(key).expect("key absent");
        match state.cursor {
            Cursor::Closed(idx) => {
                if state.dashboard.error_rate() >= self.config.error_rate_threshold {
                    tracing::trace!(key=?key, "[VOLO] move from closed list to open list");
                    if state.cursor == self.current {
                        self.current = next;
                    }
                    let s = self.closed_list.remove(idx).expect("must exists");
                    let new_idx = self.open_list.insert_last(s);
                    self.cooldown_stopwatch
                        .insert(Instant::now() + self.config.cooldown_duration, key.clone());
                    state.cursor = Cursor::Open(new_idx);
                }
            }
            Cursor::HalfOpen(idx) => {
                if state.dashboard.count() >= self.config.half_open_threshold {
                    if state.cursor == self.current {
                        self.current = next;
                    }
                    if state.dashboard.error_rate() >= self.config.error_rate_threshold {
                        let s = self.half_open_list.remove(idx).expect("must exists");
                        let new_idx = self.open_list.insert_last(s);
                        tracing::trace!(key=?key, "[VOLO] move from halfopen list to open list");
                        self.cooldown_stopwatch
                            .insert(Instant::now() + self.config.cooldown_duration, key.clone());
                        state.cursor = Cursor::Open(new_idx);
                    } else {
                        let s = self.half_open_list.remove(idx).expect("must exists");
                        tracing::trace!(key=?key, "[VOLO] move from halfopen list to closed list");
                        let new_idx = self.closed_list.insert_last(s);
                        state.cursor = Cursor::Closed(new_idx);
                        state.dashboard = Dashboard::new(self.config.window_size);
                    }
                }
            }
            Cursor::Open(_) => {}
        }

        self.check_and_correct_current_cursor();
    }

    fn check_and_correct_current_cursor(&mut self) {
        if self.no_current() {
            if !self.closed_list.is_empty() {
                self.current = Cursor::Closed(self.closed_list.first_index());
            } else if !self.half_open_list.is_empty() {
                self.current = Cursor::HalfOpen(self.half_open_list.first_index());
            }
        }
    }

    async fn check_cooled(&mut self) {
        let mut rest = self.cooldown_stopwatch.split_off(&Instant::now());
        let now = Instant::now();
        rest.keys().all(|v| v >= &now);
        std::mem::swap(&mut rest, &mut self.cooldown_stopwatch);
        for key in rest.into_values() {
            let state = match self.map.get_mut(&key) {
                Some(state) => state,
                None => continue,
            };
            let idx = match state.cursor {
                Cursor::Open(idx) => idx,
                _ => unreachable!(),
            };
            let mut s = self.open_list.remove(idx).expect("must exists");
            if let Err(err) = s.reset().await {
                tracing::error!(error=?err, "[VOLO] fail to reset circuit breaker inner connector");
            }
            let new_idx = self.half_open_list.insert_last(s);
            tracing::trace!(key=?key, "[VOLO] move from open list to halfopen list");
            state.cursor = Cursor::HalfOpen(new_idx);
            state.dashboard = Dashboard::new(self.config.window_size);
        }
        self.check_and_correct_current_cursor();
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
enum Cursor {
    Closed(Index),
    HalfOpen(Index),
    Open(Index),
}

struct Dashboard {
    success: VecDeque<bool>,
    error_count: usize,
    success_count: usize,
    window: usize,
}

impl fmt::Debug for Dashboard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Dashboard")
            .field("error_count", &self.error_count)
            .field("success_count", &self.success_count)
            .field("window", &self.window)
            .finish()
    }
}

impl Dashboard {
    fn new(window: usize) -> Dashboard {
        Dashboard {
            success: VecDeque::with_capacity(window),
            error_count: 0,
            success_count: 0,
            window,
        }
    }

    fn success(&mut self) {
        self.success_count += 1;
        self.maintain_window();
        self.success.push_back(true);
    }

    fn error(&mut self) {
        self.error_count += 1;
        self.maintain_window();
        self.success.push_back(false);
    }

    fn maintain_window(&mut self) {
        while self.success.len() + 1 > self.window {
            if let Some(is_success) = self.success.pop_front() {
                if is_success {
                    self.success_count -= 1;
                } else {
                    self.error_count -= 1;
                }
            }
        }
    }

    fn count(&self) -> usize {
        self.success.len()
    }

    fn error_rate(&self) -> f32 {
        self.error_count as f32 / self.success.len() as f32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct GenSeqNum {
        id: i32,
        num: usize,
    }

    impl GenSeqNum {
        fn new(id: i32) -> Self {
            GenSeqNum { id, num: 1 }
        }
    }

    impl CircuitBreakee for GenSeqNum {
        type Key = i32;

        fn key(&self) -> Self::Key {
            self.id
        }
    }

    impl GrpcConnector for GenSeqNum {
        type Conn = usize;

        fn connection(&mut self) -> BoxFuture<Self::Conn, BoxError> {
            let n = self.num;
            Box::pin(async move { Ok(n) })
        }

        fn reset(&mut self) -> BoxFuture<(), BoxError> {
            Box::pin(async move { Ok(()) })
        }
    }

    #[tokio::test]
    async fn test_circuit_breaker_all_success() {
        let breaker: CircuitBreakableConnector<GenSeqNum> =
            CircuitBreakableConnector::new((1..10).map(GenSeqNum::new));
        for _ in 0..200 {
            let (_, h) = breaker.create_connection().await.unwrap();
            h.success().await;
        }
        let b = breaker.inner.lock().await;
        assert_eq!(b.closed_list.len(), 9);
        assert!(b.open_list.is_empty());
        assert!(b.half_open_list.is_empty());
        assert!(b.cooldown_stopwatch.is_empty());
        for state in b.map.values() {
            assert_eq!(state.dashboard.error_count, 0);
        }
    }

    #[tokio::test]
    async fn test_circuit_breaker_all_error() {
        let breaker: CircuitBreakableConnector<GenSeqNum> = CircuitBreakableConnector::builder()
            .cooldown_duration(Duration::from_millis(300))
            .window_size(10)
            .half_open_threshold(5)
            .build((1..11).map(GenSeqNum::new));
        for _ in 0..200 {
            if let Ok((_, h)) = breaker.create_connection().await {
                h.error().await;
            }
        }
        assert!(breaker.inner.lock().await.closed_list.is_empty());
        assert_eq!(breaker.inner.lock().await.open_list.len(), 10);
        // wait for cooldown
        tokio::time::sleep(Duration::from_millis(300)).await;

        let (_, h) = breaker.create_connection().await.unwrap();
        h.error().await;
        assert_eq!(breaker.inner.lock().await.half_open_list.len(), 10);
        assert_eq!(breaker.inner.lock().await.open_list.len(), 0);
        for _ in 0..200 {
            if let Ok((_, h)) = breaker.create_connection().await {
                h.error().await;
            }
        }
        assert_eq!(breaker.inner.lock().await.closed_list.len(), 0);
        assert_eq!(breaker.inner.lock().await.half_open_list.len(), 0);
        assert_eq!(breaker.inner.lock().await.open_list.len(), 10);
    }

    #[tokio::test]
    async fn test_circuit_breaker_transit() {
        let breaker: CircuitBreakableConnector<GenSeqNum> = CircuitBreakableConnector::builder()
            .window_size(10)
            .half_open_threshold(5)
            .error_rate_threshold(0.5)
            .cooldown_duration(Duration::from_millis(300))
            .build(Some(GenSeqNum::new(1)));
        for _ in 0..200 {
            let (_, h) = breaker.create_connection().await.unwrap();
            h.success().await;
        }
        {
            let b = breaker.inner.lock().await;
            assert_eq!(b.closed_list.len(), 1);
            assert!(b.open_list.is_empty());
            assert!(b.half_open_list.is_empty());
            assert!(b.cooldown_stopwatch.is_empty());
            for state in b.map.values() {
                assert_eq!(state.dashboard.error_count, 0);
            }
        }
        for _ in 0..5 {
            let (_, h) = breaker.create_connection().await.unwrap();
            h.error().await;
        }
        {
            let b = breaker.inner.lock().await;
            assert_eq!(b.closed_list.len(), 0);
            assert_eq!(b.open_list.len(), 1);
            assert!(b.half_open_list.is_empty());
            for state in b.map.values() {
                assert_eq!(state.dashboard.error_count, 5);
                assert_eq!(state.dashboard.success_count, 5);
            }
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        let (_, h) = breaker.create_connection().await.unwrap();
        h.success().await;
        {
            let b = breaker.inner.lock().await;
            assert_eq!(b.closed_list.len(), 0);
            assert_eq!(b.open_list.len(), 0);
            assert_eq!(b.half_open_list.len(), 1);
            for state in b.map.values() {
                assert_eq!(state.dashboard.error_count, 0);
                assert_eq!(state.dashboard.success_count, 1);
            }
            println!("current={:?}", b.current);
        }
        for _ in 0..2 {
            let (_, h) = breaker.create_connection().await.unwrap();
            h.success().await;
        }
        for _ in 0..2 {
            let (_, h) = breaker.create_connection().await.unwrap();
            h.error().await;
        }
        let (_, h) = breaker.create_connection().await.unwrap();
        h.success().await;
        {
            let b = breaker.inner.lock().await;
            assert_eq!(b.closed_list.len(), 1);
            assert_eq!(b.open_list.len(), 0);
            assert_eq!(b.half_open_list.len(), 0);
        }
    }

    #[tokio::test]
    async fn test_circuit_breaker_handler() {
        let mut breaker: CircuitBreakableConnector<GenSeqNum> =
            CircuitBreakableConnector::new(Some(GenSeqNum::new(1)));
        let (conn, extra) = breaker.connection_with_extra().await.unwrap();
        let h = extra.get::<Handler<GenSeqNum>>();
        assert_eq!(conn, 1);
        assert!(h.is_some());
        let h = h.unwrap();
        h.success().await;
    }
}
