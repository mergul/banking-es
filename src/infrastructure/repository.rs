use crate::domain::{Account, AccountError, AccountEvent};
use crate::infrastructure::event_store::{EventPriority, EventStore};
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{interval, Duration, Instant};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

// Enhanced error types
#[derive(Debug, thiserror::Error)]
pub enum RepositoryError {
    #[error("Account not found: {0}")]
    NotFound(Uuid),
    #[error("Version conflict: expected {expected}, found {actual}")]
    VersionConflict { expected: i64, actual: i64 },
    #[error("Infrastructure error: {0}")]
    InfrastructureError(#[from] anyhow::Error),
    #[error("Cache error: {0}")]
    CacheError(String),
    #[error("Batch processing error: {0}")]
    BatchError(String),
}

// Repository metrics
#[derive(Debug, Default)]
struct RepositoryMetrics {
    cache_hits: std::sync::atomic::AtomicU64,
    cache_misses: std::sync::atomic::AtomicU64,
    batch_flushes: std::sync::atomic::AtomicU64,
    events_processed: std::sync::atomic::AtomicU64,
    errors: std::sync::atomic::AtomicU64,
}

// Enhanced cache entry with metadata
#[derive(Debug, Clone)]
struct CacheEntry<T> {
    data: T,
    created_at: Instant,
    last_accessed: Instant,
    version: i64,
}

// New Trait Definition
#[async_trait]
pub trait AccountRepositoryTrait: Send + Sync + 'static {
    async fn save(&self, account: &Account, events: Vec<AccountEvent>) -> Result<()>;
    async fn get_by_id(&self, id: Uuid) -> Result<Option<Account>, AccountError>;
    async fn save_batched(&self, account_id: Uuid, events: Vec<AccountEvent>) -> Result<()>;
    async fn save_immediate(&self, account: &Account, events: Vec<AccountEvent>) -> Result<()>;
    async fn flush_all(&self) -> Result<()>;
    fn start_batch_flush_task(&self);
}

#[derive(Clone)]
pub struct AccountRepository {
    event_store: EventStore,
    // Write-behind cache for batching events
    pending_events: Arc<Mutex<HashMap<Uuid, Vec<AccountEvent>>>>,
    // Read-through cache for accounts
    account_cache: Arc<RwLock<HashMap<Uuid, CacheEntry<Account>>>>,
    // Batch flush interval
    flush_interval: Duration,
    metrics: Arc<RepositoryMetrics>,
}

impl AccountRepository {
    pub fn new(event_store: EventStore) -> Self {
        let repo = Self {
            event_store,
            pending_events: Arc::new(Mutex::new(HashMap::new())),
            account_cache: Arc::new(RwLock::new(HashMap::new())),
            flush_interval: Duration::from_millis(50),
            metrics: Arc::new(RepositoryMetrics::default()),
        };

        repo.start_batch_flush_task();
        repo.start_metrics_reporter();

        repo
    }

    // Enhanced get_by_id with better caching and error handling
    async fn get_by_id(&self, id: Uuid) -> Result<Option<Account>, RepositoryError> {
        // Check cache first
        {
            let cache = self.account_cache.read().await;
            if let Some(entry) = cache.get(&id) {
                if entry.created_at.elapsed() < Duration::from_secs(300) {
                    // 5 minutes TTL
                    self.metrics
                        .cache_hits
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return Ok(Some(entry.data.clone()));
                }
            }
        }

        self.metrics
            .cache_misses
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Load from event store
        let stored_events = self
            .event_store
            .get_events(id, None)
            .await
            .map_err(RepositoryError::InfrastructureError)?;

        if stored_events.is_empty() {
            return Ok(None);
        }

        let mut account = Account::default();
        account.id = id;

        // Apply events
        for event in stored_events {
            let account_event: AccountEvent = serde_json::from_value(event.event_data)
                .map_err(|e| RepositoryError::InfrastructureError(e.into()))?;
            account.apply_event(&account_event);
        }

        // Update cache
        {
            let mut cache = self.account_cache.write().await;
            cache.insert(
                id,
                CacheEntry {
                    data: account.clone(),
                    created_at: Instant::now(),
                    last_accessed: Instant::now(),
                    version: account.version,
                },
            );
        }

        Ok(Some(account))
    }

    // Enhanced batch flush task
    fn start_batch_flush_task(&self) {
        let pending_events = Arc::clone(&self.pending_events);
        let event_store = self.event_store.clone();
        let flush_interval = self.flush_interval;
        let metrics = Arc::clone(&self.metrics);

        tokio::spawn(async move {
            let mut interval = interval(flush_interval);
            let mut retry_queue = Vec::new();
            let mut retry_interval = tokio::time::interval(Duration::from_secs(1));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let mut pending = pending_events.lock().await;
                        if pending.is_empty() {
                            continue;
                        }

                        let events_to_flush: HashMap<Uuid, Vec<AccountEvent>> = pending.drain().collect();
                        drop(pending);

                        for (account_id, events) in events_to_flush {
                            match event_store.save_events(account_id, events.clone(), 0).await {
                                Ok(_) => {
                                    metrics.events_processed.fetch_add(events.len() as u64, std::sync::atomic::Ordering::Relaxed);
                                    metrics.batch_flushes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                },
                                Err(e) => {
                                    error!("Failed to flush events for account {}: {}", account_id, e);
                                    metrics.errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    retry_queue.push((account_id, events));
                                }
                            }
                        }
                    }
                    _ = retry_interval.tick() => {
                        if !retry_queue.is_empty() {
                            let retries = std::mem::replace(&mut retry_queue, Vec::new());
                            for (account_id, events) in retries {
                                if let Err(e) = event_store.save_events(account_id, events.clone(), 0).await {
                                    error!("Retry failed for account {}: {}", account_id, e);
                                    metrics.errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    retry_queue.push((account_id, events));
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    // Metrics reporter
    fn start_metrics_reporter(&self) {
        let metrics = Arc::clone(&self.metrics);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;

                let hits = metrics
                    .cache_hits
                    .load(std::sync::atomic::Ordering::Relaxed);
                let misses = metrics
                    .cache_misses
                    .load(std::sync::atomic::Ordering::Relaxed);
                let flushes = metrics
                    .batch_flushes
                    .load(std::sync::atomic::Ordering::Relaxed);
                let processed = metrics
                    .events_processed
                    .load(std::sync::atomic::Ordering::Relaxed);
                let errors = metrics.errors.load(std::sync::atomic::Ordering::Relaxed);

                let hit_rate = if hits + misses > 0 {
                    (hits as f64 / (hits + misses) as f64) * 100.0
                } else {
                    0.0
                };

                info!(
                    "Repository Metrics - Cache Hit Rate: {:.1}%, Batch Flushes: {}, Events Processed: {}, Errors: {}",
                    hit_rate, flushes, processed, errors
                );
            }
        });
    }
}

// Implement the trait for AccountRepository
#[async_trait]
impl AccountRepositoryTrait for AccountRepository {
    async fn save(&self, account: &Account, events: Vec<AccountEvent>) -> Result<()> {
        self.event_store
            .save_events_with_priority(account.id, events, account.version, EventPriority::High)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to save account: {}", e))
    }

    async fn get_by_id(&self, id: Uuid) -> Result<Option<Account>, AccountError> {
        self.get_by_id(id).await.map_err(|e| match e {
            RepositoryError::NotFound(id) => AccountError::NotFound,
            RepositoryError::VersionConflict { expected, actual } => {
                AccountError::VersionConflict { expected, actual }
            }
            _ => AccountError::InfrastructureError(e.to_string()),
        })
    }

    async fn save_batched(&self, account_id: Uuid, events: Vec<AccountEvent>) -> Result<()> {
        let mut pending = self.pending_events.lock().await;
        pending
            .entry(account_id)
            .or_insert_with(Vec::new)
            .extend(events);

        // Invalidate cache
        {
            let mut cache = self.account_cache.write().await;
            cache.remove(&account_id);
        }

        Ok(())
    }

    async fn save_immediate(&self, account: &Account, events: Vec<AccountEvent>) -> Result<()> {
        self.event_store
            .save_events(account.id, events, account.version)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to save immediate events: {}", e))
    }

    async fn flush_all(&self) -> Result<()> {
        let mut pending = self.pending_events.lock().await;
        let events_to_flush: HashMap<Uuid, Vec<AccountEvent>> = pending.drain().collect();
        drop(pending);

        for (account_id, events) in events_to_flush {
            self.event_store
                .save_events(account_id, events, 0)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to flush events: {}", e))?;
        }

        Ok(())
    }

    fn start_batch_flush_task(&self) {
        // Already started in constructor
    }
}
