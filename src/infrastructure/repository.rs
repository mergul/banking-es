use crate::domain::{Account, AccountError, AccountEvent};
use crate::infrastructure::event_store::{EventPriority, EventStore};
// Import traits from the new module
use crate::infrastructure::redis_abstraction::{RedisClientTrait, RedisConnectionCommands};
use anyhow::Result;
use async_trait::async_trait;
use redis::RedisError; // Keep RedisError for RepositoryError enum
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{interval, Duration, Instant};
use tracing::{debug, error, info, warn};
use uuid::Uuid;
// No longer need direct Redis client imports like NativeRedisClient or AsyncCommands here,
// unless they are used for types not covered by the trait (e.g. RedisErrorKind, Value for some specific error handling logic - which they are not currently).

const ACCOUNT_CACHE_TTL_SECONDS: usize = 5 * 60;
const ACCOUNT_KEY_PREFIX: &str = "account:";
const EVENT_BATCH_KEY_PREFIX: &str = "events_for_batching:";
const DEAD_LETTER_QUEUE_KEY: &str = "event_batch_dlq";

/// Represents a batch of events for a specific aggregate, including the expected version
/// before these events are applied. This is used for Redis-based event batching.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct RedisEventBatch {
    expected_version: i64,
    events: Vec<AccountEvent>,
}

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
    #[error("Redis command error: {0}")]
    RedisError(#[from] RedisError), // Changed from redis::RedisError
    #[error("Batch processing error: {0}")]
    BatchError(String),
}

// Repository metrics
#[derive(Debug, Default)]
struct RepositoryMetrics {
    in_mem_cache_hits: std::sync::atomic::AtomicU64,
    in_mem_cache_misses: std::sync::atomic::AtomicU64,
    redis_cache_hits: std::sync::atomic::AtomicU64,
    redis_cache_misses: std::sync::atomic::AtomicU64,
    // batch_flushes: std::sync::atomic::AtomicU64, // Removed
    events_processed: std::sync::atomic::AtomicU64, // This will now count events processed by Redis flusher
    errors: std::sync::atomic::AtomicU64, // General errors
    redis_events_batched_count: std::sync::atomic::AtomicU64, // Added: Count of individual events pushed to Redis for batching
    redis_event_batches_processed: std::sync::atomic::AtomicU64, // Added: Count of batches (per account) processed by flusher
    redis_event_batch_errors: std::sync::atomic::AtomicU64,    // Added: Count of batches that failed processing
}

// Enhanced cache entry with metadata - This might be removed or repurposed if not used elsewhere.
// For now, it's unused by AccountRepository directly for accounts.
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
    // Updated signature for save_batched
    async fn save_batched(&self, account_id: Uuid, expected_version: i64, events: Vec<AccountEvent>) -> Result<()>;
    async fn save_immediate(&self, account: &Account, events: Vec<AccountEvent>) -> Result<()>;
    async fn flush_all(&self) -> Result<()>;
    fn start_batch_flush_task(&self);
}

#[derive(Clone)]
pub struct AccountRepository {
    event_store: EventStore,
    redis_client: Arc<dyn RedisClientTrait>, // Changed to use the trait
    redis_flush_interval: Duration,
    metrics: Arc<RepositoryMetrics>,
}

impl AccountRepository {
    /// Creates a new `AccountRepository`.
    ///
    /// # Arguments
    ///
    /// * `event_store`: The event store for persisting account events.
    /// * `redis_client`: An `Arc` wrapped Redis client trait object for caching and event batching.
    pub fn new(event_store: EventStore, redis_client: Arc<dyn RedisClientTrait>) -> Self { // Changed signature
        let repo = Self {
            event_store,
            redis_client, // Now an Arc<dyn RedisClientTrait>
            redis_flush_interval: Duration::from_millis(100),
            metrics: Arc::new(RepositoryMetrics::default()),
        };
        repo.start_redis_event_flusher_task(); // ADDED - Will be implemented next
        repo.start_metrics_reporter();

        repo
    }

    /// Starts the background task that periodically flushes batched events from Redis to the EventStore.
    ///
    /// This task scans Redis for event batches, processes them, saves them to the primary event store,
    /// and then invalidates the corresponding account caches.
    fn start_redis_event_flusher_task(&self) {
        let event_store = self.event_store.clone();
        let redis_client_for_task = self.redis_client.clone_client(); // Clone Arc<dyn RedisClientTrait>
        let flush_interval = self.redis_flush_interval;
        let metrics = Arc::clone(&self.metrics);

        tokio::spawn(async move {
            let mut interval_timer = interval(flush_interval);
            info!("Redis event flusher task started with interval: {:?}", flush_interval);

            loop {
                interval_timer.tick().await;
                debug!("Redis event flusher task running.");

                let mut con = match redis_client_for_task.get_async_connection().await {
                    Ok(c) => c,
                    Err(e) => {
                        error!("Flusher: Failed to get Redis connection: {}", e);
                        metrics.errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        continue; // Skip this tick
                    }
                };

                let pattern = format!("{}*", EVENT_BATCH_KEY_PREFIX);
                let mut scan_iter: redis::AsyncIter<String> = match con.scan_match(&pattern).await {
                    Ok(it) => it,
                    Err(e) => {
                        error!("Flusher: SCAN command failed for pattern {}: {}", pattern, e);
                        metrics.errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        continue;
                    }
                };

                let mut keys_to_process = Vec::new();
                while let Some(key) = scan_iter.next_item().await {
                    keys_to_process.push(key);
                }

                if keys_to_process.is_empty() {
                    debug!("Flusher: No event batches found to process.");
                    continue;
                }

                info!("Flusher: Found {} event batch keys to process.", keys_to_process.len());

                for batch_key in keys_to_process {
                    let account_id_str = batch_key.trim_start_matches(EVENT_BATCH_KEY_PREFIX);
                    let account_id = match Uuid::parse_str(account_id_str) {
                        Ok(id) => id,
                        Err(e) => {
                            error!("Flusher: Failed to parse account_id from key {}: {}", batch_key, e);
                            // Consider moving to DLQ or just logging
                            metrics.redis_event_batch_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            continue;
                        }
                    };

                    // Atomically get all events and clear the list.
                    // Using a transaction (MULTI/EXEC) for LRANGE and LTRIM/DEL.
                    // For simplicity here, let's do LRANGE 0 -1, then LTRIM list 1 0 (which empties it)
                    // or DEL if LTRIM is problematic for some Redis versions / setups with 0 elements.
                    // A more robust way is LMOVE to a processing list, but that's more complex.

                    let serialized_events: Vec<String> = match con.lrange(&batch_key, 0, -1).await {
                        Ok(evs) => evs,
                        Err(e) => {
                            error!("Flusher: LRANGE failed for key {}: {}", batch_key, e);
                            metrics.redis_event_batch_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            continue;
                        }
                    };

                    if serialized_events.is_empty() {
                        // Attempt to delete the empty list if it wasn't caught by SCAN somehow or after a partial failure.
                        let _ : redis::RedisResult<()> = con.del(&batch_key).await;
                        continue;
                    }

                    // Each item in serialized_events is now a JSON string of a RedisEventBatch
                    let mut all_items_in_list_processed_successfully = true;
                    let mut items_processed_from_this_list = 0;

                    for serialized_batch_item_str in serialized_events.iter() {
                        match serde_json::from_str::<RedisEventBatch>(serialized_batch_item_str) {
                            Ok(redis_batch) => {
                                // Process this individual RedisEventBatch
                                match event_store.save_events(account_id, redis_batch.events.clone(), redis_batch.expected_version).await {
                                    Ok(_) => {
                                        metrics.events_processed.fetch_add(redis_batch.events.len() as u64, std::sync::atomic::Ordering::Relaxed);
                                        metrics.redis_event_batches_processed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                        info!(
                                            "Flusher: Successfully processed batch of {} events (expected_version: {}) for account {}",
                                            redis_batch.events.len(), redis_batch.expected_version, account_id
                                        );
                                        items_processed_from_this_list += 1;

                                        // Invalidate account cache after successful persistence of a batch.
                                        let account_cache_key = format!("{}{}", ACCOUNT_KEY_PREFIX, account_id);
                                        if let Err(e) = con.del::<_, ()>(&account_cache_key).await {
                                            error!("Flusher: Failed to DEL account {} from Redis account cache: {}", account_id, e);
                                        }
                                    }
                                    Err(e) => {
                                        all_items_in_list_processed_successfully = false;
                                        error!(
                                            "Flusher: Failed to save events for account {} (expected_version: {}) from batch key {}: {}. Moving to DLQ.",
                                            account_id, redis_batch.expected_version, batch_key, e
                                        );
                                        metrics.redis_event_batch_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                        let dlq_payload = serde_json::json!({
                                            "original_key": batch_key,
                                            "failed_batch_item": redis_batch, // The specific RedisEventBatch that failed
                                            "error_type": "event_store_save_failure",
                                            "error_details": e.to_string(),
                                            "timestamp": Utc::now().to_rfc3339()
                                        }).to_string();
                                        if let Err(dlq_err) = con.rpush::<_, _, ()>(DEAD_LETTER_QUEUE_KEY, dlq_payload).await {
                                            error!("Flusher: CRITICAL - Failed to push failed batch item to DLQ {}: {}. Original error: {}", DEAD_LETTER_QUEUE_KEY, dlq_err, e);
                                        }
                                        // Stop processing further items from THIS list for this account_id in this cycle.
                                        // The failed item (if DLQ push failed) or subsequent items will be picked up next time if not trimmed.
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                all_items_in_list_processed_successfully = false;
                                error!("Flusher: Failed to deserialize RedisEventBatch from key {}: {}. Data: '{}'. Moving raw string to DLQ.", batch_key, e, serialized_batch_item_str);
                                metrics.redis_event_batch_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                let dlq_payload = serde_json::json!({
                                    "original_key": batch_key,
                                    "corrupted_batch_item_str": serialized_batch_item_str,
                                    "error_type": "deserialization_failure",
                                    "timestamp": Utc::now().to_rfc3339()
                                }).to_string();
                                if let Err(dlq_err) = con.rpush::<_, _, ()>(DEAD_LETTER_QUEUE_KEY, dlq_payload).await {
                                     error!("Flusher: Failed to push corrupted batch item string to DLQ {}: {}", DEAD_LETTER_QUEUE_KEY, dlq_err);
                                }
                                // Stop processing further items from THIS list for this account_id.
                                break;
                            }
                        }
                    }

                    // After iterating through all items fetched by LRANGE for this batch_key:
                    if all_items_in_list_processed_successfully {
                        // If all items were processed and saved successfully, clear the entire Redis list.
                        if let Err(e) = con.del::<_, ()>(&batch_key).await {
                            error!("Flusher: Failed to DEL successfully processed event batch key {}: {}", batch_key, e);
                        }
                    } else if items_processed_from_this_list > 0 {
                        // If some items were processed successfully before a failure, trim them from the list.
                        // LTRIM list_key number_of_items_processed -1
                        // This removes the first N items that were successfully processed or moved to DLQ.
                        if let Err(e) = con.ltrim::<_, ()>(&batch_key, items_processed_from_this_list as isize, -1).await {
                            error!("Flusher: Failed to LTRIM partially processed event batch key {}: {}", batch_key, e);
                        }
                         warn!("Flusher: Partially processed list {} due to errors. Trimmed {} items.", batch_key, items_processed_from_this_list);
                    } else {
                        // No items processed successfully (e.g. first item failed). List remains as is for next scan, or was handled by DLQ logic.
                        warn!("Flusher: No items successfully processed from list {} in this cycle due to error on first item(s). List will be re-scanned.", batch_key);
                    }
                }
            }
        });
    }

    /// Retrieves an account by its ID.
    ///
    /// It first attempts to fetch the account from the Redis cache. If not found (cache miss),
    /// it fetches events from the `EventStore`, reconstructs the account, and then caches
    /// the reconstructed account in Redis for future requests.
    async fn get_by_id(&self, id: Uuid) -> Result<Option<Account>, RepositoryError> {
        let cache_key = format!("{}{}", ACCOUNT_KEY_PREFIX, id);
        // Get connection using the trait
        let mut con = self.redis_client.get_async_connection().await.map_err(|e| {
            error!("get_by_id: Failed to get Redis connection: {}", e);
            RepositoryError::RedisError(e)
        })?;

        // Try to get from Redis using trait method
        match con.get::<_, Option<String>>(&cache_key).await { // Expect Option<String> for clarity if key not found
            Ok(Some(serialized_account)) => {
                if !serialized_account.is_empty() {
                    match serde_json::from_str::<Account>(&serialized_account) {
                        Ok(account) => {
                            self.metrics.redis_cache_hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            debug!("Account {} loaded from Redis cache", id);
                            return Ok(Some(account));
                        }
                        Err(e) => {
                            warn!("Failed to deserialize account {} from Redis: {}. Fetching from DB.", id, e);
                            // Proceed to DB fetch, cache miss will be incremented
                        }
                    }
                }
            }
            Ok(None) => { // Key not found in Redis
                debug!("Account {} not found in Redis cache (key miss).", id);
            }
            Err(e) => {
                // Log Redis error but proceed to fetch from DB as a fallback
                error!("Redis GET error for key {}: {}. Fetching from DB.", cache_key, e);
            }
        }

        self.metrics.redis_cache_misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        debug!("Account {} not in Redis cache or error, fetching from event store", id);
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

        // Store in Redis if successfully reconstructed
        match serde_json::to_string(&account) {
            Ok(serialized_account) => {
                // Use trait method for set_ex
                if let Err(e) = con
                    .set_ex(&cache_key, serialized_account, ACCOUNT_CACHE_TTL_SECONDS)
                    .await
                {
                    error!("Failed to SETEX account {} to Redis: {}", id, e);
                    // Log error but proceed, primary data source is still the event store
                } else {
                    debug!("Account {} stored in Redis cache with TTL {}s", id, ACCOUNT_CACHE_TTL_SECONDS);
                }
            }
            Err(e) => {
                error!("Failed to serialize account {} for Redis: {}", id, e);
            }
        }

        Ok(Some(account))
    }

    // Enhanced batch flush task - REMOVED (will be replaced by Redis based flusher)
    // fn start_batch_flush_task(&self) { ... }


    // Metrics reporter
    fn start_metrics_reporter(&self) {
        let metrics = Arc::clone(&self.metrics);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;

                let in_mem_hits = metrics
                    .in_mem_cache_hits
                    .load(std::sync::atomic::Ordering::Relaxed);
                let in_mem_misses = metrics
                    .in_mem_cache_misses
                    .load(std::sync::atomic::Ordering::Relaxed);
                let redis_hits = metrics
                    .redis_cache_hits
                    .load(std::sync::atomic::Ordering::Relaxed);
                let redis_misses = metrics
                    .redis_cache_misses
                    .load(std::sync::atomic::Ordering::Relaxed);
                // let flushes = metrics.batch_flushes.load(std::sync::atomic::Ordering::Relaxed); // Removed
                let processed = metrics
                    .events_processed // Counts events successfully saved by flusher
                    .load(std::sync::atomic::Ordering::Relaxed);
                let errors = metrics.errors.load(std::sync::atomic::Ordering::Relaxed); // General repo errors
                let redis_batched_count = metrics
                    .redis_events_batched_count // New metric
                    .load(std::sync::atomic::Ordering::Relaxed);
                let redis_batches_processed = metrics
                    .redis_event_batches_processed // New metric
                    .load(std::sync::atomic::Ordering::Relaxed);
                let redis_batch_errors = metrics
                    .redis_event_batch_errors // New metric
                    .load(std::sync::atomic::Ordering::Relaxed);

                let in_mem_hit_rate = if in_mem_hits + in_mem_misses > 0 {
                    (in_mem_hits as f64 / (in_mem_hits + in_mem_misses) as f64) * 100.0
                } else {
                    0.0
                };
                let redis_hit_rate = if redis_hits + redis_misses > 0 {
                    (redis_hits as f64 / (redis_hits + redis_misses) as f64) * 100.0
                } else {
                    0.0
                };

                info!(
                    "Repository Metrics - InMem Cache HR: {:.1}%, Redis Cache HR: {:.1}%, Events Processed (by flusher): {}, Total Events Batched to Redis: {}, Redis Batches Processed: {}, Redis Batch Errors: {}, General Errors: {}",
                    in_mem_hit_rate, redis_hit_rate, processed, redis_batched_count, redis_batches_processed, redis_batch_errors, errors
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
            .save_events_with_priority(account.id, events.clone(), account.version, EventPriority::High)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to save account events: {}", e))?;

        // Invalidate Redis cache
        let cache_key = format!("{}{}", ACCOUNT_KEY_PREFIX, account.id);
        let mut con = self.redis_client.get_async_connection().await.map_err(|e| {
            error!("save: Failed to get Redis connection for DEL: {}", e);
            RepositoryError::RedisError(e)
        })?;

        match con.del(&cache_key).await { // Use trait method
            Ok(()) => debug!("Account {} DEL from Redis cache", account.id),
            Err(e) => error!("Failed to DEL account {} from Redis: {}", account.id, e),
        }
        Ok(())
    }

    async fn get_by_id(&self, id: Uuid) -> Result<Option<Account>, AccountError> {
        // This is the trait method, calling the internal one.
        self.get_by_id(id).await.map_err(|e| match e {
            RepositoryError::NotFound(id) => AccountError::NotFound,
            RepositoryError::VersionConflict { expected, actual } => {
                AccountError::VersionConflict { expected, actual }
            }
            RepositoryError::RedisError(re) => AccountError::InfrastructureError(format!("Redis error: {}", re)),
            RepositoryError::CacheError(ce) => AccountError::InfrastructureError(format!("Cache error: {}", ce)),
            RepositoryError::InfrastructureError(ie) => AccountError::InfrastructureError(format!("Infra error: {}", ie)),
            RepositoryError::BatchError(be) => AccountError::InfrastructureError(format!("Batch error: {}", be)),

        })
    }

    /// Saves a batch of events with an expected version to a Redis list for later processing.
    ///
    /// The `expected_version` is crucial for optimistic concurrency control when the events
    /// are eventually persisted to the EventStore by the flusher task.
    async fn save_batched(&self, account_id: Uuid, expected_version: i64, events: Vec<AccountEvent>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let batch_key = format!("{}{}", EVENT_BATCH_KEY_PREFIX, account_id);
        let mut con = self.redis_client.get_async_connection().await.map_err(|e| {
            error!("save_batched: Failed to get Redis connection: {}", e);
            RepositoryError::RedisError(e)
        })?;

        let redis_event_batch = RedisEventBatch {
            expected_version,
            events: events.clone(), // Clone events into the batch struct
        };

        let serialized_batch = serde_json::to_string(&redis_event_batch).map_err(|e| {
            error!("save_batched: Failed to serialize RedisEventBatch: {}", e);
            RepositoryError::InfrastructureError(anyhow::anyhow!("Failed to serialize event batch: {}", e))
        })?;

        con.rpush(&batch_key, serialized_batch).await.map_err(|e| {
            error!("save_batched: Failed to RPUSH RedisEventBatch to {}: {}", batch_key, e);
            RepositoryError::RedisError(e)
        })?;

        // Metrics: redis_events_batched_count now refers to the number of events within the pushed batches.
        self.metrics.redis_events_batched_count.fetch_add(events.len() as u64, std::sync::atomic::Ordering::Relaxed);
        debug!(
            "Batched {} events (version: {}) for account {} to Redis list {}",
            events.len(), expected_version, account_id, batch_key
        );

        Ok(())
    }

    /// Saves events directly to the EventStore and invalidates the Redis cache for the account.
    ///
    /// This method bypasses the Redis event batching queue. It also attempts to clear any
    /// pending batched events for the same account from Redis to prevent potential conflicts or stale data.
    async fn save_immediate(&self, account: &Account, events: Vec<AccountEvent>) -> Result<()> {
        // 1. Clear pending batched events for this account_id from Redis
        let batch_key = format!("{}{}", EVENT_BATCH_KEY_PREFIX, account.id);
        let mut con = self.redis_client.get_async_connection().await.map_err(|e| {
            error!("save_immediate: Failed to get Redis connection for DEL batch: {}", e);
            RepositoryError::RedisError(e)
        })?;

        match con.del(&batch_key).await { // Use trait method
            Ok(num_deleted_keys) => {
                if num_deleted_keys > 0 {
                    info!("Cleared pending Redis event batch for account {} due to immediate save.", account.id);
                }
            }
            Err(e) => {
                // Log error but proceed with immediate save, as it's higher priority
                error!("Failed to DEL pending event batch for account {} during immediate save: {}. Proceeding...", account.id, e);
            }
        }

        // 2. Save events directly to event_store
        self.event_store
            .save_events(account.id, events.clone(), account.version)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to save immediate events to event_store: {}", e))?;

        // 3. Invalidate/update Redis account cache
        let account_cache_key = format!("{}{}", ACCOUNT_KEY_PREFIX, account.id);
        // Re-use connection: con is already available and is mut.
        match con.del(&account_cache_key).await { // Use trait method
            Ok(()) => debug!("Account {} DEL from Redis account cache (immediate save)", account.id),
            Err(e) => error!("Failed to DEL account {} from Redis account cache (immediate save): {}", account.id, e),
        }
        Ok(())
    }

    async fn flush_all(&self) -> Result<()> {
        // This method's behavior changes. With Redis, the flusher runs periodically.
        // A manual flush_all could:
        // 1. Signal the flusher task to run immediately (if it supports such a signal).
        // 2. Iterate through all `events_for_batching:*` keys and process them directly here.
        //    This would duplicate logic from the flusher and might lead to contention.
        // For now, let's make it a conceptual placeholder or a no-op,
        // as the flusher is designed to handle this periodically.
        info!("flush_all() called. Redis event flusher runs periodically. Manual trigger not yet implemented.");
        // TODO: Consider if a manual trigger for the Redis event flusher is needed and how to implement it.
        Ok(())
    }

    fn start_batch_flush_task(&self) {
        // This method is no longer used as the batch flush task is removed.
        // The new `start_redis_event_flusher_task` is called from `new`.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Account, AccountEvent, StoredEvent};
    use crate::infrastructure::event_store::EventStoreConfig;
    use mockall::mock;
    use redis::AsyncIter;
    use rust_decimal::Decimal;
    use std::sync::atomic::Ordering;
    // Use a specific type for Arc if needed for mock comparisons, or use Any within mockall
    use std::sync::Arc;

    // Mock for RedisConnectionCommands
    mock! {
        // Need to ensure its methods match RedisConnectionCommands exactly
        // For generic parameters K, V, P etc. in the trait, the mock needs to handle them.
        // Mockall typically requires specific types in the mock definition if the trait uses generics extensively in arguments/return types.
        // Or, we can define the mock methods with concrete types that our tests will use.
        // E.g. expect_get_string_option_string()
        pub RedisConnection {}
        #[async_trait]
        impl RedisConnectionCommands for RedisConnection {
            async fn get<K, V>(&mut self, key: K) -> Result<V, RedisError>
                where K: redis::ToRedisArgs + Send + Sync, V: redis::FromRedisValue + Send + Sync;
            async fn set_ex<K, V>(&mut self, key: K, value: V, seconds: usize) -> Result<(), RedisError>
                where K: redis::ToRedisArgs + Send + Sync, V: redis::ToRedisArgs + Send + Sync;
            async fn del<K>(&mut self, key: K) -> Result<(), RedisError>
                where K: redis::ToRedisArgs + Send + Sync;
            async fn rpush<K, V>(&mut self, key: K, values: V) -> Result<(), RedisError>
                where K: redis::ToRedisArgs + Send + Sync, V: redis::ToRedisArgs + Send + Sync;
            async fn lrange<K, V>(&mut self, key: K, start: isize, stop: isize) -> Result<V, RedisError>
                where K: redis::ToRedisArgs + Send + Sync, V: redis::FromRedisValue + Send + Sync;
            async fn scan_match<P, K>(&mut self, pattern: P) -> Result<AsyncIter<K>, RedisError>
                where P: redis::ToRedisArgs + Send + Sync, K: redis::FromRedisValue + Send + Sync;
            async fn set_options<K, V>(&mut self, key: K, value: V, options: redis::SetOptions) -> Result<Option<String>, RedisError>
                where K: redis::ToRedisArgs + Send + Sync, V: redis::ToRedisArgs + Send + Sync;
            async fn set_nx_ex<K, V>(&mut self, key: K, value: V, seconds: usize) -> Result<bool, RedisError>
                where K: redis::ToRedisArgs + Send + Sync, V: redis::ToRedisArgs + Send + Sync;
        }
    }

    // Mock for RedisClientTrait
    mock! {
        pub RedisClient {}
        #[async_trait]
        impl RedisClientTrait for RedisClient {
            async fn get_async_connection(&self) -> Result<Box<dyn RedisConnectionCommands + Send>, RedisError>;
            fn clone_client(&self) -> Arc<dyn RedisClientTrait>;
        }
    }

    // Helper to create an in-memory EventStore for tests
    async fn memory_event_store() -> EventStore {
        EventStore::new_with_config(EventStoreConfig::default_in_memory_for_tests().unwrap())
            .await
            .expect("Failed to create in-memory event store for test")
    }

    #[tokio::test]
    async fn test_get_by_id_cache_hit() {
        let account_id = Uuid::new_v4();
        let expected_account = Account {
            id: account_id,
            owner_name: "Test User".to_string(),
            balance: Decimal::new(100, 2),
            is_active: true,
            version: 1,
        };
        let serialized_account = serde_json::to_string(&expected_account).unwrap();

        let mut mock_redis_conn = MockRedisConnection::new();
        let ser_acc_clone = serialized_account.clone();
        mock_redis_conn
            .expect_get::<String, Option<String>>()
            .withf(move |key: &String| key == &format!("{}{}", ACCOUNT_KEY_PREFIX, account_id))
            .times(1)
            .returning(move |_| Ok(Some(ser_acc_clone.clone())));

        mock_redis_conn.expect_set_ex::<String, String>().times(0); // Should not be called

        let mut mock_redis_client = MockRedisClient::new();
        // This closure needs to be 'static if the mock_redis_conn is moved into it and the mock_redis_client is used across await points.
        // However, for this specific test structure, it might be okay.
        // To be safe, ensure mock_redis_conn can be created inside the closure or is Arc-ed if shared.
        // For simplicity now, assume it's fine as get_async_connection is called once per high-level repo call.
        mock_redis_client.expect_get_async_connection().times(1).returning(move || Ok(Box::new(mock_redis_conn)));

        let event_store = memory_event_store().await; // Real in-mem event store

        let repository = AccountRepository::new(
            event_store,
            Arc::new(mock_redis_client),
        );

        let result = repository.get_by_id(account_id).await;

        assert!(result.is_ok(), "get_by_id failed: {:?}", result.err());
        let fetched_account_opt = result.unwrap();
        assert!(fetched_account_opt.is_some(), "Account not found when expected from cache");
        let fetched_account = fetched_account_opt.unwrap();
        assert_eq!(fetched_account.id, expected_account.id);
        assert_eq!(fetched_account.balance, expected_account.balance);

        // Check metrics (adjust if AccountRepository::metrics is private)
        // For now, assume metrics are not directly asserted in this test, focus on behavior.
        // assert_eq!(repository.metrics.redis_cache_hits.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_get_by_id_cache_miss_found_in_event_store() {
        let account_id = Uuid::new_v4();
        // State of the account as it would be reconstructed from events
        let expected_reconstructed_account = Account {
            id: account_id,
            owner_name: "Miss User".to_string(),
            balance: Decimal::new(50, 0), // Balance after events
            is_active: true,
            version: 1, // Version after one event
        };
        let serialized_expected_account = serde_json::to_string(&expected_reconstructed_account).unwrap();

        let mut mock_redis_conn = MockRedisConnection::new();
        // 1. Redis GET returns nothing (cache miss)
        mock_redis_conn.expect_get::<String, Option<String>>()
            .withf(move |key: &String| key == &format!("{}{}", ACCOUNT_KEY_PREFIX, account_id))
            .times(1)
            .returning(|_| Ok(None));

        // 2. Redis SET_EX is called after fetching from EventStore
        let ser_exp_clone = serialized_expected_account.clone();
        mock_redis_conn.expect_set_ex::<String, String>()
            .withf(move |key: &String, value: &String, ttl: &usize| {
                key == &format!("{}{}", ACCOUNT_KEY_PREFIX, account_id) &&
                value == &ser_exp_clone &&
                ttl == &ACCOUNT_CACHE_TTL_SECONDS
            })
            .times(1)
            .returning(|_, _, _| Ok(()));

        let mut mock_redis_client = MockRedisClient::new();
        // get_async_connection is called once by get_by_id in AccountRepository
        mock_redis_client.expect_get_async_connection()
            .times(1)
            .returning(move || Ok(Box::new(mock_redis_conn)));

        let event_store = memory_event_store().await;
        let creation_event = AccountEvent::AccountCreated {
            account_id,
            owner_name: "Miss User".to_string(),
            initial_balance: Decimal::new(50, 0),
        };
        // Account version before this event must be 0 for EventStore save.
        let initial_account_state = Account { id: account_id, version: 0, ..Default::default() };
        event_store.save_events(initial_account_state.id, vec![creation_event], initial_account_state.version).await.unwrap();

        let repository = AccountRepository::new(
            event_store,
            Arc::new(mock_redis_client),
        );

        let result = repository.get_by_id(account_id).await;

        assert!(result.is_ok(), "get_by_id failed: {:?}", result.err());
        let fetched_account_opt = result.unwrap();
        assert!(fetched_account_opt.is_some(), "Account not found after cache miss and event store fetch");
        let fetched_account = fetched_account_opt.unwrap();

        assert_eq!(fetched_account.id, expected_reconstructed_account.id);
        assert_eq!(fetched_account.owner_name, expected_reconstructed_account.owner_name);
        assert_eq!(fetched_account.balance, expected_reconstructed_account.balance);
        assert_eq!(fetched_account.version, expected_reconstructed_account.version);
        // assert_eq!(repository.metrics.redis_cache_misses.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_save_invalidates_cache() {
        let account_id = Uuid::new_v4();
        let account_to_save = Account {
            id: account_id,
            owner_name: "Cache Invalidation Test".to_string(),
            balance: Decimal::new(200, 2),
            is_active: true,
            version: 0, // Version before new events
        };
        let events_to_save = vec![AccountEvent::MoneyDeposited {
            account_id,
            amount: Decimal::new(50,2),
            transaction_id: Uuid::new_v4()
        }];

        let mut mock_redis_conn = MockRedisConnection::new();
        mock_redis_conn.expect_del::<String>()
            .withf(move |key: &String| key == &format!("{}{}", ACCOUNT_KEY_PREFIX, account_id))
            .times(1)
            .returning(|_| Ok(()));

        let mut mock_redis_client = MockRedisClient::new();
        mock_redis_client.expect_get_async_connection()
            .times(1) // For the DEL operation
            .returning(move || Ok(Box::new(mock_redis_conn)));

        let event_store = memory_event_store().await;
        // Pre-populate event store if AccountRepository::save relies on prior state for versioning,
        // but for this test, we are mocking EventStore::save_events_with_priority to always succeed.
        // The actual save to EventStore is mocked here to simplify focus on cache invalidation.
        // However, AccountRepository::save calls event_store.save_events_with_priority directly.
        // For a focused test on cache invalidation, we assume event_store.save succeeds.
        // No, AccountRepository concrete type calls its event_store field directly.
        // So we use the real in-memory event_store and ensure the save call is valid.

        let repository = AccountRepository::new(
            event_store, // Real in-memory event store
            Arc::new(mock_redis_client),
        );

        let result = repository.save(&account_to_save, events_to_save).await;
        assert!(result.is_ok(), "Repository save failed: {:?}", result.err());
    }
}
