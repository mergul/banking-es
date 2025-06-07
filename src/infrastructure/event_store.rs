use crate::domain::{AccountEvent, Event};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{postgres::PgPoolOptions, PgPool, Postgres, Row, Transaction};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex, RwLock, Semaphore};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

#[derive(Clone)]
pub struct EventStore {
    pool: PgPool,
    batch_sender: mpsc::UnboundedSender<BatchedEvent>,
    snapshot_cache: Arc<RwLock<HashMap<Uuid, CachedSnapshot>>>,
    config: EventStoreConfig,
    batch_semaphore: Arc<Semaphore>,
    // Metrics for monitoring
    metrics: Arc<EventStoreMetrics>,
}

#[derive(Debug)]
struct BatchedEvent {
    aggregate_id: Uuid,
    events: Vec<AccountEvent>,
    expected_version: i64,
    response_tx: tokio::sync::oneshot::Sender<Result<()>>,
    created_at: Instant,
    priority: EventPriority,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum EventPriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

#[derive(Debug, Clone)]
struct CachedSnapshot {
    aggregate_id: Uuid,
    data: serde_json::Value,
    version: i64,
    created_at: Instant,
    ttl: Duration,
}

#[derive(Debug, Default)]
pub struct EventStoreMetrics {
    pub events_processed: AtomicU64,
    pub events_failed: AtomicU64,
    pub batch_count: AtomicU64,
    pub avg_batch_size: AtomicU64,
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
}

impl EventStore {
    pub fn new(pool: PgPool) -> Self {
        Self::new_with_config_and_pool(pool, EventStoreConfig::default())
    }

    pub fn new_with_config_and_pool(pool: PgPool, config: EventStoreConfig) -> Self {
        let (batch_sender, batch_receiver) = mpsc::unbounded_channel();
        let shared_receiver = Arc::new(Mutex::new(batch_receiver));
        let snapshot_cache = Arc::new(RwLock::new(HashMap::new()));
        let batch_semaphore = Arc::new(Semaphore::new(config.max_batch_queue_size));
        let metrics = Arc::new(EventStoreMetrics::default());

        let store = Self {
            pool: pool.clone(),
            batch_sender,
            snapshot_cache: snapshot_cache.clone(),
            config: config.clone(),
            batch_semaphore: batch_semaphore.clone(),
            metrics: metrics.clone(),
        };

        // Start multiple batch processors for better throughput
        for i in 0..config.batch_processor_count {
            tokio::spawn(Self::batch_processor(
                pool.clone(),
                shared_receiver.clone(),
                config.clone(),
                batch_semaphore.clone(),
                metrics.clone(),
                i,
            ));
        }

        // Start snapshot worker with lower priority
        tokio::spawn(Self::snapshot_worker(
            pool.clone(),
            snapshot_cache.clone(),
            config.clone(),
        ));

        // Start cache cleanup worker
        tokio::spawn(Self::cache_cleanup_worker(snapshot_cache));

        // Start metrics reporter
        tokio::spawn(Self::metrics_reporter(metrics.clone()));

        store
    }

    pub async fn new_with_config(config: EventStoreConfig) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .min_connections(config.min_connections)
            .acquire_timeout(Duration::from_secs(config.acquire_timeout_secs))
            .idle_timeout(Duration::from_secs(config.idle_timeout_secs))
            .max_lifetime(Duration::from_secs(config.max_lifetime_secs))
            // Enable prepared statement caching
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    // Pre-warm prepared statements
                    sqlx::query("SELECT 1").execute(conn).await?;
                    Ok(())
                })
            })
            .connect(&config.database_url)
            .await
            .context("Failed to connect to database")?;

        Ok(Self::new_with_config_and_pool(pool, config))
    }

    // Enhanced event saving with priority support
    pub async fn save_events_with_priority(
        &self,
        aggregate_id: Uuid,
        events: Vec<AccountEvent>,
        expected_version: i64,
        priority: EventPriority,
    ) -> Result<()> {
        let _permit = self
            .batch_semaphore
            .acquire()
            .await
            .context("Failed to acquire batch semaphore")?;

        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        let batched_event = BatchedEvent {
            aggregate_id,
            events,
            expected_version,
            response_tx,
            created_at: Instant::now(),
            priority,
        };

        self.batch_sender
            .send(batched_event)
            .context("Failed to send event to batch processor")?;

        response_rx
            .await
            .context("Failed to receive response from batch processor")?
    }

    pub async fn save_events(
        &self,
        aggregate_id: Uuid,
        events: Vec<AccountEvent>,
        expected_version: i64,
    ) -> Result<()> {
        self.save_events_with_priority(
            aggregate_id,
            events,
            expected_version,
            EventPriority::Normal,
        )
        .await
    }

    // Multi-threaded batch processor with priority queuing
    async fn batch_processor(
        pool: PgPool,
        receiver: Arc<Mutex<mpsc::UnboundedReceiver<BatchedEvent>>>,
        config: EventStoreConfig,
        semaphore: Arc<Semaphore>,
        metrics: Arc<EventStoreMetrics>,
        worker_id: usize,
    ) {
        let batch_size = config.batch_size;
        let batch_timeout = Duration::from_millis(config.batch_timeout_ms);

        let mut priority_batches: HashMap<EventPriority, Vec<BatchedEvent>> = HashMap::new();
        let mut last_flush = Instant::now();

        debug!("Starting batch processor worker {}", worker_id);

        loop {
            // Try to receive with a timeout to ensure periodic flushing
            let event = {
                let mut rx = receiver.lock().await;
                // Use try_recv to avoid blocking when checking for timeouts
                match rx.try_recv() {
                    Ok(event) => Some(event),
                    Err(mpsc::error::TryRecvError::Empty) => {
                        // No immediate message, but check if we should flush due to timeout
                        drop(rx);

                        if !priority_batches.is_empty() && last_flush.elapsed() >= batch_timeout {
                            None // Signal to flush without new event
                        } else {
                            // Wait a bit and try again
                            tokio::time::sleep(Duration::from_millis(1)).await;
                            continue;
                        }
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        debug!("Worker {} shutting down - channel closed", worker_id);
                        break;
                    }
                }
            };

            // Add event to appropriate priority batch if we received one
            if let Some(event) = event {
                priority_batches
                    .entry(event.priority)
                    .or_insert_with(Vec::new)
                    .push(event);
            }

            let total_events: usize = priority_batches.values().map(|v| v.len()).sum();

            // Flush conditions: size limit, timeout, or critical events present
            let should_flush = total_events >= batch_size
                || last_flush.elapsed() >= batch_timeout
                || priority_batches.contains_key(&EventPriority::Critical)
                || priority_batches
                    .values()
                    .flatten()
                    .any(|e| e.created_at.elapsed() >= batch_timeout);

            if should_flush && !priority_batches.is_empty() {
                // Process batches by priority (highest first)
                let mut priorities: Vec<_> = priority_batches.keys().copied().collect();
                priorities.sort_by(|a, b| b.cmp(a)); // Descending order

                for priority in priorities {
                    if let Some(batch) = priority_batches.remove(&priority) {
                        if !batch.is_empty() {
                            let batch_count = batch.len();
                            match Self::flush_batch(&pool, batch).await {
                                Ok(()) => {
                                    metrics
                                        .events_processed
                                        .fetch_add(batch_count as u64, Ordering::Relaxed);
                                    metrics.batch_count.fetch_add(1, Ordering::Relaxed);
                                    debug!(
                                        "Worker {} processed batch of {} events with priority {:?}",
                                        worker_id, batch_count, priority
                                    );
                                }
                                Err(e) => {
                                    metrics
                                        .events_failed
                                        .fetch_add(batch_count as u64, Ordering::Relaxed);
                                    error!("Worker {} failed to process batch: {}", worker_id, e);
                                }
                            }
                            semaphore.add_permits(batch_count);
                        }
                    }
                }

                last_flush = Instant::now();
            }
        }

        // Flush remaining events on shutdown
        for (_, batch) in priority_batches {
            if !batch.is_empty() {
                let batch_count = batch.len();
                let _ = Self::flush_batch(&pool, batch).await;
                semaphore.add_permits(batch_count);
            }
        }
    }

    // Optimized batch flushing with prepared statements and COPY
    async fn flush_batch(pool: &PgPool, mut batch: Vec<BatchedEvent>) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let start_time = Instant::now();

        // Sort batch by aggregate_id to improve locality and reduce locks
        batch.sort_by_key(|e| e.aggregate_id);

        let mut tx = pool.begin().await.context("Failed to begin transaction")?;

        let mut all_event_data = Vec::new();
        let mut responses = Vec::new();
        let mut failed_responses = Vec::new();

        // Pre-allocate capacity based on estimated event count
        let estimated_events: usize = batch.iter().map(|b| b.events.len()).sum();
        all_event_data.reserve(estimated_events);

        for batched_event in batch {
            match Self::prepare_events_for_insert_optimized(&batched_event) {
                Ok(event_data) => {
                    all_event_data.extend(event_data);
                    responses.push(batched_event.response_tx);
                }
                Err(e) => {
                    failed_responses.push((batched_event.response_tx, e));
                }
            }
        }

        // Send errors for failed preparations
        for (response_tx, error) in failed_responses {
            let _ = response_tx.send(Err(error));
        }

        if all_event_data.is_empty() {
            return Ok(());
        }

        // Use COPY for maximum throughput on large batches
        let result = if all_event_data.len() > 100 {
            Self::bulk_copy_events(&mut tx, all_event_data).await
        } else {
            Self::bulk_insert_events_optimized(&mut tx, all_event_data).await
        };

        match result {
            Ok(_) => {
                tx.commit().await.context("Failed to commit transaction")?;
                for response_tx in responses {
                    let _ = response_tx.send(Ok(()));
                }
                debug!("Batch flush completed in {:?}", start_time.elapsed());
            }
            Err(e) => {
                let _ = tx.rollback().await;
                let error_msg = e.to_string();
                for response_tx in responses {
                    let _ = response_tx.send(Err(anyhow::anyhow!(error_msg.clone())));
                }
                return Err(e);
            }
        }

        Ok(())
    }

    // Pre-calculate event data to reduce serialization overhead
    fn prepare_events_for_insert_optimized(batched_event: &BatchedEvent) -> Result<Vec<EventData>> {
        let mut event_data = Vec::with_capacity(batched_event.events.len());
        let base_time = Utc::now();

        for (i, event) in batched_event.events.iter().enumerate() {
            let event_id = Uuid::new_v4();
            let version = batched_event.expected_version + i as i64 + 1;

            // Pre-serialize to avoid repeated serialization
            let event_json =
                serde_json::to_value(event).context("Failed to serialize event to JSON")?;

            event_data.push(EventData {
                id: event_id,
                aggregate_id: batched_event.aggregate_id,
                event_type: event.event_type().to_string(),
                event_data: event_json,
                version,
                timestamp: base_time,
            });
        }

        Ok(event_data)
    }

    // Use PostgreSQL COPY for maximum insert performance
    async fn bulk_copy_events(
        tx: &mut Transaction<'_, Postgres>,
        event_data: Vec<EventData>,
    ) -> Result<()> {
        // Group by aggregate for version checking
        let mut aggregates: HashMap<Uuid, (i64, i64)> = HashMap::new();
        for event in &event_data {
            let entry = aggregates
                .entry(event.aggregate_id)
                .or_insert((i64::MAX, i64::MIN));
            entry.0 = entry.0.min(event.version);
            entry.1 = entry.1.max(event.version);
        }

        // Version conflict check with single query
        if !aggregates.is_empty() {
            let aggregate_ids: Vec<Uuid> = aggregates.keys().copied().collect();
            let current_versions = sqlx::query(
                "SELECT aggregate_id, MAX(version) as max_version FROM events WHERE aggregate_id = ANY($1) GROUP BY aggregate_id"
            )
            .bind(&aggregate_ids)
            .fetch_all(&mut **tx)
            .await
            .context("Failed to check current versions")?;

            for row in current_versions {
                let aggregate_id: Uuid = row.get("aggregate_id");
                let current_version: Option<i64> = row.get("max_version");

                if let Some((min_version, _)) = aggregates.get(&aggregate_id) {
                    let current_version = current_version.unwrap_or(0);
                    if current_version >= *min_version {
                        return Err(anyhow::anyhow!(
                            "Version conflict for aggregate {}: expected {}, found {}",
                            aggregate_id,
                            min_version - 1,
                            current_version
                        ));
                    }
                }
            }
        }

        // Use COPY for bulk insert - Fixed CSV formatting
        let mut copy_writer = tx
            .copy_in_raw(
                "COPY events (id, aggregate_id, event_type, event_data, version, timestamp) FROM STDIN WITH (FORMAT CSV, QUOTE '\"', ESCAPE '\"')"
            )
            .await
            .context("Failed to start COPY operation")?;

        for event in event_data {
            // Properly escape CSV values
            let event_data_str = event.event_data.to_string();
            let escaped_data = event_data_str.replace("\"", "\"\"");

            let csv_line = format!(
                "{},{},{},\"{}\",{},{}\n",
                event.id,
                event.aggregate_id,
                event.event_type,
                escaped_data,
                event.version,
                event.timestamp.format("%Y-%m-%d %H:%M:%S%.6f UTC")
            );
            copy_writer
                .send(csv_line.as_bytes())
                .await
                .context("Failed to send data to COPY")?;
        }

        copy_writer
            .finish()
            .await
            .context("Failed to finish COPY operation")?;

        Ok(())
    }

    // Optimized bulk insert for smaller batches
    async fn bulk_insert_events_optimized(
        tx: &mut Transaction<'_, Postgres>,
        event_data: Vec<EventData>,
    ) -> Result<()> {
        if event_data.is_empty() {
            return Ok(());
        }

        // Prepare arrays for bulk insert
        let ids: Vec<Uuid> = event_data.iter().map(|e| e.id).collect();
        let aggregate_ids: Vec<Uuid> = event_data.iter().map(|e| e.aggregate_id).collect();
        let event_types: Vec<String> = event_data.iter().map(|e| e.event_type.clone()).collect();
        let event_jsons: Vec<Value> = event_data.iter().map(|e| e.event_data.clone()).collect();
        let versions: Vec<i64> = event_data.iter().map(|e| e.version).collect();
        let timestamps: Vec<DateTime<Utc>> = event_data.iter().map(|e| e.timestamp).collect();

        sqlx::query!(
            r#"
            INSERT INTO events (id, aggregate_id, event_type, event_data, version, timestamp)
            SELECT * FROM UNNEST($1::uuid[], $2::uuid[], $3::text[], $4::jsonb[], $5::bigint[], $6::timestamptz[])
            "#,
            &ids,
            &aggregate_ids,
            &event_types,
            &event_jsons as &[Value],
            &versions,
            &timestamps
        )
        .execute(&mut **tx)
        .await
        .context("Failed to insert events")?;

        Ok(())
    }

    // Optimized event retrieval with better caching and batching
    pub async fn get_events(
        &self,
        aggregate_id: Uuid,
        from_version: Option<i64>,
    ) -> Result<Vec<Event>> {
        // Check cache first
        let snapshot = {
            let cache = self.snapshot_cache.read().await;
            let cached = cache
                .get(&aggregate_id)
                .filter(|s| s.created_at.elapsed() < s.ttl)
                .cloned();

            if cached.is_some() {
                self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
            } else {
                self.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
            }

            cached
        };

        let (start_version, mut events) = if let Some(snapshot) = &snapshot {
            if let Some(from_ver) = from_version {
                if from_ver <= snapshot.version {
                    (
                        Some(snapshot.version),
                        vec![self.create_snapshot_event(snapshot)],
                    )
                } else {
                    (from_version, vec![])
                }
            } else {
                (
                    Some(snapshot.version),
                    vec![self.create_snapshot_event(snapshot)],
                )
            }
        } else {
            (from_version, vec![])
        };

        // Fetch events with prepared statement
        let db_events = if let Some(version) = start_version {
            sqlx::query_as!(
                EventRow,
                r#"
                SELECT id, aggregate_id, event_type, event_data, version, timestamp
                FROM events
                WHERE aggregate_id = $1 AND version > $2
                ORDER BY version
                "#,
                aggregate_id,
                version
            )
            .fetch_all(&self.pool)
            .await
            .context("Failed to fetch events from database")?
        } else {
            sqlx::query_as!(
                EventRow,
                r#"
                SELECT id, aggregate_id, event_type, event_data, version, timestamp
                FROM events
                WHERE aggregate_id = $1
                ORDER BY version
                "#,
                aggregate_id
            )
            .fetch_all(&self.pool)
            .await
            .context("Failed to fetch events from database")?
        };

        events.extend(db_events.into_iter().map(|row| Event {
            id: row.id,
            aggregate_id: row.aggregate_id,
            event_type: row.event_type,
            event_data: row.event_data,
            version: row.version,
            timestamp: row.timestamp,
        }));

        Ok(events)
    }

    fn create_snapshot_event(&self, snapshot: &CachedSnapshot) -> Event {
        Event {
            id: Uuid::new_v4(),
            aggregate_id: snapshot.aggregate_id,
            event_type: "Snapshot".to_string(),
            event_data: snapshot.data.clone(),
            version: snapshot.version,
            timestamp: Utc::now(),
        }
    }

    // Async snapshot worker with better batching
    async fn snapshot_worker(
        pool: PgPool,
        cache: Arc<RwLock<HashMap<Uuid, CachedSnapshot>>>,
        config: EventStoreConfig,
    ) {
        let mut interval =
            tokio::time::interval(Duration::from_secs(config.snapshot_interval_secs));

        loop {
            interval.tick().await;

            let candidates = sqlx::query!(
                r#"
                SELECT aggregate_id, COUNT(*) as event_count, MAX(version) as max_version
                FROM events
                WHERE aggregate_id NOT IN (
                    SELECT aggregate_id FROM snapshots
                    WHERE created_at > NOW() - INTERVAL '1 hour'
                )
                GROUP BY aggregate_id
                HAVING COUNT(*) > $1
                ORDER BY COUNT(*) DESC
                LIMIT $2
                "#,
                config.snapshot_threshold as i64,
                config.max_snapshots_per_run as i64
            )
            .fetch_all(&pool)
            .await;

            match candidates {
                Ok(candidates) => {
                    // Process snapshots in parallel
                    let snapshot_tasks = candidates.into_iter().map(|candidate| {
                        let pool = pool.clone();
                        let cache = cache.clone();
                        let config = config.clone();

                        tokio::spawn(async move {
                            if let Some(aggregate_id) = candidate.aggregate_id {
                                if let Err(e) =
                                    Self::create_snapshot(&pool, &cache, aggregate_id, &config)
                                        .await
                                {
                                    warn!("Failed to create snapshot for {}: {}", aggregate_id, e);
                                }
                            }
                        })
                    });

                    // Wait for all snapshot tasks to complete
                    for task in snapshot_tasks {
                        let _ = task.await;
                    }
                }
                Err(e) => {
                    error!("Failed to query snapshot candidates: {}", e);
                }
            }
        }
    }

    async fn create_snapshot(
        pool: &PgPool,
        cache: &Arc<RwLock<HashMap<Uuid, CachedSnapshot>>>,
        aggregate_id: Uuid,
        config: &EventStoreConfig,
    ) -> Result<()> {
        let events = sqlx::query_as!(
            EventRow,
            r#"
            SELECT id, aggregate_id, event_type, event_data, version, timestamp
            FROM events
            WHERE aggregate_id = $1
            ORDER BY version
            "#,
            aggregate_id
        )
        .fetch_all(pool)
        .await
        .context("Failed to fetch events for snapshot")?;

        if events.is_empty() {
            return Ok(());
        }

        let mut account = crate::domain::Account::default();
        account.id = aggregate_id;

        for event_row in &events {
            let account_event: AccountEvent = serde_json::from_value(event_row.event_data.clone())
                .context("Failed to deserialize event")?;
            account.apply_event(&account_event);
        }

        let snapshot_data =
            serde_json::to_value(&account).context("Failed to serialize account snapshot")?;
        let max_version = events.last().unwrap().version;

        sqlx::query!(
            r#"
            INSERT INTO snapshots (aggregate_id, snapshot_data, version, created_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (aggregate_id) DO UPDATE SET
                snapshot_data = EXCLUDED.snapshot_data,
                version = EXCLUDED.version,
                created_at = EXCLUDED.created_at
            "#,
            aggregate_id,
            snapshot_data,
            max_version
        )
        .execute(pool)
        .await
        .context("Failed to save snapshot to database")?;

        let snapshot = CachedSnapshot {
            aggregate_id,
            data: snapshot_data,
            version: max_version,
            created_at: Instant::now(),
            ttl: Duration::from_secs(config.snapshot_cache_ttl_secs),
        };

        {
            let mut cache_guard = cache.write().await;
            cache_guard.insert(aggregate_id, snapshot);
        }

        debug!(
            "Created snapshot for aggregate {} at version {}",
            aggregate_id, max_version
        );
        Ok(())
    }

    async fn cache_cleanup_worker(cache: Arc<RwLock<HashMap<Uuid, CachedSnapshot>>>) {
        let mut interval = tokio::time::interval(Duration::from_secs(300));

        loop {
            interval.tick().await;

            let mut expired_keys = Vec::new();
            {
                let cache_guard = cache.read().await;
                for (key, snapshot) in cache_guard.iter() {
                    if snapshot.created_at.elapsed() >= snapshot.ttl {
                        expired_keys.push(*key);
                    }
                }
            }

            if !expired_keys.is_empty() {
                let mut cache_guard = cache.write().await;
                for key in expired_keys {
                    cache_guard.remove(&key);
                }
            }
        }
    }

    // Metrics reporting task
    async fn metrics_reporter(metrics: Arc<EventStoreMetrics>) {
        let mut interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            interval.tick().await;

            let processed = metrics.events_processed.load(Ordering::Relaxed);
            let failed = metrics.events_failed.load(Ordering::Relaxed);
            let batches = metrics.batch_count.load(Ordering::Relaxed);
            let cache_hits = metrics.cache_hits.load(Ordering::Relaxed);
            let cache_misses = metrics.cache_misses.load(Ordering::Relaxed);

            let success_rate = if processed > 0 {
                ((processed - failed) as f64 / processed as f64) * 100.0
            } else {
                0.0
            };

            let cache_hit_rate = if cache_hits + cache_misses > 0 {
                (cache_hits as f64 / (cache_hits + cache_misses) as f64) * 100.0
            } else {
                0.0
            };

            info!(
                "EventStore Metrics - Processed: {}, Failed: {}, Success: {:.2}%, Batches: {}, Cache Hit Rate: {:.2}%",
                processed, failed, success_rate, batches, cache_hit_rate
            );
        }
    }

    pub fn get_metrics(&self) -> EventStoreMetrics {
        EventStoreMetrics {
            events_processed: AtomicU64::new(self.metrics.events_processed.load(Ordering::Relaxed)),
            events_failed: AtomicU64::new(self.metrics.events_failed.load(Ordering::Relaxed)),
            batch_count: AtomicU64::new(self.metrics.batch_count.load(Ordering::Relaxed)),
            avg_batch_size: AtomicU64::new(self.metrics.avg_batch_size.load(Ordering::Relaxed)),
            cache_hits: AtomicU64::new(self.metrics.cache_hits.load(Ordering::Relaxed)),
            cache_misses: AtomicU64::new(self.metrics.cache_misses.load(Ordering::Relaxed)),
        }
    }

    pub fn pool_stats(&self) -> (u32, u32) {
        (self.pool.size(), self.pool.num_idle() as u32)
    }

    pub async fn health_check(&self) -> Result<EventStoreHealth> {
        let (total_connections, idle_connections) = self.pool_stats();

        let db_status = match sqlx::query("SELECT 1").fetch_one(&self.pool).await {
            Ok(_) => "healthy".to_string(),
            Err(e) => format!("unhealthy: {}", e),
        };

        let cache_size = {
            let cache = self.snapshot_cache.read().await;
            cache.len()
        };

        Ok(EventStoreHealth {
            database_status: db_status,
            total_connections,
            idle_connections,
            cache_size,
            batch_queue_permits: self.batch_semaphore.available_permits(),
            metrics: self.get_metrics(),
        })
    }
}

#[derive(Debug, serde::Serialize)]
pub struct EventStoreHealth {
    pub database_status: String,
    pub total_connections: u32,
    pub idle_connections: u32,
    pub cache_size: usize,
    pub batch_queue_permits: usize,
    pub metrics: EventStoreMetrics,
}

// Helper structs
#[derive(Debug)]
struct EventData {
    id: Uuid,
    aggregate_id: Uuid,
    event_type: String,
    event_data: Value,
    version: i64,
    timestamp: DateTime<Utc>,
}

struct EventRow {
    id: Uuid,
    aggregate_id: Uuid,
    event_type: String,
    event_data: Value,
    version: i64,
    timestamp: DateTime<Utc>,
}
#[derive(Debug, Clone)]
pub struct EventStoreConfig {
    pub database_url: String,
    pub max_connections: u32,
    pub min_connections: u32,
    pub acquire_timeout_secs: u64,
    pub idle_timeout_secs: u64,
    pub max_lifetime_secs: u64,
    pub batch_size: usize,
    pub batch_timeout_ms: u64,
    pub max_batch_queue_size: usize,
}
impl Default for EventStoreConfig {
    fn default() -> Self {
        Self {
            database_url: "postgres://user:password@localhost/db".to_string(),
            max_connections: 100,
            min_connections: 10,
            acquire_timeout_secs: 30,
            idle_timeout_secs: 300,
            max_lifetime_secs: 3600,
            batch_size: 1000,
            batch_timeout_ms: 100,
            max_batch_queue_size: 10000,
        }
    }
}
