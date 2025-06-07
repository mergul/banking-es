use sqlx::{PgPool, Row};
use uuid::Uuid;
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio::sync::{mpsc, RwLock};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use crate::domain::{AccountEvent, Event};

#[derive(Clone)]
pub struct EventStore {
    pool: PgPool,
    batch_sender: mpsc::UnboundedSender<BatchedEvent>,
    snapshot_cache: Arc<RwLock<HashMap<Uuid, CachedSnapshot>>>,
}

#[derive(Debug, Clone)]
struct BatchedEvent {
    aggregate_id: Uuid,
    events: Vec<AccountEvent>,
    expected_version: i64,
    response_tx: tokio::sync::oneshot::Sender<Result<()>>,
}

#[derive(Debug, Clone)]
struct CachedSnapshot {
    aggregate_id: Uuid,
    data: serde_json::Value,
    version: i64,
    created_at: Instant,
}

impl EventStore {
    pub fn new(pool: PgPool) -> Self {
        let (batch_sender, batch_receiver) = mpsc::unbounded_channel();
        let snapshot_cache = Arc::new(RwLock::new(HashMap::new()));

        let store = Self {
            pool: pool.clone(),
            batch_sender,
            snapshot_cache: snapshot_cache.clone(),
        };

        // Start background batch processor
        tokio::spawn(Self::batch_processor(pool.clone(), batch_receiver));

        // Start periodic snapshot creation
        tokio::spawn(Self::snapshot_worker(pool.clone(), snapshot_cache));

        store
    }

    // Asynchronous event saving with batching
    pub async fn save_events(&self, aggregate_id: Uuid, events: Vec<AccountEvent>, expected_version: i64) -> Result<()> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        let batched_event = BatchedEvent {
            aggregate_id,
            events,
            expected_version,
            response_tx,
        };

        self.batch_sender.send(batched_event)?;
        response_rx.await?
    }

    // Background batch processor for high throughput
    async fn batch_processor(
        pool: PgPool,
        mut receiver: mpsc::UnboundedReceiver<BatchedEvent>,
    ) {
        const BATCH_SIZE: usize = 1000;
        const BATCH_TIMEOUT: Duration = Duration::from_millis(10); // 10ms batching window

        let mut batch = Vec::with_capacity(BATCH_SIZE);
        let mut last_flush = Instant::now();

        while let Some(event) = receiver.recv().await {
            batch.push(event);

            // Flush batch if size limit reached or timeout exceeded
            if batch.len() >= BATCH_SIZE || last_flush.elapsed() >= BATCH_TIMEOUT {
                Self::flush_batch(&pool, &mut batch).await;
                last_flush = Instant::now();
            }
        }

        // Flush remaining events
        if !batch.is_empty() {
            Self::flush_batch(&pool, &mut batch).await;
        }
    }

    async fn flush_batch(pool: &PgPool, batch: &mut Vec<BatchedEvent>) {
        if batch.is_empty() {
            return;
        }

        let mut tx = match pool.begin().await {
            Ok(tx) => tx,
            Err(e) => {
                // Send error to all pending requests
                for event in batch.drain(..) {
                    let _ = event.response_tx.send(Err(e.into()));
                }
                return;
            }
        };

        // Prepare bulk insert data
        let mut all_event_data = Vec::new();
        let mut responses = Vec::new();

        for batched_event in batch.drain(..) {
            for (i, event) in batched_event.events.iter().enumerate() {
                let event_id = Uuid::new_v4();
                let version = batched_event.expected_version + i as i64 + 1;
                let event_json = match serde_json::to_value(event) {
                    Ok(json) => json,
                    Err(e) => {
                        let _ = batched_event.response_tx.send(Err(e.into()));
                        continue;
                    }
                };

                all_event_data.push((
                    event_id,
                    batched_event.aggregate_id,
                    event.event_type().to_string(),
                    event_json,
                    version,
                    Utc::now(),
                ));
            }
            responses.push(batched_event.response_tx);
        }

        // Bulk insert using COPY or UNNEST
        let result = Self::bulk_insert_events(&mut tx, all_event_data).await;

        match result {
            Ok(_) => {
                if let Err(e) = tx.commit().await {
                    for response_tx in responses {
                        let _ = response_tx.send(Err(e.into()));
                    }
                } else {
                    for response_tx in responses {
                        let _ = response_tx.send(Ok(()));
                    }
                }
            }
            Err(e) => {
                let _ = tx.rollback().await;
                for response_tx in responses {
                    let _ = response_tx.send(Err(e.into()));
                }
            }
        }
    }

    async fn bulk_insert_events(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        event_data: Vec<(Uuid, Uuid, String, Value, i64, DateTime<Utc>)>,
    ) -> Result<()> {
        if event_data.is_empty() {
            return Ok(());
        }

        let ids: Vec<Uuid> = event_data.iter().map(|e| e.0).collect();
        let aggregate_ids: Vec<Uuid> = event_data.iter().map(|e| e.1).collect();
        let event_types: Vec<String> = event_data.iter().map(|e| e.2.clone()).collect();
        let event_jsons: Vec<Value> = event_data.iter().map(|e| e.3.clone()).collect();
        let versions: Vec<i64> = event_data.iter().map(|e| e.4).collect();
        let timestamps: Vec<DateTime<Utc>> = event_data.iter().map(|e| e.5).collect();

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
            .await?;

        Ok(())
    }

    // Optimized event retrieval with snapshots
    pub async fn get_events(&self, aggregate_id: Uuid, from_version: Option<i64>) -> Result<Vec<Event>> {
        // Check for snapshot first
        let snapshot = {
            let cache = self.snapshot_cache.read().await;
            cache.get(&aggregate_id).cloned()
        };

        let (start_version, base_events) = if let Some(snapshot) = snapshot {
            // Load events from snapshot version onwards
            (Some(snapshot.version), vec![])
        } else {
            (from_version, vec![])
        };

        let query = if let Some(version) = start_version {
            sqlx::query_as!(
                Event,
                r#"
                SELECT id, aggregate_id, event_type, event_data, version, timestamp
                FROM events
                WHERE aggregate_id = $1 AND version > $2
                ORDER BY version
                "#,
                aggregate_id,
                version
            )
        } else {
            sqlx::query_as!(
                Event,
                r#"
                SELECT id, aggregate_id, event_type, event_data, version, timestamp
                FROM events
                WHERE aggregate_id = $1
                ORDER BY version
                "#,
                aggregate_id
            )
        };

        let mut events = query.fetch_all(&self.pool).await?;

        // Prepend snapshot data if available
        if let Some(snapshot) = snapshot {
            let snapshot_event = Event {
                id: Uuid::new_v4(),
                aggregate_id: snapshot.aggregate_id,
                event_type: "Snapshot".to_string(),
                event_data: snapshot.data,
                version: snapshot.version,
                timestamp: Utc::now(),
            };
            events.insert(0, snapshot_event);
        }

        Ok(events)
    }

    // Periodic snapshot creation worker
    async fn snapshot_worker(
        pool: PgPool,
        cache: Arc<RwLock<HashMap<Uuid, CachedSnapshot>>>,
    ) {
        let mut interval = tokio::time::interval(Duration::from_secs(60)); // Every minute

        loop {
            interval.tick().await;

            // Find aggregates that need snapshots (high event count)
            let candidates = sqlx::query!(
                r#"
                SELECT aggregate_id, COUNT(*) as event_count, MAX(version) as max_version
                FROM events
                WHERE aggregate_id NOT IN (
                    SELECT aggregate_id FROM snapshots
                    WHERE created_at > NOW() - INTERVAL '1 hour'
                )
                GROUP BY aggregate_id
                HAVING COUNT(*) > 100
                ORDER BY COUNT(*) DESC
                LIMIT 100
                "#
            )
                .fetch_all(&pool)
                .await;

            if let Ok(candidates) = candidates {
                for candidate in candidates {
                    if let Some(aggregate_id) = candidate.aggregate_id {
                        let _ = Self::create_snapshot(&pool, &cache, aggregate_id).await;
                    }
                }
            }
        }
    }

    async fn create_snapshot(
        pool: &PgPool,
        cache: &Arc<RwLock<HashMap<Uuid, CachedSnapshot>>>,
        aggregate_id: Uuid,
    ) -> Result<()> {
        // Load all events for the aggregate
        let events = sqlx::query_as!(
            Event,
            r#"
            SELECT id, aggregate_id, event_type, event_data, version, timestamp
            FROM events
            WHERE aggregate_id = $1
            ORDER BY version
            "#,
            aggregate_id
        )
            .fetch_all(pool)
            .await?;

        if events.is_empty() {
            return Ok(());
        }

        // Reconstruct aggregate state
        let mut account = crate::domain::Account::default();
        account.id = aggregate_id;

        for event in &events {
            let account_event: AccountEvent = serde_json::from_value(event.event_data.clone())?;
            account.apply_event(&account_event);
        }

        let snapshot_data = serde_json::to_value(&account)?;
        let max_version = events.last().unwrap().version;

        // Save snapshot to database
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
            .await?;

        // Update cache
        let snapshot = CachedSnapshot {
            aggregate_id,
            data: snapshot_data,
            version: max_version,
            created_at: Instant::now(),
        };

        {
            let mut cache_guard = cache.write().await;
            cache_guard.insert(aggregate_id, snapshot);
        }

        Ok(())
    }
}