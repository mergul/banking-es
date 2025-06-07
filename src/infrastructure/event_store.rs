use sqlx::{PgPool, Row, Postgres, Transaction};
use uuid::Uuid;
use chrono::Utc;
use anyhow::Result;
use crate::domain::{Event, AccountEvent};
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct EventStore {
    pool: PgPool,
    batch_sender: mpsc::UnboundedSender<BatchedEvent>,
}

#[derive(Debug, Clone)]
struct BatchedEvent {
    aggregate_id: Uuid,
    event: AccountEvent,
    expected_version: i64,
}

impl EventStore {
    pub fn new(pool: PgPool) -> Self {
        let (batch_sender, batch_receiver) = mpsc::unbounded_channel();

        // Start background batch processor
        let batch_processor = BatchProcessor::new(pool.clone(), batch_receiver);
        tokio::spawn(batch_processor.run());

        Self { pool, batch_sender }
    }

    pub async fn save_events(&self, aggregate_id: Uuid, events: Vec<AccountEvent>, expected_version: i64) -> Result<()> {
        // For high throughput, send to batch processor instead of immediate DB write
        for (i, event) in events.into_iter().enumerate() {
            let batched_event = BatchedEvent {
                aggregate_id,
                event,
                expected_version: expected_version + i as i64 + 1,
            };

            self.batch_sender.send(batched_event)
                .map_err(|_| anyhow::anyhow!("Failed to send event to batch processor"))?;
        }
        Ok(())
    }

    // Synchronous version for when immediate consistency is needed
    pub async fn save_events_sync(&self, aggregate_id: Uuid, events: Vec<AccountEvent>, expected_version: i64) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        // Use COPY for bulk inserts instead of individual INSERTs
        let mut event_data = Vec::new();
        for (i, event) in events.iter().enumerate() {
            let event_id = Uuid::new_v4();
            let version = expected_version + i as i64 + 1;
            let event_json = serde_json::to_value(event)?;

            event_data.push((
                event_id,
                aggregate_id,
                event.event_type(),
                event_json,
                version,
                Utc::now()
            ));
        }

        // Batch insert using unnest for better performance
        if !event_data.is_empty() {
            let ids: Vec<Uuid> = event_data.iter().map(|e| e.0).collect();
            let aggregate_ids: Vec<Uuid> = event_data.iter().map(|e| e.1).collect();
            let event_types: Vec<String> = event_data.iter().map(|e| e.2.to_string()).collect();
            let event_jsons: Vec<serde_json::Value> = event_data.iter().map(|e| e.3.clone()).collect();
            let versions: Vec<i64> = event_data.iter().map(|e| e.4).collect();
            let timestamps: Vec<chrono::DateTime<chrono::Utc>> = event_data.iter().map(|e| e.5).collect();

            sqlx::query!(
                r#"
                INSERT INTO events (id, aggregate_id, event_type, event_data, version, timestamp)
                SELECT * FROM UNNEST($1::uuid[], $2::uuid[], $3::text[], $4::jsonb[], $5::bigint[], $6::timestamptz[])
                "#,
                &ids,
                &aggregate_ids,
                &event_types,
                &event_jsons as &[serde_json::Value],
                &versions,
                &timestamps
            )
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn get_events(&self, aggregate_id: Uuid, from_version: Option<i64>) -> Result<Vec<Event>> {
        let from_version = from_version.unwrap_or(0);

        // Use prepared statement for better performance
        let rows = sqlx::query!(
            r#"
            SELECT id, aggregate_id, event_type, event_data, version, timestamp
            FROM events
            WHERE aggregate_id = $1 AND version > $2
            ORDER BY version ASC
            "#,
            aggregate_id,
            from_version
        )
            .fetch_all(&self.pool)
            .await?;

        let events = rows
            .into_iter()
            .map(|row| Event {
                id: row.id,
                aggregate_id: row.aggregate_id,
                event_type: row.event_type,
                event_data: row.event_data,
                version: row.version,
                timestamp: row.timestamp,
            })
            .collect();

        Ok(events)
    }

    pub async fn get_all_events(&self, limit: Option<i64>) -> Result<Vec<Event>> {
        let limit = limit.unwrap_or(1000);

        let rows = sqlx::query!(
            r#"
            SELECT id, aggregate_id, event_type, event_data, version, timestamp
            FROM events
            ORDER BY timestamp ASC
            LIMIT $1
            "#,
            limit
        )
            .fetch_all(&self.pool)
            .await?;

        let events = rows
            .into_iter()
            .map(|row| Event {
                id: row.id,
                aggregate_id: row.aggregate_id,
                event_type: row.event_type,
                event_data: row.event_data,
                version: row.version,
                timestamp: row.timestamp,
            })
            .collect();

        Ok(events)
    }
}

struct BatchProcessor {
    pool: PgPool,
    receiver: mpsc::UnboundedReceiver<BatchedEvent>,
}

impl BatchProcessor {
    fn new(pool: PgPool, receiver: mpsc::UnboundedReceiver<BatchedEvent>) -> Self {
        Self { pool, receiver }
    }

    async fn run(mut self) {
        let mut batch = Vec::new();
        let mut interval = interval(Duration::from_millis(10)); // Process every 10ms
        const BATCH_SIZE: usize = 1000;

        loop {
            tokio::select! {
                // Collect events into batch
                event = self.receiver.recv() => {
                    match event {
                        Some(event) => {
                            batch.push(event);

                            // Process batch if it's full
                            if batch.len() >= BATCH_SIZE {
                                self.process_batch(&mut batch).await;
                            }
                        }
                        None => break, // Channel closed
                    }
                }

                // Process batch on timer
                _ = interval.tick() => {
                    if !batch.is_empty() {
                        self.process_batch(&mut batch).await;
                    }
                }
            }
        }
    }

    async fn process_batch(&self, batch: &mut Vec<BatchedEvent>) {
        if batch.is_empty() {
            return;
        }

        if let Err(e) = self.write_batch(batch).await {
            tracing::error!("Failed to process event batch: {}", e);
            // In production, you might want to retry or send to DLQ
        }

        batch.clear();
    }

    async fn write_batch(&self, batch: &[BatchedEvent]) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        let mut event_data = Vec::new();
        for event in batch {
            let event_id = Uuid::new_v4();
            let event_json = serde_json::to_value(&event.event)?;

            event_data.push((
                event_id,
                event.aggregate_id,
                event.event.event_type(),
                event_json,
                event.expected_version,
                Utc::now()
            ));
        }

        // Bulk insert using unnest
        let ids: Vec<Uuid> = event_data.iter().map(|e| e.0).collect();
        let aggregate_ids: Vec<Uuid> = event_data.iter().map(|e| e.1).collect();
        let event_types: Vec<String> = event_data.iter().map(|e| e.2.to_string()).collect();        let event_jsons: Vec<serde_json::Value> = event_data.iter().map(|e| e.3.clone()).collect();
        let versions: Vec<i64> = event_data.iter().map(|e| e.4).collect();
        let timestamps: Vec<chrono::DateTime<chrono::Utc>> = event_data.iter().map(|e| e.5).collect();

        sqlx::query!(
            r#"
            INSERT INTO events (id, aggregate_id, event_type, event_data, version, timestamp)
            SELECT * FROM UNNEST($1::uuid[], $2::uuid[], $3::text[], $4::jsonb[], $5::bigint[], $6::timestamptz[])
            "#,
            &ids,
            &aggregate_ids,
            &event_types,
            &event_jsons as &[serde_json::Value],
            &versions,
            &timestamps
        )
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(())
    }
}