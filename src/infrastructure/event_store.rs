use sqlx::{PgPool, Row};
use uuid::Uuid;
use chrono::Utc;
use anyhow::Result;
use crate::domain::{Event, AccountEvent};

#[derive(Clone)]
pub struct EventStore {
    pool: PgPool,
}

impl EventStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save_events(&self, aggregate_id: Uuid, events: Vec<AccountEvent>, expected_version: i64) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        for (i, event) in events.iter().enumerate() {
            let event_id = Uuid::new_v4();
            let version = expected_version + i as i64 + 1;
            let event_data = serde_json::to_value(event)?;

            sqlx::query!(
                r#"
                INSERT INTO events (id, aggregate_id, event_type, event_data, version, timestamp)
                VALUES ($1, $2, $3, $4, $5, $6)
                "#,
                event_id,
                aggregate_id,
                event.event_type(),
                event_data,
                version,
                Utc::now()
            )
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn get_events(&self, aggregate_id: Uuid, from_version: Option<i64>) -> Result<Vec<Event>> {
        let from_version = from_version.unwrap_or(0);

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