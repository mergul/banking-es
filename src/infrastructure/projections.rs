use sqlx::PgPool;
use uuid::Uuid;
use rust_decimal::Decimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use anyhow::Result;
use bigdecimal::BigDecimal;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountProjection {
    pub id: Uuid,
    pub owner_name: String,
    pub balance: Decimal,
    pub is_active: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionProjection {
    pub id: Uuid,
    pub account_id: Uuid,
    pub transaction_type: String,
    pub amount: Decimal,
    pub timestamp: DateTime<Utc>,
}

#[derive(Clone)]
pub struct ProjectionStore {
    pool: PgPool,
}

impl ProjectionStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn get_account(&self, id: Uuid) -> Result<Option<AccountProjection>> {
        let row = sqlx::query_as!(
            AccountProjection,
            r#"
            SELECT id, owner_name, balance, is_active, created_at, updated_at
            FROM account_projections
            WHERE id = $1
            "#,
            id
        )
            .fetch_optional(&self.pool)
            .await?;

        Ok(row)
    }

    pub async fn get_all_accounts(&self) -> Result<Vec<AccountProjection>> {
        let rows = sqlx::query_as!(
            AccountProjection,
            r#"
            SELECT id, owner_name, balance, is_active, created_at, updated_at
            FROM account_projections
            ORDER BY created_at DESC
            "#
        )
            .fetch_all(&self.pool)
            .await?;

        Ok(rows)
    }

    pub async fn get_account_transactions(&self, account_id: Uuid) -> Result<Vec<TransactionProjection>> {
        let rows = sqlx::query_as!(
            TransactionProjection,
            r#"
            SELECT id, account_id, transaction_type, amount, timestamp
            FROM transaction_projections
            WHERE account_id = $1
            ORDER BY timestamp DESC
            "#,
            account_id
        )
            .fetch_all(&self.pool)
            .await?;

        Ok(rows)
    }

    pub async fn upsert_account(&self, account: &AccountProjection) -> Result<()> {
        sqlx::query!(
            r#"
            INSERT INTO account_projections (id, owner_name, balance, is_active, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (id) DO UPDATE SET
                owner_name = EXCLUDED.owner_name,
                balance = EXCLUDED.balance,
                is_active = EXCLUDED.is_active,
                updated_at = EXCLUDED.updated_at
            "#,
            account.id,
            account.owner_name,
            account.balance,
            account.is_active,
            account.created_at,
            account.updated_at
        )
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn insert_transaction(&self, transaction: &TransactionProjection) -> Result<()> {
        sqlx::query!(
            r#"
            INSERT INTO transaction_projections (id, account_id, transaction_type, amount, timestamp)
            VALUES ($1, $2, $3, $4, $5)
            "#,
            transaction.id,
            transaction.account_id,
            transaction.transaction_type,
            transaction.amount,
            transaction.timestamp
        )
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}