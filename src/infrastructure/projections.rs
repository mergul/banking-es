use sqlx::PgPool;
use uuid::Uuid;
use rust_decimal::Decimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{interval, Duration, Instant};

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
struct CacheEntry<T> {
    data: T,
    last_accessed: Instant,
}

#[derive(Clone)]
pub struct ProjectionStore {
    pool: PgPool,
    account_cache: Arc<RwLock<HashMap<Uuid, CacheEntry<AccountProjection>>>>,
    transaction_cache: Arc<RwLock<HashMap<Uuid, CacheEntry<Vec<TransactionProjection>>>>>,
}

impl ProjectionStore {
    pub fn new(pool: PgPool) -> Self {
        let store = Self {
            pool,
            account_cache: Arc::new(RwLock::new(HashMap::new())),
            transaction_cache: Arc::new(RwLock::new(HashMap::new())),
        };

        // Start cache cleanup task
        let store_clone = store.clone();
        tokio::spawn(async move {
            store_clone.cache_cleanup_task().await;
        });

        store
    }

    async fn cache_cleanup_task(&self) {
        let mut interval = interval(Duration::from_secs(60)); // Cleanup every minute

        loop {
            interval.tick().await;

            let cutoff = Instant::now() - Duration::from_secs(300); // 5 minutes TTL

            // Cleanup account cache
            {
                let mut cache = self.account_cache.write().await;
                cache.retain(|_, entry| entry.last_accessed > cutoff);
            }

            // Cleanup transaction cache
            {
                let mut cache = self.transaction_cache.write().await;
                cache.retain(|_, entry| entry.last_accessed > cutoff);
            }
        }
    }

    pub async fn get_account(&self, id: Uuid) -> Result<Option<AccountProjection>> {
        // Check cache first
        {
            let cache = self.account_cache.read().await;
            if let Some(entry) = cache.get(&id) {
                return Ok(Some(entry.data.clone()));
            }
        }

        // Load from database
        let account = self.load_account_from_db(id).await?;

        // Update cache
        if let Some(ref acc) = account {
            let mut cache = self.account_cache.write().await;
            cache.insert(id, CacheEntry {
                data: acc.clone(),
                last_accessed: Instant::now(),
            });
        }

        Ok(account)
    }

    async fn load_account_from_db(&self, id: Uuid) -> Result<Option<AccountProjection>> {
        let row = sqlx::query!(
            r#"
            SELECT id, owner_name, balance, is_active, created_at, updated_at
            FROM account_projections
            WHERE id = $1
            "#,
            id
        )
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.map(|r| AccountProjection {
            id: r.id,
            owner_name: r.owner_name,
            balance: r.balance,
            is_active: r.is_active,
            created_at: r.created_at,
            updated_at: r.updated_at,
        }))
    }

    pub async fn get_all_accounts(&self) -> Result<Vec<AccountProjection>> {
        // For bulk operations, go directly to DB to avoid cache bloat
        let rows = sqlx::query!(
            r#"
            SELECT id, owner_name, balance, is_active, created_at, updated_at
            FROM account_projections
            ORDER BY created_at DESC
            "#
        )
            .fetch_all(&self.pool)
            .await?;

        Ok(rows.into_iter().map(|r| AccountProjection {
            id: r.id,
            owner_name: r.owner_name,
            balance: r.balance,
            is_active: r.is_active,
            created_at: r.created_at,
            updated_at: r.updated_at,
        }).collect())
    }

    pub async fn get_account_transactions(&self, account_id: Uuid) -> Result<Vec<TransactionProjection>> {
        // Check cache first
        {
            let cache = self.transaction_cache.read().await;
            if let Some(entry) = cache.get(&account_id) {
                return Ok(entry.data.clone());
            }
        }

        // Load from database
        let transactions = self.load_transactions_from_db(account_id).await?;

        // Update cache
        {
            let mut cache = self.transaction_cache.write().await;
            cache.insert(account_id, CacheEntry {
                data: transactions.clone(),
                last_accessed: Instant::now(),
            });
        }

        Ok(transactions)
    }

    async fn load_transactions_from_db(&self, account_id: Uuid) -> Result<Vec<TransactionProjection>> {
        let rows = sqlx::query!(
            r#"
            SELECT id, account_id, transaction_type, amount, timestamp
            FROM transaction_projections
            WHERE account_id = $1
            ORDER BY timestamp DESC
            LIMIT 100
            "#,
            account_id
        )
            .fetch_all(&self.pool)
            .await?;

        Ok(rows.into_iter().map(|r| TransactionProjection {
            id: r.id,
            account_id: r.account_id,
            transaction_type: r.transaction_type,
            amount: r.amount,
            timestamp: r.timestamp,
        }).collect())
    }

    pub async fn upsert_account(&self, account: &AccountProjection) -> Result<()> {
        // Update database
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

        // Update cache
        {
            let mut cache = self.account_cache.write().await;
            cache.insert(account.id, CacheEntry {
                data: account.clone(),
                last_accessed: Instant::now(),
            });
        }

        Ok(())
    }

    pub async fn insert_transaction(&self, transaction: &TransactionProjection) -> Result<()> {
        // Update database
        sqlx::query!(
            r#"
            INSERT INTO transaction_projections (id, account_id, transaction_type, amount, timestamp)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (id) DO NOTHING
            "#,
            transaction.id,
            transaction.account_id,
            transaction.transaction_type,
            transaction.amount,
            transaction.timestamp
        )
            .execute(&self.pool)
            .await?;

        // Invalidate transaction cache for this account
        {
            let mut cache = self.transaction_cache.write().await;
            cache.remove(&transaction.account_id);
        }

        Ok(())
    }

    // Batch operations for high throughput
    pub async fn upsert_accounts_batch(&self, accounts: &[AccountProjection]) -> Result<()> {
        if accounts.is_empty() {
            return Ok(());
        }

        // Batch upsert using UNNEST
        let ids: Vec<Uuid> = accounts.iter().map(|a| a.id).collect();
        let owner_names: Vec<String> = accounts.iter().map(|a| a.owner_name.clone()).collect();        let balances: Vec<Decimal> = accounts.iter().map(|a| a.balance).collect();
        let is_actives: Vec<bool> = accounts.iter().map(|a| a.is_active).collect();
        let created_ats: Vec<DateTime<Utc>> = accounts.iter().map(|a| a.created_at).collect();
        let updated_ats: Vec<DateTime<Utc>> = accounts.iter().map(|a| a.updated_at).collect();

        sqlx::query!(
            r#"
            INSERT INTO account_projections (id, owner_name, balance, is_active, created_at, updated_at)
            SELECT * FROM UNNEST($1::uuid[], $2::text[], $3::decimal[], $4::boolean[], $5::timestamptz[], $6::timestamptz[])
            ON CONFLICT (id) DO UPDATE SET
                owner_name = EXCLUDED.owner_name,
                balance = EXCLUDED.balance,
                is_active = EXCLUDED.is_active,
                updated_at = EXCLUDED.updated_at
            "#,
            &ids,
            &owner_names,
            &balances,
            &is_actives,
            &created_ats,
            &updated_ats
        )
            .execute(&self.pool)
            .await?;

        // Update cache for all accounts
        {
            let mut cache = self.account_cache.write().await;
            for account in accounts {
                cache.insert(account.id, CacheEntry {
                    data: account.clone(),
                    last_accessed: Instant::now(),
                });
            }
        }

        Ok(())
    }

    pub async fn insert_transactions_batch(&self, transactions: &[TransactionProjection]) -> Result<()> {
        if transactions.is_empty() {
            return Ok(());
        }

        // Batch insert using UNNEST
        let ids: Vec<Uuid> = transactions.iter().map(|t| t.id).collect();
        let account_ids: Vec<Uuid> = transactions.iter().map(|t| t.account_id).collect();
        let types: Vec<String> = transactions.iter().map(|t| t.transaction_type.clone()).collect();        let amounts: Vec<Decimal> = transactions.iter().map(|t| t.amount).collect();
        let timestamps: Vec<DateTime<Utc>> = transactions.iter().map(|t| t.timestamp).collect();

        sqlx::query!(
            r#"
            INSERT INTO transaction_projections (id, account_id, transaction_type, amount, timestamp)
            SELECT * FROM UNNEST($1::uuid[], $2::uuid[], $3::text[], $4::decimal[], $5::timestamptz[])
            ON CONFLICT (id) DO NOTHING
            "#,
            &ids,
            &account_ids,
            &types,
            &amounts,
            &timestamps
        )
            .execute(&self.pool)
            .await?;

        // Invalidate cache for affected accounts
        {
            let mut cache = self.transaction_cache.write().await;
            for transaction in transactions {
                cache.remove(&transaction.account_id);
            }
        }

        Ok(())
    }
}