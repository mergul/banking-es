use anyhow::Result;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use uuid::Uuid;

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
    version: u64,
}

#[derive(Clone)]
pub struct ProjectionStore {
    pool: PgPool,
    account_cache: Arc<RwLock<HashMap<Uuid, CacheEntry<AccountProjection>>>>,
    transaction_cache: Arc<RwLock<HashMap<Uuid, CacheEntry<Vec<TransactionProjection>>>>>,
    update_sender: mpsc::UnboundedSender<ProjectionUpdate>,
    cache_version: Arc<std::sync::atomic::AtomicU64>,
}

#[derive(Debug)]
enum ProjectionUpdate {
    AccountBatch(Vec<AccountProjection>),
    TransactionBatch(Vec<TransactionProjection>),
}

impl ProjectionStore {
    pub fn new(pool: PgPool) -> Self {
        let (update_sender, update_receiver) = mpsc::unbounded_channel();
        let account_cache = Arc::new(RwLock::new(HashMap::new()));
        let transaction_cache = Arc::new(RwLock::new(HashMap::new()));
        let cache_version = Arc::new(std::sync::atomic::AtomicU64::new(0));

        let store = Self {
            pool: pool.clone(),
            account_cache: account_cache.clone(),
            transaction_cache: transaction_cache.clone(),
            update_sender,
            cache_version: cache_version.clone(),
        };

        // Start background batch processor
        tokio::spawn(Self::batch_update_processor(
            pool.clone(),
            update_receiver,
            account_cache.clone(),
            transaction_cache.clone(),
            cache_version,
        ));

        // Start cache cleanup worker
        tokio::spawn(Self::cache_cleanup_worker(
            account_cache.clone(),
            transaction_cache.clone(),
        ));

        store
    }
    /// Create EventStore with a specific pool size
    pub async fn new_with_pool_size(pool_size: u32) -> Result<Self> {
        let database_url = std::env::var("DATABASE_URL")
            .map_err(|_| anyhow::anyhow!("DATABASE_URL environment variable is required"))?;

        Self::new_with_pool_size_and_url(pool_size, &database_url).await
    }

    /// Create EventStore with a specific pool size and database URL
    pub async fn new_with_pool_size_and_url(pool_size: u32, database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(pool_size)
            .min_connections(1)
            .acquire_timeout(Duration::from_secs(30))
            .idle_timeout(Duration::from_secs(600))
            .max_lifetime(Duration::from_secs(1800))
            .connect(database_url)
            .await?;

        Ok(Self::new(pool))
    }
    // Asynchronous projection updates with batching
    pub async fn upsert_accounts_batch(&self, accounts: Vec<AccountProjection>) -> Result<()> {
        self.update_sender
            .send(ProjectionUpdate::AccountBatch(accounts))?;
        Ok(())
    }

    pub async fn insert_transactions_batch(
        &self,
        transactions: Vec<TransactionProjection>,
    ) -> Result<()> {
        self.update_sender
            .send(ProjectionUpdate::TransactionBatch(transactions))?;
        Ok(())
    }

    // High-performance cached reads
    pub async fn get_account(&self, account_id: Uuid) -> Result<Option<AccountProjection>> {
        // Try cache first
        {
            let cache = self.account_cache.read().await;
            if let Some(entry) = cache.get(&account_id) {
                // Update access time (we'll do this periodically to avoid write locks)
                return Ok(Some(entry.data.clone()));
            }
        }

        // Cache miss - fetch from database
        let account = sqlx::query_as!(
            AccountProjection,
            r#"
            SELECT id, owner_name, balance, is_active, created_at, updated_at
            FROM account_projections
            WHERE id = $1
            "#,
            account_id
        )
        .fetch_optional(&self.pool)
        .await?;

        // Update cache
        if let Some(ref account) = account {
            let mut cache = self.account_cache.write().await;
            cache.insert(
                account_id,
                CacheEntry {
                    data: account.clone(),
                    last_accessed: Instant::now(),
                    version: self
                        .cache_version
                        .load(std::sync::atomic::Ordering::Relaxed),
                },
            );
        }

        Ok(account)
    }

    pub async fn get_account_transactions(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<TransactionProjection>> {
        // Try cache first
        {
            let cache = self.transaction_cache.read().await;
            if let Some(entry) = cache.get(&account_id) {
                return Ok(entry.data.clone());
            }
        }

        // Cache miss - fetch from database
        let transactions = sqlx::query_as!(
            TransactionProjection,
            r#"
            SELECT id, account_id, transaction_type, amount, timestamp
            FROM transaction_projections
            WHERE account_id = $1
            ORDER BY timestamp DESC
            LIMIT 1000
            "#,
            account_id
        )
        .fetch_all(&self.pool)
        .await?;

        // Update cache
        {
            let mut cache = self.transaction_cache.write().await;
            cache.insert(
                account_id,
                CacheEntry {
                    data: transactions.clone(),
                    last_accessed: Instant::now(),
                    version: self
                        .cache_version
                        .load(std::sync::atomic::Ordering::Relaxed),
                },
            );
        }

        Ok(transactions)
    }

    pub async fn get_all_accounts(&self) -> Result<Vec<AccountProjection>> {
        // For frequently accessed data, consider a dedicated cache
        sqlx::query_as!(
            AccountProjection,
            r#"
            SELECT id, owner_name, balance, is_active, created_at, updated_at
            FROM account_projections
            WHERE is_active = true
            ORDER BY created_at DESC
            LIMIT 10000
            "#
        )
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    // Background batch processor
    async fn batch_update_processor(
        pool: PgPool,
        mut receiver: mpsc::UnboundedReceiver<ProjectionUpdate>,
        account_cache: Arc<RwLock<HashMap<Uuid, CacheEntry<AccountProjection>>>>,
        transaction_cache: Arc<RwLock<HashMap<Uuid, CacheEntry<Vec<TransactionProjection>>>>>,
        cache_version: Arc<std::sync::atomic::AtomicU64>,
    ) {
        const BATCH_SIZE: usize = 5000;
        const BATCH_TIMEOUT: Duration = Duration::from_millis(20);

        let mut account_batch = Vec::new();
        let mut transaction_batch = Vec::new();
        let mut last_flush = Instant::now();

        while let Some(update) = receiver.recv().await {
            match update {
                ProjectionUpdate::AccountBatch(accounts) => {
                    account_batch.extend(accounts);
                }
                ProjectionUpdate::TransactionBatch(transactions) => {
                    transaction_batch.extend(transactions);
                }
            }

            // Flush if batch size reached or timeout exceeded
            if account_batch.len() >= BATCH_SIZE
                || transaction_batch.len() >= BATCH_SIZE
                || last_flush.elapsed() >= BATCH_TIMEOUT
            {
                Self::flush_batches(
                    &pool,
                    &mut account_batch,
                    &mut transaction_batch,
                    &account_cache,
                    &transaction_cache,
                    &cache_version,
                )
                .await;

                last_flush = Instant::now();
            }
        }
    }

    async fn flush_batches(
        pool: &PgPool,
        account_batch: &mut Vec<AccountProjection>,
        transaction_batch: &mut Vec<TransactionProjection>,
        account_cache: &Arc<RwLock<HashMap<Uuid, CacheEntry<AccountProjection>>>>,
        transaction_cache: &Arc<RwLock<HashMap<Uuid, CacheEntry<Vec<TransactionProjection>>>>>,
        cache_version: &Arc<std::sync::atomic::AtomicU64>,
    ) {
        let mut tx = match pool.begin().await {
            Ok(tx) => tx,
            Err(e) => {
                eprintln!("Failed to start transaction: {}", e);
                return;
            }
        };

        // Process account updates
        if !account_batch.is_empty() {
            if let Err(e) = Self::bulk_upsert_accounts(&mut tx, account_batch).await {
                eprintln!("Failed to upsert accounts: {}", e);
                let _ = tx.rollback().await;
                return;
            }

            // Update cache
            {
                let mut cache = account_cache.write().await;
                let version = cache_version.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                for account in account_batch.drain(..) {
                    cache.insert(
                        account.id,
                        CacheEntry {
                            data: account,
                            last_accessed: Instant::now(),
                            version,
                        },
                    );
                }
            }
        }

        // Process transaction updates
        if !transaction_batch.is_empty() {
            if let Err(e) = Self::bulk_insert_transactions(&mut tx, transaction_batch).await {
                eprintln!("Failed to insert transactions: {}", e);
                let _ = tx.rollback().await;
                return;
            }

            // Invalidate transaction cache for affected accounts
            {
                let mut cache = transaction_cache.write().await;
                for transaction in transaction_batch.drain(..) {
                    cache.remove(&transaction.account_id);
                }
            }
        }

        if let Err(e) = tx.commit().await {
            eprintln!("Failed to commit transaction: {}", e);
        }
    }

    async fn bulk_upsert_accounts(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        accounts: &[AccountProjection],
    ) -> Result<()> {
        if accounts.is_empty() {
            return Ok(());
        }

        let ids: Vec<Uuid> = accounts.iter().map(|a| a.id).collect();
        let owner_names: Vec<String> = accounts.iter().map(|a| a.owner_name.clone()).collect();
        let balances: Vec<Decimal> = accounts.iter().map(|a| a.balance).collect();
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
            .execute(&mut **tx)
            .await?;

        Ok(())
    }

    async fn bulk_insert_transactions(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        transactions: &[TransactionProjection],
    ) -> Result<()> {
        if transactions.is_empty() {
            return Ok(());
        }

        let ids: Vec<Uuid> = transactions.iter().map(|t| t.id).collect();
        let account_ids: Vec<Uuid> = transactions.iter().map(|t| t.account_id).collect();
        let types: Vec<String> = transactions
            .iter()
            .map(|t| t.transaction_type.clone())
            .collect();
        let amounts: Vec<Decimal> = transactions.iter().map(|t| t.amount).collect();
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
            .execute(&mut **tx)
            .await?;

        Ok(())
    }

    // Cache cleanup worker
    async fn cache_cleanup_worker(
        account_cache: Arc<RwLock<HashMap<Uuid, CacheEntry<AccountProjection>>>>,
        transaction_cache: Arc<RwLock<HashMap<Uuid, CacheEntry<Vec<TransactionProjection>>>>>,
    ) {
        let mut interval = tokio::time::interval(Duration::from_secs(300)); // Every 5 minutes

        loop {
            interval.tick().await;

            let cutoff = Instant::now() - Duration::from_secs(1800); // 30 minutes

            // Clean account cache
            {
                let mut cache = account_cache.write().await;
                cache.retain(|_, entry| entry.last_accessed > cutoff);
            }

            // Clean transaction cache
            {
                let mut cache = transaction_cache.write().await;
                cache.retain(|_, entry| entry.last_accessed > cutoff);
            }
        }
    }
}
