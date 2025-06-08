use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::domain::{Account, AccountCommand, AccountError};
use crate::infrastructure::{
    AccountProjection, AccountRepositoryTrait, ProjectionStore, TransactionProjection,
    RedisClientTrait, // Added: Import the Redis client abstraction trait
};
use anyhow::Result;
use chrono::Utc;
// Remove direct redis::Client import if no longer directly used, rely on trait.
// use redis::{AsyncCommands as _, Client as RedisClient};
use rust_decimal::Decimal;
use std::sync::Arc; // Added: For Arc<dyn RedisClientTrait>
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

// Constants for command de-duplication
const COMMAND_DEDUP_KEY_PREFIX: &str = "command_dedup:";
const COMMAND_DEDUP_TTL_SECONDS: usize = 60; // Changed to usize for redis lib

// Service metrics
#[derive(Debug, Default)]
struct ServiceMetrics {
    command_processing_time: std::sync::atomic::AtomicU64,
    commands_processed: std::sync::atomic::AtomicU64,
    commands_failed: std::sync::atomic::AtomicU64,
    projection_updates: std::sync::atomic::AtomicU64,
    projection_errors: std::sync::atomic::AtomicU64,
}

#[derive(Clone)]
pub struct AccountService {
    repository: Arc<dyn AccountRepositoryTrait + 'static>,
    projections: ProjectionStore,
    redis_client: Arc<dyn RedisClientTrait>, // Changed to use the trait
    metrics: Arc<ServiceMetrics>,
}

impl AccountService {
    /// Creates a new `AccountService`.
    ///
    /// # Arguments
    ///
    /// * `repository`: The account repository for data access.
    /// * `projections`: The projection store for querying denormalized views.
    /// * `redis_client`: An `Arc` wrapped Redis client trait object, used for command de-duplication.
    pub fn new(
        repository: Arc<dyn AccountRepositoryTrait + 'static>,
        projections: ProjectionStore,
        redis_client: Arc<dyn RedisClientTrait>, // Changed signature
    ) -> Self {
        let service = Self {
            repository,
            projections,
            redis_client, // Now an Arc<dyn RedisClientTrait>
            metrics: Arc::new(ServiceMetrics::default()),
        };

        // Start metrics reporter
        let metrics = service.metrics.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                let processed = metrics
                    .commands_processed
                    .load(std::sync::atomic::Ordering::Relaxed);
                let failed = metrics
                    .commands_failed
                    .load(std::sync::atomic::Ordering::Relaxed);
                let avg_processing_time = metrics
                    .command_processing_time
                    .load(std::sync::atomic::Ordering::Relaxed)
                    as f64
                    / 1000.0; // Convert to milliseconds
                let projection_updates = metrics
                    .projection_updates
                    .load(std::sync::atomic::Ordering::Relaxed);
                let projection_errors = metrics
                    .projection_errors
                    .load(std::sync::atomic::Ordering::Relaxed);

                let success_rate = if processed > 0 {
                    ((processed - failed) as f64 / processed as f64) * 100.0
                } else {
                    0.0
                };

                info!(
                    "Account Service Metrics - Commands: {} ({}% success), Avg Processing Time: {:.2}ms, Projection Updates: {}, Errors: {}",
                    processed, success_rate, avg_processing_time, projection_updates, projection_errors
                );
            }
        });

        service
    }

    pub async fn create_account(
        &self,
        owner_name: String,
        initial_balance: Decimal,
    ) -> Result<Uuid, AccountError> {
        let start_time = Instant::now();
        let account_id = Uuid::new_v4();
        let command_id_for_dedup = account_id; // Using account_id as command_id for deduplication for now

        // Check for duplicate command
        match self.is_duplicate_command(command_id_for_dedup).await {
            Ok(true) => {
                warn!("Duplicate create_account command detected for ID: {}", command_id_for_dedup);
                return Err(AccountError::InfrastructureError(
                    "Duplicate create account command".to_string(),
                ));
            }
            Ok(false) => { /* Not a duplicate, proceed */ }
            Err(e) => {
                // Log the error and decide whether to fail open or closed.
                // Failing open (treating as not duplicate) might be risky for financial ops.
                // Failing closed (treating as duplicate or infra error) is safer.
                error!("Redis error during command deduplication for create_account ID {}: {}. Failing operation.", command_id_for_dedup, e);
                return Err(e); // Propagate the infrastructure error
            }
        }

        let command = AccountCommand::CreateAccount {
            account_id,
            owner_name: owner_name.clone(),
            initial_balance,
        };

        let account = Account::default();
        let events = account.handle_command(&command)?;

        // Save events via batching mechanism
        self.repository
            .save_batched(account.id, account.version, events.clone())
        // Save events via batching mechanism
        self.repository
            .save_batched(account.id, account.version, events.clone())
        // Save events via batching mechanism
        self.repository
            .save_batched(account.id, account.version, events.clone())
            .await
            .map_err(|e| {
                self.metrics
                    .commands_failed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                AccountError::InfrastructureError(e.to_string())
            })?;

        // Update projections
        let projection = AccountProjection {
            id: account_id,
            owner_name,
            balance: initial_balance,
            is_active: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        if let Err(e) = self
            .projections
            .upsert_accounts_batch(vec![projection])
            .await
        {
            self.metrics
                .projection_errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            warn!(
                "Failed to update projection for account {}: {}",
                account_id, e
            );
        } else {
            self.metrics
                .projection_updates
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        self.metrics
            .commands_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.metrics.command_processing_time.fetch_add(
            start_time.elapsed().as_micros() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        Ok(account_id)
    }

    pub async fn deposit_money(
        &self,
        account_id: Uuid,
        amount: Decimal,
    ) -> Result<(), AccountError> {
        let start_time = Instant::now();

        let command_id_for_dedup = account_id; // Using account_id as command_id for deduplication for now

        // Check for duplicate command
        match self.is_duplicate_command(command_id_for_dedup).await {
            Ok(true) => {
                warn!("Duplicate deposit_money command detected for ID: {}", command_id_for_dedup);
                return Err(AccountError::InfrastructureError(
                    "Duplicate deposit command".to_string(),
                ));
            }
            Ok(false) => { /* Not a duplicate, proceed */ }
            Err(e) => {
                error!("Redis error during command deduplication for deposit_money ID {}: {}. Failing operation.", command_id_for_dedup, e);
                return Err(e);
            }
        }

        let mut account = self
            .repository
            .get_by_id(account_id)
            .await?
            .ok_or(AccountError::NotFound)?;

        let command = AccountCommand::DepositMoney { account_id, amount };
        let events = account.handle_command(&command)?;

        // Apply events to account
        for event in &events {
            account.apply_event(event);
        }

        // Save events with retry logic
        self.repository
            .save(&account, events.clone())
            .await
            .map_err(|e| {
                self.metrics
                    .commands_failed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                AccountError::InfrastructureError(e.to_string())
            })?;

        // Update projections
        if let Err(e) = self.update_projections_from_events(&events).await {
            self.metrics
                .projection_errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            warn!("Failed to update projections for deposit: {}", e);
        } else {
            self.metrics
                .projection_updates
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        self.metrics
            .commands_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.metrics.command_processing_time.fetch_add(
            start_time.elapsed().as_micros() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        Ok(())
    }

    pub async fn withdraw_money(
        &self,
        account_id: Uuid,
        amount: Decimal,
    ) -> Result<(), AccountError> {
        let start_time = Instant::now();

        let command_id_for_dedup = account_id; // Using account_id as command_id for deduplication for now

        // Check for duplicate command
        match self.is_duplicate_command(command_id_for_dedup).await {
            Ok(true) => {
                warn!("Duplicate withdraw_money command detected for ID: {}", command_id_for_dedup);
                return Err(AccountError::InfrastructureError(
                    "Duplicate withdraw command".to_string(),
                ));
            }
            Ok(false) => { /* Not a duplicate, proceed */ }
            Err(e) => {
                error!("Redis error during command deduplication for withdraw_money ID {}: {}. Failing operation.", command_id_for_dedup, e);
                return Err(e);
            }
        }

        let mut account = self
            .repository
            .get_by_id(account_id)
            .await?
            .ok_or(AccountError::NotFound)?;

        let command = AccountCommand::WithdrawMoney { account_id, amount };
        let events = account.handle_command(&command)?;

        // Apply events to account
        for event in &events {
            account.apply_event(event);
        }

        // Save events with retry logic
        self.repository
            .save(&account, events.clone())
            .await
            .map_err(|e| {
                self.metrics
                    .commands_failed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                AccountError::InfrastructureError(e.to_string())
            })?;

        // Update projections
        if let Err(e) = self.update_projections_from_events(&events).await {
            self.metrics
                .projection_errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            warn!("Failed to update projections for withdrawal: {}", e);
        } else {
            self.metrics
                .projection_updates
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        self.metrics
            .commands_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.metrics.command_processing_time.fetch_add(
            start_time.elapsed().as_micros() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        Ok(())
    }

    pub async fn get_account(
        &self,
        account_id: Uuid,
    ) -> Result<Option<AccountProjection>, AccountError> {
        self.projections
            .get_account(account_id)
            .await
            .map_err(|e| AccountError::InfrastructureError(e.to_string()))
    }

    pub async fn get_all_accounts(&self) -> Result<Vec<AccountProjection>, AccountError> {
        self.projections
            .get_all_accounts()
            .await
            .map_err(|e| AccountError::InfrastructureError(e.to_string()))
    }

    pub async fn get_account_transactions(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<TransactionProjection>, AccountError> {
        self.projections
            .get_account_transactions(account_id)
            .await
            .map_err(|e| AccountError::InfrastructureError(e.to_string()))
    }

    async fn update_projections_from_events(
        &self,
        events: &[crate::domain::AccountEvent],
    ) -> Result<(), AccountError> {
        use crate::domain::AccountEvent;

        let mut account_updates = Vec::new();
        let mut transaction_updates = Vec::new();

        for event in events {
            match event {
                AccountEvent::MoneyDeposited {
                    account_id,
                    amount,
                    transaction_id,
                } => {
                    // Prepare account balance update
                    if let Some(mut account_proj) = self
                        .projections
                        .get_account(*account_id)
                        .await
                        .map_err(|e| AccountError::InfrastructureError(e.to_string()))?
                    {
                        account_proj.balance += amount;
                        account_proj.updated_at = Utc::now();
                        account_updates.push(account_proj);
                    }

                    // Prepare transaction record
                    let transaction = TransactionProjection {
                        id: *transaction_id,
                        account_id: *account_id,
                        transaction_type: "deposit".to_string(),
                        amount: *amount,
                        timestamp: Utc::now(),
                    };
                    transaction_updates.push(transaction);
                }
                AccountEvent::MoneyWithdrawn {
                    account_id,
                    amount,
                    transaction_id,
                } => {
                    // Prepare account balance update
                    if let Some(mut account_proj) = self
                        .projections
                        .get_account(*account_id)
                        .await
                        .map_err(|e| AccountError::InfrastructureError(e.to_string()))?
                    {
                        account_proj.balance -= amount;
                        account_proj.updated_at = Utc::now();
                        account_updates.push(account_proj);
                    }

                    // Prepare transaction record
                    let transaction = TransactionProjection {
                        id: *transaction_id,
                        account_id: *account_id,
                        transaction_type: "withdrawal".to_string(),
                        amount: *amount,
                        timestamp: Utc::now(),
                    };
                    transaction_updates.push(transaction);
                }
                _ => {} // Handle other events as needed
            }
        }

        // Batch update projections
        if !account_updates.is_empty() {
            self.projections
                .upsert_accounts_batch(account_updates)
                .await
                .map_err(|e| AccountError::InfrastructureError(e.to_string()))?;
        }

        if !transaction_updates.is_empty() {
            self.projections
                .insert_transactions_batch(transaction_updates)
                .await
                .map_err(|e| AccountError::InfrastructureError(e.to_string()))?;
        }

        Ok(())
    }

    /// Checks if a command with the given ID has already been processed within a defined time window.
    ///
    /// This uses Redis with a SET NX EX command to achieve atomic check-and-set.
    ///
    /// # Arguments
    /// * `command_id`: A unique identifier for the command.
    ///
    /// # Returns
    /// * `Ok(true)` if the command is a duplicate (already processed).
    /// * `Ok(false)` if the command is new.
    /// * `Err(AccountError::InfrastructureError)` if there's an issue with Redis.
    async fn is_duplicate_command(&self, command_id: Uuid) -> Result<bool, AccountError> {
        let key = format!("{}{}", COMMAND_DEDUP_KEY_PREFIX, command_id);
        // Get connection using the trait
        let mut con = self.redis_client.get_async_connection().await.map_err(|e| {
            error!("is_duplicate_command: Failed to get Redis connection: {}", e);
            // Assuming RedisError can be converted or mapped to AccountError::InfrastructureError
            AccountError::InfrastructureError(format!("Redis connection error: {}", e))
        })?;

        // Use the specific trait method for SET NX EX
        match con.set_nx_ex(&key, 1, COMMAND_DEDUP_TTL_SECONDS).await {
            Ok(true) => Ok(false), // Key was set, so command is new (not a duplicate)
            Ok(false) => Ok(true), // Key was not set (already existed), so command is a duplicate
            Err(e) => {
                error!("Redis SET NX EX command failed for key {}: {}", key, e);
                Err(AccountError::InfrastructureError(format!("Redis SET NX EX error: {}", e)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::AccountError;
    use crate::infrastructure::redis_abstraction::{RedisClientTrait, RedisConnectionCommands, RedisError}; // Assuming these are pub
    use crate::infrastructure::{AccountRepositoryTrait, ProjectionStore, EventStoreConfig, AccountRepository, EventStore}; // For real repo/store
    use mockall::mock;
    use std::sync::Arc;
    use uuid::Uuid;
    use rust_decimal::Decimal;


    // Mock for RedisConnectionCommands (can be shared if put in a common test utils)
    // For now, redefining a version sufficient for these tests.
    mock! {
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
            async fn scan_match<P, K>(&mut self, pattern: P) -> Result<redis::AsyncIter<K>, RedisError>
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

    // Mock for AccountRepositoryTrait (if needed, for now AccountService calls it but we test is_duplicate_command directly)
    // This mock needs to be updated to reflect the new signature of save_batched
    mock! {
        pub AccountRepository {}
        #[async_trait]
        impl AccountRepositoryTrait for AccountRepository {
            async fn save(&self, account: &Account, events: Vec<AccountEvent>) -> Result<()>;
            async fn get_by_id(&self, id: Uuid) -> Result<Option<Account>, AccountError>;
            async fn save_batched(&self, account_id: Uuid, expected_version: i64, events: Vec<AccountEvent>) -> Result<()>; // Updated
            async fn save_immediate(&self, account: &Account, events: Vec<AccountEvent>) -> Result<()>;
            async fn flush_all(&self) -> Result<()>;
            fn start_batch_flush_task(&self);
        }
    }


    // Helper to create AccountService with mocked Redis for is_duplicate_command tests
    fn account_service_with_mock_redis(
        mock_redis_client: Arc<dyn RedisClientTrait>,
    ) -> AccountService {
        // We need a real AccountRepository and ProjectionStore, or mocks for them.
        // For testing only `is_duplicate_command`, these might not be strictly needed
        // if we can call `is_duplicate_command` directly.
        // However, `is_duplicate_command` is not pub, so we test it via a public method like `create_account`.
        // This means we DO need to provide dependencies for `create_account`.

        // Using a placeholder mock repository for now.
        let mock_repo = Arc::new(MockAccountRepository::new());
        // ProjectionStore might also need a mock or a real in-memory version if its methods are hit.
        // Let's use a real ProjectionStore (assuming it can be created in-memory or configured for tests)

        // Dummy ProjectionStore (replace with proper test setup if AccountService methods actually use it)
        let ps_pool = futures::executor::block_on(sqlx::PgPool::connect("postgresql://user:pass@localhost/testdb")).unwrap_or_else(|_| {
            // Fallback for environments where DB isn't running but tests are specific to Redis
            // This part is tricky. For true unit tests of AccountService logic *not* involving DB,
            // ProjectionStore should be a trait and mocked.
            // For now, this will likely panic if DB not available.
            // A better approach for `is_duplicate_command` test would be to make it public for testing or use a more involved setup.
            // Given the current structure, let's assume for these specific tests, projection store calls might not be reached if duplicate check fails early.
             let db_url = std::env::var("DATABASE_URL_TEST_SERVICES").unwrap_or_else(|_| "sqlite::memory:".to_string());
             futures::executor::block_on(sqlx::SqlitePool::connect(&db_url)).expect("Failed to create in-memory sqlite for ProjectionStore test double")
        });
        let projection_store = ProjectionStore::new(ps_pool);


        AccountService::new(mock_repo, projection_store, mock_redis_client)
    }

    #[tokio::test]
    async fn test_is_duplicate_command_new_command() {
        let command_id = Uuid::new_v4();
        let mut mock_redis_conn = MockRedisConnection::new();
        mock_redis_conn.expect_set_nx_ex::<String, i32>() // value is 1 (i32)
            .withf(move |key, _val, ttl| {
                key == &format!("{}{}", COMMAND_DEDUP_KEY_PREFIX, command_id) && ttl == &COMMAND_DEDUP_TTL_SECONDS
            })
            .times(1)
            .returning(|_, _, _| Ok(true)); // true means key was set (command is new)

        let mut mock_redis_client = MockRedisClient::new();
        mock_redis_client.expect_get_async_connection().times(1).returning(move || Ok(Box::new(mock_redis_conn)));
        mock_redis_client.expect_clone_client().returning(|| Arc::new(MockRedisClient::new())); // Cloned for async task in service `new` if metrics reporter is on.

        // To test `is_duplicate_command` directly, it would need to be public.
        // Testing via a public method like `create_account`.
        // We need to mock AccountRepository::save_batched now.
        let mut mock_repo = MockAccountRepository::new();
        mock_repo.expect_save_batched().times(1).returning(|_,_,_| Ok(())); // Expect save_batched

        let ps_pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap(); // In-memory DB for projections
        let projection_store = ProjectionStore::new(ps_pool);


        let service = AccountService::new(
            Arc::new(mock_repo),
            projection_store,
            Arc::new(mock_redis_client)
        );

        // Call a method that uses is_duplicate_command
        let result = service.create_account("Test Owner".to_string(), Decimal::new(100,0)).await;
        assert!(result.is_ok(), "Create account failed when command should be new: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_is_duplicate_command_duplicate_detected() {
        let command_id = Uuid::new_v4(); // This will be used as account_id implicitly by create_account logic
                                     // The key is that create_account will call is_duplicate_command with an ID.
                                     // We need to ensure our mock responds to *that specific ID*.
                                     // Since account_id is generated inside create_account, we can't know it beforehand to set up the mock expectation for that exact ID.
                                     // This test structure is problematic for `create_account`.
                                     // Let's assume `is_duplicate_command` was public for a moment to test its logic directly.
                                     // Or, we test `deposit_money` where `account_id` (used as command_id) is an input.

        let account_id_for_deposit = Uuid::new_v4(); // This ID will be used for deduplication key

        let mut mock_redis_conn = MockRedisConnection::new();
        mock_redis_conn.expect_set_nx_ex::<String, i32>()
             .withf(move |key, _val, ttl| {
                key == &format!("{}{}", COMMAND_DEDUP_KEY_PREFIX, account_id_for_deposit) && ttl == &COMMAND_DEDUP_TTL_SECONDS
            })
            .times(1)
            .returning(|_, _, _| Ok(false)); // false means key was NOT set (it's a duplicate)

        let mut mock_redis_client = MockRedisClient::new();
        mock_redis_client.expect_get_async_connection().times(1).returning(move || Ok(Box::new(mock_redis_conn)));
        mock_redis_client.expect_clone_client().returning(|| Arc::new(MockRedisClient::new()) );


        let mut mock_repo = MockAccountRepository::new();
        // `save_batched` should NOT be called if duplicate is detected
        mock_repo.expect_save_batched().times(0);
        // `get_by_id` for deposit will be called before save_batched, but after duplicate check. So 0 times if duplicate.
        mock_repo.expect_get_by_id().times(0);


        let ps_pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        let projection_store = ProjectionStore::new(ps_pool);


        let service = AccountService::new(
            Arc::new(mock_repo),
            projection_store,
            Arc::new(mock_redis_client)
        );

        let result = service.deposit_money(account_id_for_deposit, Decimal::new(50,0)).await;

        assert!(result.is_err(), "Deposit money should have failed due to duplicate command");
        if let Err(AccountError::InfrastructureError(msg)) = result {
            assert!(msg.contains("Duplicate deposit command"));
        } else {
            panic!("Expected InfrastructureError with duplicate message, got {:?}", result);
        }
    }
}
