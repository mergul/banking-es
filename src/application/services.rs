use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::domain::{Account, AccountCommand, AccountError};
use crate::infrastructure::{
    AccountProjection, AccountRepositoryTrait, ProjectionStore, TransactionProjection,
};
use anyhow::Result;
use chrono::Utc;
use rust_decimal::Decimal;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

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
    metrics: Arc<ServiceMetrics>,
    command_cache: Arc<RwLock<std::collections::HashMap<Uuid, Instant>>>,
}

impl AccountService {
    pub fn new(
        repository: Arc<dyn AccountRepositoryTrait + 'static>,
        projections: ProjectionStore,
    ) -> Self {
        let service = Self {
            repository,
            projections,
            metrics: Arc::new(ServiceMetrics::default()),
            command_cache: Arc::new(RwLock::new(std::collections::HashMap::new())),
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

        // Check for duplicate command
        if self.is_duplicate_command(account_id).await {
            return Err(AccountError::InfrastructureError(
                "Duplicate create account command".to_string(),
            ));
        }

        let command = AccountCommand::CreateAccount {
            account_id,
            owner_name: owner_name.clone(),
            initial_balance,
        };

        let account = Account::default();
        let events = account.handle_command(&command)?;

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

        // Check for duplicate command
        if self.is_duplicate_command(account_id).await {
            return Err(AccountError::InfrastructureError(
                "Duplicate deposit command".to_string(),
            ));
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

        // Check for duplicate command
        if self.is_duplicate_command(account_id).await {
            return Err(AccountError::InfrastructureError(
                "Duplicate withdraw command".to_string(),
            ));
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

    async fn is_duplicate_command(&self, account_id: Uuid) -> bool {
        let mut cache = self.command_cache.write().await;
        let now = Instant::now();

        // Clean up old entries
        cache.retain(|_, timestamp| now.duration_since(*timestamp) < Duration::from_secs(60));

        // Check for duplicate
        if cache.contains_key(&account_id) {
            return true;
        }

        // Add new command
        cache.insert(account_id, now);
        false
    }
}
