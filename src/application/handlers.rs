use uuid::Uuid;
use anyhow::Result;
use crate::domain::{Account, AccountCommand, AccountEvent, AccountError};
use crate::infrastructure::repository::AccountRepositoryTrait;
use crate::infrastructure::cache::CacheService; // Import CacheService
use rust_decimal::Decimal;
use std::sync::Arc;

#[derive(Clone)]
pub struct AccountCommandHandler {
    repository: Arc<dyn AccountRepositoryTrait>,
    cache_service: Arc<dyn CacheService>, // Add cache service
}

impl AccountCommandHandler {
    pub fn new(repository: Arc<dyn AccountRepositoryTrait>, cache_service: Arc<dyn CacheService>) -> Self {
        Self { repository, cache_service }
    }

    // Helper to get the cache key, same as in AccountQueryHandler
    fn account_cache_key(account_id: Uuid) -> String {
        format!("account:{}", account_id)
    }

    pub async fn handle_create_account(
        &self,
        owner_name: String,
        initial_balance: Decimal,
    ) -> Result<Uuid, AccountError> {
        let account_id = Uuid::new_v4();
        let command = AccountCommand::CreateAccount {
            account_id,
            owner_name,
            initial_balance,
        };

        // CreateAccount command does not depend on prior state of 'account' for event generation.
        let events = Account::default().handle_command(&command)?;

        // For CreateAccount, the "expected_version" that the EventStore will check against
        // (if it's a new stream) should conceptually be 0.
        // The Account object passed to repository.save needs to provide this version.
        // The actual state (owner_name, balance) is primarily carried by the events.
        let account_for_save_metadata = Account {
            id: account_id,
            owner_name: String::new(), // Not strictly used by current save logic if only id/version matter for store
            balance: Decimal::ZERO,    // Same as above
            is_active: true,           // Reflects state *after* AccountCreated
            version: 0, // Represents the version *before* these events are applied
        };

        self.repository.save(&account_for_save_metadata, events).await
            .map_err(|e| AccountError::InfrastructureError(e.to_string()))?;

        // Invalidate cache for this new account_id
        let cache_key = Self::account_cache_key(account_id);
        if let Err(_e) = self.cache_service.delete(&cache_key).await {
            // Log cache delete error if necessary, but don't fail the operation
            // tracing::warn!("Failed to delete cache for key {}: {:?}", cache_key, _e);
        }

        Ok(account_id)
    }

    pub async fn handle_deposit_money(
        &self,
        account_id: Uuid,
        amount: Decimal,
    ) -> Result<Vec<AccountEvent>, AccountError> {
        let account = self.repository.get_by_id(account_id).await?
            .ok_or(AccountError::NotFound)?;

        let command = AccountCommand::DepositMoney { account_id, amount };
        let events = account.handle_command(&command)?;

        // The 'account' here is the state *before* new events.
        // If repository.save expects the version *before* new events, this is correct.
        self.repository.save(&account, events.clone()).await
            .map_err(|e| AccountError::InfrastructureError(e.to_string()))?;

        // Invalidate cache
        let cache_key = Self::account_cache_key(account_id);
        if let Err(_e) = self.cache_service.delete(&cache_key).await {
            // Log error
        }

        Ok(events)
    }

    pub async fn handle_withdraw_money(
        &self,
        account_id: Uuid,
        amount: Decimal,
    ) -> Result<Vec<AccountEvent>, AccountError> {
        let account = self.repository.get_by_id(account_id).await?
            .ok_or(AccountError::NotFound)?;

        let command = AccountCommand::WithdrawMoney { account_id, amount };
        let events = account.handle_command(&command)?;

        self.repository.save(&account, events.clone()).await
            .map_err(|e| AccountError::InfrastructureError(e.to_string()))?;

        // Invalidate cache
        let cache_key = Self::account_cache_key(account_id);
        if let Err(_e) = self.cache_service.delete(&cache_key).await {
            // Log error
        }

        Ok(events)
    }

    pub async fn handle_close_account(
        &self,
        account_id: Uuid,
        reason: String,
    ) -> Result<Vec<AccountEvent>, AccountError> {
        let account = self.repository.get_by_id(account_id).await?
            .ok_or(AccountError::NotFound)?;

        let command = AccountCommand::CloseAccount { account_id, reason };
        let events = account.handle_command(&command)?;

        self.repository.save(&account, events.clone()).await
            .map_err(|e| AccountError::InfrastructureError(e.to_string()))?;

        // Invalidate cache
        let cache_key = Self::account_cache_key(account_id);
        if let Err(_e) = self.cache_service.delete(&cache_key).await {
            // Log error
        }

        Ok(events)
    }
}

#[derive(Clone)]
pub struct AccountQueryHandler {
    repository: Arc<dyn AccountRepositoryTrait>,
    cache_service: Arc<dyn CacheService>, // Add cache service
}

impl AccountQueryHandler {
    pub fn new(repository: Arc<dyn AccountRepositoryTrait>, cache_service: Arc<dyn CacheService>) -> Self {
        Self { repository, cache_service }
    }

    fn account_cache_key(account_id: Uuid) -> String {
        format!("account:{}", account_id)
    }

    pub async fn get_account_by_id(&self, account_id: Uuid) -> Result<Option<Account>, AccountError> {
        let cache_key = Self::account_cache_key(account_id);

        match self.cache_service.get::<Account>(&cache_key).await {
            Ok(Some(cached_account)) => return Ok(Some(cached_account)),
            Ok(None) => { /* Cache miss, proceed to repository */ }
            Err(_e) => { /* Log cache error, proceed to repository */
                // In a real app, you might want to log this error:
                // tracing::error!("Cache get error for key {}: {:?}", cache_key, _e);
            }
        }

        let repo_account_option = self.repository.get_by_id(account_id).await?;

        if let Some(ref account) = repo_account_option {
            if let Err(_e) = self.cache_service.set(&cache_key, account, Some(3600)).await {
                // Log cache set error but proceed with returning the account from repo
                // In a real app:
                // tracing::warn!("Cache set error for key {}: {:?}", cache_key, _e);
            }
        }
        Ok(repo_account_option)
    }

    pub async fn get_account_balance(&self, account_id: Uuid) -> Result<Decimal, AccountError> {
        let account_option = self.get_account_by_id(account_id).await?;
        account_option.map(|acc| acc.balance).ok_or(AccountError::NotFound)
    }

    pub async fn is_account_active(&self, account_id: Uuid) -> Result<bool, AccountError> {
        let account_option = self.get_account_by_id(account_id).await?;
        account_option.map(|acc| acc.is_active).ok_or(AccountError::NotFound)
    }
}