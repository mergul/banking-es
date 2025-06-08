use uuid::Uuid;
// anyhow::Result is not directly used in public signatures here, but handlers use it.
use crate::domain::{AccountCommand, AccountError, AccountEvent}; // Added AccountEvent
use crate::infrastructure::{ProjectionStore, AccountProjection, TransactionProjection};
use crate::application::handlers::{AccountCommandHandler, AccountQueryHandler}; // Added
use rust_decimal::Decimal;
use chrono::Utc;
use std::sync::Arc; // Added

#[derive(Clone)]
pub struct AccountService {
    command_handler: Arc<AccountCommandHandler>,
    query_handler: Arc<AccountQueryHandler>,
    projections: ProjectionStore,
}

impl AccountService {
    pub fn new(
        command_handler: Arc<AccountCommandHandler>,
        query_handler: Arc<AccountQueryHandler>,
        projections: ProjectionStore,
    ) -> Self {
        Self { command_handler, query_handler, projections }
    }

    pub async fn create_account(&self, owner_name: String, initial_balance: Decimal) -> Result<Uuid, AccountError> {
        // Delegate to command handler to create the account and get the ID
        let account_id = self.command_handler.handle_create_account(owner_name.clone(), initial_balance).await?;

        // Projection update
        let projection = AccountProjection {
            id: account_id,
            owner_name, // owner_name from command input
            balance: initial_balance, // initial_balance from command input
            is_active: true, // New accounts are active
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        self.projections.upsert_account(&projection).await
            .map_err(|e| AccountError::InfrastructureError(format!("Projection error on create: {}", e)))?;

        Ok(account_id)
    }

    pub async fn deposit_money(&self, account_id: Uuid, amount: Decimal) -> Result<(), AccountError> {
        let events = self.command_handler.handle_deposit_money(account_id, amount).await?;
        // The command handler now returns the events it saved.
        self.update_projections_from_events(account_id, &events).await?;
        Ok(())
    }

    pub async fn withdraw_money(&self, account_id: Uuid, amount: Decimal) -> Result<(), AccountError> {
        let events = self.command_handler.handle_withdraw_money(account_id, amount).await?;
        self.update_projections_from_events(account_id, &events).await?;
        Ok(())
    }

    // Example for a close_account method if it were added to AccountService
    // For now, update_projections_from_events will handle AccountClosed if other commands generate it.
    // pub async fn close_account(&self, account_id: Uuid, reason: String) -> Result<(), AccountError> {
    //     let events = self.command_handler.handle_close_account(account_id, reason).await?;
    //     self.update_projections_from_events(account_id, &events).await?;
    //     Ok(())
    // }

    // get_account continues to query ProjectionStore directly for AccountProjection
    pub async fn get_account(&self, account_id: Uuid) -> Result<Option<AccountProjection>, AccountError> {
        self.projections.get_account(account_id).await
            .map_err(|e| AccountError::InfrastructureError(format!("Projection error on get_account: {}", e)))
    }

    pub async fn get_all_accounts(&self) -> Result<Vec<AccountProjection>, AccountError> {
        self.projections.get_all_accounts().await
            .map_err(|e| AccountError::InfrastructureError(format!("Projection error on get_all_accounts: {}", e)))
    }

    pub async fn get_account_transactions(&self, account_id: Uuid) -> Result<Vec<TransactionProjection>, AccountError> {
        self.projections.get_account_transactions(account_id).await
            .map_err(|e| AccountError::InfrastructureError(format!("Projection error on get_account_transactions: {}", e)))
    }

    // Modified to take account_id for fetching projection
    async fn update_projections_from_events(&self, account_id: Uuid, events: &[AccountEvent]) -> Result<(), AccountError> {
        // Fetch the current projection first to update it.
        // This is important because events only contain deltas or specific states.
        let mut account_proj = self.projections.get_account(account_id).await
            .map_err(|e| AccountError::InfrastructureError(format!("Projection get error in update: {}", e)))?
            .ok_or_else(|| AccountError::InfrastructureError(format!("Account projection not found for update: {}", account_id)))?;

        for event in events {
            match event {
                AccountEvent::MoneyDeposited { amount, transaction_id, .. } => {
                    account_proj.balance += amount;
                    account_proj.updated_at = Utc::now();
                    // Insert transaction record
                    let transaction = TransactionProjection {
                        id: *transaction_id,
                        account_id,
                        transaction_type: "deposit".to_string(),
                        amount: *amount,
                        timestamp: Utc::now(), // Ideally, event should carry its own timestamp
                    };
                    self.projections.insert_transaction(&transaction).await
                        .map_err(|e| AccountError::InfrastructureError(format!("Transaction insert error: {}", e)))?;
                }
                AccountEvent::MoneyWithdrawn { amount, transaction_id, .. } => {
                    account_proj.balance -= amount;
                    account_proj.updated_at = Utc::now();
                    // Insert transaction record
                    let transaction = TransactionProjection {
                        id: *transaction_id,
                        account_id,
                        transaction_type: "withdrawal".to_string(),
                        amount: *amount,
                        timestamp: Utc::now(), // Ideally, event should carry its own timestamp
                    };
                    self.projections.insert_transaction(&transaction).await
                         .map_err(|e| AccountError::InfrastructureError(format!("Transaction insert error: {}", e)))?;
                }
                AccountEvent::AccountClosed { .. } => {
                    account_proj.is_active = false;
                    account_proj.updated_at = Utc::now();
                }
                // AccountCreated is handled by create_account's direct projection insertion.
                // If create_account also returned events to this method, we'd need to handle it here too,
                // but that would mean upserting the projection twice.
                AccountEvent::AccountCreated { .. } => {
                    // This event leads to initial creation of projection in `create_account` method.
                    // If events from `create_account` were passed here, we'd need to ensure
                    // this doesn't conflict (e.g., by making `upsert_account` truly idempotent
                    // or by not passing AccountCreated events to this generic update method).
                    // For now, assuming AccountCreated events are not passed here.
                    // If they are, the projection should be updated with owner_name, initial_balance.
                    // account_proj.owner_name = owner_name.clone(); etc.
                }
            }
        }
        // After all events are processed for the account, upsert the final projection state.
        self.projections.upsert_account(&account_proj).await
            .map_err(|e| AccountError::InfrastructureError(format!("Projection upsert error in update: {}", e)))?;

        Ok(())
    }
}