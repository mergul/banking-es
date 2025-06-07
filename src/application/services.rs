use std::sync::Arc;

use crate::domain::{Account, AccountCommand, AccountError};
use crate::infrastructure::{
    AccountProjection, AccountRepositoryTrait, ProjectionStore, TransactionProjection,
};
use anyhow::Result;
use chrono::Utc;
use rust_decimal::Decimal;
use uuid::Uuid;

#[derive(Clone)]
pub struct AccountService {
    repository: Arc<dyn AccountRepositoryTrait + 'static>,
    projections: ProjectionStore,
}

impl AccountService {
    pub fn new(
        repository: Arc<dyn AccountRepositoryTrait + 'static>,
        projections: ProjectionStore,
    ) -> Self {
        Self {
            repository,
            projections,
        }
    }

    pub async fn create_account(
        &self,
        owner_name: String,
        initial_balance: Decimal,
    ) -> Result<Uuid, AccountError> {
        let account_id = Uuid::new_v4();
        let command = AccountCommand::CreateAccount {
            account_id,
            owner_name: owner_name.clone(),
            initial_balance,
        };

        let account = Account::default();
        let events = account.handle_command(&command)?;

        self.repository
            .save(&account, events.clone())
            .await
            .map_err(|_| AccountError::NotFound)?;

        // Update projections
        let projection = AccountProjection {
            id: account_id,
            owner_name,
            balance: initial_balance,
            is_active: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        // Use the batch method with a vector containing one item
        self.projections
            .upsert_accounts_batch(vec![projection])
            .await
            .map_err(|_| AccountError::NotFound)?;

        Ok(account_id)
    }

    pub async fn deposit_money(
        &self,
        account_id: Uuid,
        amount: Decimal,
    ) -> Result<(), AccountError> {
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

        self.repository
            .save(&account, events.clone())
            .await
            .map_err(|_| AccountError::NotFound)?;

        // Update projections
        self.update_projections_from_events(&events).await?;

        Ok(())
    }

    pub async fn withdraw_money(
        &self,
        account_id: Uuid,
        amount: Decimal,
    ) -> Result<(), AccountError> {
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

        self.repository
            .save(&account, events.clone())
            .await
            .map_err(|_| AccountError::NotFound)?;

        // Update projections
        self.update_projections_from_events(&events).await?;

        Ok(())
    }

    pub async fn get_account(
        &self,
        account_id: Uuid,
    ) -> Result<Option<AccountProjection>, AccountError> {
        self.projections
            .get_account(account_id)
            .await
            .map_err(|_| AccountError::NotFound)
    }

    pub async fn get_all_accounts(&self) -> Result<Vec<AccountProjection>, AccountError> {
        self.projections
            .get_all_accounts()
            .await
            .map_err(|_| AccountError::NotFound)
    }

    pub async fn get_account_transactions(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<TransactionProjection>, AccountError> {
        self.projections
            .get_account_transactions(account_id)
            .await
            .map_err(|_| AccountError::NotFound)
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
                        .map_err(|_| AccountError::NotFound)?
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
                        .map_err(|_| AccountError::NotFound)?
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
                .map_err(|_| AccountError::NotFound)?;
        }

        if !transaction_updates.is_empty() {
            self.projections
                .insert_transactions_batch(transaction_updates)
                .await
                .map_err(|_| AccountError::NotFound)?;
        }

        Ok(())
    }
}
