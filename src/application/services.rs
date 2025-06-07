use uuid::Uuid;
use anyhow::Result;
use crate::domain::{Account, AccountCommand, AccountError};
use crate::infrastructure::{AccountRepository, ProjectionStore, AccountProjection, TransactionProjection};
use rust_decimal::Decimal;
use chrono::Utc;

#[derive(Clone)]
pub struct AccountService {
    repository: AccountRepository,
    projections: ProjectionStore,
}

impl AccountService {
    pub fn new(repository: AccountRepository, projections: ProjectionStore) -> Self {
        Self { repository, projections }
    }

    pub async fn create_account(&self, owner_name: String, initial_balance: Decimal) -> Result<Uuid, AccountError> {
        let account_id = Uuid::new_v4();
        let command = AccountCommand::CreateAccount {
            account_id,
            owner_name: owner_name.clone(),
            initial_balance,
        };

        let account = Account::default();
        let events = account.handle_command(&command)?;

        self.repository.save(&account, events.clone()).await
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
        self.projections.upsert_account(&projection).await
            .map_err(|_| AccountError::NotFound)?;

        Ok(account_id)
    }

    pub async fn deposit_money(&self, account_id: Uuid, amount: Decimal) -> Result<(), AccountError> {
        let mut account = self.repository.get_by_id(account_id).await?
            .ok_or(AccountError::NotFound)?;

        let command = AccountCommand::DepositMoney { account_id, amount };
        let events = account.handle_command(&command)?;

        // Apply events to account
        for event in &events {
            account.apply_event(event);
        }

        self.repository.save(&account, events.clone()).await
            .map_err(|_| AccountError::NotFound)?;

        // Update projections
        self.update_projections_from_events(&events).await?;

        Ok(())
    }

    pub async fn withdraw_money(&self, account_id: Uuid, amount: Decimal) -> Result<(), AccountError> {
        let mut account = self.repository.get_by_id(account_id).await?
            .ok_or(AccountError::NotFound)?;

        let command = AccountCommand::WithdrawMoney { account_id, amount };
        let events = account.handle_command(&command)?;

        // Apply events to account
        for event in &events {
            account.apply_event(event);
        }

        self.repository.save(&account, events.clone()).await
            .map_err(|_| AccountError::NotFound)?;

        // Update projections
        self.update_projections_from_events(&events).await?;

        Ok(())
    }

    pub async fn get_account(&self, account_id: Uuid) -> Result<Option<AccountProjection>, AccountError> {
        self.projections.get_account(account_id).await
            .map_err(|_| AccountError::NotFound)
    }

    pub async fn get_all_accounts(&self) -> Result<Vec<AccountProjection>, AccountError> {
        self.projections.get_all_accounts().await
            .map_err(|_| AccountError::NotFound)
    }

    pub async fn get_account_transactions(&self, account_id: Uuid) -> Result<Vec<TransactionProjection>, AccountError> {
        self.projections.get_account_transactions(account_id).await
            .map_err(|_| AccountError::NotFound)
    }

    async fn update_projections_from_events(&self, events: &[crate::domain::AccountEvent]) -> Result<(), AccountError> {
        use crate::domain::AccountEvent;

        for event in events {
            match event {
                AccountEvent::MoneyDeposited { account_id, amount, transaction_id } => {
                    // Update account balance
                    if let Some(mut account_proj) = self.projections.get_account(*account_id).await
                        .map_err(|_| AccountError::NotFound)? {
                        account_proj.balance += amount;
                        account_proj.updated_at = Utc::now();
                        self.projections.upsert_account(&account_proj).await
                            .map_err(|_| AccountError::NotFound)?;
                    }

                    // Add transaction record
                    let transaction = TransactionProjection {
                        id: *transaction_id,
                        account_id: *account_id,
                        transaction_type: "deposit".to_string(),
                        amount: *amount,
                        timestamp: Utc::now(),
                    };
                    self.projections.insert_transaction(&transaction).await
                        .map_err(|_| AccountError::NotFound)?;
                }
                AccountEvent::MoneyWithdrawn { account_id, amount, transaction_id } => {
                    // Update account balance
                    if let Some(mut account_proj) = self.projections.get_account(*account_id).await
                        .map_err(|_| AccountError::NotFound)? {
                        account_proj.balance -= amount;
                        account_proj.updated_at = Utc::now();
                        self.projections.upsert_account(&account_proj).await
                            .map_err(|_| AccountError::NotFound)?;
                    }

                    // Add transaction record
                    let transaction = TransactionProjection {
                        id: *transaction_id,
                        account_id: *account_id,
                        transaction_type: "withdrawal".to_string(),
                        amount: *amount,
                        timestamp: Utc::now(),
                    };
                    self.projections.insert_transaction(&transaction).await
                        .map_err(|_| AccountError::NotFound)?;
                }
                _ => {} // Handle other events as needed
            }
        }

        Ok(())
    }
}