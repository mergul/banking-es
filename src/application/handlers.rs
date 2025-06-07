use uuid::Uuid;
use anyhow::Result;
use crate::domain::{Account, AccountCommand, AccountEvent, AccountError};
use crate::infrastructure::AccountRepository;
use rust_decimal::Decimal;

#[derive(Clone)]
pub struct AccountCommandHandler {
    repository: AccountRepository,
}

impl AccountCommandHandler {
    pub fn new(repository: AccountRepository) -> Self {
        Self { repository }
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

        let account = Account::default();
        let events = account.handle_command(&command)?;

        self.repository.save(&account, events).await
            .map_err(|_| AccountError::NotFound)?;

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

        self.repository.save(&account, events.clone()).await
            .map_err(|_| AccountError::NotFound)?;

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
            .map_err(|_| AccountError::NotFound)?;

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
            .map_err(|_| AccountError::NotFound)?;

        Ok(events)
    }
}

#[derive(Clone)]
pub struct AccountQueryHandler {
    repository: AccountRepository,
}

impl AccountQueryHandler {
    pub fn new(repository: AccountRepository) -> Self {
        Self { repository }
    }

    pub async fn get_account_by_id(&self, account_id: Uuid) -> Result<Option<Account>, AccountError> {
        self.repository.get_by_id(account_id).await
    }

    pub async fn get_account_balance(&self, account_id: Uuid) -> Result<Decimal, AccountError> {
        let account = self.repository.get_by_id(account_id).await?
            .ok_or(AccountError::NotFound)?;
        Ok(account.balance)
    }

    pub async fn is_account_active(&self, account_id: Uuid) -> Result<bool, AccountError> {
        let account = self.repository.get_by_id(account_id).await?
            .ok_or(AccountError::NotFound)?;
        Ok(account.is_active)
    }
}