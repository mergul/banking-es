use uuid::Uuid;
use anyhow::Result;
use crate::domain::{Account, AccountEvent, AccountError};
use crate::infrastructure::EventStore;

#[derive(Clone)]
pub struct AccountRepository {
    event_store: EventStore,
}

impl AccountRepository {
    pub fn new(event_store: EventStore) -> Self {
        Self { event_store }
    }

    pub async fn save(&self, account: &Account, events: Vec<AccountEvent>) -> Result<()> {
        self.event_store
            .save_events(account.id, events, account.version)
            .await?;
        Ok(())
    }

    pub async fn get_by_id(&self, id: Uuid) -> Result<Option<Account>, AccountError> {
        let events = self.event_store
            .get_events(id, None)
            .await
            .map_err(|_| AccountError::NotFound)?;

        if events.is_empty() {
            return Ok(None);
        }

        let mut account = Account::default();
        account.id = id;

        for event in events {
            let account_event: AccountEvent = serde_json::from_value(event.event_data)
                .map_err(|_| AccountError::NotFound)?;
            account.apply_event(&account_event);
        }

        Ok(Some(account))
    }
}