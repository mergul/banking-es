use uuid::Uuid;
use anyhow::Result;
use async_trait::async_trait; // Added
use crate::domain::{Account, AccountEvent, AccountError};
use crate::infrastructure::event_store::EventStore; // Made import more specific

// New Trait Definition
#[async_trait]
pub trait AccountRepositoryTrait: Send + Sync + 'static {
    async fn save(&self, account: &Account, events: Vec<AccountEvent>) -> Result<()>;
    async fn get_by_id(&self, id: Uuid) -> Result<Option<Account>, AccountError>;
}

#[derive(Clone)]
pub struct AccountRepository {
    event_store: EventStore,
}

impl AccountRepository {
    pub fn new(event_store: EventStore) -> Self {
        Self { event_store }
    }
    // save and get_by_id methods are now part of the trait implementation
}

// Implement the trait for AccountRepository
#[async_trait]
impl AccountRepositoryTrait for AccountRepository {
    async fn save(&self, account: &Account, events: Vec<AccountEvent>) -> Result<()> {
        self.event_store
            .save_events(account.id, events, account.version)
            .await?;
        Ok(())
    }

    async fn get_by_id(&self, id: Uuid) -> Result<Option<Account>, AccountError> {
        let stored_events_result = self.event_store.get_events(id, None).await;

        let stored_events = match stored_events_result {
            Ok(events) => events,
            Err(e) => {
                // Map the anyhow::Error from event_store to an AccountError variant
                return Err(AccountError::InfrastructureError(format!(
                    "Failed to retrieve events from store: {}",
                    e
                )));
            }
        };

        if stored_events.is_empty() {
            // This is a valid case where the account simply doesn't exist yet.
            return Ok(None);
        }

        let mut account = Account::default();
        account.id = id; // Set id, as Account::default() doesn't assign it.

        for event_wrapper_item in stored_events {
            let account_event: AccountEvent = serde_json::from_value(event_wrapper_item.event_data)
                .map_err(|e| {
                    AccountError::EventDeserializationError(format!("Failed to deserialize event: {}", e))
                })?;
            account.apply_event(&account_event);
        }
        Ok(Some(account))
    }
}