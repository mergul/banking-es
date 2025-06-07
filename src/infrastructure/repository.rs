use crate::domain::{Account, AccountError, AccountEvent};
use crate::infrastructure::event_store::EventStore;
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{interval, Duration, Interval};
use uuid::Uuid; // Added

// New Trait Definition
#[async_trait]
pub trait AccountRepositoryTrait: Send + Sync + 'static {
    async fn save(&self, account: &Account, events: Vec<AccountEvent>) -> Result<()>;
    async fn get_by_id(&self, id: Uuid) -> Result<Option<Account>, AccountError>;
    async fn save_batched(&self, account_id: Uuid, events: Vec<AccountEvent>) -> Result<()>;
    async fn save_immediate(&self, account: &Account, events: Vec<AccountEvent>) -> Result<()>;
    async fn flush_all(&self) -> Result<()>;
    fn start_batch_flush_task(&self);
}

#[derive(Clone)]
pub struct AccountRepository {
    event_store: EventStore,
    // Write-behind cache for batching events
    pending_events: Arc<Mutex<HashMap<Uuid, Vec<AccountEvent>>>>,
    // Read-through cache for accounts
    account_cache: Arc<RwLock<HashMap<Uuid, Arc<Account>>>>,
    // Batch flush interval
    flush_interval: Duration,
}

impl AccountRepository {
    pub fn new(event_store: EventStore) -> Self {
        let repo = Self {
            event_store,
            pending_events: Arc::new(Mutex::new(HashMap::new())),
            account_cache: Arc::new(RwLock::new(HashMap::new())),
            flush_interval: Duration::from_millis(100), // Flush every 100ms
        };

        // Start background flush task
        repo.start_batch_flush_task();
        repo
    }
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
    // Batch events for better throughput
    async fn save_batched(&self, account_id: Uuid, events: Vec<AccountEvent>) -> Result<()> {
        let mut pending = self.pending_events.lock().await;
        pending
            .entry(account_id)
            .or_insert_with(Vec::new)
            .extend(events);

        // Invalidate cache for this account
        let mut cache = self.account_cache.write().await;
        cache.remove(&account_id);

        Ok(())
    }

    // Immediate save for critical operations
    async fn save_immediate(&self, account: &Account, events: Vec<AccountEvent>) -> Result<()> {
        self.event_store
            .save_events(account.id, events, account.version)
            .await?;

        // Update cache
        let mut cache = self.account_cache.write().await;
        cache.insert(account.id, Arc::new(account.clone()));

        Ok(())
    }

    // Get account with caching
    async fn get_by_id(&self, id: Uuid) -> Result<Option<Account>, AccountError> {
        // Check cache first
        {
            let cache = self.account_cache.read().await;
            if let Some(account) = cache.get(&id) {
                return Ok(Some((**account).clone()));
            }
        }
        let stored_events_result = self.event_store.get_events(id, None).await;

        // Load from event store
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
            return Ok(None);
        }

        let mut account = Account::default();
        account.id = id;

        for event in stored_events {
            let account_event: AccountEvent =
                serde_json::from_value(event.event_data).map_err(|_| {
                    AccountError::InfrastructureError("Failed to deserialize event".to_string())
                })?;
            account.apply_event(&account_event);
        }

        // Cache the result
        let account_arc = Arc::new(account.clone());
        let mut cache = self.account_cache.write().await;
        cache.insert(id, account_arc);

        Ok(Some(account))
    }

    // Background task to flush batched events
    fn start_batch_flush_task(&self) {
        let pending_events = Arc::clone(&self.pending_events);
        let event_store = self.event_store.clone();
        let flush_interval = self.flush_interval;

        tokio::spawn(async move {
            let mut interval = interval(flush_interval);

            loop {
                interval.tick().await;

                let mut pending = pending_events.lock().await;
                if pending.is_empty() {
                    continue;
                }

                // Take all pending events
                let events_to_flush: HashMap<Uuid, Vec<AccountEvent>> = pending.drain().collect();
                drop(pending);

                // Flush in batches
                for (account_id, events) in events_to_flush {
                    if let Err(e) = event_store.save_events(account_id, events, 0).await {
                        eprintln!("Failed to flush events for account {}: {}", account_id, e);
                        // Could implement retry logic here
                    }
                }
            }
        });
    }

    // Force flush pending events (useful for shutdown)
    async fn flush_all(&self) -> Result<()> {
        let mut pending = self.pending_events.lock().await;
        let events_to_flush: HashMap<Uuid, Vec<AccountEvent>> = pending.drain().collect();
        drop(pending);

        for (account_id, events) in events_to_flush {
            self.event_store.save_events(account_id, events, 0).await?;
        }

        Ok(())
    }
}
