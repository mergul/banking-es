// src/application/tests/common_mocks.rs
use crate::domain::{Account, AccountEvent, AccountError};
use crate::infrastructure::repository::AccountRepositoryTrait;
use mockall::mock;
use uuid::Uuid;
use anyhow::Result; // For the save method's Result<()>
use async_trait::async_trait;


mock! {
    pub AccountRepository {} // Mock struct name
    #[async_trait]
    impl AccountRepositoryTrait for AccountRepository { // Trait being mocked
        async fn save(&self, account: &Account, events: Vec<AccountEvent>) -> Result<()>;
        async fn get_by_id(&self, id: Uuid) -> Result<Option<Account>, AccountError>;
    }
}

// Add MockCacheService
use crate::infrastructure::cache::CacheService; // Add this
use serde::{de::DeserializeOwned, Serialize};    // Add these for CacheService methods

mock! {
    pub CacheService {} // Mock struct name
    #[async_trait]
    impl CacheService for CacheService {
        async fn get<T: DeserializeOwned + Send>(&self, key: &str) -> anyhow::Result<Option<T>>;
        async fn set<T: Serialize + Send + Sync>(&self, key: &str, value: &T, expiration_secs: Option<usize>) -> anyhow::Result<()>;
        async fn delete(&self, key: &str) -> anyhow::Result<()>;
    }
}
