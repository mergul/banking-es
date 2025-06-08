// src/infrastructure/cache.rs
use anyhow::Result;
use async_trait::async_trait;
use redis::{AsyncCommands, Client as RedisClient, RedisError}; // Renamed Client to RedisClient
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;

#[async_trait]
pub trait CacheService: Send + Sync + 'static {
    async fn get<T: DeserializeOwned + Send>(&self, key: &str) -> Result<Option<T>>;
    async fn set<T: Serialize + Send + Sync>(&self, key: &str, value: &T, expiration_secs: Option<usize>) -> Result<()>;
    async fn delete(&self, key: &str) -> Result<()>;
}

#[derive(Clone)]
pub struct RedisCacheService {
    client: Arc<RedisClient>, // Store Arc<RedisClient>
}

impl RedisCacheService {
    /// Creates a new RedisCacheService.
    /// The `redis_url` is expected to be provided from application configuration,
    /// e.g., from an environment variable like `REDIS_URL`.
    /// Example URL: "redis://127.0.0.1/"
    pub fn new(redis_url: &str) -> Result<Self> {
        let client = RedisClient::open(redis_url)?;
        Ok(Self { client: Arc::new(client) })
    }
}

#[async_trait]
impl CacheService for RedisCacheService {
    async fn get<T: DeserializeOwned + Send>(&self, key: &str) -> Result<Option<T>> {
        let mut conn = self.client.get_async_connection().await?;
        let result: Option<String> = conn.get(key).await?;
        match result {
            Some(s) => Ok(Some(serde_json::from_str(&s)?)),
            None => Ok(None),
        }
    }

    async fn set<T: Serialize + Send + Sync>(&self, key: &str, value: &T, expiration_secs: Option<usize>) -> Result<()> {
        let mut conn = self.client.get_async_connection().await?;
        let s = serde_json::to_string(value)?;
        if let Some(secs) = expiration_secs {
            conn.set_ex(key, s, secs).await?;
        } else {
            conn.set(key, s).await?;
        }
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let mut conn = self.client.get_async_connection().await?;
        conn.del(key).await?;
        Ok(())
    }
}

// Basic error mapping (can be expanded)
impl From<RedisError> for anyhow::Error {
    fn from(err: RedisError) -> Self {
        anyhow::anyhow!("Redis error: {}", err)
    }
}
