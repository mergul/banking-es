mod domain;
mod infrastructure;
mod application;
mod web;

use axum::Router;
use sqlx::PgPool;
use std::env; // Already here, but good to confirm for REDIS_URL
use std::sync::Arc; // Added
use tower::ServiceBuilder;
use tracing_subscriber;

use banking_es::{
    infrastructure::{
        EventStore,
        ProjectionStore,
        AccountRepository, // Still need the concrete type for instantiation
        repository::AccountRepositoryTrait, // Added trait
        cache::RedisCacheService, // Added cache service
        CacheService, // Added CacheService trait for type hint if needed, though concrete RedisCacheService is used for init
    },
    application::{
        AccountService,
        AccountCommandHandler, // Added
        AccountQueryHandler,   // Added
    },
    web::create_routes,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres:password@localhost:5432/banking_es".to_string());

    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string()); // Added REDIS_URL

    let pool = PgPool::connect(&database_url).await?;

    // Run migrations
    sqlx::migrate!("./migrations").run(&pool).await?;

    // Initialize services
    let event_store = EventStore::new(pool.clone());
    let projection_store = ProjectionStore::new(pool.clone());

    // Repository
    let repository_impl = AccountRepository::new(event_store); // EventStore should be Clone or Arc-ed if EventStore itself is not Clone
    let repository: Arc<dyn AccountRepositoryTrait> = Arc::new(repository_impl);

    // Cache Service
    let cache_service: Arc<dyn CacheService> = Arc::new(RedisCacheService::new(&redis_url)?); // Use ? for Result

    // Handlers
    let command_handler = Arc::new(AccountCommandHandler::new(repository.clone(), cache_service.clone()));
    let query_handler = Arc::new(AccountQueryHandler::new(repository.clone(), cache_service.clone()));

    // Application Service
    let service = AccountService::new(command_handler, query_handler, projection_store);

    // Create routes
    let app = create_routes(service);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    println!("Server running on http://0.0.0.0:3000");

    axum::serve(listener, app).await?;

    Ok(())
}