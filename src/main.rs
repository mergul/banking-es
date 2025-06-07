mod domain;
mod infrastructure;
mod application;
mod web;

use axum::Router;
use sqlx::PgPool;
use std::env;
use tower::ServiceBuilder;
use tracing_subscriber;

use banking_es::{
    infrastructure::{EventStore, ProjectionStore, AccountRepository},
    application::AccountService,
    web::create_routes,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres:password@localhost:5432/banking_es".to_string());

    let pool = PgPool::connect(&database_url).await?;

    // Run migrations
    sqlx::migrate!("./migrations").run(&pool).await?;

    // Initialize services
    let event_store = EventStore::new(pool.clone());
    let projection_store = ProjectionStore::new(pool.clone());
    let repository = AccountRepository::new(event_store);
    let service = AccountService::new(repository, projection_store);

    // Create routes
    let app = create_routes(service);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    println!("Server running on http://0.0.0.0:3000");

    axum::serve(listener, app).await?;

    Ok(())
}