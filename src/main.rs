use axum::{
    routing::{get, post},
    Router,
};
use std::{net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::{compression::CompressionLayer, cors::CorsLayer, trace::TraceLayer};
use tracing::{info, Level};
use tracing_subscriber;

mod application;
mod domain;
mod infrastructure;
mod web;

use crate::application::AccountService;
use crate::infrastructure::{AccountRepository, EventStore, ProjectionStore};
use crate::web::handlers::RateLimitedService;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for observability
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    info!("Starting high-performance banking service");

    // Configuration for high throughput
    let config = AppConfig {
        database_pool_size: 50,
        max_concurrent_operations: 1000,
        max_requests_per_second: 2000,
        batch_flush_interval_ms: 100,
        cache_size: 10000,
        port: 3000,
    };

    // Initialize infrastructure with optimized settings
    let event_store = EventStore::new_with_pool_size(config.database_pool_size).await?;
    let projection_store = ProjectionStore::new_with_pool_size(config.database_pool_size).await?;

    // Create optimized repository
    let repository = Arc::new(AccountRepository::new(event_store));

    // Create high-performance service
    let account_service = AccountService::new(repository, projection_store);

    // Wrap with rate limiting
    let rate_limited_service =
        RateLimitedService::new(account_service, config.max_requests_per_second);

    // Build the router with optimized middleware stack
    let app = Router::new()
        // Account operations
        .route("/accounts", post(web::handlers::create_account))
        .route("/accounts/:id", get(web::handlers::get_account))
        .route("/accounts/:id/deposit", post(web::handlers::deposit_money))
        .route(
            "/accounts/:id/withdraw",
            post(web::handlers::withdraw_money),
        )
        .route(
            "/accounts/:id/transactions",
            get(web::handlers::get_account_transactions),
        )
        // Batch operations for high throughput
        .route(
            "/batch/transactions",
            post(web::handlers::batch_transactions),
        )
        // Health and metrics
        .route("/health", get(web::handlers::health_check))
        .route("/metrics", get(web::handlers::metrics))
        // Add optimized middleware stack
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(CompressionLayer::new())
                .layer(CorsLayer::permissive())
                .into_inner(),
        )
        .with_state(rate_limited_service);

    // Setup TCP listener with optimized settings
    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    let listener = TcpListener::bind(addr).await?;

    // Configure TCP settings for high throughput
    configure_tcp_listener(&listener)?;

    info!("Server running on {}", addr);
    info!("Configuration: {:?}", config);

    // Start the server
    axum::serve(listener, app).await?;

    Ok(())
}
#[derive(Debug)]
struct AppConfig {
    database_pool_size: u32,
    max_concurrent_operations: usize,
    max_requests_per_second: usize,
    batch_flush_interval_ms: u64,
    cache_size: usize,
    port: u16,
}
fn configure_tcp_listener(listener: &TcpListener) -> Result<(), Box<dyn std::error::Error>> {
    use libc::{setsockopt, IPPROTO_TCP, SOL_SOCKET, SO_REUSEADDR, SO_REUSEPORT, TCP_NODELAY};
    use std::os::unix::io::AsRawFd;

    let fd = listener.as_raw_fd();
    let optval: libc::c_int = 1;

    unsafe {
        // Enable SO_REUSEADDR for faster restarts
        setsockopt(
            fd,
            SOL_SOCKET,
            SO_REUSEADDR,
            &optval as *const _ as *const libc::c_void,
            std::mem::size_of_val(&optval) as libc::socklen_t,
        );

        // Enable SO_REUSEPORT for load balancing across multiple processes
        setsockopt(
            fd,
            SOL_SOCKET,
            SO_REUSEPORT,
            &optval as *const _ as *const libc::c_void,
            std::mem::size_of_val(&optval) as libc::socklen_t,
        );

        // Enable TCP_NODELAY for low latency
        setsockopt(
            fd,
            IPPROTO_TCP,
            TCP_NODELAY,
            &optval as *const _ as *const libc::c_void,
            std::mem::size_of_val(&optval) as libc::socklen_t,
        );
    }

    Ok(())
}
