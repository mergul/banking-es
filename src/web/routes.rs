use axum::{
    routing::{get, post, put},
    Router,
};
use tower_http::cors::CorsLayer;
use crate::{application::AccountService, web::handlers::*};

pub fn create_routes(service: AccountService) -> Router {
    Router::new()
        .route("/accounts", post(create_account))
        .route("/accounts", get(get_all_accounts))
        .route("/accounts/:id", get(get_account))
        .route("/accounts/:id/deposit", put(deposit_money))
        .route("/accounts/:id/withdraw", put(withdraw_money))
        .route("/accounts/:id/transactions", get(get_account_transactions))
        .with_state(service)
        .layer(CorsLayer::permissive())
}