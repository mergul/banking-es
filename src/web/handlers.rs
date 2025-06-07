use crate::application::AccountService;
use crate::infrastructure::{AccountProjection, TransactionProjection};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Semaphore;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct CreateAccountRequest {
    pub owner_name: String,
    pub initial_balance: Decimal,
}

#[derive(Debug, Serialize)]
pub struct CreateAccountResponse {
    pub account_id: Uuid,
}

#[derive(Debug, Deserialize)]
pub struct TransactionRequest {
    pub amount: Decimal,
}

#[derive(Debug, Deserialize)]
pub struct BatchTransactionRequest {
    pub transactions: Vec<SingleTransaction>,
}

#[derive(Debug, Deserialize)]
pub struct SingleTransaction {
    pub account_id: Uuid,
    pub amount: Decimal,
    pub transaction_type: String, // "deposit" or "withdraw"
}

#[derive(Debug, Serialize)]
pub struct BatchTransactionResponse {
    pub successful: usize,
    pub failed: usize,
    pub errors: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

// Rate limiting per endpoint
#[derive(Clone)]
pub struct RateLimitedService {
    service: AccountService,
    request_limiter: Arc<Semaphore>,
}

impl RateLimitedService {
    pub fn new(service: AccountService, max_requests: usize) -> Self {
        Self {
            service,
            request_limiter: Arc::new(Semaphore::new(max_requests)),
        }
    }
}

pub async fn create_account(
    State(rate_limited): State<RateLimitedService>,
    Json(request): Json<CreateAccountRequest>,
) -> Result<Json<CreateAccountResponse>, (StatusCode, Json<ErrorResponse>)> {
    let _permit = rate_limited.request_limiter.acquire().await.unwrap();

    match rate_limited
        .service
        .create_account(request.owner_name, request.initial_balance)
        .await
    {
        Ok(account_id) => Ok(Json(CreateAccountResponse { account_id })),
        Err(e) => Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

pub async fn get_account(
    State(rate_limited): State<RateLimitedService>,
    Path(account_id): Path<Uuid>,
) -> Result<Json<crate::infrastructure::AccountProjection>, (StatusCode, Json<ErrorResponse>)> {
    let _permit = rate_limited.request_limiter.acquire().await.unwrap();

    match rate_limited.service.get_account(account_id).await {
        Ok(Some(account)) => Ok(Json(account)),
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "Account not found".to_string(),
            }),
        )),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

// FIXED: Changed from AccountService to RateLimitedService
pub async fn get_all_accounts(
    State(rate_limited): State<RateLimitedService>,
) -> Result<Json<Vec<AccountProjection>>, (StatusCode, Json<ErrorResponse>)> {
    let _permit = rate_limited.request_limiter.acquire().await.unwrap();

    match rate_limited.service.get_all_accounts().await {
        Ok(accounts) => Ok(Json(accounts)),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

pub async fn deposit_money(
    State(rate_limited): State<RateLimitedService>,
    Path(account_id): Path<Uuid>,
    Json(request): Json<TransactionRequest>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let _permit = rate_limited.request_limiter.acquire().await.unwrap();

    match rate_limited
        .service
        .deposit_money(account_id, request.amount)
        .await
    {
        Ok(()) => Ok(StatusCode::OK),
        Err(e) => Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

pub async fn withdraw_money(
    State(rate_limited): State<RateLimitedService>,
    Path(account_id): Path<Uuid>,
    Json(request): Json<TransactionRequest>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let _permit = rate_limited.request_limiter.acquire().await.unwrap();

    match rate_limited
        .service
        .withdraw_money(account_id, request.amount)
        .await
    {
        Ok(()) => Ok(StatusCode::OK),
        Err(e) => Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

// New batch processing endpoint for high throughput
pub async fn batch_transactions(
    State(rate_limited): State<RateLimitedService>,
    Json(request): Json<BatchTransactionRequest>,
) -> Result<Json<BatchTransactionResponse>, (StatusCode, Json<ErrorResponse>)> {
    let _permit = rate_limited.request_limiter.acquire().await.unwrap();

    let mut successful = 0;
    let mut failed = 0;
    let mut errors = Vec::new();

    // Process transactions concurrently with bounded parallelism
    let semaphore = Arc::new(Semaphore::new(100)); // Max 100 concurrent transactions
    let service = Arc::new(rate_limited.service.clone());

    let mut tasks = Vec::new();

    for transaction in request.transactions {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let service_clone = Arc::clone(&service);

        let task = tokio::spawn(async move {
            let _permit = permit;

            let result = match transaction.transaction_type.as_str() {
                "deposit" => {
                    service_clone
                        .deposit_money(transaction.account_id, transaction.amount)
                        .await
                }
                "withdraw" => {
                    service_clone
                        .withdraw_money(transaction.account_id, transaction.amount)
                        .await
                }
                _ => Err(crate::domain::AccountError::InvalidAmount(
                    transaction.amount,
                )),
            };

            (transaction.account_id, result)
        });

        tasks.push(task);
    }

    // Wait for all tasks to complete
    for task in tasks {
        match task.await {
            Ok((account_id, Ok(()))) => successful += 1,
            Ok((account_id, Err(e))) => {
                failed += 1;
                errors.push(format!("Account {}: {}", account_id, e));
            }
            Err(e) => {
                failed += 1;
                errors.push(format!("Task error: {}", e));
            }
        }
    }

    Ok(Json(BatchTransactionResponse {
        successful,
        failed,
        errors,
    }))
}

pub async fn get_account_transactions(
    State(rate_limited): State<RateLimitedService>,
    Path(account_id): Path<Uuid>,
) -> Result<
    Json<Vec<crate::infrastructure::TransactionProjection>>,
    (StatusCode, Json<ErrorResponse>),
> {
    let _permit = rate_limited.request_limiter.acquire().await.unwrap();

    match rate_limited
        .service
        .get_account_transactions(account_id)
        .await
    {
        Ok(transactions) => Ok(Json(transactions)),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

// Health check endpoint for load balancers
pub async fn health_check() -> StatusCode {
    StatusCode::OK
}

// Metrics endpoint for monitoring
pub async fn metrics(
    State(rate_limited): State<RateLimitedService>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    // Return current system metrics
    let available_permits = rate_limited.request_limiter.available_permits();

    let metrics = serde_json::json!({
        "available_request_permits": available_permits,
        "timestamp": chrono::Utc::now(),
        "status": "healthy"
    });

    Ok(Json(metrics))
}
