use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use rust_decimal::Decimal;
use crate::application::AccountService;

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

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

pub async fn create_account(
    State(service): State<AccountService>,
    Json(request): Json<CreateAccountRequest>,
) -> Result<Json<CreateAccountResponse>, (StatusCode, Json<ErrorResponse>)> {
    match service.create_account(request.owner_name, request.initial_balance).await {
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
    State(service): State<AccountService>,
    Path(account_id): Path<Uuid>,
) -> Result<Json<crate::infrastructure::AccountProjection>, (StatusCode, Json<ErrorResponse>)> {
    match service.get_account(account_id).await {
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

pub async fn get_all_accounts(
    State(service): State<AccountService>,
) -> Result<Json<Vec<crate::infrastructure::AccountProjection>>, (StatusCode, Json<ErrorResponse>)> {
    match service.get_all_accounts().await {
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
    State(service): State<AccountService>,
    Path(account_id): Path<Uuid>,
    Json(request): Json<TransactionRequest>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    match service.deposit_money(account_id, request.amount).await {
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
    State(service): State<AccountService>,
    Path(account_id): Path<Uuid>,
    Json(request): Json<TransactionRequest>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    match service.withdraw_money(account_id, request.amount).await {
        Ok(()) => Ok(StatusCode::OK),
        Err(e) => Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

pub async fn get_account_transactions(
    State(service): State<AccountService>,
    Path(account_id): Path<Uuid>,
) -> Result<Json<Vec<crate::infrastructure::TransactionProjection>>, (StatusCode, Json<ErrorResponse>)> {
    match service.get_account_transactions(account_id).await {
        Ok(transactions) => Ok(Json(transactions)),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}