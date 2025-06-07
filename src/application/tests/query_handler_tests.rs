// src/application/tests/query_handler_tests.rs
#![allow(clippy::arc_with_non_send_sync)]

use super::super::handlers::AccountQueryHandler;
use crate::domain::{Account, AccountError}; // AccountError is needed for result checks
// Use the common mock for AccountRepositoryTrait
use super::common_mocks::MockAccountRepository;
use mockall::predicate::*;
// mockall crate itself is still needed for `eq` and other potential macros/types from it,
// though the mock! macro is no longer directly used here.
#[allow(unused_imports)]
use mockall::*; // Keep for `eq`, etc. Might be covered by `predicate::*` but explicit is fine.

use rust_decimal::Decimal;
use std::sync::Arc;
use uuid::Uuid;

#[tokio::test]
async fn test_get_account_by_id_success() {
    let account_id = Uuid::new_v4();
    let mut mock_repo = MockAccountRepository::new();
    let expected_account = Account::new(account_id, "Query User".to_string(), Decimal::new(100, 0)).unwrap();
    let cloned_account = expected_account.clone();

    mock_repo.expect_get_by_id()
        .with(eq(account_id))
        .times(1)
        .returning(move |_| Ok(Some(cloned_account.clone())));

    let query_handler = AccountQueryHandler::new(Arc::new(mock_repo));
    let result = query_handler.get_account_by_id(account_id).await;

    assert!(result.is_ok());
    let retrieved_account = result.unwrap();
    assert!(retrieved_account.is_some());
    assert_eq!(retrieved_account.unwrap().id, expected_account.id);
}

#[tokio::test]
async fn test_get_account_by_id_not_found() {
    let account_id = Uuid::new_v4();
    let mut mock_repo = MockAccountRepository::new();

    mock_repo.expect_get_by_id()
        .with(eq(account_id))
        .times(1)
        .returning(|_| Ok(None)); // Simulate account not found

    let query_handler = AccountQueryHandler::new(Arc::new(mock_repo));
    let result = query_handler.get_account_by_id(account_id).await;

    assert!(result.is_ok()); // The method itself returns Result<Option<Account>>
    assert!(result.unwrap().is_none());
}

#[tokio::test]
async fn test_get_account_by_id_repository_error() {
    let account_id = Uuid::new_v4();
    let mut mock_repo = MockAccountRepository::new();

    mock_repo.expect_get_by_id()
        .with(eq(account_id))
        .times(1)
        .returning(|_| Err(AccountError::InfrastructureError("Simulated DB error".to_string())));

    let query_handler = AccountQueryHandler::new(Arc::new(mock_repo));
    let result = query_handler.get_account_by_id(account_id).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        AccountError::InfrastructureError(msg) => assert_eq!(msg, "Simulated DB error"),
        e => panic!("Expected InfrastructureError, got {:?}", e),
    }
}

#[tokio::test]
async fn test_get_account_balance_success() {
    let account_id = Uuid::new_v4();
    let mut mock_repo = MockAccountRepository::new();
    let expected_balance = Decimal::new(123, 45);
    let account = Account {
        id: account_id,
        owner_name: "Balance Test".to_string(),
        balance: expected_balance,
        is_active: true,
        version: 1,
    };
    let cloned_account = account.clone();

    mock_repo.expect_get_by_id()
        .with(eq(account_id))
        .times(1)
        .returning(move |_| Ok(Some(cloned_account.clone())));

    let query_handler = AccountQueryHandler::new(Arc::new(mock_repo));
    let result = query_handler.get_account_balance(account_id).await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), expected_balance);
}

#[tokio::test]
async fn test_get_account_balance_not_found() {
    let account_id = Uuid::new_v4();
    let mut mock_repo = MockAccountRepository::new();

    mock_repo.expect_get_by_id()
        .with(eq(account_id))
        .times(1)
        .returning(|_| Ok(None));

    let query_handler = AccountQueryHandler::new(Arc::new(mock_repo));
    let result = query_handler.get_account_balance(account_id).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        AccountError::NotFound => {}
        e => panic!("Expected AccountError::NotFound, got {:?}", e),
    }
}

#[tokio::test]
async fn test_is_account_active_success_active() {
    let account_id = Uuid::new_v4();
    let mut mock_repo = MockAccountRepository::new();
    let account = Account {
        id: account_id,
        owner_name: "Active Test".to_string(),
        balance: Decimal::ZERO,
        is_active: true, // Active
        version: 1,
    };
    let cloned_account = account.clone();

    mock_repo.expect_get_by_id()
        .with(eq(account_id))
        .times(1)
        .returning(move |_| Ok(Some(cloned_account.clone())));

    let query_handler = AccountQueryHandler::new(Arc::new(mock_repo));
    let result = query_handler.is_account_active(account_id).await;

    assert!(result.is_ok());
    assert!(result.unwrap());
}

#[tokio::test]
async fn test_is_account_active_success_inactive() {
    let account_id = Uuid::new_v4();
    let mut mock_repo = MockAccountRepository::new();
    let account = Account {
        id: account_id,
        owner_name: "Inactive Test".to_string(),
        balance: Decimal::ZERO,
        is_active: false, // Inactive
        version: 2,
    };
    let cloned_account = account.clone();

    mock_repo.expect_get_by_id()
        .with(eq(account_id))
        .times(1)
        .returning(move |_| Ok(Some(cloned_account.clone())));

    let query_handler = AccountQueryHandler::new(Arc::new(mock_repo));
    let result = query_handler.is_account_active(account_id).await;

    assert!(result.is_ok());
    assert!(!result.unwrap()); // Expecting false
}

#[tokio::test]
async fn test_is_account_active_not_found() {
    let account_id = Uuid::new_v4();
    let mut mock_repo = MockAccountRepository::new();

    mock_repo.expect_get_by_id()
        .with(eq(account_id))
        .times(1)
        .returning(|_| Ok(None));

    let query_handler = AccountQueryHandler::new(Arc::new(mock_repo));
    let result = query_handler.is_account_active(account_id).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        AccountError::NotFound => {}
        e => panic!("Expected AccountError::NotFound, got {:?}", e),
    }
}
