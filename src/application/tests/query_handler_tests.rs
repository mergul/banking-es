// src/application/tests/query_handler_tests.rs
#![allow(clippy::arc_with_non_send_sync)]

use super::super::handlers::AccountQueryHandler;
use crate::domain::{Account, AccountError};
use super::common_mocks::{MockAccountRepository, MockCacheService}; // Updated import
use mockall::predicate::*;
#[allow(unused_imports)]
use mockall::*;

use rust_decimal::Decimal;
use std::sync::Arc;
use uuid::Uuid;

#[tokio::test]
async fn test_get_account_by_id_success_cache_miss() { // Renamed
    let account_id = Uuid::new_v4();
    // Wrap mocks in Arc for the new method of instantiation if handler expects Arc<Mocks>
    // However, AccountQueryHandler::new takes Arc<dyn Trait>, so we pass Arc created from mock instance.
    let mut mock_repo_instance = MockAccountRepository::new();
    let mut mock_cache_instance = MockCacheService::new();

    let expected_account = Account::new(account_id, "Query User".to_string(), Decimal::new(100, 0)).unwrap();
    let cloned_account_for_repo = expected_account.clone();
    let cloned_account_for_cache_set = expected_account.clone();

    // Expect cache miss
    mock_cache_instance.expect_get::<Account>()
        .with(eq(format!("account:{}", account_id)))
        .times(1)
        .returning(|_| Ok(None));

    // Expect repo call
    mock_repo_instance.expect_get_by_id()
        .with(eq(account_id))
        .times(1)
        .returning(move |_| Ok(Some(cloned_account_for_repo.clone())));

    // Expect cache set
    mock_cache_instance.expect_set::<Account>()
        .withf(move |key, account_val, expiration| {
            key == format!("account:{}", account_id) &&
            account_val.id == cloned_account_for_cache_set.id &&
            expiration.is_some() && expiration.unwrap() == 3600
        })
        .times(1)
        .returning(|_, _, _| Ok(()));

    let query_handler = AccountQueryHandler::new(Arc::new(mock_repo_instance), Arc::new(mock_cache_instance));
    let result = query_handler.get_account_by_id(account_id).await;

    assert!(result.is_ok());
    let retrieved_account = result.unwrap();
    assert!(retrieved_account.is_some());
    assert_eq!(retrieved_account.unwrap().id, expected_account.id);
}

#[tokio::test]
async fn test_get_account_by_id_success_cache_hit() { // New test
    let account_id = Uuid::new_v4();
    let mut mock_cache_instance = MockCacheService::new();
    // Repo mock is still needed for AccountQueryHandler instantiation, but won't be called.
    let mut mock_repo_instance = MockAccountRepository::new();

    let expected_account = Account::new(account_id, "Cached User".to_string(), Decimal::new(200, 0)).unwrap();
    let cloned_account_for_cache_get = expected_account.clone();

    // Expect cache hit
    mock_cache_instance.expect_get::<Account>()
        .with(eq(format!("account:{}", account_id)))
        .times(1)
        .returning(move |_| Ok(Some(cloned_account_for_cache_get.clone())));

    // Repo should not be called
    mock_repo_instance.expect_get_by_id().times(0);
    // Cache set should not be called
    mock_cache_instance.expect_set::<Account>().times(0);

    let query_handler = AccountQueryHandler::new(Arc::new(mock_repo_instance), Arc::new(mock_cache_instance));
    let result = query_handler.get_account_by_id(account_id).await;

    assert!(result.is_ok());
    let retrieved_account = result.unwrap();
    assert!(retrieved_account.is_some());
    assert_eq!(retrieved_account.unwrap().id, expected_account.id);
}

#[tokio::test]
async fn test_get_account_by_id_not_found_after_cache_miss() { // Updated to include cache logic
    let account_id = Uuid::new_v4();
    let mut mock_repo_instance = MockAccountRepository::new();
    let mut mock_cache_instance = MockCacheService::new();

    // Expect cache miss
    mock_cache_instance.expect_get::<Account>()
        .with(eq(format!("account:{}", account_id)))
        .times(1)
        .returning(|_| Ok(None));

    // Expect repo call, returns None
    mock_repo_instance.expect_get_by_id()
        .with(eq(account_id))
        .times(1)
        .returning(|_| Ok(None));

    // Cache set should not be called if account not found in repo
    mock_cache_instance.expect_set::<Account>().times(0);

    let query_handler = AccountQueryHandler::new(Arc::new(mock_repo_instance), Arc::new(mock_cache_instance));
    let result = query_handler.get_account_by_id(account_id).await;

    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[tokio::test]
async fn test_get_account_by_id_repository_error_after_cache_miss() { // Updated
    let account_id = Uuid::new_v4();
    let mut mock_repo_instance = MockAccountRepository::new();
    let mut mock_cache_instance = MockCacheService::new();

    // Expect cache miss
    mock_cache_instance.expect_get::<Account>()
        .with(eq(format!("account:{}", account_id)))
        .times(1)
        .returning(|_| Ok(None));

    // Expect repo call, returns error
    mock_repo_instance.expect_get_by_id()
        .with(eq(account_id))
        .times(1)
        .returning(|_| Err(AccountError::InfrastructureError("Simulated DB error".to_string())));

    // Cache set should not be called if repo call fails
    mock_cache_instance.expect_set::<Account>().times(0);

    let query_handler = AccountQueryHandler::new(Arc::new(mock_repo_instance), Arc::new(mock_cache_instance));
    let result = query_handler.get_account_by_id(account_id).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        AccountError::InfrastructureError(msg) => assert_eq!(msg, "Simulated DB error"),
        e => panic!("Expected InfrastructureError, got {:?}", e),
    }
}

// Tests for get_account_balance and is_account_active
// These will now use the updated get_account_by_id, so their mocking strategy needs to reflect that.
// The mocks for get_by_id (repo) and get/set (cache) will be triggered by calls to these other query methods.

#[tokio::test]
async fn test_get_account_balance_success() {
    let account_id = Uuid::new_v4();
    let mut mock_repo = MockAccountRepository::new(); // Renamed for clarity
    let mut mock_cache = MockCacheService::new();   // Renamed for clarity
    let expected_balance = Decimal::new(123, 45);
    let account = Account {
        id: account_id,
        owner_name: "Balance Test".to_string(),
        balance: expected_balance,
        is_active: true,
        version: 1,
    };
    let cloned_account_for_repo = account.clone();
    let cloned_account_for_cache_set = account.clone();

    // Cache miss
    mock_cache.expect_get::<Account>().times(1).returning(|_| Ok(None));
    // Repo hit
    mock_repo.expect_get_by_id().times(1).returning(move |_| Ok(Some(cloned_account_for_repo.clone())));
    // Cache set
    mock_cache.expect_set::<Account>().times(1).returning(|_,_,_| Ok(()));


    let query_handler = AccountQueryHandler::new(Arc::new(mock_repo), Arc::new(mock_cache));
    let result = query_handler.get_account_balance(account_id).await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), expected_balance);
}

#[tokio::test]
async fn test_get_account_balance_not_found() {
    let account_id = Uuid::new_v4();
    let mut mock_repo = MockAccountRepository::new();
    let mut mock_cache = MockCacheService::new();

    // Cache miss
    mock_cache.expect_get::<Account>().times(1).returning(|_| Ok(None));
    // Repo returns None
    mock_repo.expect_get_by_id().times(1).returning(|_| Ok(None));
    // Cache set not called
    mock_cache.expect_set::<Account>().times(0);


    let query_handler = AccountQueryHandler::new(Arc::new(mock_repo), Arc::new(mock_cache));
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
    let mut mock_cache = MockCacheService::new();
    let account = Account {
        id: account_id,
        owner_name: "Active Test".to_string(),
        balance: Decimal::ZERO,
        is_active: true,
        version: 1,
    };
    let cloned_account_for_repo = account.clone();

    // Cache miss
    mock_cache.expect_get::<Account>().times(1).returning(|_| Ok(None));
    // Repo hit
    mock_repo.expect_get_by_id().times(1).returning(move |_| Ok(Some(cloned_account_for_repo.clone())));
    // Cache set
    mock_cache.expect_set::<Account>().times(1).returning(|_,_,_| Ok(()));

    let query_handler = AccountQueryHandler::new(Arc::new(mock_repo), Arc::new(mock_cache));
    let result = query_handler.is_account_active(account_id).await;

    assert!(result.is_ok());
    assert!(result.unwrap());
}

#[tokio::test]
async fn test_is_account_active_success_inactive() {
    let account_id = Uuid::new_v4();
    let mut mock_repo = MockAccountRepository::new();
    let mut mock_cache = MockCacheService::new();
    let account = Account {
        id: account_id,
        owner_name: "Inactive Test".to_string(),
        balance: Decimal::ZERO,
        is_active: false,
        version: 2,
    };
    let cloned_account_for_repo = account.clone();

    // Cache miss
    mock_cache.expect_get::<Account>().times(1).returning(|_| Ok(None));
    // Repo hit
    mock_repo.expect_get_by_id().times(1).returning(move |_| Ok(Some(cloned_account_for_repo.clone())));
    // Cache set
    mock_cache.expect_set::<Account>().times(1).returning(|_,_,_| Ok(()));


    let query_handler = AccountQueryHandler::new(Arc::new(mock_repo), Arc::new(mock_cache));
    let result = query_handler.is_account_active(account_id).await;

    assert!(result.is_ok());
    assert!(!result.unwrap());
}

#[tokio::test]
async fn test_is_account_active_not_found() {
    let account_id = Uuid::new_v4();
    let mut mock_repo = MockAccountRepository::new();
    let mut mock_cache = MockCacheService::new();

    // Cache miss
    mock_cache.expect_get::<Account>().times(1).returning(|_| Ok(None));
    // Repo returns None
    mock_repo.expect_get_by_id().times(1).returning(|_| Ok(None));
    // Cache set not called
    mock_cache.expect_set::<Account>().times(0);

    let query_handler = AccountQueryHandler::new(Arc::new(mock_repo), Arc::new(mock_cache));
    let result = query_handler.is_account_active(account_id).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        AccountError::NotFound => {}
        e => panic!("Expected AccountError::NotFound, got {:?}", e),
    }
}
