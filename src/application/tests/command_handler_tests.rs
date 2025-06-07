#![allow(clippy::arc_with_non_send_sync)] // Allow for mocks if they aren't Send/Sync

use super::super::handlers::AccountCommandHandler;
use crate::domain::{Account, AccountCommand, AccountEvent, AccountError};
use crate::infrastructure::repository::AccountRepositoryTrait; // Import the trait
use super::common_mocks::MockAccountRepository; // Use common mock
use mockall::predicate::*;
// use mockall::*; // mockall import is still needed for predicate and other macros if used
use rust_decimal::Decimal;
use std::sync::Arc;
use uuid::Uuid;
use anyhow::Result;

// Removed local mock definition

#[tokio::test]
async fn test_handle_create_account_success() {
    let mut mock_repo = MockAccountRepository::new();
    let owner_name = "Test Owner".to_string();
    let initial_balance = Decimal::new(100, 0);

    mock_repo.expect_save()
        .withf(move |account, events| {
            // For CreateAccount, the temp_account_for_save has version 0.
            account.version == 0 &&
            events.len() == 1 &&
            matches!(&events[0], AccountEvent::AccountCreated { owner_name: o, initial_balance: i, .. } if *o == owner_name && *i == initial_balance)
        })
        .times(1)
        .returning(|_, _| Ok(()));

    let handler = AccountCommandHandler::new(Arc::new(mock_repo));
    let result = handler.handle_create_account(owner_name.clone(), initial_balance).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_handle_deposit_money_success() {
    let account_id = Uuid::new_v4();
    let existing_balance = Decimal::new(100, 0);
    let deposit_amount = Decimal::new(50, 0);

    let mut mock_repo = MockAccountRepository::new();
    let existing_account = Account::new(account_id, "Test User".to_string(), existing_balance).unwrap();
    let expected_version_at_save = existing_account.version; // Version before new events

    mock_repo.expect_get_by_id()
        .with(eq(account_id))
        .times(1)
        .returning(move |_| Ok(Some(existing_account.clone())));

    mock_repo.expect_save()
        .withf(move |account_at_save, events| {
            // Account passed to save is the one fetched, BEFORE new events are applied to its state by handler.
            // Its version and balance should be the original ones.
            account_at_save.id == account_id &&
            account_at_save.version == expected_version_at_save &&
            account_at_save.balance == existing_balance &&
            events.len() == 1 &&
            matches!(&events[0], AccountEvent::MoneyDeposited { amount, .. } if *amount == deposit_amount)
        })
        .times(1)
        .returning(|_, _| Ok(()));

    let handler = AccountCommandHandler::new(Arc::new(mock_repo));
    let result = handler.handle_deposit_money(account_id, deposit_amount).await;
    assert!(result.is_ok());
    let events_returned = result.unwrap();
    assert_eq!(events_returned.len(), 1);
    // Further event content checks are good.
}

#[tokio::test]
async fn test_handle_deposit_money_account_not_found() {
    let account_id = Uuid::new_v4();
    let deposit_amount = Decimal::new(50, 0);
    let mut mock_repo = MockAccountRepository::new();

    mock_repo.expect_get_by_id()
        .with(eq(account_id))
        .times(1)
        .returning(|_| Ok(None));

    mock_repo.expect_save().times(0);

    let handler = AccountCommandHandler::new(Arc::new(mock_repo));
    let result = handler.handle_deposit_money(account_id, deposit_amount).await;
    assert!(result.is_err());
    matches!(result.unwrap_err(), AccountError::NotFound);
}

#[tokio::test]
async fn test_handle_withdraw_money_success() {
    let account_id = Uuid::new_v4();
    let initial_balance = Decimal::new(100, 0);
    let withdraw_amount = Decimal::new(30, 0);
    let mut mock_repo = MockAccountRepository::new();
    let account_to_fetch = Account::new(account_id, "Test Withdraw".to_string(), initial_balance).unwrap();
    let expected_version_at_save = account_to_fetch.version;

    mock_repo.expect_get_by_id()
        .with(eq(account_id))
        .times(1)
        .returning(move |_| Ok(Some(account_to_fetch.clone())));

    mock_repo.expect_save()
        .withf(move |account_at_save, events| {
            account_at_save.id == account_id &&
            account_at_save.version == expected_version_at_save &&
            account_at_save.balance == initial_balance &&
            events.len() == 1 &&
            matches!(&events[0], AccountEvent::MoneyWithdrawn { amount, .. } if *amount == withdraw_amount)
        })
        .times(1)
        .returning(|_,_| Ok(()));

    let handler = AccountCommandHandler::new(Arc::new(mock_repo));
    let result = handler.handle_withdraw_money(account_id, withdraw_amount).await;
    assert!(result.is_ok());
    // Event content checks are good.
}

#[tokio::test]
async fn test_handle_withdraw_money_insufficient_funds() {
    let account_id = Uuid::new_v4();
    let initial_balance = Decimal::new(20, 0);
    let withdraw_amount = Decimal::new(50, 0);
    let mut mock_repo = MockAccountRepository::new();
    let account = Account::new(account_id, "Test Insufficient".to_string(), initial_balance).unwrap();

    mock_repo.expect_get_by_id()
        .with(eq(account_id))
        .times(1)
        .returning(move |_| Ok(Some(account.clone())));

    mock_repo.expect_save().times(0);

    let handler = AccountCommandHandler::new(Arc::new(mock_repo));
    let result = handler.handle_withdraw_money(account_id, withdraw_amount).await;
    assert!(result.is_err());
    matches!(result.unwrap_err(), AccountError::InsufficientFunds { .. });
}

#[tokio::test]
async fn test_handle_close_account_success() {
    let account_id = Uuid::new_v4();
    let mut mock_repo = MockAccountRepository::new();
    let account_to_fetch = Account::new(account_id, "Test Close".to_string(), Decimal::new(100,0)).unwrap();
    let expected_version_at_save = account_to_fetch.version;
    let initial_balance = account_to_fetch.balance;
    let reason_to_close = "Closing test".to_string();

    mock_repo.expect_get_by_id()
        .with(eq(account_id))
        .times(1)
        .returning(move |_| Ok(Some(account_to_fetch.clone())));

    mock_repo.expect_save()
        .withf(move |account_at_save, events| {
            account_at_save.id == account_id &&
            account_at_save.version == expected_version_at_save &&
            account_at_save.balance == initial_balance &&
            account_at_save.is_active && // Account is still active when passed to save
            events.len() == 1 &&
            matches!(&events[0], AccountEvent::AccountClosed { reason: r, .. } if *r == reason_to_close)
        })
        .times(1)
        .returning(|_,_| Ok(()));

    let handler = AccountCommandHandler::new(Arc::new(mock_repo));
    let result = handler.handle_close_account(account_id, "Closing test".to_string()).await;
    assert!(result.is_ok());
    // Event content checks are good.
}

#[tokio::test]
async fn test_handle_deposit_money_repository_save_error() {
    let account_id = Uuid::new_v4();
    let existing_balance = Decimal::new(100, 0);
    let deposit_amount = Decimal::new(50, 0);

    let mut mock_repo = MockAccountRepository::new();
    let existing_account = Account::new(account_id, "Test User".to_string(), existing_balance).unwrap();

    mock_repo.expect_get_by_id()
        .with(eq(account_id))
        .times(1)
        .returning(move |_| Ok(Some(existing_account.clone())));

    mock_repo.expect_save()
        .times(1)
        .returning(|_, _| Err(anyhow::anyhow!("Simulated repo save error")));

    let handler = AccountCommandHandler::new(Arc::new(mock_repo));
    let result = handler.handle_deposit_money(account_id, deposit_amount).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        AccountError::InfrastructureError(msg) => {
            assert!(msg.contains("Simulated repo save error"));
        }
        e => panic!("Expected AccountError::InfrastructureError, got {:?}", e),
    }
}
