use crate::domain::{Account, AccountCommand, AccountEvent, AccountError};
use rust_decimal::Decimal;
use uuid::Uuid;

#[test]
fn test_account_new_valid_balance() {
    let id = Uuid::new_v4();
    let owner_name = "John Doe".to_string();
    let initial_balance = Decimal::new(100, 0);
    let account_result = Account::new(id, owner_name.clone(), initial_balance);
    assert!(account_result.is_ok());
    let account = account_result.unwrap();
    assert_eq!(account.id, id);
    assert_eq!(account.owner_name, owner_name);
    assert_eq!(account.balance, initial_balance);
    assert!(account.is_active);
    assert_eq!(account.version, 0);
}

#[test]
fn test_account_new_invalid_balance() {
    let id = Uuid::new_v4();
    let owner_name = "Jane Doe".to_string();
    let initial_balance = Decimal::new(-50, 0);
    let account_result = Account::new(id, owner_name, initial_balance);
    assert!(account_result.is_err());
    match account_result.unwrap_err() {
        AccountError::InvalidAmount(amount) => assert_eq!(amount, initial_balance),
        _ => panic!("Expected InvalidAmount error"),
    }
}

#[test]
fn test_handle_create_account_command_success() {
    let account_id = Uuid::new_v4();
    let owner_name = "Test User".to_string();
    let initial_balance = Decimal::new(1000, 2);
    let command = AccountCommand::CreateAccount {
        account_id,
        owner_name: owner_name.clone(),
        initial_balance,
    };
    let account = Account::default(); // Use default for command handling context
    let result = account.handle_command(&command);
    assert!(result.is_ok());
    let events = result.unwrap();
    assert_eq!(events.len(), 1);
    match &events[0] {
        AccountEvent::AccountCreated { account_id: event_id, owner_name: event_owner, initial_balance: event_balance } => {
            assert_eq!(*event_id, account_id);
            assert_eq!(*event_owner, owner_name);
            assert_eq!(*event_balance, initial_balance);
        }
        _ => panic!("Expected AccountCreated event"),
    }
}

#[test]
fn test_handle_create_account_command_negative_balance() {
    let account_id = Uuid::new_v4();
    let owner_name = "Test User".to_string();
    let initial_balance = Decimal::new(-100, 0); // Negative
    let command = AccountCommand::CreateAccount {
        account_id,
        owner_name,
        initial_balance,
    };
    let account = Account::default();
    let result = account.handle_command(&command);
    assert!(result.is_err());
    match result.unwrap_err() {
        AccountError::InvalidAmount(amount) => assert_eq!(amount, initial_balance),
        _ => panic!("Expected InvalidAmount error for negative initial balance"),
    }
}

#[test]
fn test_apply_account_created_event() {
    let mut account = Account::default();
    let account_id = Uuid::new_v4();
    let owner_name = "Alice".to_string();
    let initial_balance = Decimal::new(500, 0);
    let event = AccountEvent::AccountCreated {
        account_id,
        owner_name: owner_name.clone(),
        initial_balance,
    };
    account.id = account_id; // Set id before applying, as Account::new is not used here.
    account.apply_event(&event);

    assert_eq!(account.owner_name, owner_name);
    assert_eq!(account.balance, initial_balance);
    assert!(account.is_active);
    assert_eq!(account.version, 1);
}


#[test]
fn test_handle_deposit_money_success() {
    let account_id = Uuid::new_v4();
    let mut account = Account::new(account_id, "Test Depositor".to_string(), Decimal::new(100, 0)).unwrap();

    let deposit_amount = Decimal::new(50, 0);
    let command = AccountCommand::DepositMoney { account_id, amount: deposit_amount };
    let result = account.handle_command(&command);
    assert!(result.is_ok());
    let events = result.unwrap();
    assert_eq!(events.len(), 1);
    match &events[0] {
        AccountEvent::MoneyDeposited { account_id: ev_acc_id, amount: ev_amount, .. } => {
            assert_eq!(*ev_acc_id, account_id);
            assert_eq!(*ev_amount, deposit_amount);
        }
        _ => panic!("Expected MoneyDeposited event"),
    }

    // Test applying the event
    account.apply_event(&events[0]);
    assert_eq!(account.balance, Decimal::new(150, 0));
    assert_eq!(account.version, 1); // Version increments after initial new()
}

#[test]
fn test_handle_deposit_money_zero_amount() {
    let account_id = Uuid::new_v4();
    let account = Account::new(account_id, "Test Depositor".to_string(), Decimal::new(100, 0)).unwrap();
    let command = AccountCommand::DepositMoney { account_id, amount: Decimal::ZERO };
    let result = account.handle_command(&command);
    assert!(result.is_err());
    match result.unwrap_err() {
        AccountError::InvalidAmount(amount) => assert_eq!(amount, Decimal::ZERO),
        _ => panic!("Expected InvalidAmount error"),
    }
}

#[test]
fn test_handle_deposit_money_negative_amount() {
    let account_id = Uuid::new_v4();
    let account = Account::new(account_id, "Test Depositor".to_string(), Decimal::new(100, 0)).unwrap();
    let negative_amount = Decimal::new(-10, 0);
    let command = AccountCommand::DepositMoney { account_id, amount: negative_amount };
    let result = account.handle_command(&command);
    assert!(result.is_err());
    match result.unwrap_err() {
        AccountError::InvalidAmount(amount) => assert_eq!(amount, negative_amount),
        _ => panic!("Expected InvalidAmount error"),
    }
}

#[test]
fn test_handle_deposit_money_to_closed_account() {
    let account_id = Uuid::new_v4();
    let mut account = Account::new(account_id, "Test Depositor".to_string(), Decimal::new(100, 0)).unwrap();
    account.is_active = false; // Manually close for testing this specific case
    let command = AccountCommand::DepositMoney { account_id, amount: Decimal::new(50,0) };
    let result = account.handle_command(&command);
    assert!(result.is_err());
    match result.unwrap_err() {
        AccountError::AccountClosed => {} // Expected
        _ => panic!("Expected AccountClosed error"),
    }
}

#[test]
fn test_apply_money_deposited_event() {
    let mut account = Account::new(Uuid::new_v4(), "Test User".to_string(), Decimal::new(100, 0)).unwrap();
    let initial_version = account.version;
    let deposit_amount = Decimal::new(50, 0);
    let event = AccountEvent::MoneyDeposited {
        account_id: account.id,
        amount: deposit_amount,
        transaction_id: Uuid::new_v4(),
    };
    account.apply_event(&event);
    assert_eq!(account.balance, Decimal::new(150, 0));
    assert_eq!(account.version, initial_version + 1);
}


#[test]
fn test_handle_withdraw_money_success() {
    let account_id = Uuid::new_v4();
    let mut account = Account::new(account_id, "Test Withdrawer".to_string(), Decimal::new(200, 0)).unwrap();
    let withdraw_amount = Decimal::new(50, 0);
    let command = AccountCommand::WithdrawMoney { account_id, amount: withdraw_amount };
    let result = account.handle_command(&command);
    assert!(result.is_ok());
    let events = result.unwrap();
    assert_eq!(events.len(), 1);
    match &events[0] {
        AccountEvent::MoneyWithdrawn { account_id: ev_acc_id, amount: ev_amount, .. } => {
            assert_eq!(*ev_acc_id, account_id);
            assert_eq!(*ev_amount, withdraw_amount);
        }
        _ => panic!("Expected MoneyWithdrawn event"),
    }

    // Test applying the event
    account.apply_event(&events[0]);
    assert_eq!(account.balance, Decimal::new(150, 0));
    assert_eq!(account.version, 1);
}

#[test]
fn test_handle_withdraw_money_insufficient_funds() {
    let account_id = Uuid::new_v4();
    let account = Account::new(account_id, "Test Withdrawer".to_string(), Decimal::new(50, 0)).unwrap();
    let withdraw_amount = Decimal::new(100, 0);
    let command = AccountCommand::WithdrawMoney { account_id, amount: withdraw_amount };
    let result = account.handle_command(&command);
    assert!(result.is_err());
    match result.unwrap_err() {
        AccountError::InsufficientFunds { available, requested } => {
            assert_eq!(available, Decimal::new(50, 0));
            assert_eq!(requested, withdraw_amount);
        }
        _ => panic!("Expected InsufficientFunds error"),
    }
}

#[test]
fn test_handle_withdraw_money_zero_amount() {
    let account_id = Uuid::new_v4();
    let account = Account::new(account_id, "Test Withdrawer".to_string(), Decimal::new(100, 0)).unwrap();
    let command = AccountCommand::WithdrawMoney { account_id, amount: Decimal::ZERO };
    let result = account.handle_command(&command);
    assert!(result.is_err());
    match result.unwrap_err() {
        AccountError::InvalidAmount(amount) => assert_eq!(amount, Decimal::ZERO),
        _ => panic!("Expected InvalidAmount error"),
    }
}

#[test]
fn test_handle_withdraw_money_negative_amount() {
    let account_id = Uuid::new_v4();
    let account = Account::new(account_id, "Test Withdrawer".to_string(), Decimal::new(100, 0)).unwrap();
    let negative_amount = Decimal::new(-20,0);
    let command = AccountCommand::WithdrawMoney { account_id, amount: negative_amount };
    let result = account.handle_command(&command);
    assert!(result.is_err());
    match result.unwrap_err() {
        AccountError::InvalidAmount(amount) => assert_eq!(amount, negative_amount),
        _ => panic!("Expected InvalidAmount error"),
    }
}

#[test]
fn test_handle_withdraw_money_from_closed_account() {
    let account_id = Uuid::new_v4();
    let mut account = Account::new(account_id, "Test Withdrawer".to_string(), Decimal::new(100, 0)).unwrap();
    account.is_active = false; // Manually close
    let command = AccountCommand::WithdrawMoney { account_id, amount: Decimal::new(50,0) };
    let result = account.handle_command(&command);
    assert!(result.is_err());
    match result.unwrap_err() {
        AccountError::AccountClosed => {} // Expected
        _ => panic!("Expected AccountClosed error"),
    }
}

#[test]
fn test_apply_money_withdrawn_event() {
    let mut account = Account::new(Uuid::new_v4(), "Test User".to_string(), Decimal::new(100, 0)).unwrap();
    let initial_version = account.version;
    let withdraw_amount = Decimal::new(30, 0);
    let event = AccountEvent::MoneyWithdrawn {
        account_id: account.id,
        amount: withdraw_amount,
        transaction_id: Uuid::new_v4(),
    };
    account.apply_event(&event);
    assert_eq!(account.balance, Decimal::new(70, 0));
    assert_eq!(account.version, initial_version + 1);
}


#[test]
fn test_handle_close_account_success() {
    let account_id = Uuid::new_v4();
    let mut account = Account::new(account_id, "Test Closer".to_string(), Decimal::new(100, 0)).unwrap();
    let reason = "User request".to_string();
    let command = AccountCommand::CloseAccount { account_id, reason: reason.clone() };
    let result = account.handle_command(&command);
    assert!(result.is_ok());
    let events = result.unwrap();
    assert_eq!(events.len(), 1);
    match &events[0] {
        AccountEvent::AccountClosed { account_id: ev_acc_id, reason: ev_reason } => {
            assert_eq!(*ev_acc_id, account_id);
            assert_eq!(*ev_reason, reason);
        }
        _ => panic!("Expected AccountClosed event"),
    }

    // Test applying the event
    account.apply_event(&events[0]);
    assert!(!account.is_active);
    assert_eq!(account.version, 1);
}

#[test]
fn test_handle_close_already_closed_account() {
    let account_id = Uuid::new_v4();
    let mut account = Account::new(account_id, "Test Closer".to_string(), Decimal::new(100, 0)).unwrap();
    account.is_active = false; // Manually close
    let reason = "Closing again".to_string();
    let command = AccountCommand::CloseAccount { account_id, reason };

    // According to current Account::handle_command, closing an inactive account is an error.
    // If the business logic allows closing an already closed account (e.g. idempotency),
    // this test and the Account::handle_command logic would need to change.
    // For now, assuming it's an error to try to operate on a closed account (except CreateAccount).
    let result = account.handle_command(&command);
    assert!(result.is_err());
    match result.unwrap_err() {
        AccountError::AccountClosed => {} // Expected
        _ => panic!("Expected AccountClosed error when closing an already closed account"),
    }
}

#[test]
fn test_apply_account_closed_event() {
    let mut account = Account::new(Uuid::new_v4(), "Test User".to_string(), Decimal::new(100, 0)).unwrap();
    let initial_version = account.version;
    assert!(account.is_active);
    let event = AccountEvent::AccountClosed {
        account_id: account.id,
        reason: "Test closure".to_string(),
    };
    account.apply_event(&event);
    assert!(!account.is_active);
    assert_eq!(account.version, initial_version + 1);
}

#[test]
fn test_account_error_messages() {
    let err_not_found = AccountError::NotFound;
    assert_eq!(format!("{}", err_not_found), "Account not found");

    let err_insufficient = AccountError::InsufficientFunds { available: Decimal::new(10,0), requested: Decimal::new(20,0) };
    assert_eq!(format!("{}", err_insufficient), "Insufficient funds: available 10, requested 20");

    let err_closed = AccountError::AccountClosed;
    assert_eq!(format!("{}", err_closed), "Account is closed");

    let err_invalid_amount = AccountError::InvalidAmount(Decimal::new(-5,0));
    assert_eq!(format!("{}", err_invalid_amount), "Invalid amount: -5");
}
