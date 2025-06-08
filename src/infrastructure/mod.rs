pub mod event_store;
pub mod projections;
pub mod repository;
pub mod redis_abstraction; // Added module

pub use event_store::*;
pub use projections::*;
pub use repository::*;
pub use redis_abstraction::*; // Exported new module's contents