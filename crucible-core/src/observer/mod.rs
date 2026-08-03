//! Observers snapshot per-run state into the shared `Observations` bag.

pub mod session;

pub use session::{EventIndex, SessionObserver};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
