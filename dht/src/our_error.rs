use std::io;
use thiserror::Error;

use crate::message::error::KrpcError;
use crate::types::TransactionId;

#[derive(Debug, Error)]
/// An old joke on soviet union
pub enum OurError {
    /// A query with a method we don't implement; BEP 5 wants a 204 Method Unknown reply
    /// carrying the same transaction id
    #[error("unsupported query method")]
    UnsupportedQuery(TransactionId),
    #[error(transparent)]
    DecodeError(eyre::Error),

    /// The queried node answered with a KRPC error
    #[error("the node answered with error {0:?}")]
    Remote(KrpcError),

    #[error("I'm sorry, {0}")]
    IoError(#[from] io::Error),

    #[error(transparent)]
    Generic(#[from] eyre::Error),

    #[error("Timed out")]
    Timeout(#[from] tokio::time::error::Elapsed),

    #[error("Internal database error")]
    DatabaseError(#[from] diesel::result::Error),
}

/// When Australians say no
macro_rules! naur {
    ($msg:literal $(,)?) => { OurError::Generic(eyre::eyre!($msg)) };
    ($fmt:literal, $($arg:tt)*) => { OurError::Generic(eyre::eyre!($fmt, $($arg)*)) };
}

// https://stackoverflow.com/questions/26731243/how-do-i-use-a-macro-across-module-files
// don't ask me why or how
pub(crate) use naur;
