use std::io;
use thiserror::Error;
use tokio::{task::JoinError, time::error::Elapsed};

#[derive(Debug, Error)]
/// An old joke on soviet union
pub enum OurError {
    // #[error("Issue with parsing bencode")]
    // #[error(transparent)]
    // DecodeError {
    //     #[from]
    //     report: color_eyre::Report,
    //
    //     backtrace: Backtrace,
    // },
    #[error(transparent)]
    DecodeError(eyre::Error),
    // stupid ass bendy library's error type only implements Debug + Display and not actually Error
    #[error("Bendy complained {0}")]
    BendyDecodeError(bendy::decoding::Error),

    #[error("Something went wrong in the DHT: {0}")]
    DhtFailure(String),

    #[error("I'm sorry, {0}")]
    IoError(#[from] io::Error),

    #[error(transparent)]
    Generic(#[from] eyre::Error),

    // wtf
    #[error("Join Error")]
    JoinError(#[from] JoinError),

    // wtf
    #[error("Timed out")]
    Timeout(#[from] Elapsed),
}

/// When Australians say no
macro_rules! naur {
    ($msg:literal $(,)?) => { OurError::Generic(eyre::eyre!($msg)) };
    ($err:expr $(,)?) => { OurError::Generic(eyre::eyre!($expr)) };
    ($fmt:expr, $($arg:tt)*) => { OurError::Generic(eyre::eyre!($fmt $($arg)*)) };
}

// https://stackoverflow.com/questions/26731243/how-do-i-use-a-macro-across-module-files
// don't ask me why or how
pub(crate) use naur;
