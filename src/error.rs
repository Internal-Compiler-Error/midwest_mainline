/// First a rant about the error handling in Rust.
/// Rust is an amazing language. I enjoy working in it for every single second of my life *except*
/// for when error handling.
use derive_more::{Display, From};
use std::{
    error::Error as StdError,
    fmt::{Display, Formatter},
    option::Option,
};
use tokio::{task::JoinError, time::error::Elapsed};

#[derive(Debug, Display, From)]
pub(crate) enum ErrorKind {
    BottomedOut,
    Cancelled,
    JoinError,
    TimedOut,
    WrongResponse(String),
}

#[derive(Debug, From)]
pub struct Error {
    pub(crate) kind: ErrorKind,
    pub(crate) source: Option<Box<dyn StdError + Send + Sync>>,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "error reason: {}, source: {:#?}", self.kind, self.source)
    }
}

impl StdError for Error {}

impl Error {
    pub(crate) fn new(kind: ErrorKind, source: Option<Box<dyn StdError + Send + Sync>>) -> Self {
        Self { kind, source }
    }

    pub(crate) fn wrong_response(description: String) -> Self {
        Self {
            kind: ErrorKind::WrongResponse(description),
            source: None,
        }
    }

    pub(crate) fn bottomed_out() -> Self {
        Self {
            kind: ErrorKind::BottomedOut,
            source: None,
        }
    }

    pub(crate) fn cancelled() -> Self {
        Self {
            kind: ErrorKind::Cancelled,
            source: None,
        }
    }
}

impl From<JoinError> for Error {
    fn from(e: JoinError) -> Self {
        Error {
            kind: ErrorKind::JoinError,
            source: Some(Box::new(e)),
        }
    }
}

impl From<Elapsed> for Error {
    fn from(e: Elapsed) -> Self {
        Error {
            kind: ErrorKind::TimedOut,
            source: Some(Box::new(e)),
        }
    }
}
