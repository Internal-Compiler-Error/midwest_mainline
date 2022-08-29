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
    SerializationError,
    IoError,
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

    pub(crate) fn serialization_error() -> Self {
        Self {
            kind: ErrorKind::SerializationError,
            source: None,
        }
    }
}

impl From<JoinError> for Error {
    fn from(e: JoinError) -> Self {
        Self {
            kind: ErrorKind::JoinError,
            source: Some(Box::new(e)),
        }
    }
}

impl From<Elapsed> for Error {
    fn from(e: Elapsed) -> Self {
        Self {
            kind: ErrorKind::TimedOut,
            source: Some(Box::new(e)),
        }
    }
}

impl From<bendy::serde::Error> for Error {
    fn from(e: bendy::serde::Error) -> Self {
        Self {
            kind: ErrorKind::SerializationError,
            source: Some(Box::new(e)),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self {
            kind: ErrorKind::IoError,
            source: Some(Box::new(e)),
        }
    }
}
