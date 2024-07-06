use std::{backtrace::Backtrace, error::Error as StdError};
use thiserror::Error;

#[derive(Error, Debug)]
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
    DecodeError(#[from] color_eyre::Report),
    // stupid ass bendy library's error type only implements Debug + Display and not actually Error
    #[error("Bendy complained {0}")]
    BendyDecodeError(bendy::decoding::Error),
}
