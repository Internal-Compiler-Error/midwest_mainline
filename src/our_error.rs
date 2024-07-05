use std::error::Error as StdError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum OurError {
    #[error("Issue with parsing bencode")]
    DecodeError(#[from] color_eyre::Report),

    // stupid ass bendy library's error type only implements Debug + Display and not actually Error
    #[error("Bendy complained {0}")]
    BendyDecodeError(bendy::decoding::Error),
}
