use bendy::encoding::SingleItemEncoder;

use crate::message::ToKrpcBody;

/// A KRPC error: a code (BEP 5's 201-204, BEP 44's 205-302) and a description
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct KrpcError {
    code: u32,
    message: String,
}

impl KrpcError {
    pub fn new(code: u32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn code(&self) -> u32 {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn new_server() -> Self {
        Self::new(202, "Server Error")
    }

    pub fn new_protocol() -> Self {
        Self::new(203, "Protocol Error")
    }

    pub fn new_method_unknown() -> Self {
        Self::new(204, "Method Unknown")
    }
}

/// The database failing us is a server error to the querier
impl From<diesel::result::Error> for KrpcError {
    fn from(_: diesel::result::Error) -> Self {
        Self::new_server()
    }
}

impl ToKrpcBody for KrpcError {
    fn encode_body(&self, enc: SingleItemEncoder) -> Result<(), bendy::encoding::Error> {
        enc.emit_list(|e| {
            e.emit(self.code)?;
            e.emit(&self.message)
        })
    }
}
