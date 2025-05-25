use bendy::encoding::SingleItemEncoder;

use crate::message::ToKrpcBody;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct KrpcError {
    code: u32,
    message: String,
}

impl KrpcError {
    pub fn new(code: u32, message: String) -> Self {
        Self { code, message }
    }

    pub fn new_generic() -> Self {
        Self {
            code: 201,
            message: "Generic Error".to_string(),
        }
    }

    pub fn new_server() -> Self {
        Self {
            code: 202,
            message: "Server Error".to_string(),
        }
    }

    pub fn new_protocol() -> Self {
        Self {
            code: 203,
            message: "Protocol Error".to_string(),
        }
    }

    pub fn new_method_unknown() -> Self {
        Self {
            code: 204,
            message: "Method Unknown".to_string(),
        }
    }
}

impl ToKrpcBody for KrpcError {
    #[allow(unused_must_use)]
    fn encode_body(&self, enc: SingleItemEncoder) {
        enc.emit_list(|e| {
            e.emit(&self.code)?;
            e.emit(&self.message)
        })
        .unwrap()
    }
}
