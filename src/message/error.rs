use crate::message::ToRawKrpc;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct KrpcError {
    transaction_id: String,
    code: u32,
    message: String,
}

impl KrpcError {
    pub fn new(transaction_id: String, code: u32, message: String) -> Self {
        Self {
            transaction_id,
            code,
            message,
        }
    }

    pub fn new_generic(transaction_id: String) -> Self {
        Self {
            transaction_id,
            code: 201,
            message: "Generic Error".to_string(),
        }
    }

    pub fn new_server(transaction_id: String) -> Self {
        Self {
            transaction_id,
            code: 202,
            message: "Server Error".to_string(),
        }
    }

    pub fn new_protocol(transaction_id: String) -> Self {
        Self {
            transaction_id,
            code: 203,
            message: "Protocol Error".to_string(),
        }
    }

    pub fn new_method_unknown(transaction_id: String) -> Self {
        Self {
            transaction_id,
            code: 204,
            message: "Method Unknown".to_string(),
        }
    }

    pub fn txn_id(&self) -> &str {
        &self.transaction_id
    }
}

impl ToRawKrpc for KrpcError {
    #[allow(unused_must_use)]
    fn to_raw_krpc(&self) -> Box<[u8]> {
        use bendy::encoding::Encoder;

        let mut encoder = Encoder::new();
        encoder.emit_and_sort_dict(|e| {
            e.emit_pair(b"t", &self.transaction_id);
            e.emit_pair(b"y", &"e");
            e.emit_pair_with(b"e", |e| {
                e.emit_list(|e| {
                    e.emit(&self.code);
                    e.emit(&self.message)
                })
            })
        });

        encoder
            .get_output()
            .expect("we know the stuff upfont")
            .into_boxed_slice()
    }
}
