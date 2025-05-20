use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use sha3::{Digest, Sha3_256};

use crate::types::{NodeId, Token};

#[derive(Debug, PartialEq, Eq, Hash)]
struct TokenPoolInner {
    state: u128,
    last_update: Instant,
}

pub const TOKEN_EXPIRATION_TIME: Duration = Duration::from_secs(60 * 5);

impl TokenPoolInner {
    pub(crate) fn new(state: u128) -> Self {
        Self {
            state,
            last_update: Instant::now(),
        }
    }

    /// Generate a new token if the address is not in the pool or expired, otherwise return the
    /// existing token.
    pub(crate) fn token_for_node(&mut self, node: &NodeId) -> Token {
        let now = Instant::now();

        if now.duration_since(self.last_update) > TOKEN_EXPIRATION_TIME {
            self.state += 1;
        }
        generate_token(self.state, &node)
    }

    /// See as the moment of calling, is the token correct?
    pub(crate) fn is_valid_token(&self, node: &NodeId, token: &Token) -> bool {
        // we accept the current token and one token before it, similar to the 10 min window in the
        // official spec
        let previous = generate_token(self.state - 1, node);
        let current = generate_token(self.state, node);

        token == &current || token == &previous
    }
}

/// generate what the token for the node should be as this current moment
fn generate_token(state: u128, node: &NodeId) -> Token {
    // token generation strategy is simply use SHA3 as a PRNG, with `<state> || <node_id>` as
    // the input, `||` is the concatination operator here
    let mut hasher = Sha3_256::new();
    hasher.update(state.to_ne_bytes());
    hasher.update(node.as_bytes());

    let digest = hasher.finalize();
    Token::from_bytes(digest.as_slice())
}

#[derive(Debug)]
pub(crate) struct TokenPool {
    inner: Arc<Mutex<TokenPoolInner>>,
}

impl TokenPool {
    pub(crate) fn new(state: u128) -> Self {
        Self {
            // i with we had the haskell function composition syntax for things like this
            inner: Arc::new(Mutex::new(TokenPoolInner::new(state))),
        }
    }

    /// Generate a new token if the address is not in the pool or expired, otherwise return the
    /// existing token.
    pub(crate) fn token_for_node(&self, node: &NodeId) -> Token {
        self.inner.lock().unwrap().token_for_node(node)
    }

    pub(crate) fn is_valid_token(&self, node: &NodeId, token: &Token) -> bool {
        self.inner.lock().unwrap().is_valid_token(node, token)
    }
}
