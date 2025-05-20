use std::{
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use sha3::{Digest, Sha3_256};

use crate::types::{NodeId, Token};

pub const TOKEN_EXPIRATION_TIME: Duration = Duration::from_secs(60 * 5);

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
struct TokenGenInner {
    state: u128,
    last_update: Instant,
}

impl TokenGenInner {
    pub fn new(state: u128) -> Self {
        Self {
            state,
            last_update: Instant::now(),
        }
    }

    fn gen_token_with_state(state: u128, node: &NodeId) -> Token {
        // token generation strategy is simply use SHA3 as a PRNG, with `<state> || <node_id>` as
        // the input, `||` is the concatination operator here
        let mut hasher = Sha3_256::new();
        hasher.update(state.to_be_bytes());
        hasher.update(node.as_bytes());

        let digest = hasher.finalize();
        Token::from_bytes(digest.as_slice())
    }

    /// See as the moment of calling, is the token correct?
    pub fn token_acceptable(&self, node: &NodeId, token: &Token) -> bool {
        // we accept the current token and one token before it, similar to the 10 min window in the
        // official spec
        let previous = Self::gen_token_with_state(self.state - 1, node);
        let current = self.generate_token(node);

        token == &current || token == &previous
    }

    pub fn needs_advancing(&self) -> bool {
        Instant::now().duration_since(self.last_update) > TOKEN_EXPIRATION_TIME
    }

    pub fn advance(&mut self) {
        self.state += 1;
        self.last_update = Instant::now();
    }

    fn generate_token(&self, node: &NodeId) -> Token {
        Self::gen_token_with_state(self.state, node)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TokenGenerator {
    inner: Arc<RwLock<TokenGenInner>>,
}

impl TokenGenerator {
    pub(crate) fn new(state: u128) -> Self {
        Self {
            // I wish we had the Haskell function composition syntax for things like this
            inner: Arc::new(RwLock::new(TokenGenInner::new(state))),
        }
    }

    /// Generate the current token for the node
    pub(crate) fn token_for_node(&self, node: &NodeId) -> Token {
        {
            let inner = self.inner.read().unwrap();
            if !inner.needs_advancing() {
                return inner.generate_token(node);
            }
        }

        let mut inner = self.inner.write().unwrap();
        if inner.needs_advancing() {
            inner.advance();
        }

        inner.generate_token(node)
    }

    pub(crate) fn is_valid_token(&self, node: &NodeId, token: &Token) -> bool {
        self.inner.read().unwrap().token_acceptable(node, token)
    }
}
