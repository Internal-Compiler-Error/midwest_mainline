use std::{
    net::Ipv4Addr,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use sha3::{Digest, Sha3_256};

use crate::types::Token;

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

    fn gen_token_with_state(state: u128, ip: &Ipv4Addr) -> Token {
        // tokens bind to the requester's IP (BEP 5): a host may only announce with a
        // token that was issued to its own address
        let mut hasher = Sha3_256::new();
        hasher.update(state.to_be_bytes());
        hasher.update(ip.octets());

        let digest = hasher.finalize();
        Token::from_bytes(digest.as_slice())
    }

    /// See as the moment of calling, is the token correct?
    pub fn token_acceptable(&self, ip: &Ipv4Addr, token: &Token) -> bool {
        // we accept the current token and one token before it, similar to the 10 min window in the
        // official spec
        let previous = Self::gen_token_with_state(self.state - 1, ip);
        let current = self.generate_token(ip);

        token == &current || token == &previous
    }

    pub fn needs_advancing(&self) -> bool {
        Instant::now().duration_since(self.last_update) > TOKEN_EXPIRATION_TIME
    }

    pub fn advance(&mut self) {
        self.state += 1;
        self.last_update = Instant::now();
    }

    fn generate_token(&self, ip: &Ipv4Addr) -> Token {
        Self::gen_token_with_state(self.state, ip)
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

    /// Generate the current token for the IP
    pub(crate) fn token_for_ip(&self, ip: &Ipv4Addr) -> Token {
        {
            let inner = self.inner.read().unwrap();
            if !inner.needs_advancing() {
                return inner.generate_token(ip);
            }
        }

        let mut inner = self.inner.write().unwrap();
        if inner.needs_advancing() {
            inner.advance();
        }

        inner.generate_token(ip)
    }

    pub(crate) fn is_valid_token(&self, ip: &Ipv4Addr, token: &Token) -> bool {
        self.inner.read().unwrap().token_acceptable(ip, token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_bind_to_the_requesters_ip() {
        let tokens = TokenGenerator::new(42);
        let a: Ipv4Addr = "1.2.3.4".parse().unwrap();
        let b: Ipv4Addr = "5.6.7.8".parse().unwrap();

        let token = tokens.token_for_ip(&a);
        assert!(tokens.is_valid_token(&a, &token));
        assert!(
            !tokens.is_valid_token(&b, &token),
            "a token must not validate from another IP"
        );
    }

    #[test]
    fn previous_state_token_stays_valid_exactly_one_rotation() {
        let tokens = TokenGenerator::new(42);
        let a: Ipv4Addr = "1.2.3.4".parse().unwrap();

        let old = tokens.token_for_ip(&a);
        tokens.inner.write().unwrap().advance();
        assert!(tokens.is_valid_token(&a, &old), "current-1 must still be accepted");

        tokens.inner.write().unwrap().advance();
        assert!(!tokens.is_valid_token(&a, &old), "current-2 must be rejected");
    }
}
