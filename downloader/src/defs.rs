#![allow(dead_code)]

use std::net::SocketAddrV4;

pub type AMutex<T> = tokio::sync::Mutex<T>;
pub type ARwLock<T> = tokio::sync::RwLock<T>;

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord, Copy)]
pub struct Identity {
    pub peer_id: [u8; 20],
    pub serving: SocketAddrV4,
}