//! # midwest_mainline
//!  
//! Midwest Mainline is a BitTorrent Distributed Hash Table written in Rust with all async network calls.
//!
//! ## why the name?
//! The BitTorrent DHT is sometimes referred to as the "Mainline" DHT since there were other non-standard DHTs that ran on
//! clients that supported the BT protocol. Since calling it rusty mainline is too boring, the name of rust conjures up the
//! images of midwestern states, hence midwest mainline!
//!
//! ## what is a DHT?
//! A DHT exist to solve the issue that of spreading a large hash table to multiple machines and providing redundancy. In
//! the case of BitTorrent, it is used to share information about other peers for a given torrent.
//!
//! The gist of a DHT is by using the same key space for the identification of us and the key in the hash table, we only
//! include what's near us in the hash table. By maintaining some contacts about other nodes that are close to our id, other
//! nodes can ask us about our neighbors and gradually lead to the node that has the exact key they want.
//!
//! - For the original paper, [see](https://www.scs.stanford.edu/~dm/home/papers/kpos.pdf)
//! - For an excellent video explaining it, [see](https://youtu.be/NxhZ_c8YX8E)
//! - For the BitTorrent specification, [see](https://www.bittorrent.org/beps/bep_0005.html)
//!
//! ## roadmap
//! - [x] routing
//! - [x] bootstrapping
//! - [x] find node
//! - [x] get peers
//! - [x] ping
//! - [x] security extension [BEP-42](https://www.bittorrent.org/beps/bep_0042.html)
//! - [x] announce
//! - [x] respond to ping
//! - [x] respond to get peers
//! - [x] respond to announce
//! - [x] respond to find node
//! - [ ] expiration for hash table
//! - [ ] ping and prune for routing table
//! - [x] upload to crate.io
//!
//! ## state of the development
//! As of right now, the code base is still experience large changes daily and very little comments are added.
//!
//! ## warning
//! I do not have any formal training in security, anything that listens for incoming traffic should be considered as
//! problematic and require attention. Especially when the protocol is effectively send over clear text.
//!
//! ## unstable tokio features
//! This library uses unstable tokio features for tracing, go see tokio's [docs](https://docs.rs/tokio/1.20.0/tokio/index.html#unstable-features).
//! about using unstable features.
//!
//! # TL;DR
//! The library is still in beta, I am aware the interfaces are quite clunky
//!
//! ``` rust
//! use std::io;
//! use midwest_mainline::dht_service;
//! use midwest_mainline::dht_service::DhtV4;
//! use std::net::SocketAddrV4;
//! use std::str::FromStr;
//!
//! #[tokio::main]
//! async fn main() -> io::Result<()> {
//!     let external_ip = public_ip::addr_v4().await.unwrap();
//!     let dht = DhtV4::bootstrap_with_random_id(
//!                     SocketAddrV4::from_str("0.0.0.0:51413").unwrap(),
//!                     external_ip,
//!                     vec![
//!                     // dht.tansmissionbt.com
//!                     "87.98.162.88:6881".parse().unwrap(),
//!                     // router.utorrent.com
//!                     "67.215.246.10:6881".parse().unwrap(),
//!                     // router.bittorrent.com, ironically that this almost never responds
//!                     "82.221.103.244:6881".parse().unwrap(),
//!                     // dht.aelitis.com
//!                     "174.129.43.152:6881".parse().unwrap(),
//!                 ],
//!         ).await?;
//!     let client = dht.client();
//!
//!     // make sure this is alive
//!     tokio::spawn(dht.run());
//!
//!     // now you can do hackerman things with the client
//!
//!     Ok(())
//! }
//! ```

pub mod dht_service;
pub mod domain_knowledge;
pub mod message;
pub mod our_error;
pub mod routing;
pub mod utils;
