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
//! ## high level design
//! The crate is layered bottom-up:
//!
//! - [`message`] — the KRPC wire protocol (BEP 5): bencode parsing and encoding for the
//!   four queries (ping, find_node, get_peers, announce_peer), their responses, and
//!   errors. Pure data, no I/O.
//! - [`dht::rpc_manager`] — the message broker and sole owner of the UDP socket. An
//!   outbound query gets a fresh transaction id and a oneshot waiting for its response
//!   (matched on both transaction id *and* sender address); every inbound packet is also
//!   fanned out to all subscribers. Send work is spawned so a slow write never stalls
//!   the receive loop.
//! - [`dht::routing_table`] — the k-bucket contact store, persisted in SQLite so contacts
//!   and our node id survive restarts. It subscribes to the broker's inbound fan-out and
//!   learns from everything we hear; dead nodes are evicted by a failure counter plus
//!   periodic refresh pings.
//! - [`dht::client`] / `dht::server` — the two halves of the node, sharing one
//!   `SharedState`. The client runs iterative lookups (closest-known nodes, `CONCURRENT_REQS`
//!   at a time, following referrals); the server answers inbound queries and stores
//!   announced peers (token-validated per BEP 5, tokens from `token_generator`).
//! - [`dht::DhtSession`] wires it all together: resumes or mints a BEP 42 node id from
//!   the database, then `run()` drives the broker, the routing table, and the server.
//!
//! Two deliberate deviations from BEP 5: the routing table is 160 flat buckets of 1024
//! nodes instead of k = 8 with bucket splitting (eviction keeps it fresh; see the note on
//! [`dht::routing_table::RoutingTable`]), and all diesel calls are synchronous — fine for
//! local SQLite, but don't hold them across network awaits.
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
//! - [x] expiration for hash table
//! - [x] ping and prune for routing table
//! - [x] upload to crate.io
//!
//! ## state of the development
//! The core BEP 5 feature set is complete (see the roadmap above); the public interfaces
//! are still subject to change.
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
//! ```no_run
//! use midwest_mainline::dht::DhtSession;
//! use std::env;
//! use std::net::SocketAddrV4;
//! use std::str::FromStr;
//! use std::sync::Arc;
//! use tokio::net::UdpSocket;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let socket = UdpSocket::bind(SocketAddrV4::from_str("0.0.0.0:51413")?).await?;
//!     // `None`: derive the BEP 42 node id from the address other nodes reported last time
//!     let dht = Arc::new(DhtSession::with_stable_id(socket, None, &env::var("DATABASE_URL")?)?);
//!
//!     let _event_loop = tokio::spawn({
//!         let dht = Arc::clone(&dht);
//!         async move { dht.run().await }
//!     });
//!
//!     dht.bootstrap(vec![
//!         // dht.tansmissionbt.com
//!         "87.98.162.88:6881".parse()?,
//!         // routing_table.utorrent.com
//!         "67.215.246.10:6881".parse()?,
//!         // routing_table.bittorrent.com, ironically that this almost never responds
//!         "82.221.103.244:8991".parse()?,
//!         // dht.aelitis.com
//!         "174.129.43.152:6881".parse()?,
//!     ])
//!     .await?;
//!
//!     // now you can do hackerman things with dht.handle()
//!
//!     Ok(())
//! }
//! ```

pub mod dht;
pub mod message;
pub(crate) mod models;
pub mod our_error;
pub(crate) mod schema;
#[cfg(test)]
mod test_support;
pub(crate) mod token_generator;
pub mod types;
pub mod utils;
