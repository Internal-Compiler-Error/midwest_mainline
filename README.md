# midwest_mainline

Midwest Mainline is a BitTorrent Distributed Hash Table written in Rust with all async network calls.

## why the name?

The BitTorrent DHT is sometimes referred to as the "Mainline" DHT since there were other non-standard DHTs that ran on
clients that supported the BT protocol. Since calling it rusty mainline is too boring, the name of rust conjures up the
images of midwestern states, hence midwest mainline!

## what is a DHT?

A DHT exist to solve the issue that of spreading a large hash table to multiple machines and providing redundancy. In
the case of BitTorrent, it is used to share information about other peers for a given torrent.

The gist of a DHT is by using the same key space for the identification of us and the key in the hash table, we only
include what's near us in the hash table. By maintaining some contacts about other nodes that are close to our id, other
nodes can ask us about our neighbors and gradually lead to the node that has the exact key they want.

- For the original paper, [see](https://www.scs.stanford.edu/~dm/home/papers/kpos.pdf)
- For an excellent video explaining it, [see](https://youtu.be/NxhZ_c8YX8E)
- For the BitTorrent specification, [see](https://www.bittorrent.org/beps/bep_0005.html)

## how the BitTorrent DHT works

This section is a summary of [BEP 5](https://www.bittorrent.org/beps/bep_0005.html) and
[BEP 42](https://www.bittorrent.org/beps/bep_0042.html).

### node IDs and distance

- Each node has a node ID. A node ID is 160 bits, the same size as an info hash. Node IDs
  and info hashes use the same key space.
- The distance between two IDs is the bitwise XOR of the IDs. A small XOR value means a
  small distance.

### routing table

- Each node keeps a routing table of contacts. A contact is a node ID, an IP address, and
  a port number.
- The routing table divides the ID space into 160 buckets. Bucket *i* contains the nodes
  whose distance from the local node ID is at least 2^*i* and less than 2^(*i* + 1).
- The specification limits a bucket to 8 nodes (k = 8). If the bucket that contains the
  local node ID becomes full, it splits into two buckets.
- If a bucket is full and a new node arrives, the local node pings the node in the bucket
  that was not seen for the longest time. If that node answers, the new node is ignored.
  If that node does not answer, it is removed and the new node is added.

### queries

- Nodes use the KRPC protocol: bencoded messages on UDP. Each message has a transaction
  ID. A response uses the same transaction ID as its query.
- There are four queries:
  - `ping` asks if a node is online.
  - `find_node` asks for the contacts closest to a target node ID.
  - `get_peers` asks for the peers of an info hash. If the node has peers, it sends them.
    If not, it sends the contacts it knows that are closest to the info hash. The response
    also contains a token.
  - `announce_peer` tells the node that the sender is a peer for an info hash. The query
    must contain a token from an earlier `get_peers` response. The token binds the
    announce to the IP address of the requester.

### lookups

- A lookup is iterative. The local node sends the query to the closest contacts it knows.
  Each response gives contacts that are closer to the target. The local node queries those
  contacts in turn. The lookup ends when the responses contain no closer contacts.
- At most 3 queries are active at the same time (alpha = 3).

### peer storage

- A node stores the contact of each peer that announces to it.
- Peers announce to the nodes whose node IDs are closest to the info hash. As a result, a
  `get_peers` lookup for the same info hash finds them.
- Stored peers expire after approximately 45 minutes.

### node ID security (BEP 42)

- A node ID is not fully random. The first 21 bits are the CRC32C checksum of the external
  IP address of the node. This prevents an attacker from choosing its node ID freely.
- A response includes the external IP address that the responder sees for the requester.

### bootstrap

- To join the network, a node pings a known node and then does a `find_node` lookup for
  its own node ID. This fills the routing table with the nodes closest to it.

## how this implementation is different

- **Bucket size.** The routing table has 160 fixed buckets of 1024 nodes. Buckets do not
  split. The specification says 8 nodes for each bucket. The larger buckets are possible
  because `find_closest` collects nodes from adjacent buckets.
- **Eviction.** A node that fails 3 or more queries goes on a replacement list. If such a
  node was not queried in the last 15 minutes, it is pinged. If it does not answer, it is
  marked as removed. The specification says to ping the least-recently-seen node when a
  bucket is full.
- **Full buckets.** If a bucket is full, the implementation pings the replacement list of
  that bucket. The new node is added only if space becomes available.
- **Persistence.** The routing table and the announced peers are stored in an SQLite
  database. Contacts and the node ID stay available after a restart. The specification
  assumes an in-memory table.
- **Stable node ID.** If the external IP address is the same as in the last session, the
  node keeps its old node ID. If the address changed, a new node ID (BEP 42) is made, and
  the bucket of each stored node is calculated again.
- **Tokens.** A token is the SHA3-256 hash of a secret value and the IP address of the
  requester. The secret value changes every 5 minutes. The current value and the previous
  value are accepted, so a token is valid for approximately 10 minutes. The specification
  does not say how to make tokens.
- **Response size.** A `get_peers` response contains a maximum of 50 peers. This keeps
  the response small enough for one UDP datagram.
- **Lookup limits.** A lookup does a maximum of 8 rounds. A `find_node` lookup stops
  immediately if the local table already contains the target node.
- **Maintenance.** Every 180 seconds, expired peers and removed nodes are deleted from
  the database, and the replacement lists are pinged.
- **IPv4 only.** IPv6 (BEP 32) is not supported.

## high level design

The crate is layered bottom-up:

- `message` — the KRPC wire protocol (BEP 5): bencode parsing and encoding for the four
  queries (ping, find_node, get_peers, announce_peer), their responses, and errors.
  Pure data, no I/O.
- `dht::rpc_manager` — the message broker and sole owner of the UDP socket. An outbound
  query gets a fresh transaction id and a oneshot waiting for its response (matched on
  both transaction id *and* sender address); every inbound packet is also fanned out to
  all subscribers. Send work is spawned so a slow write never stalls the receive loop.
- `dht::routing_table` — the k-bucket contact store, persisted in SQLite so contacts and
  our node id survive restarts. It subscribes to the broker's inbound fan-out and learns
  from everything we hear; dead nodes are evicted by a failure counter plus periodic
  refresh pings.
- `dht::client` / `dht::server` — the two halves of the node, sharing one `SharedState`.
  The client runs iterative lookups (closest-known nodes, a few at a time, following
  referrals); the server answers inbound queries and stores announced peers
  (token-validated per BEP 5).
- `dht::DhtSession` wires it all together: resumes or mints a BEP 42 node id from the
  database, then `run()` drives the broker, the routing table, and the server.

See *how this implementation is different* for the protocol-level deviations from BEP 5.
One internal note: all diesel calls are synchronous — fine for local SQLite, but don't
hold them across network awaits.

## roadmap

- [x] routing
- [x] bootstrapping
- [x] find node
- [x] get peers
- [x] ping
- [x] security extension [BEP-42](https://www.bittorrent.org/beps/bep_0042.html)
- [x] announce
- [x] respond to ping
- [x] respond to get peers
- [x] respond to announce
- [x] respond to find node
- [x] expiration for swarm table
- [x] ping and prune for routing table
- [x] upload to <https://crates.io>

## state of the development

The core BEP 5 feature set is complete (see the roadmap above); the public interfaces are
still subject to change.

## warning

I do not have any formal training in security, anything that listens for incoming traffic should be considered as
problematic and require attention. Especially when the protocol is effectively send over clear text.

## unstable tokio features

This library uses unstable tokio features for tracing, go see
tokio's [docs](https://docs.rs/tokio/1.20.0/tokio/index.html#unstable-features).
about using unstable features.

# limitations
- the behaviour is undefined if you mess with your system clock
# TL;DR

The library is still in beta, I am aware the interfaces are quite clunky

``` rust
use midwest_mainline::dht::DhtSession;
use std::env;
use std::net::SocketAddrV4;
use std::str::FromStr;
use std::sync::Arc;
use tokio::net::UdpSocket;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let external_ip = public_ip::addr_v4().await.unwrap();
    let socket = UdpSocket::bind(SocketAddrV4::from_str("0.0.0.0:51413")?).await?;
    let dht = Arc::new(DhtSession::with_stable_id(socket, external_ip, &env::var("DATABASE_URL")?)?);

    let _event_loop = tokio::spawn({
        let dht = Arc::clone(&dht);
        async move { dht.run().await }
    });

    dht.bootstrap(vec![
        // dht.tansmissionbt.com
        "87.98.162.88:6881".parse()?,
        // router.utorrent.com
        "67.215.246.10:6881".parse()?,
        // router.bittorrent.com, ironically that this almost never responds
        "82.221.103.244:8991".parse()?,
        // dht.aelitis.com
        "174.129.43.152:6881".parse()?,
    ])
    .await?;

    // now you can do hackerman things with dht.handle()

    Ok(())
}
```
