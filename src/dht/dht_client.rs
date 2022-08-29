use crate::{
    dht::{
        message_broker::{MailBoxes, PostalOffice},
        transaction_id_pool::TransactionIdPool,
        DhtServiceFailure,
    },
    domain_knowledge::{
        CompactNodeContact, CompactPeerContact, NodeId, ToCompactNodeContactVec, ToCompactNodeContactVecUnchecked,
    },
    error::{Error, ErrorKind},
    message::{InfoHash, Krpc},
    routing::RoutingTable,
    utils::ParSpawnAndAwait,
};
use async_recursion::async_recursion;
use either::Either;
use num::BigUint;
use std::{
    collections::HashSet,
    fmt::Debug,
    future::Future,
    net::{SocketAddr, SocketAddrV4},
    ops::{BitXor, DerefMut},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{oneshot, oneshot::Sender, Mutex, RwLock},
    task::JoinError,
    time::timeout,
};
use tracing::{debug, info, instrument, trace, warn};

#[derive(Debug)]
pub struct DhtClientV4<M, P>
where
    M: MailBoxes + 'static + Send + Sync,
    P: PostalOffice + 'static + Send + Sync,
{
    pub(crate) our_id: [u8; 20],
    pub(crate) routing_table: Arc<RwLock<RoutingTable>>,
    pub(crate) socket_address: SocketAddrV4,
    pub(crate) transaction_id_pool: Arc<TransactionIdPool>,
    pub(crate) mail_boxes: Arc<M>,
    pub(crate) postal_office: Arc<P>,
}

impl<M, P> DhtClientV4<M, P>
where
    M: MailBoxes + 'static + Send + Sync,
    P: PostalOffice + 'static + Send + Sync,
{
    /// A default DHT node when you really don't know anything about DHTs and just want to provide
    /// a port and IP address
    pub(crate) fn new(
        bind_addr: SocketAddrV4,
        routing_table: Arc<RwLock<RoutingTable>>,
        our_id: NodeId,
        mail_boxes: M,
        postal_office: P,
    ) -> Self {
        DhtClientV4 {
            our_id,
            routing_table,
            socket_address: bind_addr,
            transaction_id_pool: Arc::new(TransactionIdPool::new()),
            mail_boxes: Arc::new(mail_boxes),
            postal_office: Arc::new(postal_office),
        }
    }

    pub async fn ping(self: &Self, recipient: SocketAddrV4) -> Result<(), Error> {
        let transaction_id = self.transaction_id_pool.next_boxed_bytes();
        let ping_msg = Krpc::new_ping_query(transaction_id.clone(), self.our_id);

        // send the ping
        self.postal_office
            .send_to_addr(&ping_msg, &SocketAddr::V4(recipient))
            .await?;

        // await the response
        let response = self.mail_boxes.wait_for(&transaction_id).await;

        // check if the response is the right type, add to our routing table if it is
        return if let Krpc::PingAnnouncePeerResponse(response) = response {
            self.routing_table
                .write()
                .await
                .add_new_node(CompactNodeContact::from_node_id_and_addr(&response.body.id, &recipient));

            Ok(())
        } else {
            warn!("Unexpected response to ping: {:?}", response);
            Err(Error {
                kind: ErrorKind::WrongResponse(format!("{:?}", response)),
                source: None,
            })
        };
    }

    // peers means something special here so you can't use it
    // ask_node_for_nodes just sounds stupid so fuck it, it's her then.
    // Why her and not them? Because I want to piss people off
    fn ask_her_for_nodes(
        &self,
        interlocutor: SocketAddrV4,
        target: NodeId,
        time_out: Duration,
    ) -> impl Future<Output = Result<Vec<CompactNodeContact>, Error>> {
        let transaction_id_pool = Arc::clone(&self.transaction_id_pool);
        let our_id = self.our_id;
        let postal_office = Arc::clone(&self.postal_office);
        let mail_box = Arc::clone(&self.mail_boxes);

        async move {
            // construct the message to query our friends
            let transaction_id = transaction_id_pool.next_boxed_bytes();
            let query = Krpc::new_find_node_query(transaction_id.clone(), our_id, target);

            // send the message out, sending should not take long so we don't need a timeout right???
            // just wait until this is proven wrong
            postal_office
                .send_to_addr(&query, &SocketAddr::V4(interlocutor))
                .await?;

            // wait for the response with a timeout

            let response = timeout(time_out, mail_box.wait_for(&transaction_id)).await?;

            if let Krpc::FindNodeGetPeersNonCompliantResponse(find_node_response) = response {
                let nodes = unsafe { find_node_response.to_node_contact_vec_unchecked() };
                Ok(nodes)
            } else {
                Err(Error::wrong_response(format!("wrong response{:?}", response)))
            }
        }
    }

    #[instrument(skip(self))]
    fn ask_her_for_peers(
        &self,
        interlocutor: SocketAddrV4,
        target: InfoHash,
        time_out: Duration,
    ) -> impl Future<
        Output = Result<
            (
                Option<Box<[u8]>>,
                Either<Vec<CompactNodeContact>, Vec<CompactPeerContact>>,
            ),
            Error,
        >,
    > {
        let transaction_id_pool = Arc::clone(&self.transaction_id_pool);
        let our_id = self.our_id;
        let postal_office = Arc::clone(&self.postal_office);
        let mail_box = Arc::clone(&self.mail_boxes);

        async move {
            // trace!("Asking {:?} for peers", interlocutor);
            // construct the message to query our friends
            let transaction_id = transaction_id_pool.next_boxed_bytes();
            let query = Krpc::new_get_peers_query(transaction_id.clone(), our_id, target);

            // send out the message
            postal_office
                .send_to_addr(&query, &SocketAddr::V4(interlocutor))
                .await?;

            // wait for the response with a timeout
            let response = timeout(time_out, mail_box.wait_for(&transaction_id)).await?;

            // we could get a response that:
            // 1. contains all the peers => we return the Right variant of Either
            // 2. contains their K closest nodes => we return the Left variant of Either
            // 3. totally wrong response => we return an error
            return match response {
                // 2. contains their K closest nodes => we return the Left variant of Either
                Krpc::GetPeersDeferredResponse(response) => {
                    // make sure we don't get duplicate nodes
                    let nodes: Vec<_> = response.to_node_contact_vec();

                    trace!(
                        "got a deferred response from {}, returned nodes: {:#?}",
                        interlocutor,
                        &nodes
                    );
                    Ok((Some(response.body.token), Either::Left(nodes)))
                }

                // 1. contains all the peers => we return the Right variant of Either
                Krpc::FindNodeGetPeersNonCompliantResponse(response) => {
                    // make sure we don't get duplicate nodes
                    let nodes: Vec<_> = unsafe { response.to_node_contact_vec_unchecked() };

                    trace!(
                        "got a deferred response from {} (token missing), returned nodes {:#?}",
                        interlocutor,
                        &nodes
                    );

                    Ok((None, Either::Left(nodes)))
                }

                // 1. contains all the peers => we return the Right variant of Either
                Krpc::GetPeersSuccessResponse(response) => {
                    let mut values = response.body.values;
                    // todo: define an order for nodes??
                    // values.sort_unstable_by_key(|value| value.into());
                    values.dedup();

                    trace!("got a success response from {}, values {:#?}", interlocutor, &values);
                    Ok((Some(response.body.token), Either::Right(values)))
                }

                // 3. totally wrong response => we return an error
                Krpc::ErrorResponse(response) => {
                    warn!("Got an error response to get peers: {:?}", response);
                    Err(Error::wrong_response(format!("{:?}", response)))
                }
                other => {
                    warn!("Unexpected response to get peers: {:?}", other);
                    Err(Error::wrong_response(format!("{:?}", other)))
                }
            };
        }
    }

    /// starting point of trying to find any nodes on the network
    pub async fn find_node(
        self: Arc<Self>,
        target: &NodeId,
        time_out: Duration,
    ) -> Result<CompactNodeContact, DhtServiceFailure> {
        // if we already know the node, then no need for any network requests
        if let Some(node) = (&self).routing_table.read().await.find(target) {
            return Ok(node.contact.clone());
        }

        // find the closest nodes that we know
        let closest;
        {
            let table = (&self).routing_table.read().await;
            closest = table.find_closest(target).into_iter().cloned().collect::<Vec<_>>();
        }

        let returned_nodes: Vec<_> = closest
            .iter()
            .map(|node| {
                let sock_addr: SocketAddrV4 = node.into();
                sock_addr
            })
            .map(|sock_addr| self.ask_her_for_nodes(sock_addr, *target, time_out))
            .collect();

        let returned_nodes = returned_nodes.par_spawn_and_await().await?;

        let returned_nodes: Vec<_> = returned_nodes
            .into_iter()
            .filter(|node| node.is_ok())
            .map(|node| node.unwrap())
            .collect();

        // if they all ended in failure, then we can't find the node
        if returned_nodes.len() == 0 {
            return Err(DhtServiceFailure {
                message: "Could not find node, all nodes requests ended in failure".to_string(),
            });
        }

        // it's possible that some of the nodes returned are actually the node we're looking for
        // so we check for that and return it if it's the case
        let target_node = returned_nodes.iter().flatten().find(|node| node.node_id() == target);

        if target_node.is_some() {
            return Ok(target_node.unwrap().clone());
        }

        // if we don't have the node, then we find the alpha closest nodes and ask them in turn
        let mut sorted_by_distance: Vec<_> = returned_nodes
            .into_iter()
            .flatten()
            .map(|node| {
                let node_id = BigUint::from_bytes_be(node.node_id());
                let our_id = BigUint::from_bytes_be(&self.our_id);
                let distance = our_id.bitxor(node_id);

                (node, distance)
            })
            .collect();
        sorted_by_distance.sort_unstable_by_key(|(_, distance)| distance.clone());

        // add all the nodes we have visited so far
        let seen_node: Arc<Mutex<HashSet<CompactNodeContact>>> = Arc::new(Mutex::new(HashSet::new()));
        {
            let mut seen = seen_node.lock().await;
            sorted_by_distance.iter().for_each(|(node, _)| {
                seen.insert(node.clone());
            });
        }

        let (tx, rx) = oneshot::channel();
        let tx = Arc::new(Mutex::new(Some(tx)));

        let starting_pool: Vec<CompactNodeContact> =
            sorted_by_distance.into_iter().take(3).map(|(node, _)| node).collect();

        let dht = self.clone();
        let target = target.clone();
        let mut parallel_find = tokio::spawn(async move {
            let _ = dht
                .recursive_find_from_pool(starting_pool, target.clone(), seen_node, tx, time_out)
                .await;
        });

        tokio::select! {
             _ = &mut parallel_find => {
                Err(DhtServiceFailure {
                    message: "Could not find node, all nodes requests ended in failure".to_string(),
                })
            },
            Ok(target) = rx => {
                parallel_find.abort();
                Ok(target)
            },
        }
    }

    #[async_recursion]
    #[instrument(skip_all)]
    /// Given a pool of potential nodes, ask them concurrently to see if they have the node we're
    /// looking for, the target return is observed via the slot variable, once it has been filled,
    /// the caller should drop the future to cancel all remaining tasks
    ///
    /// # Note
    /// The time_out parameter does not get reduced for each level of recursion, i.e. if time_out is
    /// 15s and it took 14s for one layer of search to return, the next level will still get another
    /// 15s rather than just 1s.
    async fn recursive_find_from_pool(
        self: Arc<Self>,
        mut starting_pool: Vec<CompactNodeContact>,
        finding: NodeId,
        seen: Arc<Mutex<HashSet<CompactNodeContact>>>,
        slot: Arc<Mutex<Option<Sender<CompactNodeContact>>>>,
        time_out: Duration,
    ) -> Result<(), Error> {
        // filter the pool to only include nodes that we haven't seen yet
        starting_pool = async {
            let seen = seen.lock().await;
            let seen = starting_pool
                .into_iter()
                .filter(|node| !seen.contains(&node))
                .collect::<Vec<_>>();
            info!("len = {}", seen.len());

            seen
        }
        .await;

        // it's ok to assume that this will never get hit for the first time, since the starting
        // pool is always unseen
        if starting_pool.len() == 0 {
            return Err(Error::bottomed_out());
        }

        // ask all the nodes for target!
        let parallel_tasks: Vec<_> = starting_pool
            .into_iter()
            .map(|starting_node| {
                let seen = seen.clone();
                let dht = self.clone();
                let slot = slot.clone();
                async move {
                    let returned_nodes = dht
                        .ask_her_for_nodes((&starting_node).into(), finding, time_out)
                        .await?;

                    // add the nodes to our routing table
                    {
                        let mut table = dht.routing_table.write().await;
                        returned_nodes.iter().for_each(|node| {
                            table.add_new_node(node.clone());
                        });
                    }

                    // see if we got the node we're looking for
                    return if let Some(node) = returned_nodes.iter().find(|node| node.node_id() == &finding) {
                        // if we did, then we're done
                        let mut slot = slot.lock().await;
                        let slot = slot.deref_mut();
                        // lock the sender and sent the value
                        if let Some(_sender) = slot {
                            let slot = slot.take();
                            slot.expect("some one else should ready have finished sending and killed us")
                                .send(node.clone())
                                .expect("some one else should ready have finished sending and killed us");
                            Ok(())
                        } else {
                            Err(Error::cancelled())
                        }
                    } else {
                        // if we didn't, then we add the nodes we got to the seen list and recurse
                        seen.lock().await.insert(starting_node.clone());

                        dht.recursive_find_from_pool(returned_nodes, finding, seen, slot, time_out)
                            .await
                    };
                }
            })
            .collect();

        // spawn all the tasks and await them
        let _results = parallel_tasks.par_spawn_and_await().await?;

        // if we ever reach here, that means we haven't been cancelled, which means nothing were
        // found
        Err(Error::bottomed_out())
    }

    #[async_recursion]
    #[instrument(skip_all)]
    async fn recursive_get_peers_from_pool(
        self: Arc<Self>,
        mut starting_pool: Vec<CompactNodeContact>,
        finding: InfoHash,
        seen: Arc<Mutex<HashSet<CompactNodeContact>>>,
        slot: Arc<Mutex<Option<Sender<(Box<[u8]>, Vec<CompactPeerContact>)>>>>,
        time_out: Duration,
    ) -> Result<(), Error> {
        // filter the pool to only include nodes that we haven't seen yet
        starting_pool = async {
            let seen = seen.lock().await;
            starting_pool
                .into_iter()
                .filter(|node| !seen.contains(&node))
                .collect::<Vec<_>>()
        }
        .await;
        info!("Starting pool size: {:?}", starting_pool.len());

        if starting_pool.len() == 0 {
            debug!("bottomed out");
            return Err(Error::bottomed_out());
        }

        // ask all the nodes for target!
        let parallel_tasks: Vec<_> = starting_pool
            .into_iter()
            .map(|starting_node| {
                let seen = seen.clone();
                let dht = self.clone();
                let slot = slot.clone();
                async move {
                    let (token, returned) = dht
                        .clone()
                        .ask_her_for_peers((&starting_node).into(), finding, time_out)
                        .await?;

                    return match returned {
                        Either::Left(mut deferred) => {
                            trace!("got deferred response, {deferred:#?}");
                            // make sure we don't get duplicate nodes
                            deferred.dedup();
                            {
                                let mut seen = seen.lock().await;
                                seen.insert(starting_node.clone());
                            }

                            dht.recursive_get_peers_from_pool(deferred, finding, seen, slot, time_out)
                                .await
                        }
                        Either::Right(success) => {
                            trace!("got success response, {success:#?}");
                            let mut slot = slot.lock().await;
                            let slot = slot.take();

                            match slot {
                                Some(sender) => {
                                    let _ =
                                        sender.send((token.expect("success response must have the token"), success));
                                    Ok(())
                                }
                                None => Err(Error::cancelled()),
                            }
                        }
                    };
                }
            })
            .collect();

        // spawn all the tasks and await them
        debug!("spawning {} tasks", parallel_tasks.len());
        let _results = parallel_tasks.par_spawn_and_await().await?;
        Err(Error::bottomed_out())
    }

    pub async fn get_peers(
        self: Arc<Self>,
        info_hash: InfoHash,
        time_out: Duration,
    ) -> Result<(Box<[u8]>, Vec<CompactPeerContact>), DhtServiceFailure> {
        // get all the closest nodes to the info_hash
        let closest_nodes: Vec<_> = self
            .routing_table
            .read()
            .await
            .find_closest(&info_hash)
            .into_iter()
            .cloned()
            .collect();

        let seen = Arc::new(Mutex::new(HashSet::new()));
        let (tx, mut rx) = oneshot::channel();
        let slot = Arc::new(Mutex::new(Some(tx)));

        let mut search = tokio::spawn(async move {
            self.recursive_get_peers_from_pool(closest_nodes, info_hash.clone(), seen.clone(), slot.clone(), time_out)
                .await
        });

        return tokio::select! {
            _ = &mut search => {
                Err(DhtServiceFailure {
                    message: "all branches in get peers failed".to_string(),
                })
            }
            Ok(result) = &mut rx => {
                trace!("Received peers from channel, cancelling search");
                search.abort();
                Ok(result)
            }
            else => {
                warn!("are all the tasks done = {:?}", search.is_finished());
                eprintln!("rx = {:?}",rx);
                panic!()
            }
        };
    }

    pub async fn announce_peers(
        self: Arc<Self>,
        recipient: SocketAddrV4,
        info_hash: InfoHash,
        port: Option<u16>,
        token: Box<[u8]>,
    ) -> Result<(), Error> {
        let transaction_id = self.transaction_id_pool.next_boxed_bytes();
        let query = if let Some(port) = port {
            Krpc::new_announce_peer_query(transaction_id.clone(), info_hash, self.our_id, port, true, token)
        } else {
            Krpc::new_announce_peer_query(
                transaction_id.clone(),
                info_hash,
                self.our_id,
                self.socket_address.port(),
                true,
                token,
            )
        };

        self.postal_office
            .send_to_addr(&query, &SocketAddr::V4(recipient))
            .await?;

        let response = self.mail_boxes.wait_for(&transaction_id).await;

        return match response {
            Krpc::PingAnnouncePeerResponse(_) => Ok(()),
            Krpc::ErrorResponse(err) => Err(Error::wrong_response(format!(
                "node responded with an error to our announce peer request {err:?}"
            ))),
            _ => Err(Error::wrong_response(
                "non-compliant response from DHT node".to_string(),
            )),
        };
    }

    /// Given a list of known nodes, bootstrap ourselves to the network by doing a `find_node` to
    /// ourselves to each of the bootstrapping nodes, then performing `find_node` with random id to
    /// each of the nodes the bootstrapping nodes returned.
    #[instrument(skip_all)]
    pub async fn bootstrap_from(
        our_id: NodeId,
        table: Arc<RwLock<RoutingTable>>,
        bind_addr: SocketAddrV4,
        mail_boxes: M,
        postal_office: P,
        known_nodes: Vec<SocketAddrV4>,
        time_out: Duration,
    ) -> Result<Arc<DhtClientV4<M, P>>, Error> {
        use tokio::task::Builder;

        let client = Arc::new(DhtClientV4::new(bind_addr, table, our_id, mail_boxes, postal_office));

        // for each of the known nodes, first send a find_node request to find ourselves, then send
        // send a random find_node request in the bucket range for all the returned nodes
        for known_node in known_nodes {
            // first we ask the known_node about ourselves
            let transaction_id = (&client).transaction_id_pool.next_boxed_bytes();
            let query = Krpc::new_find_node_query(transaction_id.clone(), our_id, our_id);

            info!("bootstrapping with {known_node}");

            // send the query to the known_node
            (&client)
                .postal_office
                .send_to_addr(&query, &SocketAddr::V4(known_node))
                .await?;

            // wait for a response with timeout
            let response = timeout(time_out, (&client).mail_boxes.wait_for(&transaction_id)).await?;

            // if we got a response, then add the nodes to the routing table
            if let Krpc::FindNodeGetPeersNonCompliantResponse(response) = response {
                let nodes = unsafe { response.to_node_contact_vec_unchecked() };
                {
                    // add the bootstrapping node to our routing table
                    let mut table = (&client).routing_table.write().await;
                    table.add_new_node(CompactNodeContact::from_node_id_and_addr(
                        &response.body.id,
                        &known_node,
                    ));
                }

                // second, ask for a random node within the bucket as find_node
                for node in &nodes {
                    (&client).routing_table.write().await.add_new_node(node.clone());
                    let contact: SocketAddrV4 = node.into();
                    let our_id = (&client).our_id.clone();
                    let transaction_id = (&client).transaction_id_pool.next_boxed_bytes();
                    let query = Krpc::new_find_node_query(transaction_id.clone(), our_id, our_id.clone());
                    let client = Arc::clone(&client);
                    Builder::new()
                        .name(&*format!("leave level bootstrap to {}", contact))
                        .spawn(async move {
                            // send the message

                            (&client)
                                .postal_office
                                .send_to_addr(&query, &SocketAddr::V4(contact))
                                .await?;

                            let response =
                                timeout(time_out, async { (&client).mail_boxes.wait_for(&transaction_id).await })
                                    .await?;

                            if let Krpc::FindNodeGetPeersNonCompliantResponse(response) = response {
                                let nodes = unsafe { response.to_node_contact_vec_unchecked() };

                                for node in nodes {
                                    // add the leave level responses to our routing table
                                    let mut table = (&client).routing_table.write().await;
                                    table.add_new_node(CompactNodeContact::from_node_id_and_addr(
                                        &response.body.id,
                                        &node.into(),
                                    ));
                                }
                            }
                            Ok::<_, color_eyre::Report>(())
                        });
                }
                info!("bootstrapping with {known_node} succeeded");
            }
        }
        Ok(client)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::message::TransactionId;
    use std::{
        collections::HashMap,
        fmt::{Display, Formatter},
        net::Ipv4Addr,
        sync,
    };

    #[derive(Debug, Clone)]
    struct HashMapMailBox {
        mail_boxes: Arc<sync::Mutex<HashMap<TransactionId, Krpc>>>,
    }

    #[async_trait::async_trait]
    impl MailBoxes for HashMapMailBox {
        fn get(&self, transaction_id: &TransactionId) -> Option<Krpc> {
            self.mail_boxes.lock().unwrap().remove(transaction_id)
        }

        fn has(&self, transaction_id: &TransactionId) -> bool {
            self.mail_boxes.lock().unwrap().contains_key(transaction_id)
        }

        async fn wait_for(&self, transaction_id: &TransactionId) -> Krpc {
            match self.mail_boxes.lock().unwrap().remove(transaction_id) {
                Some(response) => response,
                None => panic!("no response for transaction id {:#?}", transaction_id),
            }
        }
    }

    #[derive(Debug, Clone)]
    struct LoggedPostalOffice {
        log: Arc<Mutex<String>>,
    }

    #[async_trait::async_trait]
    impl PostalOffice for LoggedPostalOffice {
        async fn send_to_addr(&self, message: &Krpc, addr: &SocketAddr) -> Result<(), Error> {
            self.log
                .lock()
                .await
                .push_str(&format!("{:#?} to {:#?}\n", message, addr));
            Ok(())
        }

        async fn send_to_peer(&self, message: &Krpc, to: &CompactPeerContact) -> Result<(), Error> {
            let socket_addr: SocketAddrV4 = to.into();
            self.send_to_addr(&message, &SocketAddr::from(socket_addr)).await
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn can_create() {
        let our_id = {
            let mut id = [0u8; 20];
            let mut letter = b'a';

            for digit in &mut id {
                *digit = letter;
                letter += 1
            }

            id
        };

        let bind_addr = SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 0);
        let table = RoutingTable::new(&our_id);
        let mail_boxes = HashMapMailBox {
            mail_boxes: Arc::new(sync::Mutex::new(HashMap::new())),
        };
        let postal_office = LoggedPostalOffice {
            log: Arc::new(Mutex::new(String::new())),
        };
        let _client = DhtClientV4::bootstrap_from(
            our_id,
            Arc::new(RwLock::new(table)),
            bind_addr,
            mail_boxes,
            postal_office.clone(),
            vec![
                // dht.tansmissionbt.com
                "87.98.162.88:6881".parse().unwrap(),
                // router.utorrent.com
                "67.215.246.10:6881".parse().unwrap(),
                // router.bittorrent.com, ironically that this almost never responds
                "82.221.103.244:6881".parse().unwrap(),
                // dht.aelitis.com
                "174.129.43.152:6881".parse().unwrap(),
            ],
            Duration::from_secs(60),
        )
        .await;

        println!("{}", &postal_office.log.lock().await);
    }
}
