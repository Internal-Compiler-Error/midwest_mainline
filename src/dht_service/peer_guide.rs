use std::{
    net::SocketAddrV4,
    sync::{Arc, Mutex},
};

use tokio::sync::mpsc;

use crate::{
    domain_knowledge::{NodeId, NodeInfo},
    message::Krpc,
    routing::{Node, RoutingTable},
};

#[derive(Debug)]
/// A PeerGuide will tell you who are the closest nodes that we know
pub struct PeerGuide {
    routing_table: Arc<Mutex<RoutingTable>>,
}

impl PeerGuide {
    pub fn new(id: NodeId) -> PeerGuide {
        PeerGuide {
            routing_table: Arc::new(Mutex::new(RoutingTable::new(id))),
        }
    }

    /// keep listening for all incoming responses and upate our table
    pub async fn run(&self, mut inbound: mpsc::Receiver<(Krpc, SocketAddrV4)>) {
        let routing_table = self.routing_table.clone();
        let stuff = async move {
            loop {
                // TODO: worry about the unwrap later
                let (msg, _origin) = inbound.recv().await.unwrap();

                let msg = match msg {
                    Krpc::FindNodeGetPeersResponse(msg) => msg,
                    _ => break,
                };

                {
                    let mut routing_table = routing_table.lock().unwrap();

                    let nodes = msg.nodes();

                    for node in nodes {
                        routing_table.add_new_node(*node)
                    }
                }
            }
        };

        tokio::spawn(stuff);
    }

    pub fn find_closest(&self, target: NodeId) -> Vec<NodeInfo> {
        let routing_table = self.routing_table.lock().unwrap();
        routing_table.find_closest(target)
    }

    pub fn find(&self, target: NodeId) -> Option<Node> {
        let routing_table = self.routing_table.lock().unwrap();
        routing_table.find(target)
    }
}
