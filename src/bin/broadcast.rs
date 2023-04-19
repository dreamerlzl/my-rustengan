use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
    thread::sleep,
};

use crossbeam::channel::Sender;
use my_rustengan::*;
use serde::{Deserialize, Serialize};
use threadpool::ThreadPool;

const PUSH_PERIOD: u64 = 50;
const PULL_PERIOD: u64 = 2 * PUSH_PERIOD;
type NodeID = String;
type MessageID = usize;

struct BroadcastNode {
    id: usize,
    node_id: NodeID,
    neighbors: HashSet<NodeID>,
    messages: HashSet<MessageID>,
    threadpool: ThreadPool,
    // the current node's knowledge about what its neighbors know
    knows: Arc<RwLock<HashMap<NodeID, HashSet<MessageID>>>>,
}

impl Drop for BroadcastNode {
    fn drop(&mut self) {
        self.threadpool.join();
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Broadcast {
        message: MessageID,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: HashSet<MessageID>,
    },
    Topology {
        topology: HashMap<NodeID, HashSet<NodeID>>,
    },
    TopologyOk,
}

impl Node<ThreadPool, Payload> for BroadcastNode {
    fn from_init(threadpool: ThreadPool, init: my_rustengan::Init) -> Self {
        BroadcastNode {
            id: 1,
            node_id: init.node_id,
            neighbors: HashSet::new(),
            messages: HashSet::new(),
            threadpool,
            knows: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn step(
        &mut self,
        input: my_rustengan::Message<Payload>,
        tx: &Sender<Message<Payload>>,
    ) -> anyhow::Result<()> {
        let mut reply = input.into_reply(Some(&mut self.id));
        let payload = match reply.body.payload {
            Payload::Broadcast { message } => {
                if self.messages.insert(message) {
                    // first time to hear this message; tries to gossip it with other peers
                    let mut neighbors_to_gossip = self.neighbors.clone();
                    // we know the sender has known this message
                    neighbors_to_gossip.remove(&reply.dst);
                    let knows = self.knows.clone();
                    let src = self.node_id.clone();
                    let tx = tx.clone();
                    self.threadpool.execute(move || loop {
                        sleep(std::time::Duration::from_millis(PUSH_PERIOD));
                        let mut to_remove: Option<NodeID> = None;
                        if let Some(random_peer) = neighbors_to_gossip.iter().next() {
                            if !knows
                                .read()
                                .expect("failed to read from knows; check init")
                                .get(random_peer)
                                .expect("failed to find a known neighbor")
                                .contains(&message)
                            {
                                let msg = Message {
                                    src: src.clone(),
                                    dst: random_peer.clone(),
                                    body: Body {
                                        id: None,
                                        in_reply_to: None,
                                        payload: Payload::Broadcast { message },
                                    },
                                };
                                //TODO: see whether we can improve this
                                tx.send(msg).expect("fail to pull from other nodes");
                            } else {
                                to_remove = Some(random_peer.clone());
                            }
                        } else {
                            break;
                        }
                        if let Some(peer) = to_remove {
                            neighbors_to_gossip.remove(&peer);
                        }
                    });
                } else {
                    // now the curret node know the sender knows the message
                    self.knows
                        .write()
                        .unwrap()
                        .entry(reply.dst.clone())
                        .or_insert(HashSet::new())
                        .insert(message);
                }
                Payload::BroadcastOk {}
            }

            Payload::Read => Payload::ReadOk {
                messages: self.messages.clone(),
            },

            Payload::Topology { mut topology } => {
                self.neighbors = topology
                    .remove(&self.node_id)
                    .unwrap_or_else(|| panic!("no topology for {}", self.node_id));
                // here we spawn an initial task to periodically pull from other neighbors
                // the check interval would be a bit longer than the push one
                for neighbor in self.neighbors.iter() {
                    self.knows
                        .write()
                        .expect("fail to init self.knows")
                        .entry(neighbor.clone())
                        .or_insert(HashSet::new());
                }
                if !self.neighbors.is_empty() {
                    let neighbors = self.neighbors.clone();
                    let src = self.node_id.clone();
                    let tx = tx.clone();
                    self.threadpool.execute(move || {
                        loop {
                            sleep(std::time::Duration::from_millis(PULL_PERIOD));
                            // we pull messages from a random peer
                            let random_peer = neighbors.iter().next().unwrap();
                            let msg = Message {
                                src: src.clone(),
                                dst: random_peer.clone(),
                                body: Body {
                                    id: None,
                                    in_reply_to: None,
                                    payload: Payload::Read,
                                },
                            };
                            tx.send(msg).expect("fail to send gossip to other nodes");
                        }
                    });
                }
                Payload::TopologyOk
            }

            Payload::ReadOk { messages } => {
                // self.messages.extend(messages.clone());
                for message in messages {
                    self.knows
                        .write()
                        .expect("fail to update from readok")
                        .entry(reply.dst.clone())
                        .or_insert(HashSet::new())
                        .insert(message);
                }
                return Ok(());
            }
            Payload::TopologyOk | Payload::BroadcastOk => return Ok(()),
        };
        reply.body.payload = payload;
        let _ = tx.send(reply);
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    // at most 4 concurrent gossips
    let pool = ThreadPool::new(4);
    main_loop::<_, _, BroadcastNode>(pool)
}
