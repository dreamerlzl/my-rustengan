use std::{
    collections::{HashMap, HashSet},
    thread::sleep,
};

use crossbeam::channel::Sender;
use my_rustengan::*;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};

const PUSH_PERIOD: u64 = 100;
type NodeID = String;
type MessageID = usize;

struct BroadcastNode {
    id: usize,
    node_id: NodeID,
    neighbors: Vec<NodeID>,
    messages: HashSet<MessageID>,
    // the current node's knowledge about what its neighbors know
    knows: HashMap<NodeID, HashSet<MessageID>>,
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
        topology: HashMap<NodeID, Vec<NodeID>>,
    },
    TopologyOk,
    GossipPush {
        seen: HashSet<MessageID>,
    },
    GossipPull,
}

enum InjectedGossip {
    Gossip,
}

impl Node<(), Payload, InjectedGossip> for BroadcastNode {
    fn from_init(
        _: (),
        init: my_rustengan::Init,
        tx: Sender<Event<Payload, InjectedGossip>>,
    ) -> Self {
        std::thread::spawn(move || loop {
            sleep(std::time::Duration::from_millis(PUSH_PERIOD));
            if tx.send(Event::Injected(InjectedGossip::Gossip)).is_err() {
                break;
            }
        });
        BroadcastNode {
            id: 1,
            node_id: init.node_id,
            neighbors: Vec::new(),
            messages: HashSet::new(),
            knows: HashMap::new(),
        }
    }

    fn step(
        &mut self,
        event: Event<Payload, InjectedGossip>,
        tx: &Sender<Message<Payload>>,
    ) -> anyhow::Result<()> {
        match event {
            Event::Injected(injected) => match injected {
                InjectedGossip::Gossip => {
                    if let Some(random_neighbor) = self.neighbors.choose(&mut rand::thread_rng()) {
                        // push
                        let seen: HashSet<MessageID> = self
                            .messages
                            .iter()
                            .filter(|m| !self.knows[random_neighbor].contains(m))
                            .copied()
                            .collect();
                        if !seen.is_empty() {
                            let reply = Message {
                                src: self.node_id.clone(),
                                dst: random_neighbor.clone(),
                                body: Body {
                                    id: None,
                                    in_reply_to: None,
                                    payload: Payload::GossipPush { seen },
                                },
                            };
                            tx.send(reply)?;
                        }

                        // pull
                        let reply = Message {
                            src: self.node_id.clone(),
                            dst: random_neighbor.clone(),
                            body: Body {
                                id: None,
                                in_reply_to: None,
                                payload: Payload::GossipPull,
                            },
                        };
                        tx.send(reply)?;
                    }
                }
            },
            Event::Message(input) => {
                let mut reply = input.into_reply(Some(&mut self.id));
                let payload = match reply.body.payload {
                    Payload::GossipPull => {
                        let seen: HashSet<MessageID> = self
                            .messages
                            .iter()
                            .filter(|m| !self.knows[&reply.dst].contains(m))
                            .copied()
                            .collect();
                        if seen.is_empty() {
                            return Ok(());
                        }
                        Payload::GossipPush { seen }
                    }
                    Payload::GossipPush { seen } => {
                        self.messages.extend(seen.clone());
                        self.knows
                            .entry(reply.dst.clone())
                            .or_insert(HashSet::new())
                            .extend(seen);
                        return Ok(());
                    }
                    Payload::Broadcast { message } => {
                        self.messages.insert(message);
                        // now the curret node know the sender knows the message
                        self.knows
                            .entry(reply.dst.clone())
                            .or_insert(HashSet::new())
                            .insert(message);
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
                            self.knows.entry(neighbor.clone()).or_insert(HashSet::new());
                        }
                        Payload::TopologyOk
                    }

                    Payload::ReadOk { messages } => {
                        // self.messages.extend(messages.clone());
                        for message in messages {
                            self.knows
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
            }
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    // at most 4 concurrent gossips
    main_loop::<_, _, _, BroadcastNode>(())
}
