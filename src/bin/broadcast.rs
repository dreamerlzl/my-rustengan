use std::{
    collections::{HashMap, HashSet},
    thread::sleep,
};

use crossbeam::channel::Sender;
use my_rustengan::*;
use rand::{seq::SliceRandom, Rng};
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
        to_push: HashSet<MessageID>,
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
            Event::Injected(injected) => {
                match injected {
                    InjectedGossip::Gossip => {
                        let rng = &mut rand::thread_rng();
                        if let Some(random_neighbor) = self.neighbors.choose(rng) {
                            // push
                            let (already_known, mut to_push): (HashSet<MessageID>, HashSet<_>) =
                                self.messages
                                    .iter()
                                    .copied()
                                    .partition(|m| self.knows[random_neighbor].contains(m));
                            let denominator = already_known.len() as u32;
                            let nominator = (0.1 * to_push.len() as f64) as u32;
                            to_push.extend(already_known.into_iter().filter(|_| {
                                rng.gen_ratio(nominator.min(denominator), denominator)
                            }));
                            eprintln!("to_push/total : {}/{}", to_push.len(), self.messages.len());

                            let reply = Message {
                                src: self.node_id.clone(),
                                dst: random_neighbor.clone(),
                                body: Body {
                                    id: None,
                                    in_reply_to: None,
                                    payload: Payload::GossipPush { to_push },
                                },
                            };
                            tx.send(reply)?;

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
                }
            }
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
                        Payload::GossipPush { to_push: seen }
                    }
                    Payload::GossipPush { to_push: seen } => {
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
