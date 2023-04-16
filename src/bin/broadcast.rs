use std::{collections::HashMap, io::Write};

use anyhow::Context;
use my_rustengan::*;
use serde::{Deserialize, Serialize};

struct BroadcastNode {
    id: usize,
    node_id: String,
    neighbors: Vec<String>,
    messages: Vec<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: Vec<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
}

impl Node<(), Payload> for BroadcastNode {
    fn from_init(_: (), init: my_rustengan::Init) -> Self {
        BroadcastNode {
            id: 1,
            node_id: init.node_id,
            neighbors: Vec::new(),
            messages: Vec::new(),
        }
    }

    fn step(
        &mut self,
        input: my_rustengan::Message<Payload>,
        output: &mut std::io::StdoutLock,
    ) -> anyhow::Result<()> {
        let mut reply = input.into_reply(Some(&mut self.id));
        let payload = match reply.body.payload {
            Payload::Broadcast { message } => {
                self.messages.push(message);
                Payload::BroadcastOk {}
            }

            Payload::Read => Payload::ReadOk {
                messages: self.messages.clone(),
            },

            Payload::Topology { topology } => {
                for (key, value) in topology.into_iter() {
                    if key.eq(&self.node_id) {
                        self.neighbors = value;
                    }
                }
                Payload::TopologyOk
            }

            Payload::ReadOk { .. } | Payload::TopologyOk | Payload::BroadcastOk => return Ok(()),
        };
        reply.body.payload = payload;
        serde_json::to_writer(&mut *output, &reply)
            .context("fail to write broadcast reply to output")?;
        output
            .write_all(b"\n")
            .context("fail to flush broadcast reply")?;
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, _, BroadcastNode>(())
}
