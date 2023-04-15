use std::io::Write;

use anyhow::Context;
use my_rustengan::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk {},
}

struct EchoNode {
    id: usize,
    node_id: Option<String>,
}

impl Node<Payload> for EchoNode {
    fn step(
        &mut self,
        input: Message<Payload>,
        output: &mut std::io::StdoutLock,
    ) -> anyhow::Result<()> {
        match input.body.payload {
            Payload::Echo { echo } => {
                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::EchoOk { echo },
                    },
                };
                self.id += 1;
                serde_json::to_writer(&mut *output, &reply).context("fail to write output")?;
                output.write_all(b"\n").context("fail to flush echo_ok")?;
            }

            Payload::Init { node_id, .. } => {
                self.node_id = Some(node_id);
                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: None,
                        in_reply_to: input.body.id,
                        payload: Payload::InitOk {},
                    },
                };
                serde_json::to_writer(&mut *output, &reply).context("fail to write init_ok")?;
                output.write_all(b"\n").context("fail to flush init_ok")?;
            }

            _ => {}
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop(EchoNode {
        id: 1,
        node_id: None,
    })
}
