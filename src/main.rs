use std::io::Write;

use anyhow::Context;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Message {
    src: String,
    #[serde(rename = "dest")]
    dst: String,

    body: Body,
}

#[derive(Debug, Serialize, Deserialize)]
struct Body {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "msg_id")]
    id: Option<usize>,
    in_reply_to: Option<usize>,

    #[serde(flatten)]
    payload: Payload,
}

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

impl EchoNode {
    fn step(&mut self, input: Message, output: &mut std::io::StdoutLock) -> anyhow::Result<()> {
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
    let stdin = std::io::stdin().lock();
    let mut stdout = std::io::stdout().lock();

    let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();
    let mut state = EchoNode {
        id: 1,
        node_id: None,
    };
    for input in inputs {
        let input = input.context("maelstrom input from stdin could not be deserialized")?;
        state.step(input, &mut stdout)?;
    }
    Ok(())
}
