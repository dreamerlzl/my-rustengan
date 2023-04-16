use std::io::Write;

use anyhow::Context;
use my_rustengan::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Echo { echo: String },
    EchoOk { echo: String },
}

struct EchoNode {
    id: usize,
}

impl Node<(), Payload> for EchoNode {
    fn from_init(_: (), _: Init) -> Self {
        EchoNode { id: 1 }
    }

    fn step(
        &mut self,
        input: Message<Payload>,
        output: &mut std::io::StdoutLock,
    ) -> anyhow::Result<()> {
        if let Payload::Echo { echo } = input.body.payload {
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

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, _, EchoNode>(())
}
