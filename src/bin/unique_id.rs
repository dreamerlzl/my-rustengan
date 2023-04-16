use std::io::Write;

use anyhow::Context;
use my_rustengan::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Generate {},
    GenerateOk {
        id: String,
    },
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk {},
}

struct UniqueNode {
    id: usize,
    node_id: String,
}

impl Node<(), Payload> for UniqueNode {
    fn from_init(_: (), init: Init) -> Self {
        UniqueNode {
            id: 1,
            node_id: init.node_id,
        }
    }

    fn step(
        &mut self,
        input: Message<Payload>,
        output: &mut std::io::StdoutLock,
    ) -> anyhow::Result<()> {
        let mut reply = input.into_reply(Some(&mut self.id));
        if let Payload::Generate {} = reply.body.payload {
            reply.body.payload = Payload::GenerateOk {
                id: format!(
                    "{}-{}-{}",
                    &self.node_id,
                    self.id,
                    time::OffsetDateTime::now_utc().unix_timestamp().to_string()
                ),
            };
            serde_json::to_writer(&mut *output, &reply).context("fail to write init_ok")?;
            output
                .write_all(b"\n")
                .context("fail to flush GenerateOk")?;
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, _, UniqueNode>(())
}
