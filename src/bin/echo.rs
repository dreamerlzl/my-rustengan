use crossbeam::channel::Sender;
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
    fn from_init(_: (), _: Init, _: Sender<Event<Payload>>) -> Self {
        EchoNode { id: 1 }
    }

    fn step(&mut self, event: Event<Payload>, tx: &Sender<Message<Payload>>) -> anyhow::Result<()> {
        let Event::Message(input) = event else {
            panic!("unexpected injected input")
        };
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
            tx.send(reply)?;
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, _, _, EchoNode>(())
}
