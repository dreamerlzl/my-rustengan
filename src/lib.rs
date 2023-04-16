use std::io::{BufRead, Write};

use anyhow::Context;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Message<Payload> {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,

    pub body: Body<Payload>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Body<Payload> {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "msg_id")]
    pub id: Option<usize>,
    pub in_reply_to: Option<usize>,

    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InitPayload {
    Init(Init),
    InitOk,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Init {
    pub node_id: String,
    node_ids: Vec<String>,
}

#[derive(Serialize)]
struct InitOk;

pub trait Node<S, Payload> {
    fn from_init(init_state: S, init: Init) -> Self;

    fn step(
        &mut self,
        input: Message<Payload>,
        output: &mut std::io::StdoutLock,
    ) -> anyhow::Result<()>;
}

pub fn main_loop<InitState, Payload, N>(init_state: InitState) -> anyhow::Result<()>
where
    N: Node<InitState, Payload>,
    Payload: DeserializeOwned,
{
    let mut stdin = std::io::stdin().lock().lines();
    let mut stdout = std::io::stdout().lock();
    let input =
        serde_json::from_str::<Message<InitPayload>>(&stdin.next().expect("no init msg found")?)
            .context("fail to deserialize init")?;
    let InitPayload::Init(init) =
        input.body.payload else {
                return Err(anyhow::anyhow!("the must msg type should be init"));
            };

    let mut state = N::from_init(init_state, init);
    let reply = Message {
        src: input.dst,
        dst: input.src,
        body: Body {
            id: None,
            in_reply_to: input.body.id,
            payload: InitPayload::InitOk {},
        },
    };
    serde_json::to_writer(&mut stdout, &reply).context("fail to write init_ok")?;
    stdout.write_all(b"\n").context("fail to flush init_ok")?;

    for line in stdin {
        let line = line?;
        let input: Message<Payload> =
            serde_json::from_str(&line).context("fail to deserialize step payload")?;
        state.step(input, &mut stdout).context("step failed")?;
    }
    Ok(())
}
