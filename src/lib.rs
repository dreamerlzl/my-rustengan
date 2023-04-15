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

pub trait Node<Payload> {
    fn step(
        &mut self,
        input: Message<Payload>,
        output: &mut std::io::StdoutLock,
    ) -> anyhow::Result<()>;
}

pub fn main_loop<S, Payload>(mut state: S) -> anyhow::Result<()>
where
    S: Node<Payload>,
    Payload: DeserializeOwned,
{
    let stdin = std::io::stdin().lock();
    let mut stdout = std::io::stdout().lock();

    let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message<Payload>>();
    for input in inputs {
        let input = input.context("maelstrom input from stdin could not be deserialized")?;
        state.step(input, &mut stdout)?;
    }
    Ok(())
}
