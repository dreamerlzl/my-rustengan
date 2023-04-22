use std::io::{BufRead, Write};

use anyhow::Context;
use crossbeam::channel::{unbounded, Sender};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Message<Payload> {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,

    pub body: Body<Payload>,
}

impl<Payload> Message<Payload> {
    pub fn into_reply(self, id: Option<&mut usize>) -> Self {
        Message {
            src: self.dst,
            dst: self.src,
            body: Body {
                id: id.map(|id| {
                    let mid = *id;
                    *id += 1;
                    mid
                }),
                in_reply_to: self.body.id,
                payload: self.body.payload,
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Body<Payload> {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "msg_id")]
    pub id: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
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

#[derive(Debug)]
pub enum Event<Payload, InjectedPayload = ()> {
    Message(Message<Payload>),
    Injected(InjectedPayload),
}

pub trait Node<S, Payload, InjectedPayload = ()> {
    fn from_init(init_state: S, init: Init, tx: Sender<Event<Payload, InjectedPayload>>) -> Self;

    fn step(
        &mut self,
        input: Event<Payload, InjectedPayload>,
        tx: &Sender<Message<Payload>>,
        // output: &mut std::io::StdoutLock,
    ) -> anyhow::Result<()>;
}

pub fn main_loop<InitState, Payload, InjectedPayload, N>(
    init_state: InitState,
) -> anyhow::Result<()>
where
    N: Node<InitState, Payload, InjectedPayload>,
    Payload: DeserializeOwned + Send + 'static + Serialize,
    InjectedPayload: Send + 'static,
{
    let stdin_lock = std::io::stdin().lock();
    let mut stdin_lines = stdin_lock.lines();
    let stdout = std::io::stdout();
    let mut stdout_lock = stdout.lock();
    let input = serde_json::from_str::<Message<InitPayload>>(
        &stdin_lines.next().expect("no init msg found")?,
    )
    .context("fail to deserialize init")?;
    let InitPayload::Init(init) =
        input.body.payload else {
                return Err(anyhow::anyhow!("the must msg type should be init"));
            };

    let (stdin_tx, stdin_rx) = unbounded();
    let mut state = N::from_init(init_state, init, stdin_tx.clone());
    let reply = Message {
        src: input.dst,
        dst: input.src,
        body: Body {
            id: None,
            in_reply_to: input.body.id,
            payload: InitPayload::InitOk {},
        },
    };
    send(&mut stdout_lock, &reply)?;
    drop(stdout_lock);

    // hides the details about output
    let (stdout_tx, stdout_rx) = unbounded();
    let stdout_handle = std::thread::spawn(move || {
        let mut stdout_lock = stdout.lock();
        for msg in stdout_rx {
            send(&mut stdout_lock, &msg)?;
        }
        Ok::<_, anyhow::Error>(())
    });

    drop(stdin_lines);

    std::thread::spawn(move || {
        let stdin_lines = std::io::stdin().lock().lines();
        for line in stdin_lines {
            let line = line?;
            let input: Message<Payload> =
                serde_json::from_str(&line).context("fail to deserialize step payload")?;
            let _ = stdin_tx.send(Event::Message(input));
        }
        Ok::<_, anyhow::Error>(())
    });

    for event in stdin_rx {
        state.step(event, &stdout_tx).context("step failed")?;
    }

    stdout_handle
        .join()
        .expect("fail to join stdout handle")
        .context("stdout handle exit with an error")?;
    Ok(())
}

fn send<T>(mut output: &mut std::io::StdoutLock, msg: &T) -> anyhow::Result<()>
where
    T: Sized + Serialize,
{
    serde_json::to_writer(&mut output, msg).context("fail to write init_ok")?;
    output.write_all(b"\n").context("fail to flush init_ok")?;
    Ok(())
}
