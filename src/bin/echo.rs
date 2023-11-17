use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

#[derive(Serialize, Deserialize)]
struct Message {
    src: String,
    #[serde(rename = "dest")]
    dst: String,
    #[serde(rename = "body")]
    payload: Payload,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
enum Payload {
    #[serde(rename = "init")]
    Init {
        msg_id: u32,
        node_id: String,
        node_ids: Vec<String>,
    },
    #[serde(rename = "init_ok")]
    InitOk { in_reply_to: u32 },
    #[serde(rename = "echo")]
    Echo { msg_id: u32, echo: String },
    #[serde(rename = "echo_ok")]
    EchoOk {
        msg_id: u32,
        in_reply_to: u32,
        echo: String,
    },
}

async fn stdio() -> Result<(impl tokio::io::AsyncBufRead, impl tokio::io::AsyncWrite)> {
    Ok((
        tokio::io::BufReader::new(tokio::io::stdin()),
        tokio::io::stdout(),
    ))
}

async fn net() -> Result<impl tokio::io::AsyncBufRead + tokio::io::AsyncWrite> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
    let (socket, _) = listener.accept().await?;
    let socket = tokio::io::BufReader::new(socket);
    Ok(socket)
}

async fn recv_message<T: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut T,
) -> tokio::io::Result<Option<serde_json::Result<Message>>> {
    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        return Ok(None);
    }
    Ok(Some(serde_json::from_str(&line)))
}

async fn send_message<T: tokio::io::AsyncWrite + Unpin>(
    writer: &mut T,
    message: &Message,
) -> tokio::io::Result<()> {
    let buf = serde_json::to_vec(message)?;
    writer.write_all(&buf).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    Ok(())
}

#[derive(Default)]
struct State {
    node_id: String,
    msg_id: u32,
}

async fn app() -> Result<()> {
    let (mut stdin, mut stdout) = stdio().await?;

    let mut state = State::default();

    {
        let Some(message) = recv_message(&mut stdin).await? else {
            bail!("no initial message");
        };

        let Message {
            src,
            dst: _,
            payload,
        } = message?;

        match payload {
            Payload::Init {
                msg_id,
                node_id,
                node_ids: _,
            } => {
                state.node_id = node_id;

                send_message(
                    &mut stdout,
                    &Message {
                        src: state.node_id.clone(),
                        dst: src,
                        payload: Payload::InitOk {
                            in_reply_to: msg_id,
                        },
                    },
                )
                .await?;
            }
            _ => bail!("invalid payload: {:?}", payload),
        }
    }

    while let Some(message) = recv_message(&mut stdin).await? {
        let Message {
            src,
            dst: _,
            payload,
        } = message?;

        match payload {
            Payload::Echo { msg_id, echo } => {
                send_message(
                    &mut stdout,
                    &Message {
                        src: state.node_id.clone(),
                        dst: src,
                        payload: Payload::EchoOk {
                            msg_id: {
                                let id = state.msg_id;
                                state.msg_id += 1;
                                id
                            },
                            in_reply_to: msg_id,
                            echo,
                        },
                    },
                )
                .await?;
            }
            _ => bail!("invalid payload: {:?}", payload),
        }
    }

    Ok(())
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let local_set = tokio::task::LocalSet::new();

    runtime.block_on(local_set.run_until(app()))
}
