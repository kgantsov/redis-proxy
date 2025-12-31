use anyhow::Result;
use bytes::{Buf, BytesMut};
use redis_protocol::resp2::types::OwnedFrame;

use crate::protocol::commands::Command;
use redis_protocol::resp2::{decode::decode, encode::encode};

pub struct RespDecoder {
    buf: BytesMut,
}

impl RespDecoder {
    pub fn new() -> Self {
        Self {
            buf: BytesMut::with_capacity(4096),
        }
    }

    pub fn feed(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    pub fn next_frame(&mut self) -> anyhow::Result<Option<OwnedFrame>> {
        match decode(&self.buf)? {
            Some((frame, consumed)) => {
                self.buf.advance(consumed);
                Ok(Some(frame))
            }
            None => Ok(None),
        }
    }
}

pub fn frame_to_string(f: OwnedFrame) -> anyhow::Result<String> {
    use redis_protocol::resp2::types::OwnedFrame::*;

    match f {
        BulkString(bytes) | SimpleString(bytes) => Ok(String::from_utf8(bytes)?),
        Integer(i) => Ok(i.to_string()),
        _ => anyhow::bail!("ERR invalid argument type"),
    }
}

pub fn parse_parts(parts: &[String]) -> anyhow::Result<Command> {
    if parts.is_empty() {
        anyhow::bail!("ERR empty command");
    }

    match parts[0].to_uppercase().as_str() {
        "GET" if parts.len() == 2 => Ok(Command::Get(parts[1].clone())),
        "SET" if parts.len() >= 3 => Ok(Command::Set(parts[1].clone(), parts[2].clone())),
        "DEL" if parts.len() >= 2 => Ok(Command::Del(parts[1..].to_vec())),
        "PING" => Ok(Command::Ping(parts.get(1).cloned())),
        "INFO" => Ok(Command::Info),
        "COMMAND" => Ok(Command::Command),
        "CLIENT" => Ok(Command::Client),
        _ => anyhow::bail!("ERR unknown or invalid command"),
    }
}

pub fn parse_command(frame: OwnedFrame) -> anyhow::Result<Command> {
    use redis_protocol::resp2::types::OwnedFrame::*;

    match frame {
        Array(arr) => {
            let parts: Vec<String> = arr
                .into_iter()
                .map(frame_to_string)
                .collect::<Result<_>>()?;
            parse_parts(&parts)
        }

        SimpleString(bytes) => {
            let s = std::str::from_utf8(&bytes)?;
            let parts: Vec<String> = s.split_whitespace().map(|s| s.to_string()).collect();
            parse_parts(&parts)
        }

        _ => anyhow::bail!("ERR unsupported frame"),
    }
}

pub fn encode_frame(frame: OwnedFrame) -> Vec<u8> {
    // Max Redis frame size is unknown; 4KB is safe for responses
    let mut buf = vec![0u8; 4096];

    let size = encode(&mut buf, &frame, false).expect("RESP encode failed");

    buf.truncate(size);
    buf
}

pub fn resp_ok() -> Vec<u8> {
    encode_frame(OwnedFrame::SimpleString("OK".into()))
}

pub fn resp_err(msg: &str) -> Vec<u8> {
    encode_frame(OwnedFrame::Error(msg.into()))
}

pub fn resp_bulk(opt: Option<String>) -> Vec<u8> {
    match opt {
        Some(s) => encode_frame(OwnedFrame::BulkString(s.into_bytes())),
        None => encode_frame(OwnedFrame::Null),
    }
}

pub fn resp_int(i: i64) -> Vec<u8> {
    encode_frame(OwnedFrame::Integer(i))
}
