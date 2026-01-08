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
            // Start with 8KB, will grow as needed
            buf: BytesMut::with_capacity(8192),
        }
    }

    pub fn feed(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);

        const MAX_BUFFER_SIZE: usize = 512 * 1024 * 1024; // 512MB
        if self.buf.len() > MAX_BUFFER_SIZE {
            self.buf.clear();
            // TODO: return an error here
        }
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
        "EXISTS" if parts.len() == 2 => Ok(Command::Exists(parts[1..].to_vec())),
        "INCR" if parts.len() == 2 => Ok(Command::Incr(parts[1].clone())),
        "INCRBY" if parts.len() == 3 => {
            let amount = parts[2]
                .parse::<i64>()
                .map_err(|_| anyhow::anyhow!("ERR value is not an integer or out of range"))?;
            Ok(Command::IncrBy(parts[1].clone(), amount))
        }
        "DECR" if parts.len() == 2 => Ok(Command::Decr(parts[1].clone())),
        "DECRBY" if parts.len() == 3 => {
            let amount = parts[2]
                .parse::<i64>()
                .map_err(|_| anyhow::anyhow!("ERR value is not an integer or out of range"))?;
            Ok(Command::DecrBy(parts[1].clone(), amount))
        }
        "TTL" if parts.len() == 2 => Ok(Command::Ttl(parts[1].clone())),
        "EXPIRE" if parts.len() == 3 => {
            let seconds = parts[2]
                .parse::<i64>()
                .map_err(|_| anyhow::anyhow!("ERR value is not an integer or out of range"))?;
            Ok(Command::Expire(parts[1].clone(), seconds))
        }
        "HGET" if parts.len() == 3 => Ok(Command::HGet(parts[1].clone(), parts[2].clone())),
        "HSET" if parts.len() == 4 => Ok(Command::HSet(
            parts[1].clone(),
            parts[2].clone(),
            parts[3].clone(),
        )),
        "APPEND" if parts.len() == 3 => {
            let value = parts[2].clone();
            Ok(Command::Append(parts[1].clone(), value))
        }
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
    // Start with reasonable size, will grow if needed
    let mut buf = Vec::with_capacity(1024);

    // Encode with automatic buffer growing
    loop {
        match encode(&mut buf, &frame, false) {
            Ok(size) => {
                buf.truncate(size);
                return buf;
            }
            Err(_) => {
                // Buffer too small, grow and retry
                let new_capacity = buf.capacity() * 2;
                buf.resize(new_capacity, 0);
            }
        }
    }
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
