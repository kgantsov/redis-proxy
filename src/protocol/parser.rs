use anyhow::Result;
use bytes::{Buf, BytesMut};

use crate::protocol::commands::Command;

use redis_protocol::resp2::{
    decode::decode as decode2, encode::encode as encode2, types::OwnedFrame as Resp2OwnedFrame,
};
use redis_protocol::resp3::{
    decode::complete::decode as decode3, encode::complete::encode as encode3,
    types::OwnedFrame as Resp3OwnedFrame,
};

/// Which RESP protocol a connection is using.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    Resp2,
    Resp3,
}

/// Unified frame enum to represent either RESP2 or RESP3 decoded frames.
#[derive(Debug)]
pub enum RedisFrame {
    Resp2(Resp2OwnedFrame),
    Resp3(Resp3OwnedFrame),
}

pub struct RespDecoder {
    buf: BytesMut,
    detected: Option<Protocol>,
}

impl RespDecoder {
    pub fn new() -> Self {
        Self {
            // Start with 8KB, will grow as needed
            buf: BytesMut::with_capacity(8192),
            detected: None,
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

    /// Attempt to decode the next frame. Auto-detects RESP3 first and falls back to RESP2.
    /// Returns Some((RedisFrame, Protocol)) when a complete frame is decoded.
    pub fn next_frame(&mut self) -> anyhow::Result<Option<(RedisFrame, Protocol)>> {
        // If we already detected a protocol for this connection, try that decoder first.
        if let Some(proto) = self.detected {
            match proto {
                Protocol::Resp3 => {
                    if let Some((frame, consumed)) = decode3(&self.buf)? {
                        self.buf.advance(consumed);
                        self.detected = Some(Protocol::Resp3);
                        return Ok(Some((RedisFrame::Resp3(frame), Protocol::Resp3)));
                    } else {
                        return Ok(None);
                    }
                }
                Protocol::Resp2 => {
                    if let Some((frame, consumed)) = decode2(&self.buf)? {
                        self.buf.advance(consumed);
                        self.detected = Some(Protocol::Resp2);
                        return Ok(Some((RedisFrame::Resp2(frame), Protocol::Resp2)));
                    } else {
                        return Ok(None);
                    }
                }
            }
        }

        // No protocol detected yet: try RESP3 first, then RESP2.
        if let Some((frame3, consumed3)) = decode3(&self.buf)? {
            self.buf.advance(consumed3);
            self.detected = Some(Protocol::Resp3);
            return Ok(Some((RedisFrame::Resp3(frame3), Protocol::Resp3)));
        }

        if let Some((frame2, consumed2)) = decode2(&self.buf)? {
            self.buf.advance(consumed2);
            self.detected = Some(Protocol::Resp2);
            return Ok(Some((RedisFrame::Resp2(frame2), Protocol::Resp2)));
        }

        Ok(None)
    }

    /// Returns the detected protocol for this decoder/connection (if any).
    pub fn protocol(&self) -> Option<Protocol> {
        self.detected
    }
}

/// Extract a string from a frame element that represents text/byte data or integer.
/// Accepts either RESP2 or RESP3 frames.
pub fn frame_to_string(f: &RedisFrame) -> anyhow::Result<String> {
    match f {
        RedisFrame::Resp2(rf) => {
            use redis_protocol::resp2::types::OwnedFrame::*;
            match rf {
                BulkString(bytes) | SimpleString(bytes) => Ok(String::from_utf8(bytes.clone())?),
                Integer(i) => Ok(i.to_string()),
                _ => anyhow::bail!("ERR invalid argument type"),
            }
        }
        RedisFrame::Resp3(rf) => {
            use redis_protocol::resp3::types::OwnedFrame::*;
            match rf {
                // RESP3 may expose a few different string-like variants; try to
                // handle the common ones used for commands.
                SimpleString { data, .. } => Ok(String::from_utf8(data.clone())?),
                BlobString { data, .. } => Ok(String::from_utf8(data.clone())?),
                VerbatimString { data, .. } => Ok(String::from_utf8(data.clone())?),
                Number { data, .. } => Ok(data.to_string()),
                BigNumber { data, .. } => Ok(String::from_utf8(data.clone())?),
                _ => anyhow::bail!("ERR invalid argument type"),
            }
        }
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
        "EXISTS" if parts.len() >= 2 => Ok(Command::Exists(parts[1..].to_vec())),
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
        "HGETALL" if parts.len() == 2 => Ok(Command::HGetAll(parts[1].clone())),
        "HVALS" if parts.len() == 2 => Ok(Command::HVals(parts[1].clone())),
        "APPEND" if parts.len() == 3 => {
            let value = parts[2].clone();
            Ok(Command::Append(parts[1].clone(), value))
        }
        "SADD" if parts.len() >= 3 => Ok(Command::SAdd(parts[1].clone(), parts[2..].to_vec())),
        "SREM" if parts.len() >= 3 => Ok(Command::SRem(parts[1].clone(), parts[2..].to_vec())),
        "SMEMBERS" if parts.len() == 2 => Ok(Command::SMembers(parts[1].clone())),
        "PING" => Ok(Command::Ping(parts.get(1).cloned())),
        "INFO" => Ok(Command::Info),
        "COMMAND" => Ok(Command::Command),
        "CLIENT" => Ok(Command::Client),
        _ => anyhow::bail!("ERR unknown or invalid command"),
    }
}

/// Parse a command from a decoded frame (which may be RESP2 or RESP3).
/// Returns a Command and leaves protocol detection to the caller (the decoder).
pub fn parse_command(frame: RedisFrame) -> anyhow::Result<Command> {
    match frame {
        RedisFrame::Resp2(f) => {
            use redis_protocol::resp2::types::OwnedFrame::*;
            match f {
                Array(arr) => {
                    let parts: Vec<String> = arr
                        .into_iter()
                        .map(|f| frame_to_string(&RedisFrame::Resp2(f)))
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
        RedisFrame::Resp3(f) => {
            use redis_protocol::resp3::types::OwnedFrame::*;
            match f {
                Array { data, .. } => {
                    let parts: Vec<String> = data
                        .into_iter()
                        .map(|f| frame_to_string(&RedisFrame::Resp3(f)))
                        .collect::<Result<_>>()?;
                    parse_parts(&parts)
                }
                SimpleString { data, .. } => {
                    let s = String::from_utf8(data)?;
                    let parts: Vec<String> = s.split_whitespace().map(|s| s.to_string()).collect();
                    parse_parts(&parts)
                }
                BlobString { data, .. } => {
                    let s = String::from_utf8(data)?;
                    let parts: Vec<String> = s.split_whitespace().map(|s| s.to_string()).collect();
                    parse_parts(&parts)
                }
                VerbatimString { data, .. } => {
                    let s = String::from_utf8(data)?;
                    let parts: Vec<String> = s.split_whitespace().map(|s| s.to_string()).collect();
                    parse_parts(&parts)
                }
                _ => anyhow::bail!("ERR unsupported frame"),
            }
        }
    }
}

/// Encode a RESP2 `OwnedFrame` (backwards-compatible helper).
pub fn encode_frame(frame: Resp2OwnedFrame) -> Vec<u8> {
    // Start with reasonable size, will grow if needed
    let mut buf = Vec::with_capacity(1024);

    // Encode with automatic buffer growing
    loop {
        match encode2(&mut buf, &frame, false) {
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

/// Encode a frame for the given protocol (RESP2 or RESP3).
/// Accepts our unified RedisFrame and encodes with the appropriate encoder.
pub fn encode_frame_for_protocol(frame: &RedisFrame, proto: Protocol) -> Vec<u8> {
    match (frame, proto) {
        (RedisFrame::Resp2(f), Protocol::Resp2) => encode_frame(f.clone()),
        (RedisFrame::Resp3(f), Protocol::Resp3) => {
            // Encode RESP3 frame
            let mut buf = Vec::with_capacity(1024);
            loop {
                match encode3(&mut buf, f, false) {
                    Ok(size) => {
                        buf.truncate(size);
                        return buf;
                    }
                    Err(_) => {
                        let new_capacity = buf.capacity() * 2;
                        buf.resize(new_capacity, 0);
                    }
                }
            }
        }
        // Cross-encode: client used a different protocol than the frame's native form.
        // Prefer encoding according to the requested proto by attempting to convert.
        (RedisFrame::Resp2(f2), Protocol::Resp3) => {
            // Best-effort conversion: encode a RESP2 frame to its RESP3 equivalent by
            // mapping common scalar types.
            let conv = match f2 {
                redis_protocol::resp2::types::OwnedFrame::SimpleString(b) => {
                    Resp3OwnedFrame::SimpleString {
                        data: b.clone(),
                        attributes: None,
                    }
                }
                redis_protocol::resp2::types::OwnedFrame::BulkString(b) => {
                    Resp3OwnedFrame::BlobString {
                        data: b.clone(),
                        attributes: None,
                    }
                }
                redis_protocol::resp2::types::OwnedFrame::Integer(i) => Resp3OwnedFrame::Number {
                    data: *i,
                    attributes: None,
                },
                redis_protocol::resp2::types::OwnedFrame::Null => Resp3OwnedFrame::Null,
                redis_protocol::resp2::types::OwnedFrame::Array(arr) => {
                    let mut v = Vec::with_capacity(arr.len());
                    for it in arr {
                        // recursive conversion: map OwnedFrame -> Resp3Frame
                        let rf = match it {
                            redis_protocol::resp2::types::OwnedFrame::SimpleString(b) => {
                                Resp3OwnedFrame::SimpleString {
                                    data: b.clone(),
                                    attributes: None,
                                }
                            }
                            redis_protocol::resp2::types::OwnedFrame::BulkString(b) => {
                                Resp3OwnedFrame::BlobString {
                                    data: b.clone(),
                                    attributes: None,
                                }
                            }
                            redis_protocol::resp2::types::OwnedFrame::Integer(i) => {
                                Resp3OwnedFrame::Number {
                                    data: *i,
                                    attributes: None,
                                }
                            }
                            _ => Resp3OwnedFrame::Null,
                        };
                        v.push(rf);
                    }
                    Resp3OwnedFrame::Array {
                        data: v,
                        attributes: None,
                    }
                }
                _ => Resp3OwnedFrame::Null,
            };

            let mut buf = Vec::with_capacity(1024);
            loop {
                match encode3(&mut buf, &conv, false) {
                    Ok(size) => {
                        buf.truncate(size);
                        return buf;
                    }
                    Err(_) => {
                        let new_capacity = buf.capacity() * 2;
                        buf.resize(new_capacity, 0);
                    }
                }
            }
        }
        (RedisFrame::Resp3(f3), Protocol::Resp2) => {
            // Convert common RESP3 frames into RESP2 OwnedFrame equivalents.
            use redis_protocol::resp3::types::OwnedFrame::*;
            let conv = match f3 {
                SimpleString { data, .. } => {
                    redis_protocol::resp2::types::OwnedFrame::SimpleString(data.clone())
                }
                BlobString { data, .. } => {
                    redis_protocol::resp2::types::OwnedFrame::BulkString(data.clone())
                }
                Number { data, .. } => redis_protocol::resp2::types::OwnedFrame::Integer(*data),
                VerbatimString { data, .. } => {
                    redis_protocol::resp2::types::OwnedFrame::BulkString(data.clone())
                }
                Array { data, .. } => {
                    let mut v = Vec::with_capacity(data.len());
                    for it in data {
                        match it {
                            SimpleString { data, .. } => v.push(
                                redis_protocol::resp2::types::OwnedFrame::BulkString(data.clone()),
                            ),
                            BlobString { data, .. } => v.push(
                                redis_protocol::resp2::types::OwnedFrame::BulkString(data.clone()),
                            ),
                            Number { data, .. } => {
                                v.push(redis_protocol::resp2::types::OwnedFrame::Integer(*data))
                            }
                            _ => v.push(redis_protocol::resp2::types::OwnedFrame::Null),
                        }
                    }
                    redis_protocol::resp2::types::OwnedFrame::Array(v)
                }
                _ => redis_protocol::resp2::types::OwnedFrame::Null,
            };
            encode_frame(conv)
        }
    }
}

/// Backwards-compatible helper functions for simple responses.
/// These default to RESP2 encoding. New code should prefer encoding with an explicit protocol.
pub fn resp_ok() -> Vec<u8> {
    encode_frame(redis_protocol::resp2::types::OwnedFrame::SimpleString(
        b"OK".to_vec(),
    ))
}

pub fn resp_err(msg: &str) -> Vec<u8> {
    encode_frame(redis_protocol::resp2::types::OwnedFrame::Error(msg.into()))
}

pub fn resp_bulk(opt: Option<String>) -> Vec<u8> {
    match opt {
        Some(s) => encode_frame(redis_protocol::resp2::types::OwnedFrame::BulkString(
            s.into_bytes(),
        )),
        None => encode_frame(redis_protocol::resp2::types::OwnedFrame::Null),
    }
}

pub fn resp_int(i: i64) -> Vec<u8> {
    encode_frame(redis_protocol::resp2::types::OwnedFrame::Integer(i))
}
