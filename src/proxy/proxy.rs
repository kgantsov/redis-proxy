use anyhow::{Context, Result};
use redis::aio::ConnectionManager;
use redis::RedisResult;
use redis_protocol::resp2::types::OwnedFrame as Resp2OwnedFrame;
use redis_protocol::resp3::types::OwnedFrame as Resp3OwnedFrame;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

use crate::metrics::prometheus::{COMMANDS_PROXIED_COUNTER, CONNECTIONS_GAUGE};
use crate::protocol::commands::Command;
use crate::protocol::parser::{parse_command, Protocol, RedisFrame, RespDecoder};
use crate::proxy::connection_pool::ConnectionPool;
use crate::proxy::consistent_hash::ConsistentHash;
use crate::proxy::node::RedisNode;

pub struct RedisProxy {
    consistent_hash: Arc<RwLock<ConsistentHash>>,
    connection_pool: Arc<RwLock<ConnectionPool>>,
}

impl RedisProxy {
    pub fn new(replicas: usize, pool_size: usize) -> Self {
        Self {
            consistent_hash: Arc::new(RwLock::new(ConsistentHash::new(replicas))),
            connection_pool: Arc::new(RwLock::new(ConnectionPool::new(pool_size))),
        }
    }

    pub async fn add_node(&self, node: RedisNode) -> Result<()> {
        let mut pool = self.connection_pool.write().await;
        pool.add_connection(&node).await?;

        let mut hash = self.consistent_hash.write().await;
        hash.add_node(node);

        Ok(())
    }

    pub async fn remove_node(&self, node_id: &str) {
        let mut hash = self.consistent_hash.write().await;
        hash.remove_node(node_id);
    }

    pub async fn start_server(&self, bind_addr: &str) -> Result<()> {
        let listener = TcpListener::bind(bind_addr)
            .await
            .context("Failed to bind to address")?;

        println!("Redis proxy listening on {}", bind_addr);

        loop {
            let (socket, addr) = listener.accept().await?;
            println!("New connection from {}", addr);
            CONNECTIONS_GAUGE.inc();

            let proxy = self.clone();
            tokio::spawn(async move {
                if let Err(e) = proxy.handle_client(socket).await {
                    eprintln!("Error handling client {}: {}", addr, e);
                }
            });
        }
    }

    async fn handle_client(&self, mut socket: TcpStream) -> Result<()> {
        let mut decoder = RespDecoder::new();
        let mut read_buf = vec![0u8; 8192]; // Larger initial buffer, still efficient
        let mut client_protocol: Option<Protocol> = None;

        loop {
            let n = socket.read(&mut read_buf).await?;
            if n == 0 {
                break;
            }

            decoder.feed(&read_buf[..n]);

            while let Some((frame, proto)) = decoder.next_frame()? {
                // Remember which protocol this client is using
                if client_protocol.is_none() {
                    client_protocol = Some(proto);
                }

                let cmd = match parse_command(frame) {
                    Ok(cmd) => cmd,
                    Err(e) => {
                        let err_response = encode_error(&e.to_string(), proto);
                        socket.write_all(&err_response).await?;
                        continue;
                    }
                };

                let response = self.execute_command(cmd, proto).await;
                socket.write_all(&response).await?;
                COMMANDS_PROXIED_COUNTER.inc();
            }
        }

        CONNECTIONS_GAUGE.dec();
        Ok(())
    }

    async fn execute_command(&self, cmd: Command, proto: Protocol) -> Vec<u8> {
        // Handle multi-key commands that need distribution across nodes
        match &cmd {
            Command::Del(keys) if !keys.is_empty() => {
                match self.proxy_multi_key_command("DEL", keys).await {
                    Ok(resp_string) => return resp_string.into_bytes(),
                    Err(e) => return encode_error(&e.to_string(), proto),
                }
            }
            Command::Exists(keys) if !keys.is_empty() => {
                match self.proxy_multi_key_command("EXISTS", keys).await {
                    Ok(resp_string) => return resp_string.into_bytes(),
                    Err(e) => return encode_error(&e.to_string(), proto),
                }
            }
            _ => {}
        }

        // Convert Command back to Vec<String>
        let parts: Vec<String> = match cmd {
            Command::Hello { version, auth } => {
                println!("Got hello command with {:?} {:?}", version, auth);
                let mut v = vec!["HELLO".into()];
                if let Some(version) = version {
                    v.push(version.to_string());
                }
                if let Some((username, password)) = auth {
                    v.push(username);
                    v.push(password);
                }
                v
            }
            Command::Get(k) => vec!["GET".into(), k],
            Command::Set(k, v) => vec!["SET".into(), k, v],
            Command::Del(keys) => {
                let mut v = vec!["DEL".into()];
                v.extend(keys);
                v
            }
            Command::Exists(keys) => {
                let mut v = vec!["EXISTS".into()];
                v.extend(keys);
                v
            }
            Command::Incr(k) => vec!["INCR".into(), k],
            Command::IncrBy(k, n) => vec!["INCRBY".into(), k, n.to_string()],
            Command::Decr(k) => vec!["DECR".into(), k],
            Command::DecrBy(k, n) => vec!["DECRBY".into(), k, n.to_string()],
            Command::Expire(k, s) => vec!["EXPIRE".into(), k, s.to_string()],
            Command::Ttl(k) => vec!["TTL".into(), k],
            Command::HGet(k, f) => vec!["HGET".into(), k, f],
            Command::HSet(k, f, v) => vec!["HSET".into(), k, f, v],
            Command::Append(k, v) => vec!["APPEND".into(), k, v],
            Command::SAdd(k, members) => {
                let mut v = vec!["SADD".into(), k];
                v.extend(members);
                v
            }
            Command::SMembers(k) => vec!["SMEMBERS".into(), k],

            Command::Ping(msg) => {
                return match msg {
                    Some(s) => encode_response(
                        &RedisFrame::Resp2(Resp2OwnedFrame::BulkString(s.into_bytes())),
                        proto,
                    ),
                    None => encode_response(
                        &RedisFrame::Resp2(Resp2OwnedFrame::SimpleString(b"PONG".to_vec())),
                        proto,
                    ),
                };
            }

            Command::Info => {
                return encode_response(
                    &RedisFrame::Resp2(Resp2OwnedFrame::BulkString(b"Redis Proxy v1.0".to_vec())),
                    proto,
                )
            }

            Command::Command => {
                return encode_response(&RedisFrame::Resp2(Resp2OwnedFrame::Array(vec![])), proto)
            }

            Command::Client => {
                return encode_response(
                    &RedisFrame::Resp2(Resp2OwnedFrame::SimpleString(b"OK".to_vec())),
                    proto,
                )
            }
        };

        // Commands that touch data must have a key
        if parts.len() < 2 {
            return encode_error("ERR wrong number of arguments", proto);
        }

        let key = &parts[1];

        match self.proxy_command_to_node(key, &parts).await {
            Ok(resp_string) => resp_string.into_bytes(),
            Err(e) => encode_error(&e.to_string(), proto),
        }
    }

    async fn proxy_command_to_node(&self, key: &str, parts: &[String]) -> Result<String> {
        let node = {
            let hash = self.consistent_hash.read().await;
            match hash.get_node(key) {
                Some(node) => node.clone(),
                None => return Ok("-ERR no available nodes\r\n".to_string()),
            }
        };

        let conn_manager = {
            let pool = self.connection_pool.read().await;
            match pool.get_connection(&node.id) {
                Some(conn) => conn,
                None => return Ok("-ERR node connection not available\r\n".to_string()),
            }
        };

        let mut conn = conn_manager.lock().await;
        match self.execute_redis_command(&mut conn, parts).await {
            Ok(response) => Ok(response),
            Err(e) => {
                eprintln!("Error executing command on node {}: {}", node.id, e);
                Ok(format!("-ERR {}\r\n", e))
            }
        }
    }

    async fn proxy_multi_key_command(&self, command: &str, keys: &[String]) -> Result<String> {
        use std::collections::HashMap;

        // Group keys by their target nodes
        let mut keys_by_node: HashMap<String, Vec<String>> = HashMap::new();

        {
            let hash = self.consistent_hash.read().await;
            for key in keys {
                match hash.get_node(key) {
                    Some(node) => {
                        keys_by_node
                            .entry(node.id.clone())
                            .or_insert_with(Vec::new)
                            .push(key.clone());
                    }
                    None => return Ok("-ERR no available nodes\r\n".to_string()),
                }
            }
        }

        // Execute command on each node and aggregate results
        let mut total_count = 0;

        for (node_id, node_keys) in keys_by_node {
            let conn_manager = {
                let pool = self.connection_pool.read().await;
                match pool.get_connection(&node_id) {
                    Some(conn) => conn,
                    None => return Ok("-ERR node connection not available\r\n".to_string()),
                }
            };

            let mut parts = vec![command.to_string()];
            parts.extend(node_keys);

            let mut conn = conn_manager.lock().await;
            match self.execute_redis_command(&mut conn, &parts).await {
                Ok(response) => {
                    // Parse the integer response
                    if let Some(count_str) = response.strip_prefix(':') {
                        if let Some(count_str) = count_str.strip_suffix("\r\n") {
                            if let Ok(count) = count_str.parse::<i32>() {
                                total_count += count;
                            } else {
                                return Ok(format!(
                                    "-ERR invalid response from node {}\r\n",
                                    node_id
                                ));
                            }
                        } else {
                            return Ok(format!("-ERR invalid response from node {}\r\n", node_id));
                        }
                    } else if response.starts_with("-ERR") {
                        return Ok(response);
                    } else {
                        return Ok(format!(
                            "-ERR unexpected response from node {}\r\n",
                            node_id
                        ));
                    }
                }
                Err(e) => {
                    eprintln!("Error executing command on node {}: {}", node_id, e);
                    return Ok(format!("-ERR {}\r\n", e));
                }
            }
        }

        Ok(format!(":{}\r\n", total_count))
    }

    // Original method for pooled connections
    async fn execute_redis_command(
        &self,
        conn: &mut ConnectionManager,
        parts: &[String],
    ) -> Result<String> {
        // Same implementation as in your original code
        use redis::AsyncCommands;
        let cmd = parts[0].to_uppercase();

        match cmd.as_str() {
            "GET" => {
                if parts.len() != 2 {
                    return Ok("-ERR wrong number of arguments for 'get' command\r\n".to_string());
                }
                // let value: &str = "1";
                // Ok(format!("${}\r\n{}\r\n", value.len(), value))
                let key = &parts[1];
                let result: RedisResult<Option<String>> = conn.get(key).await;
                match result {
                    Ok(Some(value)) => Ok(format!("${}\r\n{}\r\n", value.len(), value)),
                    Ok(None) => Ok("$-1\r\n".to_string()),
                    Err(e) => Ok(format!("-ERR {}\r\n", e)),
                }
            }
            "SET" => {
                if parts.len() < 3 {
                    return Ok("-ERR wrong number of arguments for 'set' command\r\n".to_string());
                }
                // Ok("+OK\r\n".to_string())
                let key = &parts[1];
                let value = &parts[2];
                let result: RedisResult<String> = conn.set(key, value).await;
                match result {
                    Ok(_) => Ok("+OK\r\n".to_string()),
                    Err(e) => Ok(format!("-ERR {}\r\n", e)),
                }
            }
            "DEL" => {
                if parts.len() < 2 {
                    return Ok("-ERR wrong number of arguments for 'del' command\r\n".to_string());
                }
                let keys = &parts[1..];
                let result: RedisResult<i32> = conn.del(keys).await;
                match result {
                    Ok(count) => Ok(format!(":{}\r\n", count)),
                    Err(e) => Ok(format!("-ERR {}\r\n", e)),
                }
            }
            "EXISTS" => {
                if parts.len() < 2 {
                    return Ok(
                        "-ERR wrong number of arguments for 'exists' command\r\n".to_string()
                    );
                }
                let keys = &parts[1..];
                let result: RedisResult<i32> = conn.exists(keys).await;
                match result {
                    Ok(count) => Ok(format!(":{}\r\n", count)),
                    Err(e) => Ok(format!("-ERR {}\r\n", e)),
                }
            }
            "INCR" => {
                if parts.len() != 2 {
                    return Ok("-ERR wrong number of arguments for 'incr' command\r\n".to_string());
                }
                let key = &parts[1];
                let result: RedisResult<i64> = conn.incr(key, 1).await;
                match result {
                    Ok(value) => Ok(format!(":{}\r\n", value)),
                    Err(e) => Ok(format!("-ERR {}\r\n", e)),
                }
            }
            "INCRBY" => {
                if parts.len() != 3 {
                    return Ok(
                        "-ERR wrong number of arguments for 'incrby' command\r\n".to_string()
                    );
                }
                let key = &parts[1];
                let value = parts[2].parse::<i64>();

                match value {
                    Ok(value) => {
                        let result: RedisResult<i64> = conn.incr(key, value).await;
                        match result {
                            Ok(value) => Ok(format!(":{}\r\n", value)),
                            Err(e) => Ok(format!("-ERR {}\r\n", e)),
                        }
                    }
                    Err(e) => Ok(format!("-ERR {}\r\n", e)),
                }
            }
            "DECR" => {
                if parts.len() != 2 {
                    return Ok("-ERR wrong number of arguments for 'decr' command\r\n".to_string());
                }
                let key = &parts[1];
                let result: RedisResult<i64> = conn.decr(key, 1).await;
                match result {
                    Ok(value) => Ok(format!(":{}\r\n", value)),
                    Err(e) => Ok(format!("-ERR {}\r\n", e)),
                }
            }
            "DECRBY" => {
                if parts.len() != 3 {
                    return Ok(
                        "-ERR wrong number of arguments for 'decrby' command\r\n".to_string()
                    );
                }
                let key = &parts[1];
                let value = parts[2].parse::<i64>();
                match value {
                    Ok(value) => {
                        let result: RedisResult<i64> = conn.decr(key, value).await;
                        match result {
                            Ok(value) => Ok(format!(":{}\r\n", value)),
                            Err(e) => Ok(format!("-ERR {}\r\n", e)),
                        }
                    }
                    Err(_) => Ok("-ERR value is not an integer or out of range\r\n".to_string()),
                }
            }
            "TTL" => {
                if parts.len() != 2 {
                    return Ok("-ERR wrong number of arguments for 'ttl' command\r\n".to_string());
                }
                let key = &parts[1];
                let result: RedisResult<i64> = conn.ttl(key).await;
                match result {
                    Ok(ttl) => Ok(format!(":{}\r\n", ttl)),
                    Err(e) => Ok(format!("-ERR {}\r\n", e)),
                }
            }
            "EXPIRE" => {
                if parts.len() != 3 {
                    return Ok(
                        "-ERR wrong number of arguments for 'expire' command\r\n".to_string()
                    );
                }
                let key = &parts[1];
                let seconds: i64 = parts[2].parse().unwrap_or(0);
                let result: RedisResult<i32> = conn.expire(key, seconds).await;
                match result {
                    Ok(result) => Ok(format!(":{}\r\n", result)),
                    Err(e) => Ok(format!("-ERR {}\r\n", e)),
                }
            }
            "HGET" => {
                if parts.len() != 3 {
                    return Ok("-ERR wrong number of arguments for 'hget' command\r\n".to_string());
                }
                let key = &parts[1];
                let field = &parts[2];
                let result: RedisResult<Option<String>> = conn.hget(key, field).await;
                match result {
                    Ok(Some(value)) => Ok(format!("${}\r\n{}\r\n", value.len(), value)),
                    Ok(None) => Ok("$-1\r\n".to_string()),
                    Err(e) => Ok(format!("-ERR {}\r\n", e)),
                }
            }
            "HSET" => {
                if parts.len() != 4 {
                    return Ok("-ERR wrong number of arguments for 'hset' command\r\n".to_string());
                }
                let key = &parts[1];
                let field = &parts[2];
                let value = &parts[3];
                let result: RedisResult<i32> = conn.hset(key, field, value).await;
                match result {
                    Ok(result) => Ok(format!(":{}\r\n", result)),
                    Err(e) => Ok(format!("-ERR {}\r\n", e)),
                }
            }
            "APPEND" => {
                if parts.len() != 3 {
                    return Ok(
                        "-ERR wrong number of arguments for 'append' command\r\n".to_string()
                    );
                }
                let key = &parts[1];
                let value = &parts[2];
                let result: RedisResult<i32> = conn.append(key, value).await;
                match result {
                    Ok(result) => Ok(format!(":{}\r\n", result)),
                    Err(e) => Ok(format!("-ERR {}\r\n", e)),
                }
            }
            "SADD" => {
                if parts.len() < 3 {
                    return Ok("-ERR wrong number of arguments for 'sadd' command\r\n".to_string());
                }
                let key = &parts[1];
                let members = &parts[2..];
                let result: RedisResult<i32> = conn.sadd(key, members).await;
                match result {
                    Ok(result) => Ok(format!(":{}\r\n", result)),
                    Err(e) => Ok(format!("-ERR {}\r\n", e)),
                }
            }
            "SMEMBERS" => {
                if parts.len() != 2 {
                    return Ok(
                        "-ERR wrong number of arguments for 'smembers' command\r\n".to_string()
                    );
                }
                let key = &parts[1];
                let result: RedisResult<Vec<String>> = conn.smembers(key).await;
                match result {
                    Ok(result) => {
                        let mut response = String::new();
                        response.push_str(&format!("*{}\r\n", result.len()));
                        for member in result {
                            response.push_str(&format!("${}\r\n{}\r\n", member.len(), member));
                        }
                        Ok(response)
                    }
                    Err(e) => Ok(format!("-ERR {}\r\n", e)),
                }
            }
            // Add other commands as needed...
            _ => {
                println!("Unknown command '{}' {:?}", cmd, parts);
                Ok(format!("-ERR unknown command '{}'\r\n", cmd))
            }
        }
    }
}

impl Clone for RedisProxy {
    fn clone(&self) -> Self {
        Self {
            consistent_hash: Arc::clone(&self.consistent_hash),
            connection_pool: Arc::clone(&self.connection_pool),
            // client_pool: Arc::clone(&self.client_pool),
        }
    }
}

/// Helper function to encode an error response using the client's protocol
fn encode_error(msg: &str, proto: Protocol) -> Vec<u8> {
    match proto {
        Protocol::Resp2 => {
            let frame = Resp2OwnedFrame::Error(msg.to_string());
            crate::protocol::parser::encode_frame(frame)
        }
        Protocol::Resp3 => {
            let frame = Resp3OwnedFrame::SimpleError {
                data: msg.to_string(),
                attributes: None,
            };
            let mut buf = Vec::with_capacity(512);
            loop {
                match redis_protocol::resp3::encode::complete::encode(&mut buf, &frame, false) {
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
    }
}

/// Helper function to encode a response frame using the client's protocol
fn encode_response(frame: &RedisFrame, proto: Protocol) -> Vec<u8> {
    use crate::protocol::parser::encode_frame_for_protocol;
    encode_frame_for_protocol(frame, proto)
}
