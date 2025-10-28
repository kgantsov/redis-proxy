use actix_web::{get, App, HttpResponse, HttpServer, Responder};
use anyhow::{anyhow, Context, Result};
use clap::Parser;
use lazy_static::lazy_static;
use prometheus::{self, gather, Encoder, TextEncoder};
use prometheus::{
    default_registry, register_histogram, register_int_counter, register_int_gauge, Histogram,
    IntCounter, IntGauge,
};
use redis::aio::ConnectionManager;
use redis::{Client, RedisResult};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
use tokio::sync::RwLock;

lazy_static! {
    pub static ref COMMANDS_PROXIED_COUNTER: IntCounter = register_int_counter!(
        "redis_proxy_commands_proxied_total",
        "Number of commands proxied"
    )
    .unwrap();
    pub static ref CONNECTIONS_GAUGE: IntGauge =
        register_int_gauge!("redis_proxy_connections", "Current number of connections").unwrap();
    pub static ref PROXY_LATENCY_HISTOGRAM: Histogram = register_histogram!(
        "redis_proxy_proxy_latency_seconds",
        "Histogram of proxy latencies in seconds",
        vec![
            0.000001, 0.000002, 0.000005, 0.00001, 0.00002, 0.00005, 0.0001, 0.0002, 0.0005, 0.005,
            0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0
        ]
    )
    .unwrap();
}

#[derive(Debug, Clone)]
pub struct RedisNode {
    pub id: String,
    pub host: String,
    pub port: u16,
}

impl RedisNode {
    pub fn new(id: String, host: String, port: u16) -> Self {
        Self { id, host, port }
    }

    pub fn connection_string(&self) -> String {
        format!("redis://{}:{}", self.host, self.port)
    }
}

#[derive(Debug)]
pub struct ConsistentHash {
    ring: std::collections::BTreeMap<u64, RedisNode>,
    replicas: usize,
}

impl ConsistentHash {
    pub fn new(replicas: usize) -> Self {
        Self {
            ring: std::collections::BTreeMap::new(),
            replicas,
        }
    }

    pub fn add_node(&mut self, node: RedisNode) {
        for i in 0..self.replicas {
            let key = format!("{}:{}", node.id, i);
            let hash = self.hash(&key);
            self.ring.insert(hash, node.clone());
        }
    }

    pub fn remove_node(&mut self, node_id: &str) {
        for i in 0..self.replicas {
            let key = format!("{}:{}", node_id, i);
            let hash = self.hash(&key);
            self.ring.remove(&hash);
        }
    }

    pub fn get_node(&self, key: &str) -> Option<&RedisNode> {
        if self.ring.is_empty() {
            return None;
        }

        let hash = self.hash(key);

        if let Some((&_, node)) = self.ring.range(hash..).next() {
            Some(node)
        } else {
            self.ring.values().next()
        }
    }

    fn hash(&self, key: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }
}

// Improved connection pool with multiple connections per node
pub struct ConnectionPool {
    pools: HashMap<String, Vec<Arc<tokio::sync::Mutex<ConnectionManager>>>>,
    pool_size: usize,
    current_index: HashMap<String, std::sync::atomic::AtomicUsize>,
}

impl ConnectionPool {
    pub fn new(pool_size: usize) -> Self {
        Self {
            pools: HashMap::new(),
            pool_size,
            current_index: HashMap::new(),
        }
    }

    pub async fn add_connection(&mut self, node: &RedisNode) -> Result<()> {
        let mut connections = Vec::new();

        // Create multiple connections for this node
        for _ in 0..self.pool_size {
            let client =
                Client::open(node.connection_string()).context("Failed to create Redis client")?;
            let conn = ConnectionManager::new(client)
                .await
                .context("Failed to get async Redis connection manager")?;
            connections.push(Arc::new(tokio::sync::Mutex::new(conn)));
        }

        self.pools.insert(node.id.clone(), connections);
        self.current_index
            .insert(node.id.clone(), std::sync::atomic::AtomicUsize::new(0));
        Ok(())
    }

    // Round-robin connection selection
    pub fn get_connection(
        &self,
        node_id: &str,
    ) -> Option<Arc<tokio::sync::Mutex<ConnectionManager>>> {
        if let Some(pool) = self.pools.get(node_id) {
            if let Some(counter) = self.current_index.get(node_id) {
                let index = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % pool.len();
                Some(pool[index].clone())
            } else {
                pool.first().cloned()
            }
        } else {
            None
        }
    }
}

// Simple RESP parser (unchanged)
#[derive(Debug)]
pub struct SimpleRespParser {
    buffer: String,
}

impl SimpleRespParser {
    pub fn new() -> Self {
        Self {
            buffer: String::new(),
        }
    }

    pub fn parse_command(&mut self, data: &str) -> Result<Option<Vec<String>>> {
        self.buffer.push_str(data);

        if let Some(result) = self.try_parse_array()? {
            return Ok(Some(result));
        }

        Ok(None)
    }

    fn try_parse_array(&mut self) -> Result<Option<Vec<String>>> {
        let lines: Vec<&str> = self.buffer.lines().collect();
        if lines.is_empty() {
            return Ok(None);
        }

        if lines[0].starts_with('*') {
            let array_len: usize = lines[0][1..]
                .parse()
                .context("Failed to parse array length")?;

            if array_len == 0 {
                self.buffer.clear();
                return Ok(Some(vec![]));
            }

            let needed_lines = 1 + array_len * 2;
            if lines.len() < needed_lines {
                return Ok(None);
            }

            let mut result = Vec::new();
            let mut line_idx = 1;

            for _ in 0..array_len {
                if line_idx >= lines.len() {
                    return Ok(None);
                }

                if lines[line_idx].starts_with('$') {
                    line_idx += 1;
                    if line_idx < lines.len() {
                        result.push(lines[line_idx].to_string());
                        line_idx += 1;
                    }
                } else {
                    return Err(anyhow!("Expected bulk string"));
                }
            }

            self.buffer.clear();
            return Ok(Some(result));
        }

        if !self.buffer.contains('\n') {
            return Ok(None);
        }

        let line = self.buffer.lines().next().unwrap();
        let parts: Vec<String> = line.split_whitespace().map(|s| s.to_string()).collect();
        self.buffer.clear();
        Ok(Some(parts))
    }

    pub fn reset(&mut self) {
        self.buffer.clear();
    }
}

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
        let mut parser = SimpleRespParser::new();
        let mut buffer = [0; 4096];

        loop {
            let n = socket.read(&mut buffer).await?;
            if n == 0 {
                break;
            }

            let data = String::from_utf8_lossy(&buffer[..n]);

            let timer = PROXY_LATENCY_HISTOGRAM.start_timer();
            if let Some(command) = parser.parse_command(&data)? {
                let response = self.process_command(&command).await?;
                socket.write_all(response.as_bytes()).await?;
            }
            timer.observe_duration();
            COMMANDS_PROXIED_COUNTER.inc();
        }

        println!("Client disconnected");
        CONNECTIONS_GAUGE.dec();
        Ok(())
    }

    async fn process_command(&self, parts: &[String]) -> Result<String> {
        if parts.is_empty() {
            return Ok("-ERR empty command\r\n".to_string());
        }

        let cmd = parts[0].to_uppercase();

        match cmd.as_str() {
            "GET" | "SET" | "DEL" | "EXISTS" | "INCR" | "DECR" | "EXPIRE" | "TTL" | "HGET"
            | "HSET" => {
                if parts.len() < 2 {
                    return Ok("-ERR wrong number of arguments\r\n".to_string());
                }

                let key = &parts[1];
                self.proxy_command_to_node(key, parts).await
            }
            "PING" => {
                if parts.len() == 1 {
                    Ok("+PONG\r\n".to_string())
                } else {
                    Ok(format!("${}\r\n{}\r\n", parts[1].len(), parts[1]))
                }
            }
            "INFO" => Ok("$17\r\nRedis Proxy v1.0\r\n".to_string()),
            "COMMAND" => Ok("*0\r\n".to_string()),
            _ => Ok(format!("-ERR unknown command '{}'\r\n", cmd)),
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
            // Add other commands as needed...
            _ => Ok(format!("-ERR unknown command '{}'\r\n", cmd)),
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

/// Redis proxy CLI that accepts multiple node IDs and hosts
#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// List of node IDs (e.g. --node-ids node1 node2 node3)
    #[arg(long, num_args = 1.., value_name = "NODE_ID")]
    node_ids: Vec<String>,

    /// List of hosts (e.g. --hosts 127.0.0.1:6379 127.0.0.1:6380)
    #[arg(long, num_args = 1.., value_name = "HOST")]
    hosts: Vec<String>,

    // Address to listen on (e.g. --address 0.0.0.0:6379)
    #[arg(long, value_name = "ADDRESS", default_value = "0.0.0.0:6379")]
    address: String,
}

#[get("/metrics")]
async fn metrics() -> impl Responder {
    let encoder = TextEncoder::new();
    let metric_families = gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    HttpResponse::Ok()
        .content_type(encoder.format_type())
        .body(buffer)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    if args.node_ids.len() != args.hosts.len() {
        eprintln!("Error: node_ids and hosts must have the same length");
        std::process::exit(1);
    }

    let mut nodes: Vec<RedisNode> = Vec::new();

    for (id, host) in args.node_ids.iter().zip(args.hosts.iter()) {
        let parts: Vec<&str> = host.split(':').collect();
        let host = parts[0].to_string();
        let port = parts[1].parse::<u16>().unwrap_or(6379);

        println!("Node {id} -> {host}:{port}");
        nodes.push(RedisNode::new(id.clone(), host, port));
    }

    let proxy = RedisProxy::new(150, 50);

    for node in nodes {
        if let Err(e) = proxy.add_node(node.clone()).await {
            eprintln!("Failed to add node {}: {}", node.id, e);
        } else {
            println!(
                "Added Redis node: {} at {}:{}",
                node.id, node.host, node.port
            );
        }
    }

    // Start metrics server in background
    let metrics_server = HttpServer::new(|| App::new().service(metrics))
        .bind("127.0.0.1:9090")?
        .run();

    // Start Redis proxy server in background
    let proxy_server = proxy.start_server(&args.address);

    // Graceful shutdown on Ctrl+C
    tokio::select! {
        res = metrics_server => {
            if let Err(e) = res {
                eprintln!("Metrics server error: {}", e);
            }
        }
        res = proxy_server => {
            if let Err(e) = res {
                eprintln!("Proxy server error: {}", e);
            }
        }
        _ = signal::ctrl_c() => {
            println!("Received Ctrl+C, shutting down...");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consistent_hash() {
        let mut hash = ConsistentHash::new(3);

        let node1 = RedisNode::new("node1".to_string(), "127.0.0.1".to_string(), 6379);
        let node2 = RedisNode::new("node2".to_string(), "127.0.0.1".to_string(), 6380);

        hash.add_node(node1.clone());
        hash.add_node(node2.clone());

        let key = "test_key";
        let node_a = hash.get_node(key);
        let node_b = hash.get_node(key);

        assert!(node_a.is_some());
        assert!(node_b.is_some());
        assert_eq!(node_a.unwrap().id, node_b.unwrap().id);
    }

    #[test]
    fn test_node_removal() {
        let mut hash = ConsistentHash::new(3);

        let node1 = RedisNode::new("node1".to_string(), "127.0.0.1".to_string(), 6379);
        let node2 = RedisNode::new("node2".to_string(), "127.0.0.1".to_string(), 6380);

        hash.add_node(node1.clone());
        hash.add_node(node2.clone());

        hash.remove_node("node1");

        let key = "test_key";
        let node = hash.get_node(key);
        assert!(node.is_some());
        assert_eq!(node.unwrap().id, "node2");
    }
}
