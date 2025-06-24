use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncBufReadExt, BufReader};
use tokio::sync::RwLock;
use redis::{Client, Connection, RedisResult};
use anyhow::{Result, Context, anyhow};

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
        
        // Find the first node with hash >= key hash
        if let Some((&_, node)) = self.ring.range(hash..).next() {
            Some(node)
        } else {
            // Wrap around to the first node
            self.ring.values().next()
        }
    }

    fn hash(&self, key: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }
}

#[derive(Debug)]
pub struct ConnectionPool {
    connections: HashMap<String, Client>,
}

impl ConnectionPool {
    pub fn new() -> Self {
        Self {
            connections: HashMap::new(),
        }
    }

    pub fn add_connection(&mut self, node: &RedisNode) -> Result<()> {
        let client = Client::open(node.connection_string())
            .context("Failed to create Redis client")?;
        self.connections.insert(node.id.clone(), client);
        Ok(())
    }

    pub fn get_connection(&self, node_id: &str) -> Option<&Client> {
        self.connections.get(node_id)
    }
}

// Simple RESP parser that handles the basic cases
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
        
        // Check if we have a complete command
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

        // Handle array format: *N
        if lines[0].starts_with('*') {
            let array_len: usize = lines[0][1..].parse()
                .context("Failed to parse array length")?;
            
            if array_len == 0 {
                self.buffer.clear();
                return Ok(Some(vec![]));
            }

            // We need array_len * 2 lines (length + content for each bulk string)
            let needed_lines = 1 + array_len * 2;
            if lines.len() < needed_lines {
                return Ok(None); // Not enough data yet
            }

            let mut result = Vec::new();
            let mut line_idx = 1;
            
            for _ in 0..array_len {
                if line_idx >= lines.len() {
                    return Ok(None); // Not enough data
                }
                
                // Skip the $N line and get the actual content
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

            // Clear the processed data from buffer
            self.buffer.clear();
            return Ok(Some(result));
        }

        // Handle simple commands (space-separated)
        if !self.buffer.contains('\n') {
            return Ok(None); // Not complete yet
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

#[derive(Debug)]
pub struct RedisProxy {
    consistent_hash: Arc<RwLock<ConsistentHash>>,
    connection_pool: Arc<RwLock<ConnectionPool>>,
}

impl RedisProxy {
    pub fn new(replicas: usize) -> Self {
        Self {
            consistent_hash: Arc::new(RwLock::new(ConsistentHash::new(replicas))),
            connection_pool: Arc::new(RwLock::new(ConnectionPool::new())),
        }
    }

    pub async fn add_node(&self, node: RedisNode) -> Result<()> {
        // Add to connection pool
        {
            let mut pool = self.connection_pool.write().await;
            pool.add_connection(&node)?;
        }

        // Add to consistent hash
        {
            let mut hash = self.consistent_hash.write().await;
            hash.add_node(node);
        }

        Ok(())
    }

    pub async fn remove_node(&self, node_id: &str) {
        let mut hash = self.consistent_hash.write().await;
        hash.remove_node(node_id);
    }

    pub async fn start_server(&self, bind_addr: &str) -> Result<()> {
        let listener = TcpListener::bind(bind_addr).await
            .context("Failed to bind to address")?;
        
        println!("Redis proxy listening on {}", bind_addr);

        loop {
            let (socket, addr) = listener.accept().await?;
            println!("New connection from {}", addr);

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
                break; // Connection closed
            }

            let data = String::from_utf8_lossy(&buffer[..n]);
            
            if let Some(command) = parser.parse_command(&data)? {
                let response = self.process_command(&command).await?;
                socket.write_all(response.as_bytes()).await?;
            }
        }

        print!("Client disconnected\n");

        Ok(())
    }

    async fn process_command(&self, parts: &[String]) -> Result<String> {
        if parts.is_empty() {
            return Ok("-ERR empty command\r\n".to_string());
        }

        let cmd = parts[0].to_uppercase();
        
        match cmd.as_str() {
            "GET" | "SET" | "DEL" | "EXISTS" | "INCR" | "DECR" | "EXPIRE" | "TTL" | "HGET" | "HSET" => {
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
            "COMMAND" => Ok("*0\r\n".to_string()), // Return empty array for COMMAND
            _ => Ok(format!("-ERR unknown command '{}'\r\n", cmd)),
        }
    }

    async fn proxy_command_to_node(&self, key: &str, parts: &[String]) -> Result<String> {
        // Get the appropriate node for this key
        let node = {
            let hash = self.consistent_hash.read().await;
            match hash.get_node(key) {
                Some(node) => node.clone(),
                None => return Ok("-ERR no available nodes\r\n".to_string()),
            }
        };

        // Get connection for the node
        let client = {
            let pool = self.connection_pool.read().await;
            match pool.get_connection(&node.id) {
                Some(client) => client.clone(),
                None => return Ok("-ERR node connection not available\r\n".to_string()),
            }
        };

        // Execute command on the target Redis node
        match self.execute_redis_command(&client, parts).await {
            Ok(response) => {
                // release the connection back to the pool
                // {
                //     let mut pool = self.connection_pool.write().await;
                //     pool.connections.insert(node.id.clone(), client);
                // }
                Ok(response)
            },
            Err(e) => {
                eprintln!("Error executing command on node {}: {}", node.id, e);
                Ok(format!("-ERR {}\r\n", e))
            }
        }
    }

    async fn execute_redis_command(&self, client: &Client, parts: &[String]) -> Result<String> {
        let mut conn = client.get_connection()
            .context("Failed to get Redis connection")?;

        let cmd = parts[0].to_uppercase();

        match cmd.as_str() {
            "GET" => {
                if parts.len() != 2 {
                    return Ok("-ERR wrong number of arguments for 'get' command\r\n".to_string());
                }
                let key = &parts[1];
                let result: RedisResult<Option<String>> = redis::cmd("GET").arg(key).query(&mut conn);
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
                let key = &parts[1];
                let value = &parts[2];
                
                let mut cmd = redis::cmd("SET");
                cmd.arg(key).arg(value);
                
                // Add any additional arguments (like EX, PX, NX, XX)
                for arg in &parts[3..] {
                    cmd.arg(arg);
                }
                
                let result: RedisResult<String> = cmd.query(&mut conn);
                match result {
                    Ok(_) => Ok("+OK\r\n".to_string()),
                    Err(e) => Ok(format!("-ERR {}\r\n", e)),
                }
            }
            "DEL" => {
                if parts.len() < 2 {
                    return Ok("-ERR wrong number of arguments for 'del' command\r\n".to_string());
                }
                let mut cmd = redis::cmd("DEL");
                for key in &parts[1..] {
                    cmd.arg(key);
                }
                let result: RedisResult<i32> = cmd.query(&mut conn);
                match result {
                    Ok(count) => Ok(format!(":{}\r\n", count)),
                    Err(e) => Ok(format!("-ERR {}\r\n", e)),
                }
            }
            "EXISTS" => {
                if parts.len() < 2 {
                    return Ok("-ERR wrong number of arguments for 'exists' command\r\n".to_string());
                }
                let mut cmd = redis::cmd("EXISTS");
                for key in &parts[1..] {
                    cmd.arg(key);
                }
                let result: RedisResult<i32> = cmd.query(&mut conn);
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
                let result: RedisResult<i64> = redis::cmd("INCR").arg(key).query(&mut conn);
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
                let result: RedisResult<i64> = redis::cmd("DECR").arg(key).query(&mut conn);
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
                let result: RedisResult<i64> = redis::cmd("TTL").arg(key).query(&mut conn);
                match result {
                    Ok(ttl) => Ok(format!(":{}\r\n", ttl)),
                    Err(e) => Ok(format!("-ERR {}\r\n", e)),
                }
            }
            "EXPIRE" => {
                if parts.len() != 3 {
                    return Ok("-ERR wrong number of arguments for 'expire' command\r\n".to_string());
                }
                let key = &parts[1];
                let seconds = &parts[2];
                let result: RedisResult<i32> = redis::cmd("EXPIRE").arg(key).arg(seconds).query(&mut conn);
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
                let result: RedisResult<Option<String>> = redis::cmd("HGET").arg(key).arg(field).query(&mut conn);
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
                let result: RedisResult<i32> = redis::cmd("HSET").arg(key).arg(field).arg(value).query(&mut conn);
                match result {
                    Ok(result) => Ok(format!(":{}\r\n", result)),
                    Err(e) => Ok(format!("-ERR {}\r\n", e)),
                }
            }
            _ => Ok(format!("-ERR unknown command '{}'\r\n", cmd)),
        }
    }
}

impl Clone for RedisProxy {
    fn clone(&self) -> Self {
        Self {
            consistent_hash: Arc::clone(&self.consistent_hash),
            connection_pool: Arc::clone(&self.connection_pool),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Create proxy with 150 virtual replicas per node
    let proxy = RedisProxy::new(150);

    // Add Redis nodes
    let nodes = vec![
        RedisNode::new("node1".to_string(), "127.0.0.1".to_string(), 6379),
        RedisNode::new("node2".to_string(), "127.0.0.1".to_string(), 6380),
        RedisNode::new("node3".to_string(), "127.0.0.1".to_string(), 6381),
    ];

    for node in nodes {
        if let Err(e) = proxy.add_node(node.clone()).await {
            eprintln!("Failed to add node {}: {}", node.id, e);
        } else {
            println!("Added Redis node: {} at {}:{}", node.id, node.host, node.port);
        }
    }

    // Start the proxy server
    proxy.start_server("127.0.0.1:46379").await?;

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

        // Test that keys consistently map to the same node
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

        // Remove node1
        hash.remove_node("node1");
        
        // All keys should now map to node2
        let key = "test_key";
        let node = hash.get_node(key);
        assert!(node.is_some());
        assert_eq!(node.unwrap().id, "node2");
    }
}