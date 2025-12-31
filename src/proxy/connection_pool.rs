use anyhow::{Context, Result};
use redis::aio::ConnectionManager;
use redis::Client;
use std::collections::HashMap;
use std::sync::Arc;

use crate::proxy::node::RedisNode;

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
