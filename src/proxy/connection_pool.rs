use anyhow::{Context, Result};
use redis::aio::ConnectionManager;
use redis::Client;
use std::collections::HashMap;
use std::sync::Arc;

use crate::proxy::node::RedisNode;

// Improved connection pool with multiple connections per node and health checking
pub struct ConnectionPool {
    pools: HashMap<String, Vec<Arc<tokio::sync::Mutex<ConnectionManager>>>>,
    clients: HashMap<String, Vec<Client>>,
    pool_size: usize,
    current_index: HashMap<String, std::sync::atomic::AtomicUsize>,
}

impl ConnectionPool {
    pub fn new(pool_size: usize) -> Self {
        Self {
            pools: HashMap::new(),
            clients: HashMap::new(),
            pool_size,
            current_index: HashMap::new(),
        }
    }

    pub async fn add_connection(&mut self, node: &RedisNode) -> Result<()> {
        let mut connections = Vec::new();
        let mut clients = Vec::new();

        // Create multiple connections for this node
        for _ in 0..self.pool_size {
            let client =
                Client::open(node.connection_string()).context("Failed to create Redis client")?;
            let conn = ConnectionManager::new(client.clone())
                .await
                .context("Failed to get async Redis connection manager")?;
            connections.push(Arc::new(tokio::sync::Mutex::new(conn)));
            clients.push(client);
        }

        self.pools.insert(node.id.clone(), connections);
        self.clients.insert(node.id.clone(), clients);
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

    // Recreate a specific connection when it fails
    pub async fn recreate_connection(&mut self, node_id: &str, index: usize) -> Result<()> {
        if let Some(clients) = self.clients.get(node_id) {
            if let Some(client) = clients.get(index) {
                eprintln!("Recreating connection {} for node {}", index, node_id);
                let new_conn = ConnectionManager::new(client.clone())
                    .await
                    .context("Failed to recreate connection manager")?;

                if let Some(pool) = self.pools.get_mut(node_id) {
                    if let Some(conn_arc) = pool.get_mut(index) {
                        *conn_arc = Arc::new(tokio::sync::Mutex::new(new_conn));
                        eprintln!(
                            "Successfully recreated connection {} for node {}",
                            index, node_id
                        );
                        return Ok(());
                    }
                }
            }
        }
        Err(anyhow::anyhow!(
            "Failed to recreate connection: node or index not found"
        ))
    }

    // Recreate all connections for a node
    pub async fn recreate_all_connections(&mut self, node_id: &str) -> Result<()> {
        eprintln!("Recreating all connections for node {}", node_id);

        if let Some(clients) = self.clients.get(node_id) {
            let mut new_connections = Vec::new();

            for (i, client) in clients.iter().enumerate() {
                match ConnectionManager::new(client.clone()).await {
                    Ok(conn) => {
                        new_connections.push(Arc::new(tokio::sync::Mutex::new(conn)));
                        eprintln!("  Recreated connection {} for node {}", i, node_id);
                    }
                    Err(e) => {
                        eprintln!(
                            "  Failed to recreate connection {} for node {}: {}",
                            i, node_id, e
                        );
                        return Err(anyhow::anyhow!(
                            "Failed to recreate connection {}: {}",
                            i,
                            e
                        ));
                    }
                }
            }

            if let Some(pool) = self.pools.get_mut(node_id) {
                *pool = new_connections;
                eprintln!(
                    "Successfully recreated all {} connections for node {}",
                    pool.len(),
                    node_id
                );
                return Ok(());
            }
        }

        Err(anyhow::anyhow!(
            "Failed to recreate connections: node not found"
        ))
    }

    // Check if we have connections for a node
    pub fn has_node(&self, node_id: &str) -> bool {
        self.pools.contains_key(node_id)
    }

    // Get the number of connections for a node
    pub fn connection_count(&self, node_id: &str) -> usize {
        self.pools.get(node_id).map(|p| p.len()).unwrap_or(0)
    }
}
