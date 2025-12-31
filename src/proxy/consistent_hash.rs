use crate::proxy::node::RedisNode;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

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
