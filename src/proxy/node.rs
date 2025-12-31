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
