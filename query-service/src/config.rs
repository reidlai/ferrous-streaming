use serde::{Deserialize, Serialize};

pub const DEFAULT_BATCH_SIZE: usize = 8192;
pub const DEFAULT_FLIGHT_PORT: u16 = 50051;
pub const DEFAULT_HTTP_PORT: u16 = 8085;
pub const DEFAULT_DATA_DIR: &str = "./store/data/delta";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryConfig {
    pub batch_size: usize,
    pub memory_limit_bytes: Option<usize>,
    pub flight_port: u16,
    pub http_port: u16,
    pub data_dir: String,
    pub information_schema: bool,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_BATCH_SIZE,
            memory_limit_bytes: Some(512 * 1024 * 1024), // 512MB default execution pool
            flight_port: DEFAULT_FLIGHT_PORT,
            http_port: DEFAULT_HTTP_PORT,
            data_dir: DEFAULT_DATA_DIR.to_string(),
            information_schema: true,
        }
    }
}

impl QueryConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn with_memory_limit(mut self, bytes: Option<usize>) -> Self {
        self.memory_limit_bytes = bytes;
        self
    }

    pub fn with_http_port(mut self, port: u16) -> Self {
        self.http_port = port;
        self
    }

    pub fn with_flight_port(mut self, port: u16) -> Self {
        self.flight_port = port;
        self
    }

    pub fn with_data_dir(mut self, data_dir: impl Into<String>) -> Self {
        self.data_dir = data_dir.into();
        self
    }

    pub fn with_information_schema(mut self, enable: bool) -> Self {
        self.information_schema = enable;
        self
    }
}

