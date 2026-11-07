use thiserror::Error;

#[derive(Error, Debug)]
pub enum QueryError {
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Query execution error: {0}")]
    Execution(String),

    #[error("Server error: {0}")]
    Server(String),
}

pub type Result<T> = std::result::Result<T, QueryError>;

