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

    #[error("Delta table error: {0}")]
    Delta(String),

    #[error("Catalog error: {0}")]
    Catalog(String),

    #[error("Server error: {0}")]
    Server(String),
}

impl From<deltalake::errors::DeltaTableError> for QueryError {
    fn from(err: deltalake::errors::DeltaTableError) -> Self {
        QueryError::Delta(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, QueryError>;
