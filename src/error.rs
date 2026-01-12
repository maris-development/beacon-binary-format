#[derive(Debug, thiserror::Error)]
pub enum BBFError {
    #[error("External error: {0}")]
    External(#[from] Box<dyn std::error::Error + Send + Sync>),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Object store error: {0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("Writing Error: {0}")]
    Writing(Box<dyn std::error::Error + Send + Sync>),
    #[error("Reading Error: {0}")]
    Reading(Box<dyn std::error::Error + Send + Sync>),
    #[error("Nd Arrow Array Error: {0}")]
    NdArrowArray(Box<dyn std::error::Error + Send + Sync>),
}
