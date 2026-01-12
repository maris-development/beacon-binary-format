use crate::index::pruning::PruningIndex;

pub mod pruning;

#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum Index {
    PruningIndex(PruningIndex),
}
