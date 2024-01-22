pub mod concat_iterator;
pub mod merge_iterator;
pub mod two_merge_iterator;

pub trait StorageIterator {
    /// Get the current value.
    fn value(&self) -> &[u8];

    /// Get the current key.
    fn key(&self) -> &[u8];

    /// Check if the current iterator is valid.
    fn is_valid(&self) -> bool;

    /// Move to the next position.
    fn next(&mut self) -> anyhow::Result<()>;

    /// Number of underlying active iterators for this iterator.
    fn num_active_iterators(&self) -> usize {
        1
    }
}
