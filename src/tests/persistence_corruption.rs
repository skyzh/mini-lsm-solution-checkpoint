// Copyright (c) 2022-2026 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::path::Path;

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use tempfile::tempdir;

use crate::block::Block;
use crate::key::{KeySlice, TS_DEFAULT};
use crate::lsm_storage::{LsmStorageOptions, MiniLsm, WriteBatchRecord};
use crate::manifest::{Manifest, ManifestRecord};
use crate::table::bloom::Bloom;
use crate::table::{FileObject, SsTable, SsTableBuilder};
use crate::wal::Wal;

fn read_u32(buf: &[u8], offset: usize) -> usize {
    u32::from_be_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
    ]) as usize
}

fn build_sst(path: &Path) {
    let mut builder = SsTableBuilder::new(64);
    builder.add(KeySlice::from_slice_with_ts(b"key1", TS_DEFAULT), b"value1");
    builder.add(KeySlice::from_slice_with_ts(b"key2", TS_DEFAULT), b"value2");
    builder.build_for_test(path).unwrap();
}

fn open_sst(path: &Path) -> anyhow::Result<SsTable> {
    SsTable::open(0, None, FileObject::open(path)?)
}

#[test]
fn checkpoint_write_batch_preserves_put_delete_semantics() {
    let dir = tempdir().unwrap();
    let storage = MiniLsm::open(&dir, LsmStorageOptions::default_for_week1_test()).unwrap();
    storage
        .write_batch(&[
            WriteBatchRecord::Put(b"key1".as_slice(), b"value1".as_slice()),
            WriteBatchRecord::Put(b"key2".as_slice(), b"value2".as_slice()),
            WriteBatchRecord::Del(b"key1".as_slice()),
        ])
        .unwrap();
    assert_eq!(storage.get(b"key1").unwrap(), None);
    assert_eq!(storage.get(b"key2").unwrap().unwrap().as_ref(), b"value2");
}

#[test]
fn checkpoint_block_checksum_and_bounds() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("block.sst");
    build_sst(&path);
    let table = open_sst(&path).unwrap();
    assert_eq!(table.read_block(0).unwrap().offsets.len(), 2);
    assert!(table.read_block(1).is_err());
    drop(table);

    let mut data = std::fs::read(&path).unwrap();
    data[0] ^= 1;
    std::fs::write(&path, data).unwrap();
    assert!(open_sst(&path).unwrap().read_block(0).is_err());
    assert!(Block::decode_checked(&[]).is_err());
}

#[test]
fn checkpoint_sst_metadata_checksum_and_bounds() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("metadata.sst");
    build_sst(&path);
    let original = std::fs::read(&path).unwrap();
    let bloom_offset = read_u32(&original, original.len() - 4);
    let meta_offset = read_u32(&original, bloom_offset - 4);

    let mut flipped = original.clone();
    flipped[meta_offset + 4] ^= 1;
    std::fs::write(&path, flipped).unwrap();
    assert!(open_sst(&path).is_err());

    let mut oversized_count = original.clone();
    oversized_count[meta_offset..meta_offset + 4].copy_from_slice(&u32::MAX.to_be_bytes());
    std::fs::write(&path, oversized_count).unwrap();
    assert!(open_sst(&path).is_err());

    let truncated = dir.path().join("truncated.sst");
    std::fs::write(&truncated, &original[..3]).unwrap();
    assert!(open_sst(&truncated).is_err());

    let mut bad_offset = original;
    let trailer = bad_offset.len() - 4;
    bad_offset[trailer..].copy_from_slice(&u32::MAX.to_be_bytes());
    std::fs::write(&path, bad_offset).unwrap();
    assert!(open_sst(&path).is_err());
}

#[test]
fn checkpoint_bloom_checksum_and_truncation() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("bloom.sst");
    build_sst(&path);
    let mut data = std::fs::read(&path).unwrap();
    let bloom_offset = read_u32(&data, data.len() - 4);
    data[bloom_offset] ^= 1;
    std::fs::write(&path, data).unwrap();
    assert!(open_sst(&path).is_err());
    assert!(Bloom::decode(&[0; 4]).is_err());
}

#[test]
fn checkpoint_manifest_round_trip_corruption_and_truncation() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("MANIFEST");
    let manifest = Manifest::create(&path).unwrap();
    manifest
        .add_record_when_init(ManifestRecord::Flush(7))
        .unwrap();
    drop(manifest);
    let original = std::fs::read(&path).unwrap();
    let (_, records) = Manifest::recover(&path).unwrap();
    assert_eq!(records.len(), 1);

    let flipped_path = dir.path().join("MANIFEST-flipped");
    let mut flipped = original.clone();
    flipped[8] ^= 1;
    std::fs::write(&flipped_path, flipped).unwrap();
    assert!(Manifest::recover(&flipped_path).is_err());

    let bounded_path = dir.path().join("MANIFEST-bounded");
    let mut bounded = original.clone();
    bounded[..8].copy_from_slice(&u64::MAX.to_be_bytes());
    std::fs::write(&bounded_path, bounded).unwrap();
    let (_, records) = Manifest::recover(&bounded_path).unwrap();
    assert!(records.is_empty());
    assert_eq!(std::fs::metadata(&bounded_path).unwrap().len(), 0);

    let truncated_path = dir.path().join("MANIFEST-truncated");
    std::fs::write(&truncated_path, &original[..original.len() - 1]).unwrap();
    let (_, records) = Manifest::recover(&truncated_path).unwrap();
    assert!(records.is_empty());
    assert_eq!(std::fs::metadata(&truncated_path).unwrap().len(), 0);
}

#[test]
fn checkpoint_wal_round_trip_corruption_and_truncation() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("00000.wal");
    let wal = Wal::create(&path).unwrap();
    wal.put(b"key", b"value").unwrap();
    wal.sync().unwrap();
    drop(wal);
    let original = std::fs::read(&path).unwrap();
    let recovered = SkipMap::<Bytes, Bytes>::new();
    Wal::recover(&path, &recovered).unwrap();
    assert_eq!(
        recovered.get(b"key".as_slice()).unwrap().value().as_ref(),
        b"value"
    );

    let flipped_path = dir.path().join("flipped.wal");
    let mut flipped = original.clone();
    flipped[2] ^= 1;
    std::fs::write(&flipped_path, flipped).unwrap();
    assert!(Wal::recover(&flipped_path, &SkipMap::new()).is_err());

    let bounded_path = dir.path().join("bounded.wal");
    let mut bounded = original.clone();
    bounded[..2].copy_from_slice(&u16::MAX.to_be_bytes());
    std::fs::write(&bounded_path, bounded).unwrap();
    Wal::recover(&bounded_path, &SkipMap::new()).unwrap();
    assert_eq!(std::fs::metadata(&bounded_path).unwrap().len(), 0);

    let truncated_path = dir.path().join("truncated.wal");
    std::fs::write(&truncated_path, &original[..original.len() - 1]).unwrap();
    Wal::recover(&truncated_path, &SkipMap::new()).unwrap();
    assert_eq!(std::fs::metadata(&truncated_path).unwrap().len(), 0);
}
