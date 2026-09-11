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
#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::{Context, Result, bail};
use bytes::{BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::key::KeySlice;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    /// Week 2 Day 6: create a new write-ahead log.
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(path)
                    .context("failed to create WAL")?,
            ))),
        })
    }

    /// Week 2 Day 6: recover entries from a write-ahead log.
    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to recover WAL")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut valid_len = 0usize;
        while valid_len < buf.len() {
            let remaining = &buf[valid_len..];
            if remaining.len() < std::mem::size_of::<u16>() {
                break;
            }
            let key_len = u16::from_be_bytes([remaining[0], remaining[1]]) as usize;
            let Some(value_len_offset) = std::mem::size_of::<u16>().checked_add(key_len) else {
                break;
            };
            let Some(value_offset) = value_len_offset.checked_add(std::mem::size_of::<u16>())
            else {
                break;
            };
            if remaining.len() < value_offset {
                break;
            }
            let value_len =
                u16::from_be_bytes([remaining[value_len_offset], remaining[value_len_offset + 1]])
                    as usize;
            let Some(checksum_offset) = value_offset.checked_add(value_len) else {
                break;
            };
            let Some(record_len) = checksum_offset.checked_add(std::mem::size_of::<u32>()) else {
                break;
            };
            if remaining.len() < record_len {
                break;
            }
            let expected_checksum = u32::from_be_bytes([
                remaining[checksum_offset],
                remaining[checksum_offset + 1],
                remaining[checksum_offset + 2],
                remaining[checksum_offset + 3],
            ]);
            if crc32fast::hash(&remaining[..checksum_offset]) != expected_checksum {
                bail!("WAL checksum mismatched at byte offset {valid_len}");
            }
            let key = Bytes::copy_from_slice(&remaining[2..value_len_offset]);
            let value = Bytes::copy_from_slice(&remaining[value_offset..checksum_offset]);
            skiplist.insert(key, value);
            valid_len += record_len;
        }
        if valid_len < buf.len() {
            file.set_len(valid_len as u64)
                .context("failed to truncate incomplete WAL tail")?;
            file.sync_all()
                .context("failed to sync truncated WAL tail")?;
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    /// Week 2 Day 6: append a key-value pair to the write-ahead log.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let key_len = u16::try_from(key.len()).context("WAL key is too large")?;
        let value_len = u16::try_from(value.len()).context("WAL value is too large")?;
        let mut buf = Vec::with_capacity(
            key.len() + value.len() + std::mem::size_of::<u16>() * 2 + std::mem::size_of::<u32>(),
        );
        buf.put_u16(key_len);
        buf.put_slice(key);
        buf.put_u16(value_len);
        buf.put_slice(value);
        buf.put_u32(crc32fast::hash(&buf));
        self.file.lock().write_all(&buf)?;
        Ok(())
    }

    /// Week 3 Day 5: append a batch of key-value pairs.
    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        unimplemented!()
    }

    /// Week 2 Day 6: synchronize the write-ahead log to storage.
    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
