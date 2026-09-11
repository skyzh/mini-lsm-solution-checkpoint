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
use anyhow::{Context, Result, bail, ensure};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::key::{KeyBytes, KeySlice};

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
    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let path = path.as_ref();
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to recover WAL")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut remaining: &[u8] = &buf;
        let mut valid_len = 0;
        let mut has_truncated_tail = false;
        while remaining.has_remaining() {
            if remaining.remaining() < std::mem::size_of::<u32>() {
                has_truncated_tail = true;
                break;
            }
            let batch_size = remaining.get_u32() as usize;
            if batch_size
                > remaining
                    .remaining()
                    .saturating_sub(std::mem::size_of::<u32>())
            {
                has_truncated_tail = true;
                break;
            }
            let mut batch = &remaining[..batch_size];
            let checksum = crc32fast::hash(batch);
            let mut component_hasher = crc32fast::Hasher::new();
            let mut records = Vec::new();
            while batch.has_remaining() {
                ensure!(
                    batch.remaining() >= std::mem::size_of::<u16>(),
                    "incomplete WAL key length"
                );
                let key_len = batch.get_u16() as usize;
                component_hasher.write(&(key_len as u16).to_be_bytes());
                ensure!(
                    batch.remaining()
                        >= key_len + std::mem::size_of::<u64>() + std::mem::size_of::<u16>(),
                    "incomplete WAL key"
                );
                let key = Bytes::copy_from_slice(&batch[..key_len]);
                component_hasher.write(&key);
                batch.advance(key_len);
                let ts = batch.get_u64();
                component_hasher.write(&ts.to_be_bytes());
                let value_len = batch.get_u16() as usize;
                component_hasher.write(&(value_len as u16).to_be_bytes());
                ensure!(batch.remaining() >= value_len, "incomplete WAL value");
                let value = Bytes::copy_from_slice(&batch[..value_len]);
                component_hasher.write(&value);
                batch.advance(value_len);
                records.push((key, ts, value));
            }
            ensure!(
                component_hasher.finalize() == checksum,
                "WAL component checksum disagrees with frame checksum"
            );
            remaining.advance(batch_size);
            if remaining.get_u32() != checksum {
                bail!("WAL checksum mismatched at byte offset {valid_len}");
            }
            for (key, ts, value) in records {
                skiplist.insert(KeyBytes::from_bytes_with_ts(key, ts), value);
            }
            valid_len = buf.len() - remaining.len();
        }
        if has_truncated_tail {
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
    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    /// Week 3 Day 5: append a batch of key-value pairs.
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut payload = Vec::new();
        for (key, value) in data {
            let key_len = u16::try_from(key.key_len()).context("WAL key is too large")?;
            let value_len = u16::try_from(value.len()).context("WAL value is too large")?;
            payload.put_u16(key_len);
            payload.put_slice(key.key_ref());
            payload.put_u64(key.ts());
            payload.put_u16(value_len);
            payload.put_slice(value);
        }
        let batch_size = u32::try_from(payload.len()).context("WAL batch is too large")?;
        let checksum = crc32fast::hash(&payload);
        let mut frame = Vec::with_capacity(std::mem::size_of::<u32>() * 2 + payload.len());
        frame.put_u32(batch_size);
        frame.put_slice(&payload);
        frame.put_u32(checksum);
        self.file.lock().write_all(&frame)?;
        Ok(())
    }

    /// Week 2 Day 6: synchronize the write-ahead log to storage.
    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
