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

use anyhow::{Context, Result};
use bytes::{Buf, BufMut, Bytes};
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
        let mut record = buf.as_slice();
        while record.has_remaining() {
            let key_len = record.get_u16() as usize;
            let key = Bytes::copy_from_slice(&record[..key_len]);
            record.advance(key_len);
            let value_len = record.get_u16() as usize;
            let value = Bytes::copy_from_slice(&record[..value_len]);
            record.advance(value_len);
            skiplist.insert(key, value);
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    /// Week 2 Day 6: append a key-value pair to the write-ahead log.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut buf = Vec::with_capacity(key.len() + value.len() + std::mem::size_of::<u16>() * 2);
        buf.put_u16(key.len() as u16);
        buf.put_slice(key);
        buf.put_u16(value.len() as u16);
        buf.put_slice(value);
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
