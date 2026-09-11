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

mod leveled;
mod simple_leveled;
mod tiered;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (
                CompactionController::NoCompaction,
                CompactionTask::ForceFullCompaction {
                    l0_sstables,
                    l1_sstables,
                },
            ) => {
                let mut snapshot = snapshot.clone();
                let mut captured = l0_sstables.iter().copied().collect::<HashSet<_>>();
                snapshot.l0_sstables.retain(|id| !captured.remove(id));
                assert!(captured.is_empty());
                assert_eq!(snapshot.levels[0].1, *l1_sstables);
                snapshot.levels[0].1 = output.to_vec();
                let removed = l0_sstables.iter().chain(l1_sstables).copied().collect();
                (snapshot, removed)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact_generate_sst_from_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
        _compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut builder = None;
        let mut entries_in_builder = 0;
        let mut new_ssts = Vec::new();
        let mut last_key = Vec::<u8>::new();

        while iter.is_valid() {
            if builder.is_none() {
                builder = Some(SsTableBuilder::new(self.options.block_size));
            }
            let same_as_last_key = iter.key().key_ref() == last_key;

            if entries_in_builder > 0
                && builder.as_ref().unwrap().estimated_size() >= self.options.target_sst_size
                && !same_as_last_key
            {
                let sst_id = self.next_sst_id();
                let old_builder = builder.take().unwrap();
                new_ssts.push(Arc::new(old_builder.build(
                    sst_id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(sst_id),
                )?));
                builder = Some(SsTableBuilder::new(self.options.block_size));
                entries_in_builder = 0;
            }

            builder.as_mut().unwrap().add(iter.key(), iter.value());
            entries_in_builder += 1;
            if !same_as_last_key {
                last_key.clear();
                last_key.extend(iter.key().key_ref());
            }
            iter.next()?;
        }

        if let Some(builder) = builder
            && entries_in_builder > 0
        {
            let sst_id = self.next_sst_id();
            new_ssts.push(Arc::new(builder.build(
                sst_id,
                Some(self.block_cache.clone()),
                self.path_of_sst(sst_id),
            )?));
        }
        Ok(new_ssts)
    }

    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = self.state.read().clone();
        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let mut l0_iters = Vec::with_capacity(l0_sstables.len());
                for id in l0_sstables {
                    l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                        snapshot.sstables[id].clone(),
                    )?));
                }
                let l1_ssts = l1_sstables
                    .iter()
                    .map(|id| snapshot.sstables[id].clone())
                    .collect();
                let iter = TwoMergeIterator::create(
                    MergeIterator::create(l0_iters),
                    SstConcatIterator::create_and_seek_to_first(l1_ssts)?,
                )?;
                self.compact_generate_sst_from_iter(iter, task.compact_to_bottom_level())
            }
            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level_sst_ids,
                ..
            }) => {
                let lower_ssts = lower_level_sst_ids
                    .iter()
                    .map(|id| snapshot.sstables[id].clone())
                    .collect();
                let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;
                if upper_level.is_some() {
                    let upper_ssts = upper_level_sst_ids
                        .iter()
                        .map(|id| snapshot.sstables[id].clone())
                        .collect();
                    let upper_iter = SstConcatIterator::create_and_seek_to_first(upper_ssts)?;
                    self.compact_generate_sst_from_iter(
                        TwoMergeIterator::create(upper_iter, lower_iter)?,
                        task.compact_to_bottom_level(),
                    )
                } else {
                    let mut upper_iters = Vec::with_capacity(upper_level_sst_ids.len());
                    for id in upper_level_sst_ids {
                        upper_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                            snapshot.sstables[id].clone(),
                        )?));
                    }
                    self.compact_generate_sst_from_iter(
                        TwoMergeIterator::create(MergeIterator::create(upper_iters), lower_iter)?,
                        task.compact_to_bottom_level(),
                    )
                }
            }
            CompactionTask::Leveled(LeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level_sst_ids,
                ..
            }) => {
                let lower_ssts = lower_level_sst_ids
                    .iter()
                    .map(|id| snapshot.sstables[id].clone())
                    .collect();
                let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;
                if upper_level.is_some() {
                    let upper_ssts = upper_level_sst_ids
                        .iter()
                        .map(|id| snapshot.sstables[id].clone())
                        .collect();
                    let upper_iter = SstConcatIterator::create_and_seek_to_first(upper_ssts)?;
                    self.compact_generate_sst_from_iter(
                        TwoMergeIterator::create(upper_iter, lower_iter)?,
                        task.compact_to_bottom_level(),
                    )
                } else {
                    let mut upper_iters = Vec::with_capacity(upper_level_sst_ids.len());
                    for id in upper_level_sst_ids {
                        upper_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                            snapshot.sstables[id].clone(),
                        )?));
                    }
                    self.compact_generate_sst_from_iter(
                        TwoMergeIterator::create(MergeIterator::create(upper_iters), lower_iter)?,
                        task.compact_to_bottom_level(),
                    )
                }
            }
            CompactionTask::Tiered(TieredCompactionTask { tiers, .. }) => {
                let mut tier_iters = Vec::with_capacity(tiers.len());
                for (_, tier_sst_ids) in tiers {
                    let ssts = tier_sst_ids
                        .iter()
                        .map(|id| snapshot.sstables[id].clone())
                        .collect();
                    tier_iters.push(Box::new(SstConcatIterator::create_and_seek_to_first(ssts)?));
                }
                self.compact_generate_sst_from_iter(
                    MergeIterator::create(tier_iters),
                    task.compact_to_bottom_level(),
                )
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let CompactionOptions::NoCompaction = self.options.compaction_options else {
            panic!("full compaction can only be called when compaction is not enabled")
        };
        let snapshot = self.state.read().clone();
        let l0_sstables = snapshot.l0_sstables.clone();
        let l1_sstables = snapshot.levels[0].1.clone();
        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };
        let new_ssts = self.compact(&task)?;
        let output_ids = new_ssts.iter().map(|sst| sst.sst_id()).collect::<Vec<_>>();
        self.sync_dir()?;

        {
            let state_lock = self.state_lock.lock();
            let mut state = self.state.read().as_ref().clone();
            for id in l0_sstables.iter().chain(&l1_sstables) {
                assert!(state.sstables.remove(id).is_some());
            }
            for sst in new_ssts {
                assert!(state.sstables.insert(sst.sst_id(), sst).is_none());
            }
            assert_eq!(state.levels[0].1, l1_sstables);
            state.levels[0].1 = output_ids.clone();
            let mut captured = l0_sstables.iter().copied().collect::<HashSet<_>>();
            state.l0_sstables.retain(|id| !captured.remove(id));
            assert!(captured.is_empty());
            *self.state.write() = Arc::new(state);
            self.manifest
                .as_ref()
                .unwrap()
                .add_record(&state_lock, ManifestRecord::Compaction(task, output_ids))?;
        }

        for id in l0_sstables.iter().chain(&l1_sstables) {
            std::fs::remove_file(self.path_of_sst(*id))?;
        }
        self.sync_dir()?;
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = self.state.read().clone();
        let Some(task) = self
            .compaction_controller
            .generate_compaction_task(&snapshot)
        else {
            return Ok(());
        };
        let new_ssts = self.compact(&task)?;
        let output = new_ssts.iter().map(|sst| sst.sst_id()).collect::<Vec<_>>();
        self.sync_dir()?;
        let removed_ssts = {
            let state_lock = self.state_lock.lock();
            let mut snapshot = self.state.read().as_ref().clone();
            for sst in new_ssts {
                assert!(snapshot.sstables.insert(sst.sst_id(), sst).is_none());
            }
            let (mut snapshot, removed_ids) = self
                .compaction_controller
                .apply_compaction_result(&snapshot, &task, &output, false);
            let mut removed_ssts = Vec::with_capacity(removed_ids.len());
            for id in removed_ids {
                removed_ssts.push(snapshot.sstables.remove(&id).unwrap());
            }
            *self.state.write() = Arc::new(snapshot);
            self.manifest
                .as_ref()
                .unwrap()
                .add_record(&state_lock, ManifestRecord::Compaction(task, output))?;
            removed_ssts
        };
        for sst in removed_ssts {
            std::fs::remove_file(self.path_of_sst(sst.sst_id()))?;
        }
        self.sync_dir()?;
        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let should_flush = {
            let snapshot = self.state.read();
            snapshot.imm_memtables.len() >= self.options.num_memtable_limit
        };
        if should_flush {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
