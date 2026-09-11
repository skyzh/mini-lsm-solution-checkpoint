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

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> Vec<usize> {
        let begin_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].first_key())
            .min()
            .cloned()
            .unwrap();
        let end_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].last_key())
            .max()
            .cloned()
            .unwrap();
        snapshot.levels[in_level - 1]
            .1
            .iter()
            .filter(|id| {
                let sst = &snapshot.sstables[id];
                !(sst.last_key() < &begin_key || sst.first_key() > &end_key)
            })
            .copied()
            .collect()
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let mut target_sizes = vec![0; self.options.max_levels];
        let real_sizes = snapshot
            .levels
            .iter()
            .map(|(_, files)| {
                files
                    .iter()
                    .map(|id| snapshot.sstables[id].table_size() as usize)
                    .sum::<usize>()
            })
            .collect::<Vec<_>>();
        let base_size = self.options.base_level_size_mb * 1024 * 1024;
        target_sizes[self.options.max_levels - 1] =
            real_sizes[self.options.max_levels - 1].max(base_size);
        let mut base_level = self.options.max_levels;
        for level in (0..self.options.max_levels - 1).rev() {
            let next_size = target_sizes[level + 1];
            if next_size > base_size {
                target_sizes[level] = next_size / self.options.level_size_multiplier;
            }
            if target_sizes[level] > 0 {
                base_level = level + 1;
            }
        }

        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: base_level,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &snapshot.l0_sstables,
                    base_level,
                ),
                is_lower_level_bottom_level: base_level == self.options.max_levels,
            });
        }

        let mut priorities = Vec::new();
        for level in 0..self.options.max_levels {
            let priority = real_sizes[level] as f64 / target_sizes[level] as f64;
            if priority > 1.0 {
                priorities.push((priority, level + 1));
            }
        }
        priorities.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
        let (_, upper_level) = *priorities.first()?;
        let selected_sst = snapshot.levels[upper_level - 1]
            .1
            .iter()
            .min()
            .copied()
            .unwrap();
        let lower_level = upper_level + 1;
        Some(LeveledCompactionTask {
            upper_level: Some(upper_level),
            upper_level_sst_ids: vec![selected_sst],
            lower_level,
            lower_level_sst_ids: self.find_overlapping_ssts(snapshot, &[selected_sst], lower_level),
            is_lower_level_bottom_level: lower_level == self.options.max_levels,
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let mut upper_ids = task
            .upper_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        let mut lower_ids = task
            .lower_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();

        if let Some(upper_level) = task.upper_level {
            snapshot.levels[upper_level - 1]
                .1
                .retain(|id| !upper_ids.remove(id));
        } else {
            snapshot.l0_sstables.retain(|id| !upper_ids.remove(id));
        }
        assert!(upper_ids.is_empty());

        let mut lower_level = snapshot.levels[task.lower_level - 1]
            .1
            .iter()
            .filter(|id| !lower_ids.remove(id))
            .copied()
            .collect::<Vec<_>>();
        assert!(lower_ids.is_empty());
        lower_level.extend(output);
        if !in_recovery {
            lower_level.sort_by(|x, y| {
                snapshot.sstables[x]
                    .first_key()
                    .cmp(snapshot.sstables[y].first_key())
            });
        }
        snapshot.levels[task.lower_level - 1].1 = lower_level;

        let mut removed = task.upper_level_sst_ids.clone();
        removed.extend(&task.lower_level_sst_ids);
        (snapshot, removed)
    }
}
