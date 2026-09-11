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

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        assert!(
            snapshot.l0_sstables.is_empty(),
            "should not add l0 ssts in tiered compaction"
        );
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        let upper_size = snapshot.levels[..snapshot.levels.len() - 1]
            .iter()
            .map(|(_, files)| files.len())
            .sum::<usize>();
        let bottom_size = snapshot.levels.last().unwrap().1.len();
        if upper_size * 100 >= bottom_size * self.options.max_size_amplification_percent {
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        let mut accumulated_size = 0;
        for id in 0..snapshot.levels.len() - 1 {
            accumulated_size += snapshot.levels[id].1.len();
            let merge_width = id + 1;
            let next_size = snapshot.levels[id + 1].1.len();
            if merge_width >= self.options.min_merge_width
                && next_size * 100 > accumulated_size * (100 + self.options.size_ratio)
            {
                return Some(TieredCompactionTask {
                    tiers: snapshot.levels[..merge_width].to_vec(),
                    bottom_tier_included: false,
                });
            }
        }

        let merge_width = snapshot
            .levels
            .len()
            .min(self.options.max_merge_width.unwrap_or(usize::MAX));
        Some(TieredCompactionTask {
            tiers: snapshot.levels[..merge_width].to_vec(),
            bottom_tier_included: merge_width == snapshot.levels.len(),
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        assert!(
            snapshot.l0_sstables.is_empty(),
            "should not add l0 ssts in tiered compaction"
        );
        let mut snapshot = snapshot.clone();
        let mut tiers_to_remove = task
            .tiers
            .iter()
            .map(|(tier_id, files)| (*tier_id, files))
            .collect::<HashMap<_, _>>();
        let mut levels = Vec::new();
        let mut output_added = false;
        let mut files_to_remove = Vec::new();
        for (tier_id, files) in &snapshot.levels {
            if let Some(expected_files) = tiers_to_remove.remove(tier_id) {
                assert_eq!(
                    expected_files, files,
                    "file changed after issuing compaction task"
                );
                files_to_remove.extend(files.iter().copied());
            } else {
                levels.push((*tier_id, files.clone()));
            }
            if tiers_to_remove.is_empty() && !output_added && !output.is_empty() {
                levels.push((output[0], output.to_vec()));
                output_added = true;
            }
        }
        assert!(tiers_to_remove.is_empty(), "some tiers not found");
        snapshot.levels = levels;
        (snapshot, files_to_remove)
    }
}
