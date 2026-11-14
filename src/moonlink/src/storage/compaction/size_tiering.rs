//! Size-tiered selection for background maintenance.
//!
//! Background compaction used to merge any files below the final size, so every round rewrote
//! the previous round's output together with a few new small files: a table receiving small
//! writes rewrote its data many times over. Merging only files of similar size makes each round
//! produce a file at least `SIZE_TIER_RATIO` times larger than any of its inputs, so a byte is
//! rewritten a logarithmic number of times.

/// Items whose sizes are within this factor of the smallest one may be merged together.
pub(crate) const SIZE_TIER_RATIO: u64 = 4;
/// Sizes below this are treated as equal, so tiny files always merge with each other.
pub(crate) const SIZE_TIER_FLOOR_BYTES: u64 = 64 * 1024;

/// A candidate for merging: its size in bytes, how much it counts against the min/max limits
/// (e.g. the number of data files of a compaction group), and the item itself.
pub(crate) struct SizedCandidate<T> {
    pub(crate) bytes: u64,
    pub(crate) weight: usize,
    pub(crate) item: T,
}

/// Choose candidates of similar size, preferring the smallest ones: the first run (in size order)
/// of candidates within `SIZE_TIER_RATIO` of its smallest member whose total weight reaches
/// `min_weight`, taking whole candidates up to `max_weight`. Returns nothing if no run qualifies.
pub(crate) fn pick_similar_sized<T>(
    mut candidates: Vec<SizedCandidate<T>>,
    min_weight: usize,
    max_weight: usize,
) -> Vec<T> {
    candidates.sort_by_key(|c| c.bytes);
    let tier_bytes = |bytes: u64| bytes.max(SIZE_TIER_FLOOR_BYTES);
    let mut chosen_range: Option<Vec<usize>> = None;
    for start in 0..candidates.len() {
        let limit = tier_bytes(candidates[start].bytes).saturating_mul(SIZE_TIER_RATIO);
        let mut picked = Vec::new();
        let mut total = 0;
        for (idx, candidate) in candidates.iter().enumerate().skip(start) {
            if tier_bytes(candidate.bytes) > limit {
                break;
            }
            if total + candidate.weight > max_weight {
                continue;
            }
            total += candidate.weight;
            picked.push(idx);
        }
        if total >= min_weight && !picked.is_empty() {
            chosen_range = Some(picked);
            break;
        }
    }
    let Some(picked) = chosen_range else {
        return Vec::new();
    };
    let mut keep = vec![false; candidates.len()];
    for idx in picked {
        keep[idx] = true;
    }
    candidates
        .into_iter()
        .zip(keep)
        .filter_map(|(c, k)| k.then_some(c.item))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidates(sizes: &[u64]) -> Vec<SizedCandidate<u64>> {
        sizes
            .iter()
            .map(|&bytes| SizedCandidate {
                bytes,
                weight: 1,
                item: bytes,
            })
            .collect()
    }

    const MIB: u64 = 1024 * 1024;

    #[test]
    fn test_equal_small_files_merge() {
        let picked = pick_similar_sized(candidates(&[10 * MIB; 16]), 16, 32);
        assert_eq!(picked.len(), 16);
    }

    #[test]
    fn test_large_output_is_not_merged_with_small_files() {
        // One earlier output plus 15 small files: not enough similar files yet.
        let mut sizes = vec![10 * MIB; 15];
        sizes.push(160 * MIB);
        assert!(pick_similar_sized(candidates(&sizes), 16, 32).is_empty());
        // The 16th small file completes a run that leaves the large file alone.
        sizes.push(10 * MIB);
        let picked = pick_similar_sized(candidates(&sizes), 16, 32);
        assert_eq!(picked.len(), 16);
        assert!(picked.iter().all(|&b| b == 10 * MIB));
    }

    #[test]
    fn test_similar_files_across_a_power_of_four_boundary_merge() {
        // 3.9 MiB and 4.1 MiB are in different fixed tiers but within the ratio of each other.
        let mut sizes = vec![39 * MIB / 10; 8];
        sizes.extend(vec![41 * MIB / 10; 8]);
        assert_eq!(pick_similar_sized(candidates(&sizes), 16, 32).len(), 16);
    }

    #[test]
    fn test_prefers_smallest_tier_and_respects_max() {
        let mut sizes = vec![100 * MIB; 20];
        sizes.extend(vec![MIB; 40]);
        let picked = pick_similar_sized(candidates(&sizes), 16, 32);
        assert_eq!(picked.len(), 32);
        assert!(picked.iter().all(|&b| b == MIB));
    }

    #[test]
    fn test_tiny_files_merge_regardless_of_ratio() {
        // All below the floor: footers dominate, sizes vary a lot, they still merge.
        let sizes: Vec<u64> = (1..=16).map(|i| i * 1024).collect();
        assert_eq!(pick_similar_sized(candidates(&sizes), 16, 32).len(), 16);
    }

    #[test]
    fn test_whole_weighted_candidates() {
        // A group of 10 files is taken whole or not at all.
        let c = vec![
            SizedCandidate {
                bytes: MIB,
                weight: 10,
                item: 'a',
            },
            SizedCandidate {
                bytes: MIB,
                weight: 10,
                item: 'b',
            },
            SizedCandidate {
                bytes: MIB,
                weight: 3,
                item: 'c',
            },
        ];
        let picked = pick_similar_sized(c, 12, 16);
        assert_eq!(picked, vec!['a', 'c']);
    }

    /// Simulate a table receiving one small flush per round, merging whenever the selection
    /// allows: each flushed byte is rewritten only a few times (the old any-small-file policy
    /// rewrote the growing output on every round).
    #[test]
    fn test_trickle_rewrites_each_byte_a_logarithmic_number_of_times() {
        const FLUSH: u64 = 100 * 1024;
        const FINAL: u64 = 512 * MIB;
        let mut files: Vec<u64> = Vec::new();
        let mut flushed = 0u64;
        let mut rewritten = 0u64;
        for round in 0..20_000u64 {
            // Flush sizes vary a little, like real flushes do.
            let flush_bytes = FLUSH + round % 7 * 1024;
            files.push(flush_bytes);
            flushed += flush_bytes;
            loop {
                let eligible: Vec<SizedCandidate<usize>> = files
                    .iter()
                    .enumerate()
                    .filter(|(_, &b)| b < FINAL)
                    .map(|(i, &b)| SizedCandidate {
                        bytes: b,
                        weight: 1,
                        item: i,
                    })
                    .collect();
                let picked = pick_similar_sized(eligible, 16, 32);
                if picked.is_empty() {
                    break;
                }
                let merged: u64 = picked.iter().map(|&i| files[i]).sum();
                rewritten += merged;
                let mut keep = vec![true; files.len()];
                for i in picked {
                    keep[i] = false;
                }
                files = files
                    .into_iter()
                    .zip(keep)
                    .filter_map(|(b, k)| k.then_some(b))
                    .collect();
                files.push(merged);
            }
        }
        let rewrite_factor = rewritten as f64 / flushed as f64;
        assert!(
            rewrite_factor < 5.0,
            "each flushed byte was rewritten {rewrite_factor:.1} times"
        );
        // Small files don't pile up either.
        assert!(files.len() < 100, "{} files left", files.len());
    }
}
