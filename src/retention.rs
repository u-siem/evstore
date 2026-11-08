//! Retention, quotas and disk pressure.
//!
//! Design: `docs/design/08-components.md` (retention and disk pressure).
//!
//! - Streaming tenants drop whole segments; sealed tenants are deleted whole.
//! - `min_age` protection floor, legal hold, quotas per tenant or group.
//! - Disk watermarks; a full disk applies backpressure and never overwrites
//!   data in a circular way.
//!
//! # MVP implementation
//!
//! - Segments are partitioned by **ingest day** (UTC), recorded when the
//!   segment is flushed. The design's volume-adaptive partition span is
//!   deferred.
//! - [`RetentionPolicy`]: a partition is dropped (all its segments) once
//!   its newest possible ingest time is older than `max(max_age, min_age)`.
//!   Without `max_age` nothing is dropped.
//! - Compaction is size-tiered inside a day: when a day holds at least
//!   `fanout` segments of the same level, the oldest `fanout` of them are
//!   merged into one segment of the next level. Partitions never merge
//!   across days. Levels stop at [`MAX_LEVEL`].
//! - Quotas, disk watermarks and legal hold are deferred.

use std::time::Duration;

use crate::directory::SegmentEntry;

/// Nanoseconds per day.
pub const DAY_NS: i64 = 86_400 * 1_000_000_000;

/// Highest compaction level; segments at this level are not merged again.
pub const MAX_LEVEL: u8 = 3;

/// How long data is kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionPolicy {
    /// Drop data older than this (`None` keeps everything).
    pub max_age: Option<Duration>,
    /// Never drop data younger than this, whatever `max_age` says.
    pub min_age: Duration,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        RetentionPolicy {
            max_age: None,
            min_age: Duration::from_secs(86_400),
        }
    }
}

/// Day index (days since the Unix epoch) of a nanosecond timestamp.
pub fn day_of(ts_ns: i64) -> i64 {
    ts_ns.div_euclid(DAY_NS)
}

fn duration_ns(d: Duration) -> i64 {
    i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)
}

/// Ids of the segments whose partition has expired at `now`.
pub(crate) fn expired(
    segments: &[SegmentEntry],
    policy: &RetentionPolicy,
    now_ns: i64,
) -> Vec<u64> {
    let Some(max_age) = policy.max_age else {
        return Vec::new();
    };
    let keep = duration_ns(max_age).max(duration_ns(policy.min_age));
    segments
        .iter()
        .filter(|s| {
            let partition_end = s.day.saturating_add(1).saturating_mul(DAY_NS);
            now_ns.saturating_sub(partition_end) >= keep
        })
        .map(|s| s.id)
        .collect()
}

/// The next group of segment ids to merge, oldest first, if any.
pub(crate) fn compaction_pick(segments: &[SegmentEntry], fanout: usize) -> Option<Vec<u64>> {
    let fanout = fanout.max(2);
    let mut keys: Vec<(i64, u8)> = segments
        .iter()
        .filter(|s| s.level < MAX_LEVEL)
        .map(|s| (s.day, s.level))
        .collect();
    keys.sort_unstable();
    keys.dedup();
    for (day, level) in keys {
        let mut group: Vec<&SegmentEntry> = segments
            .iter()
            .filter(|s| s.day == day && s.level == level)
            .collect();
        if group.len() >= fanout {
            group.sort_by_key(|s| s.id);
            return Some(group.into_iter().take(fanout).map(|s| s.id).collect());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seg(id: u64, day: i64, level: u8) -> SegmentEntry {
        SegmentEntry {
            id,
            day,
            rows: 1,
            ts_min: 0,
            ts_max: 0,
            bytes: 1,
            level,
        }
    }

    #[test]
    fn day_index() {
        assert_eq!(day_of(0), 0);
        assert_eq!(day_of(DAY_NS - 1), 0);
        assert_eq!(day_of(DAY_NS), 1);
        assert_eq!(day_of(-1), -1);
    }

    #[test]
    fn retention_boundaries() {
        let segs = [seg(1, 10, 0), seg(2, 11, 0), seg(3, 12, 0)];
        let days = |n: u64| Duration::from_secs(86_400 * n);
        let policy = RetentionPolicy {
            max_age: Some(days(1)),
            min_age: Duration::ZERO,
        };
        // Day 10 ends at the start of day 11; one day later it expires.
        assert_eq!(expired(&segs, &policy, 12 * DAY_NS - 1), [] as [u64; 0]);
        assert_eq!(expired(&segs, &policy, 12 * DAY_NS), vec![1]);
        assert_eq!(expired(&segs, &policy, 14 * DAY_NS), vec![1, 2, 3]);
        // min_age is a floor.
        let floored = RetentionPolicy {
            max_age: Some(days(1)),
            min_age: days(3),
        };
        assert_eq!(expired(&segs, &floored, 13 * DAY_NS), [] as [u64; 0]);
        assert_eq!(expired(&segs, &floored, 14 * DAY_NS), vec![1]);
        // No max_age keeps everything.
        assert_eq!(
            expired(&segs, &RetentionPolicy::default(), i64::MAX),
            [] as [u64; 0]
        );
    }

    #[test]
    fn tiered_picks() {
        let mut segs: Vec<SegmentEntry> = (1..=3).map(|i| seg(i, 5, 0)).collect();
        assert_eq!(compaction_pick(&segs, 4), None);
        segs.push(seg(4, 6, 0));
        assert_eq!(compaction_pick(&segs, 4), None, "days never mix");
        segs.push(seg(5, 5, 0));
        assert_eq!(compaction_pick(&segs, 4), Some(vec![1, 2, 3, 5]));
        let top: Vec<SegmentEntry> = (1..=5).map(|i| seg(i, 5, MAX_LEVEL)).collect();
        assert_eq!(compaction_pick(&top, 4), None);
    }
}
