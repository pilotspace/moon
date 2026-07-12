//! Shared tier-ladder vocabulary for cross-plane memory residency.
//!
//! K4 (kernel-m2-brief-2026-07-12 stage 2, item 5): **types only** in this
//! milestone (M2). No plane adopts [`ResidencyTier`] or [`TierPolicy`] yet
//! -- this module exists to give a name to a concept vector already
//! implements ad hoc (`src/vector/segment/holder.rs`'s `mutable` /
//! `immutable` (HOT) vs `warm` fields, `WarmSearchSegment`'s HOT->WARM
//! transition), so KV (disk-offload `ColdIndex`), graph (CSR segments),
//! and FTS can adopt the same vocabulary incrementally in M4 instead of
//! each inventing a bespoke tier concept.
//!
//! Non-goal (per the K4 brief): forcing one eviction policy on all planes.
//! Each plane keeps its own trigger/threshold logic (idle timeout, byte
//! cap, memory pressure) -- this module only standardizes the RESIDENCY
//! STATE an entry/segment can report itself in, and the query surface a
//! future shared driver (idle sweep, memory-pressure cascade) could use to
//! ask "what tier is this in" / "may this demote" without reaching into
//! plane internals.
//!
//! Adopting a plane onto this trait is explicitly OUT of scope here: doing
//! so touches each plane's compact/demote/promote call sites, which is real
//! behavior-affecting work that belongs in M4 with its own red/green tests,
//! not folded into an accounting-only milestone.

/// A segment/entry's residency state along the shared tier ladder.
///
/// `Hot` -> `WarmReloadable` -> `ColdStub`, in decreasing RAM cost and
/// increasing reload latency to serve a request. Not every plane
/// implements every tier:
///
/// - KV disk-offload (`storage::tiered::cold_index::ColdIndex`) today only
///   has `Hot` (DashTable-resident) and `ColdStub` (spilled, index-only) --
///   no reloadable middle tier.
/// - Vector immutable segments have `Hot` and `WarmReloadable`
///   (`WarmSearchSegment`, mmap-backed) but no `ColdStub`: the 2026-07-10
///   two-stack decision deleted vector COLD/DiskANN entirely (see
///   `project_vector_two_stack_decision` in project memory).
/// - Graph CSR segments are currently `Hot`-only (no demotion path yet).
///
/// This is exactly why the enum has three variants even though no single
/// plane uses all three today: it is the union of what every plane's
/// ladder could eventually reach, not a description of current behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResidencyTier {
    /// Fully materialized in RAM. Serving a request pays no reload cost.
    Hot,
    /// Partially resident (e.g. mmap-backed, quantized-codes-only,
    /// index-only-with-on-disk-payload): serving a request may pay a
    /// reload/decode cost, but the bookkeeping needed to KNOW what to
    /// reload and where is still in RAM.
    WarmReloadable,
    /// A pointer/stub only. Serving a request requires a full reload from
    /// durable storage before any work on this entry/segment can proceed.
    ColdStub,
}

impl ResidencyTier {
    /// Whether this tier can still answer a query without a disk read
    /// (`Hot`) versus needing one (`WarmReloadable` partially, `ColdStub`
    /// fully). A cheap, plane-agnostic classification a shared driver could
    /// use for e.g. "how much of this plane's data is disk-free right now".
    #[must_use]
    pub fn is_fully_resident(self) -> bool {
        matches!(self, ResidencyTier::Hot)
    }
}

/// Per-plane policy hook for tier transitions.
///
/// **Trait skeleton only (M2) -- no implementation exists yet.** Intended
/// shape for M4 adoption: a plane's segment/entry container implements this
/// so a future shared driver (idle sweep, memory-pressure cascade) can ask
/// "what tier is `id` in" / "should `id` demote" without knowing the
/// plane's internals, while the plane retains full control over its OWN
/// demote/promote mechanics -- this trait deliberately does NOT prescribe
/// *how* a demotion is carried out, only how it is queried and decided.
pub trait TierPolicy {
    /// Opaque identifier for one entry/segment in this plane (e.g. a vector
    /// segment id, a KV key, a graph CSR segment id).
    type Id;

    /// Current residency tier of `id`, or `None` if `id` is unknown to this
    /// plane's tier bookkeeping (plane doesn't track tiers for it, or it
    /// does not exist).
    fn tier_of(&self, id: &Self::Id) -> Option<ResidencyTier>;

    /// Whether `id` is eligible to demote one step down the ladder right
    /// now, per the plane's own idle/byte-cap/memory-pressure policy
    /// (entirely plane-defined -- this trait imposes no threshold).
    /// Returning `true` is advisory: the caller decides WHEN to actually
    /// invoke the plane's demotion mechanics; this only answers "may I".
    fn eligible_to_demote(&self, id: &Self::Id) -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_fully_resident_true_only_for_hot() {
        assert!(ResidencyTier::Hot.is_fully_resident());
        assert!(!ResidencyTier::WarmReloadable.is_fully_resident());
        assert!(!ResidencyTier::ColdStub.is_fully_resident());
    }

    #[test]
    fn residency_tier_is_copy_eq_hash() {
        // Compile-time + runtime smoke test that the derives hold: the
        // trait skeleton above is meant to be usable as a map key /
        // comparison target by a future shared driver.
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(ResidencyTier::Hot);
        set.insert(ResidencyTier::Hot);
        set.insert(ResidencyTier::WarmReloadable);
        assert_eq!(set.len(), 2);
    }

    /// Minimal mock plane proving `TierPolicy` is implementable and usable
    /// generically -- exercises the intended M4 adoption shape without
    /// adopting any real plane onto it (that is explicitly out of scope
    /// here; see module doc comment).
    struct MockPlane {
        tiers: std::collections::HashMap<u64, ResidencyTier>,
    }

    impl TierPolicy for MockPlane {
        type Id = u64;

        fn tier_of(&self, id: &Self::Id) -> Option<ResidencyTier> {
            self.tiers.get(id).copied()
        }

        fn eligible_to_demote(&self, id: &Self::Id) -> bool {
            matches!(self.tier_of(id), Some(ResidencyTier::Hot))
        }
    }

    #[test]
    fn tier_policy_skeleton_is_implementable() {
        let mut tiers = std::collections::HashMap::new();
        tiers.insert(1u64, ResidencyTier::Hot);
        tiers.insert(2u64, ResidencyTier::ColdStub);
        let plane = MockPlane { tiers };

        assert_eq!(plane.tier_of(&1), Some(ResidencyTier::Hot));
        assert_eq!(plane.tier_of(&2), Some(ResidencyTier::ColdStub));
        assert_eq!(plane.tier_of(&999), None);

        assert!(plane.eligible_to_demote(&1));
        assert!(!plane.eligible_to_demote(&2));
        assert!(!plane.eligible_to_demote(&999));
    }
}
