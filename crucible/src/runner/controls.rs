//! What the screen can ask of a running campaign.

use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

/// Held by both the screen and the dispatch loop.
#[derive(Clone, Default)]
pub struct Controls {
    paused: Arc<AtomicBool>,
    /// Set when the user asks for the report on what the campaign has so far.
    finishing: Arc<AtomicBool>,
    /// Schedules the user has skipped. The dispatch loop takes these the
    /// next time it looks.
    skipping: Arc<Mutex<BTreeSet<u32>>>,
}

impl Controls {
    /// Whether the campaign is paused.
    pub fn paused(&self) -> bool {
        self.paused.load(Ordering::Relaxed)
    }

    /// Toggle pause on / off.
    ///
    /// Runs already in flight finish, a paused campaign just stops picking up
    /// new work.
    pub fn pause(&self, holding: bool) {
        self.paused.store(holding, Ordering::Relaxed);
    }

    /// Whether the campaign is wrapping up.
    pub fn finishing(&self) -> bool {
        self.finishing.load(Ordering::Relaxed)
    }

    /// Stop picking up new work and report on what the campaign has.
    pub fn finish(&self) {
        self.finishing.store(true, Ordering::Relaxed);
    }

    /// Give up on a schedule, whether it is running or scheduled.
    pub fn skip(&self, schedule: u32) {
        self.hold().insert(schedule);
    }

    /// The schedules given up on since this was last called.
    pub fn skipping(&self) -> BTreeSet<u32> {
        std::mem::take(&mut self.hold())
    }

    /// The set of skipped schedules.
    // Poisoning is ignored. The set holds user requests, not state a run
    // depends on, so a panicking holder cannot leave it inconsistent.
    fn hold(&self) -> std::sync::MutexGuard<'_, BTreeSet<u32>> {
        self.skipping
            .lock()
            .unwrap_or_else(|held| held.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[rstest::rstest]
    #[case::held(true)]
    #[case::let_go(false)]
    fn a_campaign_takes_work_on_until_it_is_held(#[case] holding: bool) {
        let controls = Controls::default();
        controls.pause(!holding);

        controls.pause(holding);

        assert_eq!(controls.paused(), holding);
    }

    #[test]
    fn asking_to_finish_leaves_the_pause_alone() {
        let controls = Controls::default();
        controls.pause(true);

        controls.finish();

        assert!(controls.paused(), "finishing does not resume the campaign");
        assert!(controls.finishing());
    }

    #[test]
    fn a_campaign_asked_to_finish_takes_nothing_else_on() {
        let controls = Controls::default();
        assert!(!controls.finishing());

        controls.finish();

        assert!(controls.finishing());
        assert!(!controls.paused(), "finishing is not holding");
    }

    #[test]
    fn a_skip_is_taken_once_and_then_forgotten() {
        let controls = Controls::default();
        controls.skip(7);
        controls.skip(9);

        assert_eq!(controls.skipping(), BTreeSet::from([7, 9]));
        assert!(
            controls.skipping().is_empty(),
            "the loop takes each request once"
        );
    }

    #[test]
    fn what_one_holder_asks_every_holder_sees() {
        let controls = Controls::default();
        let screen = controls.clone();

        screen.pause(true);

        assert!(
            controls.paused(),
            "the dispatch loop reads what the screen set"
        );
    }
}
