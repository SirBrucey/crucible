use std::collections::BTreeMap;

use super::operation::{Channel, Tag};

/// The publishes one connection has carried, in the order they crossed, and
/// the tag the broker will confirm each with.
///
/// Both directions share it, since a publish and its confirm cross opposite
/// ways. One connection is one stream, so a publish holds the same position
/// here in every run that drives the same work.
#[derive(Clone, Default, Debug)]
pub struct Publishing(std::sync::Arc<std::sync::Mutex<Publishes>>);

#[derive(Default, Debug)]
struct Publishes {
    /// Whether this log has already been told the scenario started.
    started: bool,
    /// Each publish the scenario caused, in the order it crossed.
    sent: Vec<(Channel, Tag)>,
    /// The broker's numbering on each channel.
    tags: BTreeMap<Channel, Tags>,
}

/// The broker's numbering on one channel.
///
/// Its own sequence, which runs from the channel entering confirm mode and so
/// does not restart when a scenario does.
#[derive(Default, Debug)]
struct Tags {
    /// What it will call the next publish.
    next: Tag,
    /// The highest tag it has answered.
    answered: Tag,
}

impl Publishing {
    fn held(&self) -> std::sync::MutexGuard<'_, Publishes> {
        self.0.lock().expect("no panic holds this lock")
    }

    /// Record a publish.
    pub(super) fn sent(&self, channel: Channel) -> usize {
        let mut held = self.held();
        let tags = held.tags.entry(channel).or_default();
        tags.next = tags.next.increment();
        let tag = tags.next;
        held.sent.push((channel, tag));
        held.sent.len()
    }

    /// Which publishes a confirm of `tag` on `channel` answers, lowest first.
    ///
    /// Several of them, where `multiple` is set. A publish from before the
    /// scenario is not held here and names nothing.
    pub(super) fn answered(&self, channel: Channel, tag: Tag, multiple: bool) -> Vec<usize> {
        let mut held = self.held();
        let tags = held.tags.entry(channel).or_default();
        let from = if multiple {
            tags.answered
        } else {
            tag.decrement()
        };
        tags.answered = tag;
        held.sent
            .iter()
            .enumerate()
            .filter(|(_, (on, sent))| *on == channel && *sent > from && *sent <= tag)
            .map(|(at, _)| at + 1)
            .collect()
    }

    /// Clears the publishes from before the scenario.
    ///
    /// Only the first call clears. Both directions share this log and each is
    /// told on its own first read, so the second would throw away publishes
    /// the first has already named.
    pub(super) fn scenario_started(&self) {
        let mut held = self.held();
        if held.started {
            return;
        }
        held.started = true;
        held.sent.clear();
    }
}

#[cfg(test)]
mod tests {
    use crucible_protocol::{Direction, Kind as _};
    use rstest::rstest;

    use crate::message::consuming::Consuming;
    use crate::message::reader::Reader;
    use crate::message::tests::{acked, confirming, confirming_on, marks, publish, publish_on};

    use super::*;

    /// The direction carrying confirms reads nothing until the scenario has
    /// published, so it is always the second to be told.
    #[test]
    fn the_second_direction_told_the_scenario_started_does_not_renumber() {
        let publishing = Publishing::default();
        let mut sender = Reader::new(
            Direction::ClientToUpstream,
            Consuming::default(),
            publishing.clone(),
        );
        let mut broker = Reader::new(
            Direction::UpstreamToClient,
            Consuming::default(),
            publishing,
        );

        sender.scenario_started();
        sender.carry(&publish(b"first"), false);
        sender.carry(&publish(b"second"), false);
        // The confirms for those two are the first thing this half reads, so
        // this is when it is told.
        broker.scenario_started();

        let named = marks(sender.carry(&publish(b"third"), false).found);
        assert!(
            named.iter().any(|mark| mark == "publish:3:before"),
            "{named:?}"
        );
    }

    /// The publishes a scenario causes are numbered from the scenario. What the
    /// fleet published while it was starting up is not the scenario's work.
    #[test]
    fn publishes_are_numbered_from_the_scenario_and_not_the_connection() {
        let mut sender = Reader::new(
            Direction::ClientToUpstream,
            Consuming::default(),
            Publishing::default(),
        );
        for _ in 0..2 {
            sender.carry(&publish(b"while coming up"), false);
        }
        sender.scenario_started();
        let named = marks(sender.carry(&publish(b"an order"), false).found);
        assert!(
            named.iter().any(|mark| mark == "publish:1:before"),
            "{named:?}"
        );
    }

    /// The consumer and the broker have to agree which message an ack ends,
    /// and this reader does not get a say.
    #[test]
    fn the_brokers_own_numbering_is_left_alone() {
        let mut consumer = Reader::new(
            Direction::ClientToUpstream,
            Consuming::default(),
            Publishing::default(),
        );
        consumer.scenario_started();
        let named = marks(consumer.carry(&acked(7), false).found);
        assert!(named.iter().any(|mark| mark == "ack:7:before"), "{named:?}");
    }

    /// Six publishes come back as anything from six frames to one, and
    /// counting frames would name a different set of moments each way.
    #[rstest]
    #[case::one_frame_each(&[(1, false), (2, false), (3, false), (4, false), (5, false), (6, false)])]
    #[case::two_batches_of_three(&[(3, true), (6, true)])]
    #[case::one_batch_for_all(&[(6, true)])]
    #[case::four_then_a_pair(&[(1, false), (2, false), (3, false), (4, false), (6, true)])]
    fn a_confirm_names_the_publishes_it_answers_however_the_broker_batches(
        #[case] batching: &[(u64, bool)],
    ) {
        let consuming = Consuming::default();
        let publishing = Publishing::default();
        let mut client = Reader::new(
            Direction::ClientToUpstream,
            consuming.clone(),
            publishing.clone(),
        );
        let mut broker = Reader::new(Direction::UpstreamToClient, consuming, publishing.clone());
        for i in 0..6 {
            client.carry(&publish(format!("m{i}").as_bytes()), false);
        }

        let mut named = Vec::new();
        for (tag, multiple) in batching {
            named.extend(
                marks(broker.carry(&confirming(*tag, *multiple), false).found)
                    .into_iter()
                    .filter(|mark| mark.starts_with("confirm:")),
            );
        }
        assert_eq!(
            named,
            [
                "confirm:1",
                "confirm:2",
                "confirm:3",
                "confirm:4",
                "confirm:5",
                "confirm:6"
            ]
        );
    }

    /// The tags are the broker's and run per channel, so they collide. The
    /// order the frames crossed does not, because one connection is one stream.
    #[test]
    fn publishes_are_ordered_across_the_channels_of_one_connection() {
        let consuming = Consuming::default();
        let publishing = Publishing::default();
        let mut client = Reader::new(
            Direction::ClientToUpstream,
            consuming.clone(),
            publishing.clone(),
        );
        let mut broker = Reader::new(Direction::UpstreamToClient, consuming, publishing.clone());
        for (channel, body) in [(1, "a"), (2, "b"), (1, "c"), (2, "d")] {
            let channel = Channel::new(channel);
            client.carry(&publish_on(channel, body.as_bytes()), false);
        }

        // Channel 2 answers both of its publishes, which are the second and
        // fourth the connection carried.
        let named: Vec<String> = marks(
            broker
                .carry(&confirming_on(Channel::new(2), Tag::new(2), true), false)
                .found,
        )
        .into_iter()
        .filter(|mark| mark.starts_with("confirm:"))
        .collect();
        assert_eq!(named, ["confirm:2", "confirm:4"]);
    }
}
