//! The kind plugins compiled into this build, and what they read.
//!
//! Kinds are resolved by name at run time but linked at build time, so what a
//! fleet can be read as is settled when the framework is built. Whatever holds
//! this registry is the only thing that names a protocol; everything else asks
//! for a kind and gets something that can read it.

use crucible_protocol::Operations;

/// Something to read one direction of one connection with.
///
/// A kind with no plugin to read it still has to be counted, so its bytes are
/// carried and each read off the wire counts as one. That is what an anchor
/// meant before any protocol could be parsed, and it is the honest answer when
/// nothing can say where one operation ends.
#[must_use]
pub fn reader_for(kind: &str) -> Box<dyn Operations> {
    match kind {
        crucible_kind_amqp::NAME => Box::new(crucible_kind_amqp::message::Reader::new()),
        _ => Box::new(Chunks),
    }
}

/// Whether a kind has a plugin that can read it, for a proxy to say what it is
/// counting.
#[must_use]
pub fn is_read(kind: &str) -> bool {
    matches!(kind, crucible_kind_amqp::NAME)
}

/// Counts a read off the wire as one operation.
struct Chunks;

impl Operations for Chunks {
    fn read(&mut self, _bytes: &[u8]) -> usize {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_kind_with_a_plugin_is_read_by_it() {
        assert!(is_read(crucible_kind_amqp::NAME));
    }

    /// A fleet speaking something with no plugin still runs, and its traffic is
    /// still counted.
    #[test]
    fn a_kind_with_no_plugin_counts_each_read() {
        assert!(!is_read("http"));
        let mut reader = reader_for("http");
        assert_eq!(reader.read(b"anything at all"), 1);
        assert_eq!(reader.read(b""), 1);
    }
}
