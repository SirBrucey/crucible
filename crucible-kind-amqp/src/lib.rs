//! What the framework understands of AMQP 0-9-1.

/// The kind a service declares to be read as this.
pub const NAME: &str = "amqp";

pub mod frame;
pub mod message;
