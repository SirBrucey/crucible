pub mod codec;
mod message;

pub use message::{RunnerToWorker, Verdict, WorkerEvent, WorkerToRunner};
