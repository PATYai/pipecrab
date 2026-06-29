//! The [`Stage`] trait: the async, effecting half of a pipeline stage.
//!
//! A stage is a [`Processor`] — synchronous, state-owning `decide_*` — plus an
//! async [`Stage::perform`] that interprets the effects `decide_*` emitted and
//! does the actual I/O. The split is the core invariant: `decide_*` takes
//! `&mut self` and is the *only* place state changes; `perform` takes `&self`
//! and must never mutate state, so the run loop can drop an in-flight `perform`
//! future on an interrupt without leaving torn state behind.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use pipecrab_core::Processor;

use crate::Outbound;

/// Why a [`Stage::perform`] call failed.
///
/// `perform` is the fallible, I/O-doing half of a stage. The run loop surfaces
/// a returned error as a `SystemFrame::Error` travelling upstream; `fatal`
/// decides whether the pipeline should tear down rather than carry on.
///
/// Mirrors the shape of `SystemFrame::Error` (a message plus a `fatal` flag) so
/// the conversion at the run-loop boundary is direct.
#[derive(Debug, Clone)]
pub struct StageError {
    /// Human-readable description of what went wrong.
    pub message: Arc<str>,
    /// Whether the failure is unrecoverable and the pipeline should shut down.
    pub fatal: bool,
}

impl StageError {
    /// A recoverable error: the pipeline may keep running.
    pub fn new(message: impl Into<Arc<str>>) -> Self {
        Self { message: message.into(), fatal: false }
    }

    /// An unrecoverable error: the pipeline should shut down.
    pub fn fatal(message: impl Into<Arc<str>>) -> Self {
        Self { message: message.into(), fatal: true }
    }
}

impl fmt::Display for StageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = if self.fatal { "fatal stage error" } else { "stage error" };
        write!(f, "{kind}: {}", self.message)
    }
}

impl std::error::Error for StageError {}

impl From<String> for StageError {
    fn from(message: String) -> Self {
        Self::new(message)
    }
}

impl From<&str> for StageError {
    fn from(message: &str) -> Self {
        Self::new(message)
    }
}

/// The async, effecting half of a pipeline stage.
///
/// `Stage` extends [`Processor`]: `decide_data` / `decide_system` (synchronous,
/// `&mut self`) own all state mutation and emit [`Effect`](Processor::Effect)
/// values; [`perform`](Stage::perform) interprets one effect, does its I/O, and
/// pushes any resulting frames through `out`.
///
/// # `?Send` is deliberate
///
/// pipecrab commits to a single-threaded execution model, so the returned
/// futures are **not** required to be `Send`. One `Stage` definition then runs
/// unchanged both on a tokio current-thread runtime and in the browser
/// (`wasm32`), where `Send` bounds are impossible to satisfy. CPU-bound or
/// blocking work must not run inline on the orchestrator thread — push it
/// off-thread with the `offload` helper and `await` the result, so an interrupt
/// can still preempt `perform` promptly.
///
/// The trait is dyn-compatible (via `async_trait`), so a pipeline can hold its
/// stages as `Box<dyn Stage<Effect = _>>`.
#[async_trait(?Send)]
pub trait Stage: Processor {
    /// Interpret one effect emitted by `decide_*` and carry out its I/O, sending
    /// any resulting frames through `out`.
    ///
    /// Takes `&self`: `perform` must not mutate stage state. The run loop races
    /// this future against the system lane, so a barge-in `Interrupt` can drop
    /// it mid-flight; because only `decide_*` ever mutated state, dropping the
    /// future leaves the stage intact. Barge-in is only as responsive as
    /// `perform` yields, so never block the thread inline — offload heavy work
    /// and `await` it.
    async fn perform(&self, effect: Self::Effect, out: &Outbound) -> Result<(), StageError>;
}
