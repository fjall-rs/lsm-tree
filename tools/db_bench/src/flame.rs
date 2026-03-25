//! Flamegraph instrumentation (compiled only with the `flamegraph` feature).
//!
//! Sets up a global [`tracing`] subscriber backed by [`tracing_flame::FlameLayer`].
//! All tracing spans emitted during the benchmark run are collected into a single
//! folded-stacks file (`all.folded`) and rendered into an SVG by `inferno-flamegraph`.

use std::path::{Path, PathBuf};
use tracing_flame::FlameLayer;
use tracing_subscriber::prelude::*;

/// RAII guard — dropping it flushes the folded-stacks output.
pub struct FlameGuard {
    _guard: tracing_flame::FlushGuard<std::io::BufWriter<std::fs::File>>,
    output_dir: PathBuf,
}

impl FlameGuard {
    /// Directory where `.folded` and `.svg` files are written.
    pub fn output_dir(&self) -> &Path {
        &self.output_dir
    }
}

/// Initialise the global tracing subscriber with a `FlameLayer`.
///
/// All spans are written to `{output_dir}/all.folded`.
/// Thread IDs are collapsed for cleaner flamegraphs.
/// The returned guard **must** be held until all workloads have finished;
/// dropping it flushes the output file.
pub fn setup(output_dir: &Path) -> Result<FlameGuard, Box<dyn std::error::Error>> {
    std::fs::create_dir_all(output_dir)?;

    let folded_path = output_dir.join("all.folded");
    let (flame_layer, guard) = FlameLayer::with_file(&folded_path)?;
    let flame_layer = flame_layer.with_threads_collapsed(true);

    tracing_subscriber::registry()
        .with(flame_layer)
        .try_init()
        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;

    Ok(FlameGuard {
        _guard: guard,
        output_dir: output_dir.to_path_buf(),
    })
}
