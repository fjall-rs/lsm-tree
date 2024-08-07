use path_absolutize::Absolutize;
use std::path::{Path, PathBuf};

#[allow(clippy::module_name_repetitions)]
pub fn absolute_path<P: AsRef<Path>>(path: P) -> PathBuf {
    // TODO: std::path::absolute 1.79
    path.as_ref()
        .absolutize()
        .expect("should be absolute path")
        .into()
}
