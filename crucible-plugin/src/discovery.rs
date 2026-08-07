//! Finding the plugins installed on this machine.
//!
//! A plugin is an executable under one of the search directories. Which one it
//! is comes from asking it rather than from what it is called, so installing a
//! plugin is dropping a binary in place.

use std::{
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

use crate::{external::Plugin, protocol::Description};

/// Overrides the directories searched, as a colon-separated list.
const PATH_VAR: &str = "CRUCIBLE_PLUGIN_PATH";
/// Where a package installs a plugin.
const SYSTEM_DIR: &str = "/usr/lib/crucible/plugins";
/// The execute bits of a file's mode, for its owner, its group, and everyone
/// else. A file none of them are set on is not something to try to run.
const EXECUTABLE: u32 = 0o111;

/// A plugin, and where it was found.
pub struct Found {
    pub description: Description,
    pub path: PathBuf,
}

/// The directories searched, in the order they are searched.
#[must_use]
pub fn search_path() -> Vec<PathBuf> {
    if let Some(overridden) = std::env::var_os(PATH_VAR) {
        return std::env::split_paths(&overridden).collect();
    }
    let mut dirs = Vec::new();
    if let Some(data) = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/share")))
    {
        dirs.push(data.join("crucible/plugins"));
    }
    dirs.push(PathBuf::from(SYSTEM_DIR));
    dirs
}

/// Every plugin on the search path, in the order found. One that cannot say
/// what it is is left out with a warning: a plugin nothing uses should not stop
/// a campaign.
pub async fn discover() -> Vec<Found> {
    let mut found = Vec::new();
    for dir in search_path() {
        for path in executables_in(&dir).await {
            match Plugin::describe(path.clone()).await {
                Ok(description) => found.push(Found { description, path }),
                Err(e) => {
                    tracing::warn!(path = %path.display(), error = %e, "ignoring plugin");
                }
            }
        }
    }
    found
}

/// The executable files directly in `dir`, sorted so a machine finds its
/// plugins in the same order twice running. A directory that is not there is
/// not an error: most machines have none of these.
async fn executables_in(dir: &Path) -> Vec<PathBuf> {
    let Ok(mut entries) = tokio::fs::read_dir(dir).await else {
        return Vec::new();
    };
    let mut paths = Vec::new();
    while let Ok(Some(entry)) = entries.next_entry().await {
        let Ok(metadata) = entry.metadata().await else {
            continue;
        };
        if metadata.is_file() && metadata.permissions().mode() & EXECUTABLE != 0 {
            paths.push(entry.path());
        }
    }
    paths.sort();
    paths
}
