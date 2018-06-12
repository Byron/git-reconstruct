extern crate failure;
extern crate failure_tools;
extern crate git2;
extern crate indicatif;
#[macro_use]
extern crate structopt;
extern crate crossbeam;
extern crate fixedbitset;
extern crate num_cpus;
extern crate walkdir;

use failure_tools::ok_or_exit;
use std::path::PathBuf;
use git2::ObjectType;
use structopt::StructOpt;

mod lut;
mod cli;

fn main() {
    let opts = Options::from_args();
    ok_or_exit(cli::run(opts));
}

/// A basic example
#[derive(StructOpt, Debug)]
#[structopt(name = "git-reconstruct")]
pub struct Options {
    /// The amount of threads to use. If unset, defaults to amount of physical CPUs
    #[structopt(short = "t", long = "threads")]
    threads: Option<usize>,

    /// If set, you will trade in about 35% increase in memory for about 30% less time till ready
    /// for queries
    #[structopt(long = "no-compact")]
    no_compact: bool,

    /// If set, traversal will only happen along the checked-out head.
    /// Otherwise it will take into consideration all remote branches, too
    /// Also useful for bare-repositories
    #[structopt(long = "head-only")]
    head_only: bool,

    /// the repository to index for queries
    #[structopt(name = "REPOSITORY", parse(from_os_str))]
    repository: PathBuf,

    /// The directory tree for which to figure out the merge commit.
    #[structopt(name = "tree-to-integrate", parse(from_os_str))]
    tree: PathBuf,
}

mod find {
    use failure::{Error, ResultExt};
    use std::path::Path;
    use git2::Oid;
    use walkdir::WalkDir;
    use git2::ObjectType;
    use indicatif::ProgressBar;

    const HASHING_PROGRESS_RATE: usize = 25;

    pub fn generate_blob_hashes(tree: &Path) -> Result<Vec<Oid>, Error> {
        let progress = ProgressBar::new_spinner();
        let mut blobs = Vec::new();
        for (eid, entry) in WalkDir::new(tree)
            .sort_by(|a, b| a.file_name().cmp(b.file_name()))
            .min_depth(1)
            .follow_links(false)
            .into_iter()
            .enumerate()
        {
            let entry = entry?;
            // TODO: assure symlinks are hashed correctly (must assure not follow it, which it does)
            if !entry.file_type().is_file() {
                continue;
            }
            blobs.push(Oid::hash_file(ObjectType::Blob, entry.path())
                .with_context(|_| format!("Could not hash file '{}'", entry.path().display()))?);
            if eid % HASHING_PROGRESS_RATE == 0 {
                progress.set_message(&format!("Hashed {} files...", eid));
                progress.tick();
            }
        }
        progress.finish_and_clear();
        Ok(blobs)
    }
}
