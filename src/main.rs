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
use git2::{ObjectType, Oid};
use structopt::StructOpt;

mod lut;
mod cli;

fn main() {
    let opts = Options::from_args();
    ok_or_exit(cli::run(opts));
}

#[derive(Clone)]
pub enum Capsule {
    Normal(Vec<Oid>),
    Compact(Vec<usize>),
}

/// A basic example
#[derive(StructOpt, Debug)]
#[structopt(name = "git-commits-by-blob")]
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
    /// If unspecified, the program will serve as blob-to-commits lookup table,
    /// receiving hex-shas of blobs, one per line, on stdin and outputting
    /// all commits knowing that blob on stdout, separated by space, terminated
    /// by newline.
    #[structopt(name = "tree-to-integrate", parse(from_os_str))]
    tree: Option<PathBuf>,
}

mod find {
    use fixedbitset::FixedBitSet;
    use failure::{Error, ResultExt};
    use std::{collections::BTreeMap, path::Path};
    use git2::Oid;
    use lut::{self, MultiReverseCommitGraph};
    use walkdir::WalkDir;
    use git2::ObjectType;
    use indicatif::ProgressBar;

    const HASHING_PROGRESS_RATE: usize = 25;
    const BITMAP_PROGRESS_RATE: usize = 25;

    pub fn commit(tree: &Path, luts: MultiReverseCommitGraph) -> Result<(), Error> {
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

        // TODO PERFORMANCE: allow compacting memory so lookup only contains the tree reachable
        // by blobs. It looks like it's too intense to prune the existing map. Instead one should
        // rebuild a new lut, but that would generate a memory spike. Maybe less of a problem
        // if compaction ran before (so we have enough). It's also unclear if that will make
        // anything faster, and if not, those who have no memory anyway can't afford the spike
        // If there is a good way, it could be valuable, as 55k is way less than 1832k!
        // Given the numbers, the spike might not be that huge!
        let mut commit_to_blobs = BTreeMap::new();
        {
            let all_oids = lut::commit_oids_table(&luts);
            let mut commits = Vec::new();
            let mut total_commits = 0;
            for (bid, blob) in blobs.iter().enumerate() {
                commits.clear();
                lut::commits_by_blob(&blob, &luts, &all_oids, &mut commits);
                total_commits += commits.len();

                for commit in &commits {
                    commit_to_blobs
                        .entry(commit.clone())
                        .or_insert_with(|| FixedBitSet::with_capacity(blobs.len()))
                        .put(bid);
                }

                progress.set_message(&format!(
                    "{}/{}: Ticking blob bits, saw {} commits so far...",
                    bid,
                    blobs.len(),
                    total_commits
                ));
                progress.tick();
            }
            drop(luts);
        }
        progress.finish_and_clear();
        unimplemented!();
    }
}
