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
#[macro_use]
extern crate serde_derive;
extern crate bincode;
extern crate crossbeam_channel;
extern crate lz4;
extern crate serde;

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

#[derive(Default)]
pub struct Stack {
    indices: Vec<usize>,
}

/// A basic example
#[derive(StructOpt, Debug)]
#[structopt(name = "git-reconstruct")]
pub struct Options {
    /// The amount of threads to use. If unset, defaults to amount of physical CPUs
    #[structopt(short = "t", long = "threads")]
    threads: Option<usize>,

    /// If set, traversal will only happen along the checked-out head.
    /// Otherwise it will take into consideration all remote branches, too
    /// Also useful for bare-repositories
    #[structopt(long = "head-only")]
    head_only: bool,

    /// The path at which to look for a graph cache. If a file exists at the given path,
    /// it will be loaded as graph cache.
    /// Otherwise a graph cache will be written out before proceeding as normal.
    /// Refresh the cache by deleting the file.
    #[structopt(name = "CACHE", long = "cache-path", parse(from_os_str))]
    cache_path: Option<PathBuf>,

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
    use std::path::Path;
    use walkdir::WalkDir;
    use git2::ObjectType;
    use indicatif::ProgressBar;
    use Stack;
    use lut::ReverseGraph;
    use crossbeam_channel;
    use crossbeam;
    use num_cpus;
    use git2::Oid;

    const HASHING_PROGRESS_RATE: usize = 25;

    pub fn commit(tree: &Path, graph: ReverseGraph) -> Result<(), Error> {
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

        let mut commit_indices_to_blobs = vec![FixedBitSet::with_capacity(0); graph.len()];
        let num_threads = num_cpus::get_physical();
        crossbeam::scope(|scope| {
            let or = {
                let (is, ir) = crossbeam_channel::bounded::<(usize, Oid)>(num_threads);
                let (os, or) = crossbeam_channel::bounded::<(usize, Vec<usize>)>(num_threads);
                let blobs = &blobs;
                scope.spawn(move || {
                    for bid_and_blob in blobs.iter().cloned().enumerate() {
                        is.send(bid_and_blob);
                    }
                });
                for _ in 0..num_threads {
                    let graph = &graph;
                    let ir = ir.clone();
                    let os = os.clone();
                    scope.spawn(move || {
                        let mut stack = Stack::default();
                        for (bid, blob) in ir {
                            let mut commits = Vec::new();
                            graph.lookup_idx(&blob, &mut stack, &mut commits);
                            os.send((bid, commits));
                        }
                    });
                }
                or
            };

            let mut total_commits = 0;
            for (bid, commits) in or {
                for &commit_index in &commits {
                    let bits = unsafe { commit_indices_to_blobs.get_unchecked_mut(commit_index) };
                    if bits.len() == 0 {
                        bits.grow(blobs.len());
                    }
                    bits.put(bid);
                }
                total_commits += commits.len();
                progress.set_message(&format!(
                    "{}/{}: Ticking blob bits, saw {} commits so far...",
                    bid,
                    blobs.len(),
                    total_commits
                ));
                progress.tick();
            }
            progress.finish_and_clear();
            eprintln!(
                "Ticked {} blob bits in {} commits",
                blobs.len(),
                total_commits
            );
        });
        drop(graph);

        eprintln!("unimplemented");
        Ok(())
    }
}
