extern crate failure;
extern crate failure_tools;
extern crate git2;
extern crate indicatif;
#[macro_use]
extern crate structopt;
extern crate bv;
extern crate crossbeam;
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
mod find;

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
