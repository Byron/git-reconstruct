extern crate failure;
extern crate failure_tools;
extern crate git2;
extern crate indicatif;
#[macro_use]
extern crate structopt;
extern crate crossbeam;
extern crate num_cpus;

use failure::Error;
use failure_tools::ok_or_exit;
use std::{collections::BTreeMap, io::{stdin, stdout, BufRead, BufReader, BufWriter, Write},
          path::PathBuf};
use git2::{ObjectType, Oid};
use structopt::StructOpt;

mod lut;

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

fn deplete_requests_from_stdin(luts: &Vec<BTreeMap<Oid, Capsule>>) -> Result<(), Error> {
    let stdin = stdin();
    let read = BufReader::new(stdin.lock());
    let stdout = stdout();
    let mut out = BufWriter::new(stdout.lock());
    let all_oids: Vec<Vec<_>> = luts.iter()
        .map(|lut| lut.keys().cloned().collect())
        .collect();
    eprintln!("Waiting for input...");
    for hexsha in read.lines().filter_map(Result::ok) {
        let oid = Oid::from_str(&hexsha)?;
        for (lid, lut) in luts.iter().enumerate() {
            match lut.get(&oid) {
                None => writeln!(out)?,
                Some(Capsule::Compact(parent_indices)) => {
                    let mut indices_to_traverse = parent_indices.clone();
                    while let Some(idx) = indices_to_traverse.pop() {
                        match lut.get(&all_oids[lid][idx]) {
                            Some(Capsule::Compact(parent_indices)) => {
                                if parent_indices.is_empty() {
                                    write!(out, "{} ", all_oids[lid][idx])?
                                } else {
                                    indices_to_traverse.extend(parent_indices)
                                }
                            }
                            Some(Capsule::Normal(_)) => {
                                unreachable!("LUT must be completely compacted in this branch")
                            }
                            None => unreachable!("Every item we see must be in the LUT"),
                        }
                    }
                }
                Some(Capsule::Normal(parent_oids)) => {
                    let mut oids_to_traverse = parent_oids.clone();
                    while let Some(oid) = oids_to_traverse.pop() {
                        match lut.get(&oid) {
                            Some(Capsule::Normal(parent_oids)) => {
                                if parent_oids.is_empty() {
                                    write!(out, "{} ", oid)?
                                } else {
                                    oids_to_traverse.extend(parent_oids)
                                }
                            }
                            Some(Capsule::Compact(_)) => {
                                unreachable!("LUT must be completely uncompacted in this branch")
                            }
                            None => unreachable!("Every item we see must be in the LUT"),
                        }
                    }
                }
            }
        }
        writeln!(out)?;
        out.flush()?;
    }
    Ok(())
}

fn run(opts: Options) -> Result<(), Error> {
    let luts = lut::build(opts)?;
    deplete_requests_from_stdin(&luts)
}

fn main() {
    let opts = Options::from_args();
    ok_or_exit(run(opts));
}
