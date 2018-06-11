extern crate failure;
extern crate failure_tools;
extern crate git2;
extern crate indicatif;
#[macro_use]
extern crate structopt;
extern crate rayon;

use rayon::prelude::*;
use failure::{Error, ResultExt};
use failure_tools::ok_or_exit;
use std::{mem, collections::{BTreeMap, btree_map::Entry},
          io::{stdin, stdout, BufRead, BufReader, BufWriter, Write}, path::PathBuf};
use git2::{ObjectType, Oid, Repository, Revwalk, Tree};
use indicatif::ProgressBar;
use structopt::StructOpt;

const COMMIT_PROGRESS_RATE: usize = 100;
const COMPACTION_PROGRESS_RATE: usize = 10000;

/// A basic example
#[derive(StructOpt, Debug)]
#[structopt(name = "git-commits-by-blob")]
struct Options {
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
}

#[derive(Clone)]
enum Capsule {
    Normal(Vec<Oid>),
    Compact(Vec<usize>),
}

fn insert_parent_and_has_not_seen_child(
    parent_oid: Oid,
    child_oid: Oid,
    lut: &mut BTreeMap<Oid, Capsule>,
) -> bool {
    match lut.entry(child_oid) {
        Entry::Occupied(mut entry) => {
            if let Capsule::Normal(ref mut parents) = entry.get_mut() {
                parents.push(parent_oid);
            }
            false
        }
        Entry::Vacant(entry) => {
            entry.insert(Capsule::Normal(vec![parent_oid]));
            true
        }
    }
}

fn recurse_tree(repo: &Repository, tree: Tree, lut: &mut BTreeMap<Oid, Capsule>) -> usize {
    use ObjectType::*;
    let mut refs = 0;
    for item in tree.iter() {
        match item.kind() {
            Some(Tree) => {
                if insert_parent_and_has_not_seen_child(tree.id(), item.id(), lut) {
                    refs += recurse_tree(
                        repo,
                        item.to_object(repo)
                            .expect("valid object")
                            .into_tree()
                            .expect("tree"),
                        lut,
                    )
                }
            }
            Some(Blob) => {
                refs += 1;
                if let Capsule::Normal(ref mut parents) = lut.entry(item.id())
                    .or_insert_with(|| Capsule::Normal(Vec::new()))
                {
                    parents.push(tree.id());
                }
            }
            _ => continue,
        }
    }
    refs
}

fn build_lut(opts: Options) -> Result<Vec<BTreeMap<Oid, Capsule>>, Error> {
    let mut total_refs = 0;
    let repo = Repository::open(opts.repository)?;

    let commits: Vec<_> = {
        let mut walk = repo.revwalk()?;
        walk.set_sorting(git2::Sort::TOPOLOGICAL);
        setup_walk(&repo, &mut walk, opts.head_only)?;
        walk.filter_map(Result::ok).collect()
    };

    let mut num_commits = 0;
    let progress = ProgressBar::new_spinner();
    progress.set_draw_target(indicatif::ProgressDrawTarget::stderr());
    let mut lut: Vec<_> = commits
        .par_iter()
        .fold(
            || BTreeMap::new(),
            |mut lut, &commit_oid| {
                num_commits += 1;
                if let Ok(object) = repo.find_object(commit_oid, Some(ObjectType::Commit)) {
                    let commit = object.into_commit().expect("to have commit");
                    let tree = commit.tree().expect("commit to have tree");
                    lut.insert(commit_oid, Capsule::Normal(Vec::new()));
                    if insert_parent_and_has_not_seen_child(commit_oid, tree.id(), &mut lut) {
                        total_refs += recurse_tree(&repo, tree, &mut lut);
                    }
                }
                if num_commits % COMMIT_PROGRESS_RATE == 0 {
                    progress.set_message(&format!(
                    "{} Commits done; reverse-tree with {} entries and a total of {} parent-edges",
                    num_commits,
                    lut.len(),
                    total_refs
                ));
                    progress.tick();
                }
                lut
            },
        )
        .collect();

    //    if !opts.no_compact {
    //        compact_memory(&mut lut, &progress);
    //    } else {
    //        eprintln!("INFO: Not compacting memory will safe about 1/3 of used time, at the cost of about 35% more memory")
    //    }

    progress.finish_and_clear();
    eprintln!(
        "READY: Build reverse-tree from {} commits with table of {} entries and {} parent-edges",
        num_commits,
        lut.len(),
        total_refs
    );
    Ok(lut)
}

fn setup_walk(repo: &Repository, walk: &mut Revwalk, head_only: bool) -> Result<(), Error> {
    if head_only {
        walk.push_head()?;
    } else {
        let mut refs_pushed = 0;
        for remote_head in repo.branches(Some(git2::BranchType::Remote))?
            .filter_map(|b| b.map(|(b, _bt)| b).ok().and_then(|b| b.get().target()))
        {
            walk.push(remote_head)?;
            refs_pushed += 1;
        }
        if refs_pushed == 0 {
            eprintln!(
                "Didn't find a single remote - pushing head instead to avoid empty traversal"
            );
            walk.push_head()?;
        }
    }
    Ok(())
}

fn compact_memory(lut: &mut BTreeMap<Oid, Capsule>, progress: &ProgressBar) -> () {
    let all_oids: Vec<_> = lut.keys().cloned().collect();
    for (cid, capsule) in lut.values_mut().enumerate() {
        let mut compacted = Vec::new();
        if let Capsule::Normal(ref mut parent_oids) = capsule {
            compacted = Vec::with_capacity(parent_oids.len());
            for oid in parent_oids {
                let parent_idx = all_oids
                    .binary_search(oid)
                    .expect("parent to be found in sorted list");
                compacted.push(parent_idx);
            }
        }
        mem::replace(capsule, Capsule::Compact(compacted));
        if cid % COMPACTION_PROGRESS_RATE == 0 {
            progress.set_message(&format!("Compacted {} of {} edges...", cid, all_oids.len(),));
            progress.tick();
        }
    }
}

fn deplete_requests_from_stdin(lut: &BTreeMap<Oid, Capsule>) -> Result<(), Error> {
    let stdin = stdin();
    let read = BufReader::new(stdin.lock());
    let stdout = stdout();
    let mut out = BufWriter::new(stdout.lock());
    let all_oids: Vec<_> = lut.keys().cloned().collect();
    eprintln!("Waiting for input...");
    for hexsha in read.lines().filter_map(Result::ok) {
        let oid = Oid::from_str(&hexsha)?;
        match lut.get(&oid) {
            None => writeln!(out)?,
            Some(Capsule::Compact(parent_indices)) => {
                let mut indices_to_traverse = parent_indices.clone();
                while let Some(idx) = indices_to_traverse.pop() {
                    match lut.get(&all_oids[idx]) {
                        Some(Capsule::Compact(parent_indices)) => {
                            if parent_indices.is_empty() {
                                write!(out, "{} ", all_oids[idx])?
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
                writeln!(out)?
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
                writeln!(out)?
            }
        }
        out.flush()?;
    }
    Ok(())
}

fn run(opts: Options) -> Result<(), Error> {
    let lut = build_lut(opts)?;
    deplete_requests_from_stdin(&lut)
}

fn main() {
    ok_or_exit(run(Options::from_args()).with_context(|_| "Failed to count git objects"))
}
