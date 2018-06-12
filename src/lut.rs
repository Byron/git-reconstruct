use failure::Error;
use std::{mem, collections::{BTreeMap, btree_map::Entry}};
use git2::{ObjectType, Oid, Repository, Revwalk, Tree};
use indicatif::{MultiProgress, ProgressBar};
use Capsule;
use Options;
use num_cpus;
use git2;
use crossbeam;

const COMMIT_PROGRESS_RATE: usize = 100;
const COMPACTION_PROGRESS_RATE: usize = 10000;

pub type MultiReverseCommitGraph = Vec<BTreeMap<Oid, Capsule>>;

pub fn commit_oids_table(luts: &MultiReverseCommitGraph) -> Vec<Vec<Oid>> {
    luts.iter()
        .map(|lut| lut.keys().cloned().collect())
        .collect()
}

pub fn commits_by_blob(
    blob: &Oid,
    luts: &MultiReverseCommitGraph,
    all_oids: &Vec<Vec<Oid>>,
    out: &mut Vec<Oid>,
) {
    for (lid, lut) in luts.iter().enumerate() {
        match lut.get(&blob) {
            None => {}
            Some(Capsule::Compact(parent_indices)) => {
                let mut indices_to_traverse = parent_indices.clone();
                while let Some(idx) = indices_to_traverse.pop() {
                    match lut.get(&all_oids[lid][idx]) {
                        Some(Capsule::Compact(parent_indices)) => {
                            if parent_indices.is_empty() {
                                out.push(all_oids[lid][idx]);
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
                                out.push(oid)
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
}

pub fn build(opts: Options) -> Result<MultiReverseCommitGraph, Error> {
    let repo = Repository::open(&opts.repository)?;

    let commits: Vec<_> = {
        let mut walk = repo.revwalk()?;
        walk.set_sorting(git2::Sort::TOPOLOGICAL);
        setup_walk(&repo, &mut walk, opts.head_only)?;
        walk.filter_map(Result::ok).collect()
    };

    let multiprogress = MultiProgress::new();

    let mut luts: Vec<_> = Vec::new();
    let num_threads = opts.threads.unwrap_or_else(num_cpus::get_physical);
    let mut total_refs = 0;

    crossbeam::scope(|scope| {
        let mut guards = Vec::with_capacity(num_threads);
        for (chunk_idx, chunk_of_commits) in commits.chunks(commits.len() / num_threads).enumerate()
        {
            let progress = multiprogress.add(ProgressBar::new_spinner());
            let repo =
                Repository::open(&opts.repository).expect("successful repository initialization");
            let no_compact = opts.no_compact;
            let mut lut = BTreeMap::new();

            let guard = scope.spawn(move || {
                let (mut num_commits, mut total_refs) = (0, 0);
                for &commit_oid in chunk_of_commits {
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
                }
                if !no_compact {
                    compact_memory(&mut lut, &progress);
                } else {
                    eprintln!("INFO: Not compacting memory will safe about 1/3 of used time, at the cost of about 35% more memory")
                }
                progress.finish_and_clear();
                (lut, total_refs, chunk_idx)
            });
            guards.push(guard);
        }
        multiprogress.join_and_clear().ok();
        for guard in guards {
            let (lut, edges, chunk_idx) = guard.join();
            luts.push((chunk_idx, lut));
            total_refs += edges;
        }
    });

    luts.sort_by_key(|(chunk_idx, _)| *chunk_idx);
    let luts: Vec<_> = luts.drain(..).map(|(_, lut)| lut).collect();

    eprintln!(
        "READY: Build reverse-tree from {} commits with table of {} entries and {} parent-edges",
        commits.len(),
        luts.iter().map(|l| l.len()).sum::<usize>(),
        total_refs
    );
    Ok(luts)
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
            eprintln!("Didn't find a single remote - using head instead to avoid empty traversal");
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
