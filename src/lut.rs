use failure::Error;
use std::collections::{BTreeMap, btree_map::Entry};
use git2::{ObjectType, Oid, Repository, Revwalk, Tree};
use indicatif::{MultiProgress, ProgressBar};
use Options;
use num_cpus;
use git2;
use crossbeam;
use fixedbitset::FixedBitSet;

const COMMIT_PROGRESS_RATE: usize = 100;

pub type CommitBlobMasks = Vec<(Oid, FixedBitSet)>;

pub fn build(_blobs: &Vec<Oid>, opts: Options) -> Result<CommitBlobMasks, Error> {
    let repo = Repository::open(&opts.repository)?;

    let commits: Vec<_> = {
        let mut walk = repo.revwalk()?;
        walk.set_sorting(git2::Sort::TOPOLOGICAL);
        setup_walk(&repo, &mut walk, opts.head_only)?;
        walk.filter_map(Result::ok).collect()
    };

    let multiprogress = MultiProgress::new();

    let mut results: Vec<_> = Vec::new();
    let num_threads = opts.threads.unwrap_or_else(num_cpus::get_physical);
    let mut total_refs = 0;

    crossbeam::scope(|scope| {
        let mut guards = Vec::with_capacity(num_threads);
        for (chunk_idx, chunk_of_commits) in commits.chunks(commits.len() / num_threads).enumerate()
        {
            let progress = multiprogress.add(ProgressBar::new_spinner());
            let repo =
                Repository::open(&opts.repository).expect("successful repository initialization");
            let mut mask = CommitBlobMasks::new();
            let mut seen = BTreeMap::<Oid, ()>::new();

            let guard = scope.spawn(move || {
                let (mut num_commits, mut total_refs) = (0, 0);
                for &commit_oid in chunk_of_commits {
                    num_commits += 1;
                    if let Ok(object) = repo.find_object(commit_oid, Some(ObjectType::Commit)) {
                        let commit = object.into_commit().expect("to have commit");
                        let tree = commit.tree().expect("commit to have tree");
                        if not_has_seen(tree.id(), &mut seen) {
                            total_refs += recurse_tree(&repo, tree, &mut seen);
                        }
                    }
                    if num_commits % COMMIT_PROGRESS_RATE == 0 {
                        progress.set_message(&format!(
                            "{} Commits done; traversed tree with {} vertices and a total of {} edges",
                            num_commits,
                            seen.len(),
                            total_refs
                        ));
                        progress.tick();
                    }
                }
                progress.finish_and_clear();
                (mask, total_refs, chunk_idx)
            });
            guards.push(guard);
        }
        multiprogress.join_and_clear().ok();
        for guard in guards {
            let (masks, edges, chunk_idx) = guard.join();
            results.push((chunk_idx, masks));
            total_refs += edges;
        }
    });

    results.sort_by_key(|(chunk_idx, _)| *chunk_idx);
    let mut all_masks = CommitBlobMasks::new();
    for mut mask in results.drain(..).map(|(_, r)| r) {
        all_masks.extend(mask.drain(..));
    }
    Ok(all_masks)
}

fn not_has_seen(child_oid: Oid, lut: &mut BTreeMap<Oid, ()>) -> bool {
    match lut.entry(child_oid) {
        Entry::Occupied(_) => false,
        Entry::Vacant(entry) => {
            entry.insert(());
            true
        }
    }
}

fn recurse_tree(repo: &Repository, tree: Tree, seen: &mut BTreeMap<Oid, ()>) -> usize {
    use ObjectType::*;
    let mut refs = 0;
    for item in tree.iter() {
        match item.kind() {
            Some(Tree) => {
                if not_has_seen(item.id(), seen) {
                    refs += recurse_tree(
                        repo,
                        item.to_object(repo)
                            .expect("valid object")
                            .into_tree()
                            .expect("tree"),
                        seen,
                    )
                }
            }
            Some(Blob) => {
                refs += 1;
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
