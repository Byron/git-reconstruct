use failure::Error;
use std::{mem, collections::{BTreeMap, btree_map::Entry}};
use git2::{ObjectType, Oid, Repository, Revwalk, Tree};
use indicatif::{MultiProgress, ProgressBar};
use {Capsule, Stack};
use Options;
use num_cpus;
use git2;
use crossbeam;

const COMMIT_PROGRESS_RATE: usize = 100;
const COMPACTION_PROGRESS_RATE: usize = 10000;

pub type MultiReverseCommitGraph = Vec<BTreeMap<Oid, Capsule>>;
#[derive(Default)]
pub struct ReverseGraph {
    vertices_to_oid: Vec<Oid>,
    vertices_to_edges: Vec<Vec<usize>>,
    oids_to_vertices: BTreeMap<Oid, usize>,
}

impl ReverseGraph {
    fn compact(&mut self, progress: &ProgressBar) {
        let own_len = self.vertices_to_edges.len();
        for (eid, mut edges) in &mut self.vertices_to_edges.iter_mut().enumerate() {
            edges.shrink_to_fit();
            if eid % COMPACTION_PROGRESS_RATE == 0 {
                progress.set_message(&format!("Compacted {} of {} edges...", eid, own_len,));
                progress.tick();
            }
        }
    }
    fn append(&mut self, oid: Oid) -> usize {
        let idx = self.vertices_to_oid.len();
        self.vertices_to_oid.push(oid.clone());
        self.oids_to_vertices.insert(oid, idx);
        self.vertices_to_edges.push(Vec::new());
        idx
    }
    fn insert_parent_get_new_child_id(&mut self, parent: usize, child: Oid) -> Option<usize> {
        match self.oids_to_vertices.entry(child) {
            Entry::Occupied(entry) => {
                self.vertices_to_edges[*entry.get()].push(parent);
                None
            }
            Entry::Vacant(entry) => {
                let child_idx = self.vertices_to_oid.len();
                self.vertices_to_oid.push(entry.key().clone());
                entry.insert(child_idx);
                self.vertices_to_edges.push(vec![parent]);
                Some(child_idx)
            }
        }
    }
    fn len(&self) -> usize {
        self.vertices_to_oid.len()
    }
}

pub fn commit_oids_table(luts: &MultiReverseCommitGraph) -> Vec<Vec<Oid>> {
    luts.iter()
        .map(|lut| lut.keys().cloned().collect())
        .collect()
}

pub fn commits_by_blob(
    blob: &Oid,
    luts: &MultiReverseCommitGraph,
    all_oids: &Vec<Vec<Oid>>,
    stack: &mut Stack,
    out: &mut Vec<Oid>,
) {
    out.clear();
    for (lut, all_oids) in luts.iter().zip(all_oids) {
        lookup_oid(&blob, lut, all_oids, stack, out)
    }
}

fn lookup_oid(
    blob: &Oid,
    lut: &BTreeMap<Oid, Capsule>,
    all_oids: &Vec<Oid>,
    stack: &mut Stack,
    out: &mut Vec<Oid>,
) -> () {
    match lut.get(blob) {
        None => {}
        Some(Capsule::Compact(parent_indices)) => {
            let indices_to_traverse = &mut stack.indices;
            indices_to_traverse.clear();
            indices_to_traverse.extend(parent_indices);
            while let Some(idx) = indices_to_traverse.pop() {
                match lut.get(&all_oids[idx]) {
                    Some(Capsule::Compact(parent_indices)) => {
                        if parent_indices.is_empty() {
                            out.push(all_oids[idx]);
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
            let oids_to_traverse = &mut stack.oids;
            oids_to_traverse.clear();
            oids_to_traverse.extend(parent_oids);
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

pub fn build2(opts: Options) -> Result<Vec<ReverseGraph>, Error> {
    let repo = Repository::open(&opts.repository)?;

    let commits: Vec<_> = {
        let mut walk = repo.revwalk()?;
        walk.set_sorting(git2::Sort::TOPOLOGICAL);
        setup_walk(&repo, &mut walk, opts.head_only)?;
        walk.filter_map(Result::ok).collect()
    };

    let multiprogress = MultiProgress::new();

    let num_threads = opts.threads.unwrap_or_else(num_cpus::get_physical);
    let mut graphs = Vec::new();
    let mut edges_total = 0;

    crossbeam::scope(|scope| {
        let mut guards = Vec::with_capacity(num_threads);
        for (chunk_idx, chunk_of_commits) in commits.chunks(commits.len() / num_threads).enumerate()
        {
            let progress = multiprogress.add(ProgressBar::new_spinner());
            let repo =
                Repository::open(&opts.repository).expect("successful repository initialization");
            let compact = !opts.no_compact;
            let mut state = ReverseGraph::default();

            let guard = scope.spawn(move || {
                let (mut num_commits, mut num_edges) = (0, 0);
                for &commit_oid in chunk_of_commits {
                    num_commits += 1;
                    if let Ok(object) = repo.find_object(commit_oid, Some(ObjectType::Commit)) {
                        let commit = object.into_commit().expect("to have commit");
                        let tree = commit.tree().expect("commit to have tree");
                        let commit_idx = state.append(commit_oid);
                        if let Some(tree_idx) =
                            state.insert_parent_get_new_child_id(commit_idx, tree.id())
                        {
                            num_edges += recurse_tree2(&repo, tree, tree_idx, &mut state);
                        }
                    }
                    if num_commits % COMMIT_PROGRESS_RATE == 0 {
                        progress.set_message(&format!(
                                "{} Commits done; reverse-tree with {} entries and a total of {} parent-edges",
                                num_commits,
                                state.len(),
                                num_edges
                            ));
                        progress.tick();
                    }
                }
                if compact {
                    state.compact(&progress);
                } else {
                    eprintln!("INFO: Not compacting memory will safe about 1/3 of used time, at the cost of about 35% more memory")
                }
                progress.finish_and_clear();
                (state, num_edges, chunk_idx)
            });
            guards.push(guard);
        }
        multiprogress.join_and_clear().ok();
        for guard in guards {
            let (state, edges, chunk_idx) = guard.join();
            graphs.push((chunk_idx, state));
            edges_total += edges;
        }
    });

    graphs.sort_by_key(|(chunk_idx, _)| *chunk_idx);
    let graphs: Vec<_> = graphs.drain(..).map(|(_, lut)| lut).collect();

    eprintln!(
        "READY: Build reverse-tree from {} commits with table of {} entries and {} parent-edges",
        commits.len(),
        graphs.iter().map(|s| s.len()).sum::<usize>(),
        edges_total
    );
    Ok(graphs)
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
            let compact = !opts.no_compact;
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
                if compact {
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

fn recurse_tree2(
    repo: &Repository,
    tree: Tree,
    tree_idx: usize,
    state: &mut ReverseGraph,
) -> usize {
    use ObjectType::*;
    let mut refs = 0;
    for item in tree.iter() {
        match item.kind() {
            Some(Tree) => {
                if let Some(item_idx) = state.insert_parent_get_new_child_id(tree_idx, item.id()) {
                    refs += recurse_tree2(
                        repo,
                        item.to_object(repo)
                            .expect("valid object")
                            .into_tree()
                            .expect("tree"),
                        item_idx,
                        state,
                    )
                }
            }
            Some(Blob) => {
                refs += 1;
                state.insert_parent_get_new_child_id(tree_idx, item.id());
            }
            _ => continue,
        }
    }
    refs
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
