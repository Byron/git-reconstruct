use failure::Error;
use std::collections::{BTreeMap, btree_map::Entry};
use git2::{ObjectType, Oid, Repository, Revwalk, Tree};
use indicatif::{MultiProgress, ProgressBar};
use Stack;
use Options;
use git2;

const COMMIT_PROGRESS_RATE: usize = 100;
const COMPACTION_PROGRESS_RATE: usize = 10000;

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
                unsafe { self.vertices_to_edges.get_unchecked_mut(*entry.get()) }.push(parent);
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
    // TODO: dedup
    pub fn _lookup_idx(&self, blob: &Oid, stack: &mut Stack, out: &mut Vec<usize>) {
        match self.oids_to_vertices.get(blob) {
            None => {}
            Some(idx) => {
                let indices_to_traverse = &mut stack.indices;
                indices_to_traverse.clear();
                indices_to_traverse.extend(unsafe { self.vertices_to_edges.get_unchecked(*idx) });
                while let Some(idx) = indices_to_traverse.pop() {
                    let parent_indices = unsafe { self.vertices_to_edges.get_unchecked(idx) };
                    if parent_indices.is_empty() {
                        out.push(idx);
                    } else {
                        indices_to_traverse.extend(parent_indices)
                    }
                }
            }
        }
    }
    pub fn lookup(&self, blob: &Oid, stack: &mut Stack, out: &mut Vec<Oid>) {
        match self.oids_to_vertices.get(blob) {
            None => {}
            Some(idx) => {
                let indices_to_traverse = &mut stack.indices;
                indices_to_traverse.clear();
                indices_to_traverse.extend(unsafe { self.vertices_to_edges.get_unchecked(*idx) });
                while let Some(idx) = indices_to_traverse.pop() {
                    let parent_indices = unsafe { self.vertices_to_edges.get_unchecked(idx) };
                    if parent_indices.is_empty() {
                        out.push(unsafe { *self.vertices_to_oid.get_unchecked(idx) });
                    } else {
                        indices_to_traverse.extend(parent_indices)
                    }
                }
            }
        }
    }
}

pub fn build(opts: Options) -> Result<ReverseGraph, Error> {
    let repo = Repository::open(&opts.repository)?;

    let commits: Vec<_> = {
        let mut walk = repo.revwalk()?;
        walk.set_sorting(git2::Sort::TOPOLOGICAL);
        setup_walk(&repo, &mut walk, opts.head_only)?;
        walk.filter_map(Result::ok).collect()
    };

    let multiprogress = MultiProgress::new();

    let mut graph = ReverseGraph::default();
    let (mut num_commits, mut edges_total) = (0, 0);

    let progress = multiprogress.add(ProgressBar::new_spinner());
    let repo = Repository::open(&opts.repository).expect("successful repository initialization");

    for commit_oid in commits {
        num_commits += 1;
        if let Ok(object) = repo.find_object(commit_oid, Some(ObjectType::Commit)) {
            let commit = object.into_commit().expect("to have commit");
            let tree = commit.tree().expect("commit to have tree");
            let commit_idx = graph.append(commit_oid);
            if let Some(tree_idx) = graph.insert_parent_get_new_child_id(commit_idx, tree.id()) {
                edges_total += recurse_tree(&repo, tree, tree_idx, &mut graph);
            }
        }
        if num_commits % COMMIT_PROGRESS_RATE == 0 {
            progress.set_message(&format!(
                "{} Commits done; reverse-tree with {} entries and a total of {} parent-edges",
                num_commits,
                graph.len(),
                edges_total
            ));
            progress.tick();
        }
    }
    graph.compact(&progress);
    progress.finish_and_clear();

    eprintln!(
        "READY: Build reverse-tree from {} commits with graph with {} vertices and {} parent-edges",
        num_commits,
        graph.len(),
        edges_total
    );
    Ok(graph)
}

fn recurse_tree(repo: &Repository, tree: Tree, tree_idx: usize, state: &mut ReverseGraph) -> usize {
    use ObjectType::*;
    let mut refs = 0;
    for item in tree.iter() {
        match item.kind() {
            Some(Tree) => {
                if let Some(item_idx) = state.insert_parent_get_new_child_id(tree_idx, item.id()) {
                    refs += recurse_tree(
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
