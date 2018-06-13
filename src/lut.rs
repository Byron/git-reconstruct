use failure::Error;
use std::collections::{BTreeMap, btree_map::Entry};
use git2::{ObjectType, Oid, Repository, Revwalk, Tree};
use indicatif::ProgressBar;
use Stack;
use Options;
use git2;
use bincode::{deserialize_from, serialize_into};
use std::io;

const COMMIT_PROGRESS_RATE: usize = 100;
const COMPACTION_PROGRESS_RATE: usize = 10000;

#[derive(Default)]
pub struct ReverseGraph {
    vertices_to_oid: Vec<Oid>,
    vertices_to_edges: Vec<Vec<usize>>,
    oids_to_vertices: BTreeMap<Oid, usize>,
}

#[derive(Deserialize, Serialize)]
struct Sha1([u8; 20]);

impl From<Oid> for Sha1 {
    fn from(f: Oid) -> Self {
        let mut s = [0; 20];
        s.copy_from_slice(f.as_bytes());
        Sha1(s)
    }
}

impl From<Sha1> for Oid {
    fn from(s: Sha1) -> Self {
        Oid::from_bytes(&s.0).expect("sha1 to have just 20 bytes")
    }
}

#[derive(Default, Deserialize, Serialize)]
pub struct StorableReverseGraph {
    vertices_to_oid: Vec<Sha1>,
    vertices_to_edges: Vec<Vec<usize>>,
    oids_to_vertices: Vec<(Sha1, usize)>,
}

impl StorableReverseGraph {
    pub fn save(self, out: impl io::Write) -> Result<Self, Error> {
        eprintln!("Saving graph...");
        serialize_into(out, &self)?;
        Ok(self)
    }
    pub fn load(input: impl io::Read) -> Result<StorableReverseGraph, Error> {
        eprintln!("Loading graph...");
        deserialize_from(input).map_err(Into::into)
    }
    pub fn into_memory(self) -> ReverseGraph {
        ReverseGraph {
            vertices_to_oid: self.vertices_to_oid.into_iter().map(Into::into).collect(),
            vertices_to_edges: self.vertices_to_edges,
            oids_to_vertices: self.oids_to_vertices.into_iter().fold(
                BTreeMap::new(),
                |mut acc, (oid, vtx)| {
                    acc.insert(oid.into(), vtx);
                    acc
                },
            ),
        }
    }
}

impl ReverseGraph {
    pub fn into_storage(self) -> StorableReverseGraph {
        StorableReverseGraph {
            vertices_to_oid: self.vertices_to_oid.into_iter().map(Into::into).collect(),
            vertices_to_edges: self.vertices_to_edges,
            oids_to_vertices: self.oids_to_vertices
                .into_iter()
                .map(|(oid, vtx)| (oid.into(), vtx))
                .collect(),
        }
    }
    fn optimize_topology(&mut self, progress: &ProgressBar) -> Option<(usize, usize)> {
        let mut total_removed = 0;
        let mut last_pass = 0;
        for pass in 1.. {
            last_pass = pass;
            let edges_removed = self.optimize_topology_once();
            if edges_removed == 0 {
                break;
            }
            total_removed += edges_removed;
            progress.set_message(&format!("Pass {}: {} edges removed", pass, edges_removed));
            progress.tick();
        }
        if total_removed == 0 {
            None
        } else {
            Some((last_pass, total_removed))
        }
    }

    fn optimize_topology_once(&mut self) -> usize {
        let mut parents_to_adjust = Vec::new();

        for vtx in 0..self.len() {
            let edges = &self.vertices_to_edges[vtx];
            if edges.len() == 1 {
                let parent_vtx = edges[0];
                let parent_edges = &self.vertices_to_edges[parent_vtx];
                if parent_edges.len() < 2 {
                    parents_to_adjust.push((vtx, parent_vtx));
                }
            }
        }

        let removed = parents_to_adjust.len();
        for (child, parent_to_skip) in parents_to_adjust {
            let parent_edges = self.vertices_to_edges[parent_to_skip].clone();
            self.vertices_to_edges[child] = parent_edges;
        }

        removed
    }
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
    pub fn len(&self) -> usize {
        self.vertices_to_oid.len()
    }
    // TODO: dedup
    pub fn lookup_idx(&self, blob: &Oid, stack: &mut Stack, out: &mut Vec<usize>) {
        out.clear();
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
        out.clear();
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

pub fn build(opts: &Options) -> Result<ReverseGraph, Error> {
    let repo = Repository::open(&opts.repository)?;

    let mut walk = repo.revwalk()?;
    walk.set_sorting(git2::Sort::TOPOLOGICAL);
    setup_walk(&repo, &mut walk, opts.head_only)?;

    let progress = ProgressBar::new_spinner();
    let mut graph = ReverseGraph::default();
    let (mut num_commits, mut edges_total) = (0, 0);

    for commit_oid in walk.filter_map(Result::ok) {
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
    if let Some((passes, total_removed)) = graph.optimize_topology(&progress) {
        edges_total -= total_removed;
        eprintln!(
            "Removed {} unnecessary edges in {} passes",
            total_removed, passes
        );
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
