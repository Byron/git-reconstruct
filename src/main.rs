#[macro_use]
extern crate failure;
extern crate failure_tools;
extern crate git2;
extern crate indicatif;

use failure::{Error, ResultExt};
use failure_tools::ok_or_exit;
use std::collections::{BTreeMap, btree_map::Entry};
use std::io::{stdin, stdout, BufRead, BufReader, BufWriter, Write};
use git2::{Oid, Repository};

fn insert_parent_and_has_not_seen_child(
    parent_oid: Oid,
    child_oid: Oid,
    lut: &mut BTreeMap<Oid, Vec<Oid>>,
) -> bool {
    match lut.entry(child_oid) {
        Entry::Occupied(mut entry) => {
            entry.get_mut().push(parent_oid);
            false
        }
        Entry::Vacant(entry) => {
            entry.insert(vec![parent_oid]);
            true
        }
    }
}

fn recurse_tree(
    repo: &git2::Repository,
    tree: git2::Tree,
    lut: &mut BTreeMap<Oid, Vec<Oid>>,
) -> usize {
    use git2::ObjectType::*;
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
                lut.entry(item.id())
                    .or_insert_with(Vec::new)
                    .push(tree.id())
            }
            _ => continue,
        }
    }
    refs
}

fn build_lut(repo: &Repository) -> Result<BTreeMap<Oid, Vec<Oid>>, Error> {
    let mut walk = repo.revwalk()?;
    let mut total_refs = 0;
    walk.set_sorting(git2::Sort::TOPOLOGICAL);
    walk.push_head()?;
    let mut lut = BTreeMap::new();
    let mut num_commits = 0;
    let progress = indicatif::ProgressBar::new_spinner();
    progress.set_draw_target(indicatif::ProgressDrawTarget::stderr());
    for commit_oid in walk.filter_map(Result::ok) {
        num_commits += 1;
        if let Ok(object) = repo.find_object(commit_oid, Some(git2::ObjectType::Commit)) {
            let commit = object.into_commit().expect("to have commit");
            let tree = commit.tree().expect("commit to have tree");
            if insert_parent_and_has_not_seen_child(commit_oid, tree.id(), &mut lut) {
                total_refs += recurse_tree(&repo, tree, &mut lut);
            }
        }
        progress.set_message(&format!(
            "{} Commits done; reverse-tree with {} entries and a total of {} parent-edges",
            num_commits,
            lut.len(),
            total_refs
        ));
        progress.tick();
    }
    progress.finish_and_clear();
    eprintln!("Compacting memory...");
    for value in lut.values_mut() {
        value.shrink_to_fit();
    }
    eprintln!(
        "READY: Build reverse-tree from {} commits with table of {} entries and {} parent-edges",
        num_commits,
        lut.len(),
        total_refs
    );
    Ok(lut)
}

fn depelete_requests_from_stdin(lut: &BTreeMap<Oid, Vec<Oid>>) -> Result<(), Error> {
    let stdin = stdin();
    let read = BufReader::new(stdin.lock());
    let stdout = stdout();
    let mut out = BufWriter::new(stdout.lock());
    for hexsha in read.lines().filter_map(Result::ok) {
        let oid = Oid::from_str(&hexsha)?;
        match lut.get(&oid) {
            None => writeln!(out)?,
            Some(parents) => {
                let mut oids_to_traverse = parents.clone();
                while let Some(oid) = oids_to_traverse.pop() {
                    match lut.get(&oid) {
                        Some(parents) => oids_to_traverse.extend(parents),
                        None => write!(out, "{} ", oid)?,
                    }
                }
                writeln!(out)?
            }
        }
        out.flush()?;
    }
    Ok(())
}

fn run() -> Result<(), Error> {
    let repo = git2::Repository::open(std::env::args()
        .skip(1)
        .next()
        .ok_or_else(|| format_err!("USAGE: <me> <repository>"))?)?;

    let lut = build_lut(&repo)?;
    depelete_requests_from_stdin(&lut)
}

fn main() {
    ok_or_exit(run().with_context(|_| "Failed to count git objects"))
}
