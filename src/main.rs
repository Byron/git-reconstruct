#[macro_use]
extern crate failure;
extern crate bit_vec;
extern crate failure_tools;
extern crate git2;
extern crate indicatif;

use failure::{Error, ResultExt};
use failure_tools::ok_or_exit;
use std::collections::BTreeMap;
use std::io::{stdin, stdout, BufRead, BufReader, BufWriter, Write};
use bit_vec::BitVec;

const COMMITS_PROGRESS_RESOLUTION: usize = 250;

fn recurse_tree(
    repo: &git2::Repository,
    commit_idx: usize,
    num_commits: usize,
    tree: git2::Tree,
    lut: &mut BTreeMap<git2::Oid, BitVec>,
) -> usize {
    use git2::ObjectType::*;
    let mut refs = 0;
    for item in tree.iter() {
        match item.kind() {
            Some(Tree) => {
                refs += recurse_tree(
                    repo,
                    commit_idx,
                    num_commits,
                    item.to_object(repo)
                        .expect("valid object")
                        .into_tree()
                        .expect("tree"),
                    lut,
                )
            }
            Some(Blob) => {
                refs += 1;
                lut.entry(item.id())
                    .or_insert_with(|| BitVec::from_elem(num_commits, false))
                    .set(commit_idx, true)
            }
            _ => continue,
        }
    }
    refs
}

fn run() -> Result<(), Error> {
    let repo = git2::Repository::open(std::env::args()
        .skip(1)
        .next()
        .ok_or_else(|| format_err!("USAGE: <me> <repository>"))?)?;
    let mut walk = repo.revwalk()?;
    let (mut iteration_count, mut total_refs) = (0, 0);
    walk.set_sorting(git2::Sort::TOPOLOGICAL);
    walk.push_head()?;

    let mut lut = BTreeMap::new();
    let mut commits = Vec::new();
    let progress = indicatif::ProgressBar::new_spinner();
    progress.set_draw_target(indicatif::ProgressDrawTarget::stderr());

    for oid in walk.filter_map(Result::ok) {
        iteration_count += 1;
        if iteration_count % COMMITS_PROGRESS_RESOLUTION == 0 {
            progress.set_message(&format!("Traversed {} commits...", iteration_count,));
            progress.tick();
        }
        commits.push(oid);
    }
    let num_commits = commits.len();
    progress.set_style(indicatif::ProgressStyle::default_bar());
    progress.set_length(num_commits as u64);
    for (cid, commit_oid) in commits.iter().enumerate() {
        if let Ok(object) = repo.find_object(*commit_oid, Some(git2::ObjectType::Commit)) {
            let commit = object.into_commit().expect("to have commit");
            let tree = commit.tree().expect("commit to have tree");
            total_refs += recurse_tree(&repo, cid, num_commits, tree, &mut lut);
        }
        progress.set_message(&format!(
            "Table with {} blobs and a total of {} back-refs",
            lut.len(),
            total_refs
        ));
        progress.set_position(cid as u64);
    }
    progress.finish_and_clear();
    eprintln!(
        "READY: Build cache from {} commits with table of {} blobs and {} refs",
        num_commits,
        lut.len(),
        total_refs
    );

    let stdin = stdin();
    let read = BufReader::new(stdin.lock());
    let stdout = stdout();
    let mut out = BufWriter::new(stdout.lock());

    for hexsha in read.lines().filter_map(Result::ok) {
        let oid = git2::Oid::from_str(&hexsha)?;
        match lut.get(&oid) {
            None => writeln!(out)?,
            Some(commits_indices) => {
                for cidx in commits_indices
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, seen)| if seen { Some(idx) } else { None })
                {
                    write!(out, "{} ", commits[cidx])?;
                }
                writeln!(out)?
            }
        }
        out.flush()?;
    }

    Ok(())
}

fn main() {
    ok_or_exit(run().with_context(|_| "Failed to count git objects"))
}
