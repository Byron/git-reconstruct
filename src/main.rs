#[macro_use]
extern crate failure;
extern crate failure_tools;
extern crate git2;
extern crate indicatif;

use failure::{Error, ResultExt};
use failure_tools::ok_or_exit;
use std::collections::BTreeMap;
use std::io::{stdin, stdout, BufRead, BufReader, BufWriter, Write};

const PROGRESS_RESOLUTION: usize = 10;

fn recurse_tree(
    repo: &git2::Repository,
    commit_oid: &git2::Oid,
    tree: git2::Tree,
    lut: &mut BTreeMap<git2::Oid, Vec<git2::Oid>>,
) {
    use git2::ObjectType::*;
    for item in tree.iter() {
        match item.kind() {
            Some(Tree) => recurse_tree(
                repo,
                commit_oid,
                item.to_object(repo)
                    .expect("valid object")
                    .into_tree()
                    .expect("tree"),
                lut,
            ),
            Some(Blob) => lut.entry(item.id())
                .or_insert_with(|| Vec::new())
                .push(commit_oid.clone()),
            _ => continue,
        }
    }
}

fn run() -> Result<(), Error> {
    let repo = git2::Repository::open(std::env::args()
        .skip(1)
        .next()
        .ok_or_else(|| format_err!("USAGE: <me> <repository>"))?)?;
    let mut walk = repo.revwalk()?;
    let mut num_commits = 0;
    walk.set_sorting(git2::Sort::TOPOLOGICAL);
    walk.push_head()?;

    let mut lut = BTreeMap::<git2::Oid, Vec<git2::Oid>>::new();
    let progress = indicatif::ProgressBar::new_spinner();
    progress.set_draw_target(indicatif::ProgressDrawTarget::stderr());

    for oid in walk.filter_map(Result::ok) {
        num_commits += 1;
        if num_commits % PROGRESS_RESOLUTION == 0 {
            progress.set_message(&format!("Indexed {} commits...", num_commits));
            progress.tick();
        }
        if let Ok(object) = repo.find_object(oid, Some(git2::ObjectType::Commit)) {
            let commit = object.into_commit().expect("to have commit");
            let tree = commit.tree().expect("commit to have tree");
            recurse_tree(&repo, &oid, tree, &mut lut);
        }
    }
    progress.finish_and_clear();
    eprintln!(
        "READY: Build cache from {} commits with table of {} blobs",
        num_commits,
        lut.len()
    );

    let stdin = stdin();
    let read = BufReader::new(stdin.lock());
    let stdout = stdout();
    let mut out = BufWriter::new(stdout.lock());

    for hexsha in read.lines().filter_map(Result::ok) {
        let oid = git2::Oid::from_str(&hexsha)?;
        match lut.get(&oid) {
            None => writeln!(out)?,
            Some(commits) => {
                for oid in commits {
                    write!(out, "{} ", oid)?;
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
