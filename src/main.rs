#[macro_use]
extern crate failure;
extern crate failure_tools;
extern crate git2;
extern crate indicatif;

use failure::{Error, ResultExt};
use failure_tools::ok_or_exit;

const PROGRESS_RESOLUTION: usize = 250;

fn run() -> Result<(), Error> {
    let repo = git2::Repository::open(std::env::args()
        .skip(1)
        .next()
        .ok_or_else(|| format_err!("USAGE: <me> <repository>"))?)?;
    let mut walk = repo.revwalk()?;
    let mut num_commits = 0;
    walk.set_sorting(git2::Sort::TOPOLOGICAL);
    walk.push_head()?;

    let progress = indicatif::ProgressBar::new_spinner();
    progress.set_draw_target(indicatif::ProgressDrawTarget::stderr());

    for oid in walk.filter_map(Result::ok) {
        num_commits += 1;
        if num_commits % PROGRESS_RESOLUTION == 0 {
            progress.set_message(&format!("Counted {} objects...", num_commits));
            progress.tick();
        }
    }
    progress.finish_and_clear();
    eprintln!("READY: Build cache with {} commits", num_commits);

    Ok(())
}

fn main() {
    ok_or_exit(run().with_context(|_| "Failed to count git objects"))
}
