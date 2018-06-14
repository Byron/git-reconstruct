use fixedbitset::FixedBitSet;
use failure::{Error, ResultExt};
use std::path::Path;
use walkdir::WalkDir;
use git2::ObjectType;
use indicatif::ProgressBar;
use Stack;
use lut::ReverseGraph;
use crossbeam_channel;
use crossbeam;
use num_cpus;
use git2::Oid;
use Options;

const HASHING_PROGRESS_RATE: usize = 25;

pub fn commit(tree: &Path, graph: ReverseGraph, opts: &Options) -> Result<(), Error> {
    let progress = ProgressBar::new_spinner();
    let mut blobs = Vec::new();
    for (eid, entry) in WalkDir::new(tree)
        .sort_by(|a, b| a.file_name().cmp(b.file_name()))
        .min_depth(1)
        .follow_links(false)
        .into_iter()
        .enumerate()
    {
        let entry = entry?;
        // TODO: assure symlinks are hashed correctly (must assure not follow it, which it does)
        if !entry.file_type().is_file() {
            continue;
        }
        blobs.push(Oid::hash_file(ObjectType::Blob, entry.path())
            .with_context(|_| format!("Could not hash file '{}'", entry.path().display()))?);
        if eid % HASHING_PROGRESS_RATE == 0 {
            progress.set_message(&format!("Hashed {} files...", eid));
            progress.tick();
        }
    }

    let mut commit_indices_to_blobs = vec![FixedBitSet::with_capacity(0); graph.len()];
    let num_threads = opts.threads.unwrap_or(num_cpus::get_physical());
    crossbeam::scope(|scope| {
        let or = {
            let (is, ir) = crossbeam_channel::bounded::<(usize, Oid)>(num_threads);
            let (os, or) = crossbeam_channel::bounded::<(usize, Vec<usize>)>(num_threads);
            let blobs = &blobs;
            scope.spawn(move || {
                for bid_and_blob in blobs.iter().cloned().enumerate() {
                    is.send(bid_and_blob);
                }
            });
            for _ in 0..num_threads {
                let graph = &graph;
                let ir = ir.clone();
                let os = os.clone();
                scope.spawn(move || {
                    let mut stack = Stack::default();
                    for (bid, blob) in ir {
                        let mut commits = Vec::new();
                        graph.lookup_idx(&blob, &mut stack, &mut commits);
                        os.send((bid, commits));
                    }
                });
            }
            or
        };

        let mut total_commits = 0;
        for (bid, commits) in or {
            for &commit_index in &commits {
                let bits = unsafe { commit_indices_to_blobs.get_unchecked_mut(commit_index) };
                if bits.len() == 0 {
                    bits.grow(blobs.len());
                }
                bits.put(bid);
            }
            total_commits += commits.len();
            progress.set_message(&format!(
                "{}/{}: Ticking blob bits, saw {} commits so far...",
                bid,
                blobs.len(),
                total_commits
            ));
            progress.tick();
        }
        progress.finish_and_clear();
        eprintln!(
            "Ticked {} blob bits in {} commits",
            blobs.len(),
            total_commits
        );
    });
    drop(graph);

    eprintln!("unimplemented");
    Ok(())
}
