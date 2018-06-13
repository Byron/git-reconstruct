use failure::Error;
use lut;
use std::{fs::{metadata, File, OpenOptions},
          io::{stdin, stdout, BufRead, BufReader, BufWriter, Write}};
use git2::Oid;
use {Options, Stack};
use find;
use indicatif::ProgressBar;
use lut::{ReverseGraph, StorableReverseGraph};
use lz4;

const PROGRESS_RATE: usize = 25;

fn deplete_requests_from_stdin(graph: ReverseGraph) -> Result<(), Error> {
    let mut commits = Vec::new();

    let stdin = stdin();
    let stdout = stdout();

    let read = BufReader::new(stdin.lock());
    let mut out = stdout.lock();
    let mut obuf = String::new();
    let progress = ProgressBar::new_spinner();

    eprintln!("Waiting for input...");
    let mut total_commits = 0;
    let mut num_blobs = 0;
    let mut stack = Stack::default();
    for hexsha in read.lines().filter_map(Result::ok) {
        num_blobs += 1;
        let oid = Oid::from_str(&hexsha)?;

        graph.lookup(&oid, &mut stack, &mut commits);
        total_commits += commits.len();

        obuf.clear();
        let len = commits.len();
        for (cid, commit_oid) in commits.iter().enumerate() {
            use std::fmt::Write;
            write!(obuf, "{}", commit_oid)?;
            if cid + 1 < len {
                obuf.push(' ');
            }
        }
        obuf.push('\n');

        write!(out, "{}", obuf)?;
        out.flush()?;

        if num_blobs % PROGRESS_RATE == 0 {
            progress.set_message(&format!(
                "Looked up {} blobs with a total of {} commits",
                num_blobs, total_commits
            ));
            progress.tick();
        }
    }
    eprintln!(
        "DONE: Looked up {} blobs with a total of {} commits",
        num_blobs, total_commits
    );
    progress.finish_and_clear();
    Ok(())
}

pub fn run(opts: Options) -> Result<(), Error> {
    let tree = opts.tree.clone();
    let graph = match &opts.cache_path {
        Some(cache_path) => {
            if metadata(cache_path).is_ok() {
                StorableReverseGraph::load(lz4::Decoder::new(BufReader::new(File::open(
                    &cache_path,
                )?))?)?.into_memory()
            } else {
                lut::build(&opts)?
                    .into_storage()
                    .save(
                        lz4::EncoderBuilder::new().build(BufWriter::new(OpenOptions::new()
                            .create(true)
                            .write(true)
                            .open(&cache_path)?))?,
                    )?
                    .into_memory()
            }
        }
        None => lut::build(&opts)?,
    };
    match tree {
        None => deplete_requests_from_stdin(graph),
        Some(tree) => find::commit(&tree, graph),
    }
}
