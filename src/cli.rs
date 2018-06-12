use failure::Error;
use lut::{self, MultiReverseCommitGraph};
use std::io::{stdin, stdout, BufRead, BufReader, Write};
use git2::Oid;
use Options;
use find;

fn deplete_requests_from_stdin(luts: &MultiReverseCommitGraph) -> Result<(), Error> {
    let all_oids = lut::commit_oids_table(luts);
    let mut commits = Vec::new();

    let stdin = stdin();
    let stdout = stdout();

    let read = BufReader::new(stdin.lock());
    let mut out = stdout.lock();
    let mut obuf = String::new();

    eprintln!("Waiting for input...");
    for hexsha in read.lines().filter_map(Result::ok) {
        let oid = Oid::from_str(&hexsha)?;

        commits.clear();
        lut::commits_by_blob(&oid, luts, &all_oids, &mut commits);

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
    }
    Ok(())
}

pub fn run(opts: Options) -> Result<(), Error> {
    let tree = opts.tree.clone();
    let luts = lut::build(opts)?;
    match tree {
        None => deplete_requests_from_stdin(&luts),
        Some(tree) => find::commit(&tree, luts),
    }
}
