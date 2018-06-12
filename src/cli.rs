use failure::Error;
use lut;
use find;
use Options;

pub fn run(opts: Options) -> Result<(), Error> {
    let blobs = find::generate_blob_hashes(&opts.tree)?;
    let _luts = lut::build(&blobs, opts)?;
    Ok(())
}
