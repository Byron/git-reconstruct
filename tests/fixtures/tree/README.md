A backend for https://github.com/ali1234/gitxref/tree/master/gitxref

Compile and use:

```
# Be sure it's at least Rust 1.26 
cargo build --release
```

Now run it like this:

```
echo dc595f7f016a0cff8b176a4c1e67483986f14816 | git-commits-by-blobs <path-to-repo>
```

### Usage 

Pipe one hex-sha per line to stdin, and get space-separated hex-shas of all commits that use them,
followed by newline.

### Limitations

 * it only reads commits reachable from the HEAD of the repository. This can easily be fixed by
   adding more targets to the `Revwalk` instance.
