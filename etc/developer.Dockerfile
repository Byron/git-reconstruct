from guangie88/rustfmt-clippy:nightly

run cargo install hyperfine watchexec

run apt-get update
run apt-get install -y valgrind
run apt-get install -y cmake

env PATH=$PATH:/root/.cargo/bin
