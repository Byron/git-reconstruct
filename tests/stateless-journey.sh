#!/usr/bin/env bash
set -eu

exe=${1:?First argument must be the executable to test}

root="$(cd "${0%/*}" && pwd)"
# shellcheck disable=1090
source "$root/utilities.sh"
fixture="$root/fixtures"
snapshot="$root/snapshots"

SUCCESSFULLY=0
WITH_FAILURE=1
commit=dc595f7f016a0cff8b176a4c1e67483986f14816

title "backend mode - lookup commits by blob"
(when "finding the best commit by specifying a source tree"
  it "succeeds" && {
    WITH_SNAPSHOT="$snapshot/generate-merge-commit-info-success" \
    expect_run ${SUCCESSFULLY} "$exe" --head-only $PWD "$fixture/tree"
  }
)

title "backend mode - lookup commits by blob"
(when "only iterating the current head (--head-only)"
  (with "memory compaction"
    it "succeeds" && {
      echo $commit \
      | expect_run ${SUCCESSFULLY} "$exe" --head-only $PWD
    }
  )
  (with "no memory compaction (--no-compact)"
    it "succeeds" && {
      echo $commit \
      | expect_run ${SUCCESSFULLY} "$exe" --head-only --no-compact $PWD
    }
  )
)
(when "iterating all remote heads (and memory compaction"
  it "succeeds" && {
    echo $commit \
    | expect_run ${SUCCESSFULLY} "$exe" $PWD
  }
)
