#!/usr/bin/env bash
set -eu

exe=${1:?First argument must be the executable to test}

root="$(cd "${0%/*}" && pwd)"
exe="$root/../$exe"
# shellcheck disable=1090
source "$root/utilities.sh"
fixture="$root/fixtures"
snapshot="$root/snapshots"

SUCCESSFULLY=0
commit=dc595f7f016a0cff8b176a4c1e67483986f14816

title "Frontend mode - find merge commit"
(when "finding the best commit by specifying a source tree"
  (with "cache specified"
    cache_file=cache.bincode
    (sandbox
      it "succeeds" && {
        expect_run ${SUCCESSFULLY} "$exe" --head-only --cache-path $cache_file "$root/.." "$fixture/tree"
      }
      it "writes the cache" && {
        expect_exists $cache_file
      }
      
      (when "finding the best commit with existing cache" 
        it "loads the cache and succeeds" && {
          expect_run ${SUCCESSFULLY} "$exe" --head-only --cache-path $cache_file "$root/.." "$fixture/tree"
        }
      )
    )
  )
  (with "no cache specified"
    it "succeeds" && {
      expect_run ${SUCCESSFULLY} "$exe" --head-only "$PWD" "$fixture/tree"
    }
  )
)

title "backend mode - lookup commits by blob"
(when "only iterating the current head (--head-only)"
  it "succeeds" && {
    echo $commit \
    | expect_run ${SUCCESSFULLY} "$exe" --head-only $PWD
  }
)
(when "iterating all remote heads"
  it "succeeds" && {
    echo $commit \
    | expect_run ${SUCCESSFULLY} "$exe" $PWD
  }
)
