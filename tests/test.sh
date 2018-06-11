#!/bin/bash

exe=${1:?Need executable}

echo dc595f7f016a0cff8b176a4c1e67483986f14816 | "$exe" --head-only $PWD
echo dc595f7f016a0cff8b176a4c1e67483986f14817 | "$exe" $PWD
echo dc595f7f016a0cff8b176a4c1e67483986f14816 | "$exe" --head-only --no-compact $PWD
echo dc595f7f016a0cff8b176a4c1e67483986f14816 | "$exe" $PWD
