#!/usr/bin/env bash
set -euo pipefail


# Prepare the data

# Spawn the coordinator process

build/coordinator "filelist.csv" 4243  &
#gdb -ex r --args build/coordinator "file://$(dirname "$(realpath "$1")")/filelist.csv" 4243 
# Spawn some workers
for _ in {1..4}; do
  # gdb -ex r --args build/worker "localhost" "4243"
  build/worker "localhost" "4243" &
done

#gdb -ex r --args build/coordinator "file://$(dirname "$(realpath "$1")")/filelist.csv" 4243 

# And wait for completion
time wait
