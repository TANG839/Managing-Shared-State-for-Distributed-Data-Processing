#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $(basename "$0") <path/to/file.csv>"
  exit 1
fi
test -f "$1" || (echo "\"$1\": No such file or directory" && exit 1)

# Prepare the data

# Spawn the coordinator process

build/coordinator "file://$(dirname "$(realpath "$1")")/filelist.csv" 4243  &
#gdb -ex r --args build/coordinator "file://$(dirname "$(realpath "$1")")/filelist.csv" 4243 
# Spawn some workers
for _ in {1..10}; do
  # gdb -ex r --args build/worker "localhost" "4243"
  build/worker "localhost" "4243" &
done

#gdb -ex r --args build/coordinator "file://$(dirname "$(realpath "$1")")/filelist.csv" 4243 

# And wait for completion
time wait
