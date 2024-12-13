#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $(basename "$0") <URL://filelist.csv>"
  exit 1
fi

# Monitor the IO read activity of a command
traceIO() {
  touch "$1"
  TRACE="$1" "${@:2}"
}


workerCount=4
# Spawn the coordinator process
traceIO trace0.txt \
  build/coordinator "$1" 4242 &

# Spawn some workers
for i in {1..4}; do
  traceIO "trace$i.txt" \
    build/worker "localhost" "4242" &
done

# And wait for completion
time wait
