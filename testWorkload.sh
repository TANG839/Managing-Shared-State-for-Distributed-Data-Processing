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

# Then check how much work each worker did
for i in {1..4}; do
  # We need to download 100 files. While we might get some skew, each of our
  # four workers should do about 20 of them. But definitely more than 10.
  downloads=$(wc -l <"trace$i.txt")
  if [ "$downloads" -lt 10 ]; then
    echo "Workload was not distributed evenly!" >&2
    echo "Worker $i was slacking." >&2
    for j in {0..4}; do rm "trace$j.txt"; done
    exit 1
  fi
done

for i in {0..4}; do rm "trace$i.txt"; done
