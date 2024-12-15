#!/bin/bash

# Number of workers to start (can be passed as argument)
workers=$1

# Validate input
if ! [[ "$workers" =~ ^[0-9]+$ ]]; then
    workers=3  # Default to 3 if invalid or no input
fi

# Start coordinator
docker run -d --network=cbdp_net --name=coordinator cbdp_coordinator

# Start workers in parallel
for i in $(seq 1 $workers)
do
    docker run -d --network=cbdp_net --name="worker$i" cbdp_worker &
done

# Wait for all background processes to complete
wait

# Show running containers
docker ps

# Show coordinator logs
docker logs coordinator