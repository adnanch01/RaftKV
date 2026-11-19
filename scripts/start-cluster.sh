#!/usr/bin/env bash
# Starts a 5-node cluster on this machine (in-memory storage, no Docker needed).
# Logs go to logs/nX.log. Stop with: ./scripts/stop-cluster.sh
set -euo pipefail
cd "$(dirname "$0")/.."

# Always do a clean build: IDEs also compile into target/classes and can leave stale classes.
./mvnw -q -DskipTests clean package

mkdir -p logs
: > logs/pids.txt
for i in 1 2 3 4 5; do
  NODE_ID="n$i" HTTP_PORT="808$i" GRPC_PORT="909$i" \
    nohup java -jar target/raftkv-1.0.0.jar > "logs/n$i.log" 2>&1 &
  echo $! >> logs/pids.txt
  echo "Started n$i (pid $!)  http://localhost:808$i"
done
echo
echo "Watch the election:  tail -f logs/n1.log"
echo "Cluster status:      curl -s localhost:8081/raft/status"
