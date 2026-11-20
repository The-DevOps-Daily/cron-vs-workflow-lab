#!/bin/bash
# Anonymous (non-cache) memory each setup keeps resident, from cgroup memory.stat,
# sampled every 10 s. Cron jobs run inside cron.service, so take the minimum for cron.
# Usage (on the lab box): ./lab/footprint.sh 18 > results/footprint.jsonl
N=${1:-18}
anon() { awk '/^anon /{print $2}' "/sys/fs/cgroup/$1/memory.stat" 2>/dev/null || echo 0; }
for i in $(seq "$N"); do
  line="{\"ts\": $(date +%s), \"cron\": $(anon system.slice/cron.service), \"k3s\": $(( $(anon system.slice/k3s.service) + $(anon kubepods.slice) ))"
  for c in $(docker ps --format '{{.Names}}'); do
    id=$(docker inspect -f '{{.Id}}' "$c")
    line+=", \"$c\": $(anon "system.slice/docker-$id.scope")"
  done
  echo "$line}"
  sleep 10
done
