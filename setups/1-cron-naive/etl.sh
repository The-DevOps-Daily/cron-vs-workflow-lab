#!/bin/bash
# Setup 1: plain cron plus a shell script. This is the first version most teams ship.
# It does have strict mode; without it the results are worse.
set -euo pipefail

# the run at minute T loads partition T-1, the way a nightly job loads yesterday
set -a; . /opt/lab/.env; set +a   # DATABASE_URL
PARTITION=${PARTITION:-$(date -u -d '1 minute ago' +%Y-%m-%dT%H:%M)}
WORK=/var/tmp/etl-naive   # fixed path, shared by every run
mkdir -p "$WORK"
cd /opt/lab/etl

python3 etl.py extract   --setup naive --partition "$PARTITION" --workdir "$WORK"
python3 etl.py transform --setup naive --partition "$PARTITION" --workdir "$WORK"
python3 etl.py load      --setup naive --partition "$PARTITION" --workdir "$WORK" --mode append
python3 etl.py report    --setup naive --partition "$PARTITION" --workdir "$WORK" --report-mode always
