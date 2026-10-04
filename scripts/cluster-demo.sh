#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
root=${1:-target/cluster}
mkdir -p "$(dirname "$root")"
root=$(realpath -m "$root")
[[ ! -e "$root" ]] || { echo "Choose a fresh data root: $root" >&2; exit 1; }
if [[ $(uname -s) != Linux || $(findmnt -T "$(dirname "$root")" -n -o FSTYPE) != ext4 ]]; then
  echo 'Strict demo requires Linux/WSL and an ext4 data root.' >&2
  exit 1
fi
mkdir "$root"
cp='target/classes:target/dependency/*'
cluster=28b76eca-7521-43cf-baaa-3376f059c05b
voters='0@127.0.0.1:19090,1@127.0.0.1:19091,2@127.0.0.1:19092'
pids=()
cleanup() {
  for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  local until=$((SECONDS + 10))
  while [[ $SECONDS -lt $until ]]; do
    local alive=0
    for pid in "${pids[@]}"; do
      if kill -0 "$pid" 2>/dev/null; then alive=1; fi
    done
    [[ $alive == 0 ]] && break
    sleep 0.1
  done
  for pid in "${pids[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then kill -KILL "$pid" 2>/dev/null || true; fi
  done
  for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT
trap 'exit 130' INT TERM
java_opts=(-Xms32m -Xmx256m -cp "$cp")
for id in 1 2 3; do
  node=$((id - 1))
  java "${java_opts[@]}" vn.huyqt.logbroker.controller.client.ControllerCli format \
    --data "$root/controller-$id" --node "$node" --cluster "$cluster" --voters "$voters" --metadata-version 2
  sed "s|^data.dir=.*|data.dir=$root/controller-$id|" "config/cluster/controller-$id.properties" > "$root/controller-$id.properties"
  java "${java_opts[@]}" vn.huyqt.logbroker.controller.ControllerMain --config "$root/controller-$id.properties" \
    > "$root/controller-$id.log" 2>&1 &
  pids+=("$!")
done
for id in 1 2 3; do
  java "${java_opts[@]}" vn.huyqt.logbroker.broker.BrokerMain format \
    --config "config/cluster/broker-$id.properties" --data "$root/broker-$id"
  java "${java_opts[@]}" vn.huyqt.logbroker.broker.BrokerMain \
    --config "config/cluster/broker-$id.properties" --data "$root/broker-$id" > "$root/broker-$id.log" 2>&1 &
  pids+=("$!")
done
deadline=$((SECONDS + 60))
while true; do
  ready=0
  for id in 1 2 3; do
    if grep -q 'state=RUNNING' "$root/broker-$id.log"; then ready=$((ready + 1)); fi
  done
  [[ $ready == 3 ]] && break
  for pid in "${pids[@]}"; do
    kill -0 "$pid" 2>/dev/null || { echo "Node exited; inspect $root/*.log" >&2; exit 1; }
  done
  [[ $SECONDS -lt $deadline ]] || { echo "Startup timed out; inspect $root/*.log" >&2; exit 1; }
  sleep 0.2
done
java "${java_opts[@]}" vn.huyqt.logbroker.controller.client.ControllerCli describe-quorum --cluster "$cluster" --voters "$voters" --node 0
java "${java_opts[@]}" vn.huyqt.logbroker.example.ClientExample 127.0.0.1:9092 demo
java "${java_opts[@]}" vn.huyqt.logbroker.controller.client.ControllerCli metadata --cluster "$cluster" --voters "$voters"
java "${java_opts[@]}" vn.huyqt.logbroker.controller.client.ControllerCli local-metadata --cluster "$cluster" --voters "$voters" --node 0
echo "Demo complete; logs and data retained at $root"
