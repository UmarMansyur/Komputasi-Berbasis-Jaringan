#!/bin/bash
# Demo: 3 workers + broker, test least_served lalu round_robin.
set -u
cd "$(dirname "$0")"

cleanup() {
  kill $W1 $W2 $W3 $B 2>/dev/null
  wait 2>/dev/null
}
trap cleanup EXIT

echo "== start workers =="
python3 worker.py --id 1 --port 5001 &
W1=$!
python3 worker.py --id 2 --port 5002 &
W2=$!
python3 worker.py --id 3 --port 5003 &
W3=$!
sleep 1

echo "== strategy: least_served =="
python3 broker.py --port 5000 --strategy least_served &
B=$!
sleep 1
python3 client.py --num 6 --app mix --payload tugas
kill $B; wait $B 2>/dev/null
sleep 1

echo "== strategy: round_robin =="
python3 broker.py --port 5000 --strategy round_robin &
B=$!
sleep 1
python3 client.py --num 6 --app mix --payload tugas
kill $B; wait $B 2>/dev/null
sleep 1

echo "== done =="
