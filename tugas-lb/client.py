"""Client: sends REQ lines to broker, prints RESP lines.

Example:
  python3 client.py --num 6 --app mix --payload hello
  python3 client.py --num 3 --app SHORT
  python3 client.py --num 3 --app LONG --payload hitung-berat
"""

import argparse
import json
import socket
import time


def build_requests(num, app_mode, payload):
    apps = []
    mode = app_mode.upper()
    for i in range(num):
        if mode == "MIX":
            apps.append("SHORT" if i % 2 == 0 else "LONG")
        else:
            apps.append(mode)
    reqs = []
    for i, app in enumerate(apps):
        reqs.append({
            "type": "REQ",
            "req_id": f"req-{i + 1:03d}",
            "app": app,
            "payload": f"{payload}-{i + 1}",
        })
    return reqs


def run(host, port, requests):
    sock = socket.create_connection((host, port), timeout=30)
    sock.settimeout(30)
    f = sock.makefile("r", encoding="utf-8")
    hits = {}
    for req in requests:
        line = json.dumps(req) + "\n"
        print(f"[Client] SEND req_id={req['req_id']} app={req['app']} "
              f"payload={req['payload']!r}", flush=True)
        t0 = time.time()
        sock.sendall(line.encode("utf-8"))
        resp_line = f.readline()
        rtt = time.time() - t0
        if not resp_line:
            print("[Client] connection closed by broker", flush=True)
            break
        resp = json.loads(resp_line)
        wid = resp.get("worker_id", "?")
        hits[wid] = hits.get(wid, 0) + 1
        print(f"[Client] RECV req_id={resp.get('req_id')} "
              f"worker={wid} app={resp.get('app')} "
              f"rtt={rtt:.2f}s result={str(resp.get('result'))[:80]}",
              flush=True)
    print(f"[Client] distribution by worker: {hits}", flush=True)
    sock.close()


def main():
    ap = argparse.ArgumentParser(description="LB test client")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5000)
    ap.add_argument("--num", type=int, default=6)
    ap.add_argument("--app", default="mix",
                    help="SHORT, LONG, or mix")
    ap.add_argument("--payload", default="data")
    args = ap.parse_args()
    if args.app.upper() not in ("SHORT", "LONG", "MIX"):
        ap.error("--app must be SHORT, LONG, or mix")
    run(args.host, args.port, build_requests(args.num, args.app, args.payload))


if __name__ == "__main__":
    main()
