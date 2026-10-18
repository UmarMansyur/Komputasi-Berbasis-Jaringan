#!/usr/bin/env python3
"""
Client untuk simulasi load balancing.

Mengirim request ke broker (port 5000) dan menampilkan respons
beserta worker yang memprosesnya.

Contoh:
    # 1 request SHORT
    python client.py --app SHORT --payload "Halo Server"

    # 1 request LONG (hitung prima <= 100000, hasil harus 9592)
    python client.py --app LONG --limit 100000

    # 1 request LONG faktorial
    python client.py --app LONG --operation factorial --n 20

    # 6 request SHORT untuk melihat distribusi Round Robin
    python client.py --num 6 --app SHORT --payload "Halo Server"

    # 6 request campuran SHORT/LONG bergantian
    python client.py --num 6 --app MIXED
"""

import argparse
import json
import socket
from collections import Counter


def build_payload(args, i: int):
    """Bangun (application_id, payload) untuk request ke-i (0-based)."""
    app = args.app.upper()
    if app == "MIXED":
        # Bergantian SHORT, LONG, SHORT, LONG, ...
        app = "SHORT" if i % 2 == 0 else "LONG"

    if app == "SHORT":
        return "SHORT", args.payload

    if app == "LONG":
        if args.operation == "factorial":
            return "LONG", {"operation": "factorial", "n": args.n}
        return "LONG", {"operation": "prime_count", "limit": args.limit}

    raise ValueError(f"--app harus SHORT, LONG, atau MIXED (diberi: {args.app})")


def send_requests(host: str, port: int, requests: list, timeout: float = 20.0):
    """Kirim semua request lewat SATU koneksi TCP, baca respons satu per satu."""
    responses = []
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        f = sock.makefile("r")
        for req in requests:
            sock.sendall((json.dumps(req) + "\n").encode())
            print(f"[CLIENT] Sent {req['request_id']} ({req['application_id']})")
            line = f.readline()
            if not line:
                raise ConnectionError("broker menutup koneksi tanpa respons")
            resp = json.loads(line)
            responses.append(resp)
            wid = resp.get("worker_id", "?")
            print(f"[CLIENT] Reply {resp.get('request_id')} from Worker {wid}: "
                  f"{resp.get('result', resp.get('message'))}")
    return responses


def main():
    ap = argparse.ArgumentParser(description="Client load balancing")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5000, help="Port broker")
    ap.add_argument("--app", default="SHORT", help="SHORT | LONG | MIXED")
    ap.add_argument("--payload", default="Halo Server", help="Payload SHORT")
    ap.add_argument("--operation", default="prime_count",
                    help="Operasi LONG: prime_count | factorial")
    ap.add_argument("--limit", type=int, default=100000,
                    help="Limit prime_count (default 100000 -> hasil 9592)")
    ap.add_argument("--n", type=int, default=20, help="Nilai n faktorial")
    ap.add_argument("--num", type=int, default=1, help="Jumlah request yang dikirim")
    ap.add_argument("--start", type=int, default=1, help="Nomor awal request_id")
    ap.add_argument("--timeout", type=float, default=20.0)
    args = ap.parse_args()

    requests = []
    for i in range(args.num):
        app_id, payload = build_payload(args, i)
        requests.append({
            "request_id": f"REQ-{args.start + i:03d}",
            "application_id": app_id,
            "payload": payload,
        })

    responses = send_requests(args.host, args.port, requests, args.timeout)

    # Ringkasan distribusi -> berguna untuk membandingkan kedua algoritma
    dist = Counter(str(r.get("worker_id", "?")) for r in responses)
    print("\n[CLIENT] --- Ringkasan distribusi ---")
    for wid in sorted(dist):
        print(f"[CLIENT] Worker {wid}: {dist[wid]} request")
    print(f"[CLIENT] Total: {len(responses)} request")


if __name__ == "__main__":
    main()
