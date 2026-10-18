#!/usr/bin/env python3
"""
Broker / Load Balancer.

Menerima request dari client (port 5000), memilih salah satu worker
berdasarkan strategi, meneruskan request, lalu mengembalikan respons
worker kepada client.

Strategi:
  - round_robin    : Worker 1 -> 2 -> 3 -> 1 -> 2 -> 3 -> ...
  - least_requests : worker dengan jumlah request yang SUDAH dilayani
                     paling sedikit (tie-break: ID terkecil).
                     Alias yang diterima: least_connections, least_served.

Protokol: satu JSON per baris diakhiri newline (\\n), melalui TCP.

Contoh:
    python broker.py --strategy round_robin
    python broker.py --strategy least_requests --port 5000
"""

import argparse
import json
import socket
import threading

STRATEGY_ROUND_ROBIN = "ROUND_ROBIN"
STRATEGY_LEAST_REQUESTS = "LEAST_REQUESTS"


def normalize_strategy(s: str) -> str:
    s = s.strip().lower()
    if s in ("round_robin", "round-robin", "rr"):
        return STRATEGY_ROUND_ROBIN
    if s in ("least_requests", "least-requests", "least_connections",
             "least-connections", "least_served", "least-served", "lc", "lr"):
        # Tugas menyebutnya "Least Connections", tetapi yang dihitung adalah
        # jumlah request yang telah selesai dilayani -> least requests.
        return STRATEGY_LEAST_REQUESTS
    raise ValueError(f"strategi tidak dikenal: {s}")


def parse_workers(spec: str):
    """Ubah '127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003' jadi [(host,port)]."""
    workers = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" not in part:
            raise ValueError(f"format worker salah: {part} (harap host:port)")
        host, port = part.rsplit(":", 1)
        workers.append((host.strip(), int(port.strip())))
    if not workers:
        raise ValueError("daftar worker kosong")
    return workers


class Broker:
    def __init__(self, workers, strategy: str, timeout: float = 15.0):
        # worker_id = 1..N sesuai urutan daftar workers
        self.workers = workers
        self.strategy = strategy
        self.timeout = timeout
        self.lock = threading.Lock()
        self.rr_counter = 0
        self.served = {i + 1: 0 for i in range(len(workers))}

    # -- pemilihan worker -------------------------------------------------
    def select_worker(self) -> int:
        """Kembalikan worker_id terpilih (1-based)."""
        with self.lock:
            if self.strategy == STRATEGY_ROUND_ROBIN:
                wid = (self.rr_counter % len(self.workers)) + 1
                self.rr_counter += 1
                return wid
            # LEAST_REQUESTS: jumlah tersedikit, tie-break ID terkecil
            return min(self.served, key=lambda wid: (self.served[wid], wid))

    def candidate_order(self, first_wid: int):
        """Urutan fallback jika worker pertama gagal: lainnya menyusul."""
        if self.strategy == STRATEGY_ROUND_ROBIN:
            n = len(self.workers)
            return [((first_wid - 1 + i) % n) + 1 for i in range(n)]
        with self.lock:
            rest = sorted(self.served, key=lambda w: (self.served[w], w))
        # first_wid tetap di depan, sisanya mengikuti urutan least
        return [first_wid] + [w for w in rest if w != first_wid]

    def mark_served(self, wid: int):
        with self.lock:
            self.served[wid] += 1

    # -- penerusan --------------------------------------------------------
    def forward(self, worker_addr, request: dict) -> dict:
        """Buka koneksi ke worker, kirim request, tunggu satu baris respons."""
        timeout = self.timeout
        with socket.create_connection(worker_addr, timeout=timeout) as sock:
            sock.settimeout(timeout)
            sock.sendall((json.dumps(request) + "\n").encode())
            f = sock.makefile("r")
            line = f.readline()
            if not line:
                raise ConnectionError("worker menutup koneksi tanpa respons")
            return json.loads(line)

    # -- penanganan client -----------------------------------------------
    def handle_client(self, conn: socket.socket, addr):
        try:
            with conn:
                f = conn.makefile("r")
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        request = json.loads(line)
                    except json.JSONDecodeError:
                        err = {"request_id": "UNKNOWN", "status": "error",
                               "message": "invalid JSON"}
                        conn.sendall((json.dumps(err) + "\n").encode())
                        continue

                    request_id = request.get("request_id", "REQ-???")
                    app_id = request.get("application_id", "?")
                    print(f"[BROKER] Request {request_id} received", flush=True)
                    print(f"[BROKER] Application: {app_id}", flush=True)
                    print(f"[BROKER] Strategy: {self.strategy}", flush=True)

                    first = self.select_worker()
                    order = self.candidate_order(first)
                    last_err = None
                    response = None
                    used_wid = None
                    for wid in order:
                        print(f"[BROKER] Selected Worker {wid}", flush=True)
                        try:
                            response = self.forward(self.workers[wid - 1], request)
                            # Pastikan worker_id pada respons benar
                            if "worker_id" not in response:
                                response["worker_id"] = wid
                            used_wid = response.get("worker_id", wid)
                            print(f"[BROKER] Response received from Worker {used_wid}",
                                  flush=True)
                            break
                        except Exception as exc:  # noqa: BLE001
                            last_err = exc
                            print(f"[BROKER] Worker {wid} failed ({exc}), trying next...",
                                  flush=True)
                    if response is None:
                        response = {"request_id": request_id, "status": "error",
                                    "message": f"all workers failed: {last_err}"}
                    else:
                        self.mark_served(int(used_wid))

                    conn.sendall((json.dumps(response) + "\n").encode())
                    print("[BROKER] Response sent to Client", flush=True)
        except (ConnectionResetError, BrokenPipeError):
            pass
        except Exception as exc:  # noqa: BLE001
            print(f"[BROKER] Client handler error: {exc}", flush=True)


def main():
    ap = argparse.ArgumentParser(description="Broker / Load Balancer")
    ap.add_argument("--port", type=int, default=5000, help="Port broker (default 5000)")
    ap.add_argument("--host", default="127.0.0.1", help="Host bind (default 127.0.0.1)")
    ap.add_argument("--strategy", default="round_robin",
                    help="round_robin | least_requests (alias: least_connections)")
    ap.add_argument("--workers", default="127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003",
                    help="Daftar worker host:port dipisah koma")
    ap.add_argument("--timeout", type=float, default=15.0,
                    help="Timeout penerusan ke worker (detik)")
    args = ap.parse_args()

    strategy = normalize_strategy(args.strategy)
    workers = parse_workers(args.workers)
    broker = Broker(workers, strategy, timeout=args.timeout)

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((args.host, args.port))
    srv.listen(50)
    print(f"[BROKER] Listening on port {args.port}", flush=True)
    print(f"[BROKER] Strategy: {strategy}", flush=True)
    for i, (h, p) in enumerate(workers, start=1):
        print(f"[BROKER] Worker {i} -> {h}:{p}", flush=True)
    try:
        while True:
            conn, addr = srv.accept()
            t = threading.Thread(target=broker.handle_client,
                                 args=(conn, addr), daemon=True)
            t.start()
    except KeyboardInterrupt:
        print("\n[BROKER] Shutting down", flush=True)
    finally:
        srv.close()


if __name__ == "__main__":
    main()
