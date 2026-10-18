#!/usr/bin/env python3
"""
Worker server untuk simulasi load balancing.

Menjalankan satu worker dengan ID dan port tertentu.
Menangani dua jenis aplikasi:
  - SHORT : echo / operasi cepat
  - LONG  : komputasi berat (prime_count / factorial)

Protokol: satu JSON per baris, diakhiri newline (\\n), melalui TCP.

Contoh menjalankan:
    python worker.py --id 1 --port 5001
    python worker.py --id 2 --port 5002
    python worker.py --id 3 --port 5003
"""

import argparse
import json
import math
import socket
import threading


# ---------------------------------------------------------------------------
# Logika aplikasi
# ---------------------------------------------------------------------------

def prime_count(limit: int) -> int:
    """Hitung jumlah bilangan prima <= limit dengan Sieve of Eratosthenes."""
    if limit < 2:
        return 0
    sieve = bytearray(b"\x01") * (limit + 1)
    sieve[0:2] = b"\x00\x00"
    for i in range(2, int(limit ** 0.5) + 1):
        if sieve[i]:
            step = i
            start = i * i
            sieve[start:limit + 1:step] = b"\x00" * ((limit - start) // step + 1)
    return sum(sieve)


def process_request(request: dict, worker_id: int) -> dict:
    """Proses satu request dan kembalikan dict respons."""
    request_id = request.get("request_id", "REQ-???")
    app_id = str(request.get("application_id", "")).upper()
    payload = request.get("payload")

    if app_id == "SHORT":
        # Echo: kembalikan payload apa adanya (cepat, tanpa komputasi berat).
        if isinstance(payload, dict) and "message" in payload:
            result = payload["message"]
        else:
            result = payload
        return {
            "request_id": request_id,
            "worker_id": worker_id,
            "status": "success",
            "result": result,
        }

    if app_id == "LONG":
        # Komputasi berat. Dukung dua bentuk payload:
        #   {"operation": "prime_count", "limit": 100000}
        #   {"operation": "factorial", "n": 20}
        #   <angka>  -> dianggap sebagai limit prime_count
        if isinstance(payload, dict):
            operation = str(payload.get("operation", "prime_count")).lower()
            if operation == "prime_count":
                limit = int(payload.get("limit", 100000))
                if limit < 0 or limit > 2_000_000:
                    return {
                        "request_id": request_id,
                        "worker_id": worker_id,
                        "status": "error",
                        "message": "limit harus 0..2000000",
                    }
                return {
                    "request_id": request_id,
                    "worker_id": worker_id,
                    "status": "success",
                    "result": prime_count(limit),
                }
            if operation == "factorial":
                n = int(payload.get("n", 20))
                if n < 0 or n > 1000:
                    return {
                        "request_id": request_id,
                        "worker_id": worker_id,
                        "status": "error",
                        "message": "n harus 0..1000",
                    }
                return {
                    "request_id": request_id,
                    "worker_id": worker_id,
                    "status": "success",
                    "result": math.factorial(n),
                }
            return {
                "request_id": request_id,
                "worker_id": worker_id,
                "status": "error",
                "message": f"unknown LONG operation: {operation}",
            }
        if isinstance(payload, (int, float)):
            # Payload angka mentah -> prime_count
            return {
                "request_id": request_id,
                "worker_id": worker_id,
                "status": "success",
                "result": prime_count(int(payload)),
            }
        if isinstance(payload, str) and payload.strip().lstrip("-").isdigit():
            return {
                "request_id": request_id,
                "worker_id": worker_id,
                "status": "success",
                "result": prime_count(int(payload.strip())),
            }
        return {
            "request_id": request_id,
            "worker_id": worker_id,
            "status": "error",
            "message": "payload LONG tidak valid",
        }

    return {
        "request_id": request_id,
        "worker_id": worker_id,
        "status": "error",
        "message": f"unknown application_id: {request.get('application_id')}",
    }


# ---------------------------------------------------------------------------
# Jaringan
# ---------------------------------------------------------------------------

def handle_connection(conn: socket.socket, addr, worker_id: int):
    """Tangani satu koneksi dari broker (bisa berisi banyak request baris JSON)."""
    tag = f"[WORKER-{worker_id}]"
    try:
        with conn:
            buf = conn.makefile("r")
            for line in buf:
                line = line.strip()
                if not line:
                    continue
                try:
                    request = json.loads(line)
                except json.JSONDecodeError:
                    resp = {"request_id": "UNKNOWN", "worker_id": worker_id,
                            "status": "error", "message": "invalid JSON"}
                    conn.sendall((json.dumps(resp) + "\n").encode())
                    continue

                request_id = request.get("request_id", "REQ-???")
                app_id = request.get("application_id", "?")
                print(f"{tag} Received {request_id}", flush=True)
                print(f"{tag} Application: {app_id}", flush=True)
                print(f"{tag} Processing request...", flush=True)

                resp = process_request(request, worker_id)

                print(f"{tag} Request completed", flush=True)
                conn.sendall((json.dumps(resp) + "\n").encode())
                print(f"{tag} Response sent to Broker", flush=True)
    except (ConnectionResetError, BrokenPipeError):
        pass
    except Exception as exc:  # noqa: BLE001 - jangan matikan worker karena 1 koneksi rusak
        print(f"[WORKER-{worker_id}] Error: {exc}", flush=True)


def run_worker(host: str, port: int, worker_id: int):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, port))
    srv.listen(50)
    print(f"[WORKER-{worker_id}] Listening on port {port}", flush=True)
    try:
        while True:
            conn, addr = srv.accept()
            t = threading.Thread(target=handle_connection,
                                 args=(conn, addr, worker_id), daemon=True)
            t.start()
    except KeyboardInterrupt:
        print(f"\n[WORKER-{worker_id}] Shutting down", flush=True)
    finally:
        srv.close()


def main():
    ap = argparse.ArgumentParser(description="Worker server load balancing")
    ap.add_argument("--id", type=int, required=True, help="ID worker (1/2/3)")
    ap.add_argument("--port", type=int, required=True, help="Port worker")
    ap.add_argument("--host", default="127.0.0.1", help="Host bind (default 127.0.0.1)")
    args = ap.parse_args()
    run_worker(args.host, args.port, args.id)


if __name__ == "__main__":
    main()
