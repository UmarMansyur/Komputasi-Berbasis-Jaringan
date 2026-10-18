"""Broker: accepts client requests, forwards to one of 3 workers.

Client -> broker (one JSON per line):
  {"type": "REQ", "req_id": "...", "app": "SHORT"|"LONG", "payload": "..."}

Broker -> client (one JSON per line, passed through from worker):
  {"type": "RESP", "req_id": "...", "app": "...", "worker_id": N,
   "result": "...", "proc_time": 0.12}

Scheduling (--strategy):
  least_served : pick worker with smallest served counter (tie -> smallest id).
  round_robin  : pick workers[counter % 3], counter starts at 0.

The served counter is incremented at dispatch time under a lock, so
concurrent clients still spread evenly. Broker logs every decision.
"""

import argparse
import json
import socket
import socketserver
import threading
import time

WORKERS_DEFAULT = [
    (1, "127.0.0.1", 5001),
    (2, "127.0.0.1", 5002),
    (3, "127.0.0.1", 5003),
]

_state_lock = threading.Lock()
_counter = 0
_served = {1: 0, 2: 0, 3: 0}


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    allow_reuse_address = True


def pick_worker(strategy, workers):
    """Return (wid, host, port). Updates counter/served under lock."""
    global _counter
    with _state_lock:
        if strategy == "round_robin":
            wid, host, port = workers[_counter % len(workers)]
            _served[wid] += 1
            _counter += 1
            return wid, host, port, dict(_served), _counter - 1
        # least_served
        wid, host, port = min(workers, key=lambda w: (_served[w[0]], w[0]))
        _served[wid] += 1
        _counter += 1
        return wid, host, port, dict(_served), _counter - 1


def forward_to_worker(host, port, fwd_obj, timeout=15.0):
    """Open a short TCP connection to the worker, send one line, read one line."""
    raw = (json.dumps(fwd_obj) + "\n").encode("utf-8")
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.sendall(raw)
        buf = b""
        while b"\n" not in buf:
            chunk = sock.recv(65536)
            if not chunk:
                break
            buf += chunk
        line = buf.split(b"\n", 1)[0]
        return json.loads(line.decode("utf-8"))


def make_handler(workers, strategy):
    class BrokerHandler(socketserver.BaseRequestHandler):
        def handle(self):
            self.request.settimeout(60)
            buf = b""
            while True:
                try:
                    chunk = self.request.recv(65536)
                except socket.timeout:
                    break
                if not chunk:
                    break
                buf += chunk
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    if not line.strip():
                        continue
                    self.handle_line(line)

        def handle_line(self, line):
            try:
                msg = json.loads(line.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                self.send_obj({"type": "ERROR", "message": "bad JSON"})
                return
            if msg.get("type") != "REQ":
                self.send_obj({"type": "ERROR", "req_id": msg.get("req_id"),
                               "message": "expected type REQ"})
                return
            req_id = str(msg.get("req_id", "-"))
            app = str(msg.get("app", "SHORT")).upper()
            payload = str(msg.get("payload", ""))
            if app not in ("SHORT", "LONG"):
                self.send_obj({"type": "ERROR", "req_id": req_id,
                               "message": "app must be SHORT or LONG"})
                return

            wid, whost, wport, counts, seq = pick_worker(strategy, workers)
            print(f"[Broker:{strategy}] REQ req_id={req_id} app={app} "
                  f"from {self.client_address} seq={seq} -> Worker-{wid} "
                  f"counts={counts}", flush=True)
            fwd = {"type": "FWD", "req_id": req_id, "app": app,
                   "payload": payload,
                   "client": f"{self.client_address[0]}:{self.client_address[1]}"}
            t0 = time.time()
            try:
                resp = forward_to_worker(whost, wport, fwd)
            except Exception as exc:  # worker down / timeout
                print(f"[Broker] Worker-{wid} failed for req_id={req_id}: {exc}",
                      flush=True)
                self.send_obj({"type": "ERROR", "req_id": req_id,
                               "message": f"worker-{wid} failed: {exc}"})
                return
            rtt = time.time() - t0
            print(f"[Broker] RESP req_id={req_id} <- Worker-{wid} "
                  f"in {rtt:.2f}s -> client {self.client_address}", flush=True)
            self.send_obj(resp)

        def send_obj(self, obj):
            self.request.sendall((json.dumps(obj) + "\n").encode("utf-8"))

    return BrokerHandler


def main():
    ap = argparse.ArgumentParser(description="LB broker")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5000)
    ap.add_argument("--strategy", choices=["least_served", "round_robin"],
                    default="least_served",
                    help="allocation policy")
    args = ap.parse_args()

    workers = WORKERS_DEFAULT
    handler = make_handler(workers, args.strategy)
    with ThreadedTCPServer((args.host, args.port), handler) as srv:
        print(f"[Broker] listening on {args.host}:{args.port} "
              f"strategy={args.strategy} workers={[(w[0], w[2]) for w in workers]}",
              flush=True)
        try:
            srv.serve_forever()
        except KeyboardInterrupt:
            print("[Broker] stopping", flush=True)


if __name__ == "__main__":
    main()
