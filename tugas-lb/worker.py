"""Worker server for simple load-balancing simulation.

Listens on TCP, accepts JSON-per-line messages from broker:
  {"type": "FWD", "req_id": "...", "app": "SHORT"|"LONG", "payload": "..."}

Replies with one JSON line:
  {"type": "RESP", "req_id": "...", "app": "...", "worker_id": N,
   "result": "...", "proc_time": 0.12}

App behavior (simulated):
  SHORT -> immediate echo, no artificial delay.
  LONG  -> 2s sleep + deterministic CPU work to mimic heavy compute.
"""

import argparse
import json
import socket
import socketserver
import time


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    allow_reuse_address = True


def make_handler(worker_id):
    class WorkerHandler(socketserver.BaseRequestHandler):
        def handle(self):
            buf = b""
            self.request.settimeout(30)
            while True:
                try:
                    chunk = self.request.recv(4096)
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
            # connection closed by broker; return

        def handle_line(self, line):
            try:
                msg = json.loads(line.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                self.send_obj({"type": "ERROR", "message": "bad JSON, expected one object per line"})
                return

            if msg.get("type") != "FWD":
                self.send_obj({"type": "ERROR",
                               "req_id": msg.get("req_id"),
                               "message": "expected type FWD"})
                return

            req_id = msg.get("req_id", "-")
            app = str(msg.get("app", "SHORT")).upper()
            payload = str(msg.get("payload", ""))
            if app not in ("SHORT", "LONG"):
                self.send_obj({"type": "ERROR", "req_id": req_id,
                               "message": "app must be SHORT or LONG"})
                return

            print(f"[Worker-{worker_id}] got req_id={req_id} app={app} "
                  f"payload={payload!r} from broker {self.client_address}",
                  flush=True)
            start = time.time()
            if app == "SHORT":
                result = f"ECHO[{payload}]"
            else:
                # Simulate complex calculation: blocking delay + real CPU work.
                time.sleep(2.0)
                n = 200_000
                s = sum(i * i for i in range(n))
                result = f"LONG_RESULT[{payload}]:sum_sq({n})={s}"
            proc_time = time.time() - start
            print(f"[Worker-{worker_id}] done req_id={req_id} app={app} "
                  f"in {proc_time:.2f}s", flush=True)
            self.send_obj({
                "type": "RESP",
                "req_id": req_id,
                "app": app,
                "worker_id": worker_id,
                "result": result,
                "proc_time": round(proc_time, 3),
            })

        def send_obj(self, obj):
            data = (json.dumps(obj) + "\n").encode("utf-8")
            self.request.sendall(data)

    return WorkerHandler


def main():
    ap = argparse.ArgumentParser(description="LB worker server")
    ap.add_argument("--id", type=int, required=True, help="worker id, e.g. 1")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, required=True)
    args = ap.parse_args()

    handler = make_handler(args.id)
    with ThreadedTCPServer((args.host, args.port), handler) as srv:
        print(f"[Worker-{args.id}] listening on {args.host}:{args.port} "
              f"(SHORT=echo, LONG=2s sleep + cpu work)", flush=True)
        try:
            srv.serve_forever()
        except KeyboardInterrupt:
            print(f"[Worker-{args.id}] stopping", flush=True)


if __name__ == "__main__":
    main()
