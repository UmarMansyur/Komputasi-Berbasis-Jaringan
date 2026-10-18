# Simulasi Load Balancing: Broker + 3 Worker (Python Socket)

Implementasi TCP socket untuk tugas KBJ. Satu broker menerima request client
dan meneruskannya ke 3 worker. Semua pesan memakai JSON satu baris
diakhiri `\n`.

## Arsitektur

```text
Client  --->  Broker :5000  --->  Worker-1 :5001
                             --->  Worker-2 :5002
                             --->  Worker-3 :5003
```

Broker memakai `socketserver.ThreadingMixIn`, jadi banyak client bisa
dilayani paralel. Worker juga threaded dengan pola yang sama.
Setiap koneksi broker-worker bersifat pendek: buka, kirim 1 baris,
baca 1 baris, tutup.

## Protokol

Framing: satu objek JSON per baris, encoding UTF-8.

Client -> Broker:

```json
{"type": "REQ", "req_id": "req-001", "app": "SHORT", "payload": "tugas-1"}
```

`app` hanya `SHORT` atau `LONG`. `req_id` dibuat client dan dipakai
untuk mencocokkan respons.

Broker -> Worker (field `client` hanya info debugging):

```json
{"type": "FWD", "req_id": "req-001", "app": "SHORT", "payload": "tugas-1", "client": "127.0.0.1:58011"}
```

Worker -> Broker -> Client (diteruskan apa adanya):

```json
{"type": "RESP", "req_id": "req-001", "app": "SHORT", "worker_id": 1, "result": "ECHO[tugas-1]", "proc_time": 0.0}
```

Pesan error:

```json
{"type": "ERROR", "req_id": "req-001", "message": "..."}
```

## Alokasi worker

Broker menyimpan `counter` dan `served = {1,2,3}` di bawah `threading.Lock`.
Dua mode tersedia lewat `--strategy`:

1. `least_served` — pilih worker dengan nilai `served` terkecil.
   Seri diputus ke ID terkecil. Counter tetap naik untuk logging.
2. `round_robin` — pilih `workers[counter % 3]`, lalu `counter += 1`.

Counter `served` naik saat dispatch. Pilihan ini membuat request
konkuren tetap tersebar rata. Perbedaan kedua mode terlihat jelas
saat counter tidak mulai dari nol, misalnya setelah satu worker
lama mati atau broker sudah melayani banyak request.

## Perilaku worker

- `SHORT`: echo langsung, `result = "ECHO[<payload>]"`, tanpa sleep.
- `LONG`: `sleep(2.0)` + hitung `sum(i*i for i in range(200_000))`.
  Kombinasi delay dan kerja CPU ini mensimulasikan perhitungan berat.

Worker mencatat tiap request masuk dan waktu prosesnya ke stdout.
Broker mencatat tiap keputusan routing, isi counter, dan RTT ke worker.

## Cara menjalankan

Terminal 1-3 (worker):

```bash
python3 worker.py --id 1 --port 5001
python3 worker.py --id 2 --port 5002
python3 worker.py --id 3 --port 5003
```

Terminal 4 (broker, pilih satu strategi):

```bash
python3 broker.py --port 5000 --strategy least_served
python3 broker.py --port 5000 --strategy round_robin
```

Terminal 5 (client):

```bash
python3 client.py --num 6 --app mix --payload tugas
python3 client.py --num 3 --app SHORT
python3 client.py --num 3 --app LONG --payload hitung-berat
```

Atau sekali jalan:

```bash
bash run_demo.sh
```

## Contoh output (6 request mix, `least_served`)

Client:

```text
[Client] SEND req_id=req-001 app=SHORT payload='tugas-1'
[Client] RECV req_id=req-001 worker=1 app=SHORT rtt=0.00s result=ECHO[tugas-1]
[Client] SEND req_id=req-002 app=LONG payload='tugas-2'
[Client] RECV req_id=req-002 worker=2 app=LONG rtt=2.02s result=LONG_RESULT[tugas-2]:sum_sq(200000)=...
[Client] distribution by worker: {1: 2, 2: 2, 3: 2}
```

Broker:

```text
[Broker] listening on 127.0.0.1:5000 strategy=least_served workers=[(1, 5001), (2, 5002), (3, 5003)]
[Broker:least_served] REQ req_id=req-001 app=SHORT from ('127.0.0.1', 58011) seq=0 -> Worker-1 counts={1: 1, 2: 0, 3: 0}
[Broker] RESP req_id=req-001 <- Worker-1 in 0.00s -> client ('127.0.0.1', 58011)
```

Worker:

```text
[Worker-1] got req_id=req-001 app=SHORT payload='tugas-1' from broker ('127.0.0.1', 58012)
[Worker-1] done req_id=req-001 app=SHORT in 0.00s
```

## File

- `worker.py` — server pekerja, argumen `--id --port [--host]`.
- `broker.py` — penyeimbang beban, argumen `--strategy least_served|round_robin`.
- `client.py` — pengirim request uji, argumen `--num --app SHORT|LONG|mix`.
- `run_demo.sh` — menyalakan cluster, menguji dua strategi, mematikan proses.
