# Simulasi Load Balancing — Socket Programming Python

Arsitektur client-server: **1 client, 1 broker, 3 worker**.

```
Client --(TCP JSON)--> Broker --(TCP JSON)--> Worker 1/2/3
Client <-- respons --- Broker <-- respons ----- Worker
```

| Komponen | Port default |
| -------- | ------------ |
| Broker   | 5000         |
| Worker 1 | 5001         |
| Worker 2 | 5002         |
| Worker 3 | 5003         |

Protokol: satu JSON per baris, diakhiri `\n` (TCP tidak menjaga batas pesan,
jadi framing newline dipakai dan dibaca sampai newline diterima).

## 1. Menjalankan

Butuh 5 terminal (atau jalankan worker di background). Dari folder ini:

**Terminal 1–3 — worker:**
```bash
python worker.py --id 1 --port 5001
python worker.py --id 2 --port 5002
python worker.py --id 3 --port 5003
```

**Terminal 4 — broker** (pilih salah satu strategi):
```bash
python broker.py --strategy round_robin
# atau
python broker.py --strategy least_requests
```

**Terminal 5 — client:**
```bash
# 1 request SHORT (echo)
python client.py --app SHORT --payload "Halo Server"

# 1 request LONG (prima <= 100000, hasil = 9592)
python client.py --app LONG --limit 100000

# 6 request untuk melihat distribusi
python client.py --num 6 --app SHORT --payload "Halo Server"

# 6 request campuran SHORT/LONG bergantian
python client.py --num 6 --app MIXED
```

Client mencetak `worker_id` setiap balasan plus **ringkasan distribusi**,
misalnya:

```
[CLIENT] Worker 1: 2 request
[CLIENT] Worker 2: 2 request
[CLIENT] Worker 3: 2 request
```

## 2. Dua algoritma

| Strategi | Cara memilih worker |
| -------- | ------------------- |
| `round_robin` | Counter internal: 1 → 2 → 3 → 1 → 2 → 3 … |
| `least_requests` (`least_connections` juga diterima sebagai alias) | Worker dengan jumlah request yang **sudah dilayani** paling sedikit; seri → ID terkecil |

Uji perbandingan yang disarankan:

1. Jalankan broker `round_robin`, kirim 6 request (`--num 6`), catat distribusi
   (harus 2-2-2 berurutan 1,2,3,1,2,3 — lihat log `[BROKER] Selected Worker N`).
2. Restart broker dengan `least_requests`, kirim 6 request lagi, catat distribusi.
3. Bandingkan log broker + ringkasan client untuk laporan.

> Catatan: `least_requests` menyeimbangkan **jumlah request yang dilayani**,
> bukan waktu CPU. Satu request LONG tetap jauh lebih berat daripada SHORT.

## 3. Aplikasi LONG dan SHORT

Kedua aplikasi tersedia di **semua** worker (tidak ada worker yang dikhususkan).

| Application ID | Perilaku | Contoh payload |
| -------------- | -------- | -------------- |
| `SHORT` | Echo cepat | `"Halo Server"` |
| `LONG` | Berat: `prime_count` (sieve) atau `factorial` | `{"operation": "prime_count", "limit": 100000}` → `9592` |

Contoh pesan (JSON + `\n`):

```json
{"request_id": "REQ-001", "application_id": "SHORT", "payload": "Halo Server"}
{"request_id": "REQ-002", "application_id": "LONG", "payload": {"operation": "prime_count", "limit": 100000}}
```

Respons worker:

```json
{"request_id": "REQ-002", "worker_id": 2, "status": "success", "result": 9592}
```

## 4. Opsi lanjutan

```bash
# Broker di port lain / daftar worker kustom
python broker.py --port 5000 --strategy least_requests \
  --workers 127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003

# Faktorial
python client.py --app LONG --operation factorial --n 20
```

Jika satu worker mati, broker otomatis mencoba worker berikutnya dan
mencatat `Worker N failed ... trying next...` di log.
