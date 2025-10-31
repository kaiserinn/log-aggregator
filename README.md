# UTS Sistem Terdistribusi - Pub-Sub Log Aggregator

Proyek ini adalah implementasi layanan *Pub-Sub log aggregator* untuk Ujian Tengah Semester (UTS) Sistem Terdistribusi.

Tujuan utama layanan ini adalah menerima *event* (log) dari *publisher*, memprosesnya secara asinkron, dan menyimpannya. Fitur inti mencakup idempotency dan deduplication untuk menangani pengiriman *event* duplikat (*at-least-once delivery*), serta memastikan persistensi status deduplikasi saat *container* di-*restart*.

## Cara Build dan Run

### Opsi 1: Docker Compose

Metode ini akan membangun dan menjalankan kedua layanan (`aggregator` dan `publisher`) dalam jaringan Docker internal. Layanan `publisher` akan berjalan sebagai *job* yang mengirimkan 5.000+ *event* (termasuk duplikat) ke `aggregator` lalu berhenti.

1.  Dari direktori utama proyek (yang berisi `compose.yaml`), jalankan:

    ```bash
    docker compose up --build -d
    ```

2.  Layanan *aggregator* sekarang dapat diakses di `http://localhost:7878`.
3.  Untuk menghentikan dan menghapus *container*:

    ```bash
    docker compose down
    ```

### Opsi 2: Docker Manual

1.  **Build Image Aggregator**
    Gunakan `Dockerfile.aggregator` untuk membangun *image* layanan utama.

    ```bash
    docker build -t kaiserin0/aggregator --file Dockerfile.aggregator .
    ```

2.  **Run Container Aggregator**
    Jalankan *image* yang telah di-*build*.

    ```bash
    docker run -d \
      -p 7878:7878 \
      --name aggregator \
      kaiserin0/aggregator
    ```

3.  **Build dan Run Publisher (Opsional, untuk Pengujian)**
    Untuk mengirim *event* (termasuk simulasi duplikat) ke *aggregator*, Anda dapat membangun dan menjalankan *image publisher* secara terpisah.

    ```bash
    docker build -t kaiserin0/publisher -f Dockerfile.publisher .

    docker run --rm --network=host --env AGG_HOST=aggregator kaiserin0/publisher
    ```

## Endpoint API

Layanan *aggregator* mengekspos *endpoint* berikut di `http://localhost:7878`:

### `POST /publish`

Menerima satu atau *batch* *event* untuk diproses.

* **Body (Single Event)**:
    ```json
    {
      "topic": "string",
      "event_id": "unique-string",
      "timestamp": "ISO8601",
      "source": "string",
      "payload": { ... }
    }
    ```
* **Body (Batch Events)**:
    ```json
    [
      { "topic": "...", "event_id": "...", ... },
      { "topic": "...", "event_id": "...", ... }
    ]
    ```
* **Response**:
    ```json
    { "message": "Event(s) queued" }
    ```

### `GET /events`

Mengembalikan daftar semua *event* **unik** yang telah berhasil diproses.

* **Query Params**:
    * `topic`: Filter *event* berdasarkan nama *topic*. (Contoh: `/events?topic=system_logs`)
* **Response**:
    ```json
    [
      { "topic": "...", "event_id": "...", ... },
      ...
    ]
    ```

### `GET /stats`

Menampilkan statistik operasional dari *aggregator*.

* **Response**:
    ```json
    {
      "uptime_seconds": 120.5,
      "total_received": 5000,
      "unique_processed": 4000,
      "duplicate_dropped": 1000,
      "topics": {
        "system_logs": 2000,
        "user_activity": 2000
      }
    }
    ```

---

## Asumsi dan Keputusan Desain

* **Framework**: Layanan dibangun menggunakan **FastAPI** karena performa tinggi dan dukungan *async* bawaan, yang cocok untuk I/O *non-blocking* seperti penerimaan *event* dan pemrosesan *queue*.
* **Internal Queue**: Komunikasi antara *endpoint* `/publish` dan *consumer* internal menggunakan `asyncio.Queue` (in-memory). Ini memisahkan penerimaan *event* (respon cepat ke *publisher*) dari pemrosesan (deduplikasi dan penyimpanan).
* **Deduplication Store**: Persistensi untuk deduplikasi menggunakan *database* **SQLite** *file-based* (`./data/store.db`). Dipilih karena ringan, *embedded*, dan memenuhi syarat "lokal dalam *container*" serta "tahan *restart*" (selama *volume* di-*mount*).
* **Idempotency**: Idempotensi dicapai dengan memeriksa *primary key* `(topic, event_id)` di *database* SQLite *sebelum* memproses *event* dari *queue*. Jika `event_id` untuk `topic` tersebut sudah ada, *event* akan dilewati (dianggap duplikat) dan *counter* `duplicate_dropped` akan diinkremen.
* **Ordering**: Sistem ini **tidak menjamin *total ordering***. *Event* diproses berdasarkan urutan kedatangan (*arrival order*) di *internal queue*. Dalam konteks *log aggregator* ini, konsistensi akhir (memastikan semua *event* unik tercatat) dianggap lebih penting daripada urutan pemrosesan yang ketat.
* **Delivery Semantics**: *Publisher* (klien) dirancang untuk mensimulasikan *at-least-once delivery* dengan mengirimkan beberapa *event* dengan `event_id` yang sama. *Aggregator* (server) menjamin *effectively-once processing* melalui mekanisme *idempotency* dan *deduplication*.
