# Hotel Data Platform

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Branch](https://img.shields.io/badge/Branch-develop-green.svg)](https://github.com/your-org/hotel_data_platform/tree/develop)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> **Pipeline Data Engineering Tahap Bronze** untuk mengumpulkan data hotel dan pendukung (Google Maps, Google Trends, Traveloka Review, GDELT, BPS, BI Kurs, Cuaca, Hari Libur, POI, Transport, Event) dengan ketahanan tinggi: checkpointing, auto-resume, graceful interrupt handling, dan multi-channel alerting.

---

## 📋 Daftar Isi
- [Overview & Arsitektur](#-overview--arsitektur)
- [Fitur Utama](#-fitur-utama-key-features)
- [Struktur Proyek](#-struktur-proyek)
- [Instalasi & Konfigurasi](#-instalasi--konfigurasi)
- [Panduan Eksekusi](#-panduan-eksekusi-usage)
- [Penjadwalan via Crontab](#-penjadwalan-via-crontab)
- [Data Lake Layout](#-data-lake-layout)
- [Troubleshooting](#-troubleshooting)

---

## 🏗️ Overview & Arsitektur

**Hotel Data Platform** adalah pipeline **extract-only (Bronze layer)** yang menarik data dari 12 sumber eksternal dan menyimpannya ke **Data Lake berbasis Parquet** dengan struktur partisi per sumber, dataset, dan periode.

```mermaid
flowchart TD
    subgraph EXT[EXTRACT BRONZE]
        BPS[BPS Web API\n6 dataset]
        KURS[BI Kurs XML\n1 dataset]
        GDELT[GDELT 2.0\nEvent Indonesia]
        CUACA[Open-Meteo Archive\n5 wilayah DKI]
        HARILIBUR[Kalender Libur\nTahunan]
        TRENDS[Google Trends\nPlaywright]
        GMAPS[Google Travel Harga\nPlaywright]
        TRAVELREV[Traveloka Review\nAPI v2 + Cookie]
        TRANSPORT[Moovit\nPlaywright]
        POI[Overpass API\nPOI Jakarta]
        EVENT[Event Data\nAPI]
    end

    subgraph STORAGE[DATA LAKE PARQUET]
        BRONZE[(data_lake/bronze/\n{source}/{dataset}/{YYYY}.parquet)]
        STATE[(data_lake/_state/\ncheckpoint + staging)]
    end

    EXT -->|write_parquet_by_year<br/>append + dedupe| BRONZE
    EXT -->|Checkpoint per item<br/>save_every=1| STATE

    subgraph ORCH[ORKESTRASI]
        MAIN[main.py\nTyper CLI]
        PARALLEL[parallel.py\nProcessPoolExecutor\n4 workers default]
        WRAPPER[run_pipeline.sh\nRetry 3x + Alerting]
    end

    MAIN --> PARALLEL
    WRAPPER --> MAIN
```

### Karakteristik Utama
| Aspek | Detail |
|-------|--------|
| **Arsitektur** | Medallion (Bronze only; Silver/Gold placeholder di `src/silver/`, `src/gold/`) |
| **Format Storage** | Apache Parquet (columnar, compressed, schema evolution) |
| **Partisi** | Per tahun (`{YYYY}.parquet`) untuk grain `year`; single file (`data.parquet`) untuk grain `snapshot` |
| **Eksekusi** | Paralel 4 worker default (`ProcessPoolExecutor`); override via `HOTEL_WORKERS` env |
| **State Management** | JSON checkpoint di `data_lake/_state/{source}.json` + staging parts per run |
| **Scheduling** | CLI flags (`--harian`, `--bulanan`, `--tahunan`) + `run_pipeline.sh` wrapper untuk cron |

---

## ⭐ Fitur Utama (Key Features)

### 1. Robust Checkpointing (`src/shared/checkpoint.py`)
- **Per-item checkpoint**: Setiap hotel/tanggal/bulan yang selesai dicatat (`save_every=1` default).
- **State file**: `data_lake/_state/{source}.json` berisi `tanggal`, `backfill`, `jumlah`, `selesai[]`.
- **Backfill mode**: `--backfill` abaikan validasi tanggal checkpoint (untuk backfill multi-hari/tahun).
- **Toleransi cross-midnight**: Default 1 hari toleransi untuk run yang melewati tengah malam.

### 2. Auto-Resume (`--lanjut` / `--resume`)
```bash
# Lanjutkan dari titik terakhir (skip hotel/tanggal yang sudah selesai)
python main.py --lanjut
python main.py gmaps --lanjut
./run_pipeline.sh --harian --bronze --lanjut
```
- Load checkpoint → filter item yang sudah `sudah()` → lanjutkan sisanya.
- Otomatis bersihkan checkpoint jika semua item selesai.

### 3. Graceful Interrupt Handling (SIGINT/SIGTERM)
- **Signal handlers** di `main.py` dan `parallel.py/_init_worker()`.
- Saat `Ctrl+C` (SIGINT) atau `SIGTERM` (systemd/kubernetes):
  1. Trap signal
  2. `force_save_all_checkpoints()` → flush semua dirty checkpoint ke disk
  3. Exit dengan code `128 + signum`
- **Windows-compatible**: `signal.SIGTERM` dicek `hasattr(signal, 'SIGTERM')` sebelum register.

### 4. Auto-Retry & Multi-Channel Alerting (`run_pipeline.sh` + `pipeline_alert.py`)
| Komponen | Perilaku |
|----------|----------|
| **Retry** | 3× percobaan (default), jeda 30 detik (`MAX_RETRIES`, `RETRY_DELAY` env) |
| **Alert Priority** | 1️⃣ Telegram Bot → 2️⃣ Discord Webhook → 3️⃣ Generic Webhook → 4️⃣ File Log (`logs/pipeline_alerts.log`) |
| **Payload** | Timestamp, schedule, exit code, pesan error (HTML untuk Telegram, JSON untuk webhook) |
| **Konfigurasi** | Via `config/.env` (lihat [Instalasi](#-instalasi--konfigurasi)) |

---

## 📁 Struktur Proyek

```
hotel_data_platform/
├── main.py                    # Entry point CLI (Typer + argparse fallback)
├── run_pipeline.sh            # Wrapper bash: retry + alerting
├── requirements.txt           # 11 dependencies utama
├── config/
│   ├── sources.yaml           # Definisi 12 sumber: enabled, schedule, grain, params
│   ├── .env.example           # Template environment variables
│   └── .env                   # **Jangan commit** - isi credentials di sini
├── src/
│   ├── pipeline.py            # Registry _PIPES (source -> bronze module) + run_stage()
│   ├── parallel.py            # Paralel execution + live view (rich)
│   ├── shared/                # Utilitas bersama (dipakai semua scraper)
│   │   ├── config.py          # Load YAML + .env, get_source_config()
│   │   ├── db.py              # PostgreSQL upsert, connection pool
│   │   ├── storage.py         # Parquet I/O, FileLock, write_parquet_by_year()
│   │   ├── checkpoint.py      # Checkpoint class + global registry
│   │   ├── staging.py         # Intermediate staging parts per run
│   │   ├── periods.py         # Normalisasi periode (year/month/day/historis)
│   │   ├── hotel_master.py    # Baca registry hotel dari DB (public.hotel_master)
│   │   ├── run_options.py     # Global flags: dry_run, resume, historis, dll
│   │   ├── row_limit.py       # Test mode row limiting
│   │   ├── logging_utils.py   # Structured logging setup
│   │   ├── live_view.py       # In-place terminal UI (rich)
│   │   ├── pipeline_alert.py  # Alert notifier (Telegram/Discord/Webhook/File)
│   │   └── dataset_filter.py  # Dataset filtering utility
│   ├── bronze/                # 12 Extractors (kontrak: extract() -> dict[dataset, DataFrame])
│   │   ├── bps_scraper.py           # BPS Web API (6 dataset statistik)
│   │   ├── kurs_scraper.py          # BI Kurs harian (XML)
│   │   ├── cuaca_scraper.py         # Open-Meteo hourly (5 wilayah DKI)
│   │   ├── gdelt_scraper.py         # GDELT 2.0 15-min windows (Indonesia only)
│   │   ├── hari_libur_scraper.py    # Kalender libur nasional (ICS)
│   │   ├── trends_scraper.py        # Google Trends (Playwright)
│   │   ├── gmaps_harga_scraper.py   # Google Travel harga OTA (Playwright)
│   │   ├── gmaps_browser.py         # Shared browser utilities
│   │   ├── traveloka_review_scraper.py # Traveloka Review API v2 + cookie auth
│   │   ├── transport_scraper.py     # Transport Moovit (Playwright)
│   │   ├── poi_scraper.py           # POI via Overpass API
│   │   └── event_scraper.py         # Event data
│   ├── silver/                # Transform layer (placeholder)
│   └── gold/                  # Load/Serving layer (placeholder)
├── data_lake/
│   ├── bronze/                # Output Parquet per sumber/dataset/tahun
│   └── _state/                # Checkpoint JSON + staging parts
├── logs/                      # Log per stage + pipeline_alerts.log
├── tools/                     # Utility scripts (kosong - sudah dibersihkan)
└── .venv/                     # Virtual environment (gitignored)
```

---

## ⚙️ Instalasi & Konfigurasi

### 1. Prasyarat
- **Python 3.11+** (tested on 3.13)
- **PostgreSQL** (untuk layer Gold & `hotel_master` registry)
- **Playwright Chromium** (untuk scraper browser-based)

### 2. Setup Virtual Environment
```bash
# Windows PowerShell
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Linux/macOS / Git Bash
python -m venv .venv
source .venv/bin/activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
playwright install chromium
```

### 4. Konfigurasi Environment (`config/.env`)
```bash
cp config/.env.example config/.env
# Edit config/.env dengan editor favorit
```

**Variabel wajib:**
```ini
# PostgreSQL (Gold layer + hotel_master)
DB_HOST=localhost
DB_PORT=5432
DB_NAME=hotel_data
DB_USER=postgres
DB_PASS=your_password
DB_SCHEMA_GOLD=gold

# BPS Web API (wajib untuk sumber bps)
BPS_API_KEY=your_bps_api_key
```

**Variabel opsional (Alerting via `run_pipeline.sh`):**
```ini
# Telegram Bot (buat via @BotFather)
TELEGRAM_BOT_TOKEN=123456:ABC-DEF...
TELEGRAM_CHAT_ID=-1001234567890

# Discord Webhook (Server Settings > Integrations > Webhooks)
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/xxx/yyy

# Generic Webhook (custom monitoring server)
ALERT_WEBHOOK_URL=https://your-monitoring-server.com/api/pipeline-alert
```

### 5. Inisialisasi Database (Gold Schema)
```bash
python main.py db-init
# Output: Schema 'gold' siap di localhost:5432/hotel_data
```

### 6. Siapkan Hotel Master Registry
Table `public.hotel_master` harus terisi minimal kolom:
```sql
id_kompetitor, nama_hotel, alamat, kota, latitude, longitude, traveloka_object_id
```
Digunakan oleh: `gmaps_harga_scraper`, `traveloka_review_scraper`.

---

## 🚀 Panduan Eksekusi (Usage)

### A. Manual Execution (PowerShell / CMD / Bash)

#### Jalankan Semua Sumber (Full Pipeline)
```bash
# Normal (semua sumber enabled, parallel 4 workers)
python main.py

# Mode uji: maks 5 baris per dataset, TIDAK tulis ke DB Gold
python main.py [5]

# Hanya stage Bronze (extract only)
python main.py --bronze
python main.py --bronze [5]
```

#### Jalankan Sumber Tertentu
```bash
# Satu sumber (PowerShell butuh kutip untuk kurung)
python main.py "(gmaps)"
python main.py "(traveloka_review)"

# Satu sumber + batas baris
python main.py "(gmaps[5])"
python main.py "gmaps[5]"      # PS-native tanpa kurung

# Format typer asli
python main.py run bps
python main.py run cuaca --stage extract
python main.py run gdelt --period 20250101 --dry-run
```

#### Resume / Lanjutkan Run Terputus
```bash
# Lanjutkan semua dari checkpoint
python main.py --lanjut

# Lanjutkan sumber tertentu
python main.py gmaps --lanjut
python main.py traveloka_review --lanjut

# Kombinasi dengan mode uji
python main.py --lanjut [5]
```

#### Opsi Lanjutan
| Flag | Fungsi |
|------|--------|
| `--dry-run` | Jalankan tanpa menulis file/DB |
| `--headful` | Tampilkan browser (debug selector Playwright) |
| `--force-login` | Login fresh via browser (Traveloka) |
| `--headless` | Login headless (butuh cookie valid) |
| `--keep-browser-open` | Jaga browser terbuka setelah login |
| `--backfill` | Mode backfill historis (abaikan validasi tanggal checkpoint) |
| `--historis` | Full backfill dari `start_year` config s.d. hari ini |
| `--test` | Mode test: batas 3 baris per dataset, tetap tulis parquet |

### B. Wrapper Script (`run_pipeline.sh`) - **Recommended for Production/Cron**

```bash
# Default: --harian --bronze --lanjut (3 retry, 30s delay)
./run_pipeline.sh

# Explisit schedule
./run_pipeline.sh --harian --bronze --lanjut
./run_pipeline.sh --bulanan --bronze --lanjut
./run_pipeline.sh --tahunan

# Custom args (forward ke main.py)
./run_pipeline.sh --bulanan --bronze --lanjut --force-login

# Override retry config via env
MAX_RETRIES=5 RETRY_DELAY=60 ./run_pipeline.sh --harian --bronze --lanjut
```

**Exit codes:**
- `0` = Sukses
- `1` = Gagal setelah semua retry (alert sudah terkirim)

### C. PowerShell Notes
```powershell
# Kurung WAJIB dikutip di PowerShell
python main.py "(poi)"        # ✅
python main.py (poi)          # ❌ PowerShell parse sebagai subexpression

# Kurung siku AMAN tanpa kutip
python main.py [5]            # ✅

# Bentuk tanpa kurung juga aman
python main.py gmaps[5]       # ✅
```

---

## ⏰ Penjadwalan via Crontab (Linux/macOS)

### Contoh Entri Crontab
```bash
# Edit crontab
crontab -e

# ============================================================
# HARIAN (jam 02:30 WIB) - sources: bps, kurs, gdelt, cuaca, event, trends, gmaps
# ============================================================
30 2 * * * cd /path/to/hotel_data_platform && \
  MAX_RETRIES=3 RETRY_DELAY=30 \
  ./run_pipeline.sh --harian --bronze --lanjut >> logs/cron_harian.log 2>&1

# ============================================================
# BULANAN (tanggal 1 jam 03:00 WIB) - sources: traveloka_review, poi, transport, hari_libur
# ============================================================
0 3 1 * * cd /path/to/hotel_data_platform && \
  MAX_RETRIES=3 RETRY_DELAY=60 \
  ./run_pipeline.sh --bulanan --bronze --lanjut >> logs/cron_bulanan.log 2>&1

# ============================================================
# TAHUNAN (1 Jan jam 04:00 WIB) - sources: bps (full), hari_libur (full)
# ============================================================
0 4 1 1 * cd /path/to/hotel_data_platform && \
  MAX_RETRIES=2 RETRY_DELAY=120 \
  ./run_pipeline.sh --tahunan >> logs/cron_tahunan.log 2>&1
```

### Tips Produksi
- **Log rotation**: Gunakan `logrotate` untuk `logs/*.log` dan `logs/pipeline_alerts.log`
- **Monitoring**: Cek `logs/pipeline_alerts.log` untuk riwayat kegagalan
- **Timezone**: Server disarankan set `TZ=Asia/Jakarta` (cocok dengan `config/sources.yaml` default)

---

## 🗄️ Data Lake Layout

```
data_lake/
├── bronze/
│   ├── bps/
│   │   ├── asal_turis/{YYYY}.parquet
│   │   ├── inflasi/{YYYY}.parquet
│   │   ├── lama_menginap/{YYYY}.parquet
│   │   ├── tpk_bintang/{YYYY}.parquet
│   │   ├── wisman_pintu_masuk/{YYYY}.parquet
│   │   └── wisnus_tujuan/{YYYY}.parquet
│   ├── kurs/
│   │   └── kurs/{YYYY}.parquet
│   ├── gdelt/
│   │   └── berita/{YYYY}.parquet          # append + dedupe global_event_id
│   ├── cuaca/
│   │   └── cuaca/{YYYY}.parquet
│   ├── hari_libur/
│   │   └── hari_libur/{YYYY}.parquet
│   ├── trends/
│   │   └── trends/{YYYY}.parquet
│   ├── gmaps/
│   │   ├── harga/{YYYY}.parquet           # append + dedupe (id_kompetitor, checkin_date, sumber, scrape_date)
│   │   └── hotel_master/latest.parquet    # snapshot
│   ├── traveloka_review/
│   │   └── review/{YYYY-MM}.parquet       # grain month
│   ├── transport/
│   │   └── transport/data.parquet         # snapshot
│   ├── poi/
│   │   └── poi/data.parquet               # snapshot
│   └── event/
│       └── events/{YYYY}.parquet
└── _state/
    ├── bps.json              # Checkpoint per sumber
    ├── gmaps.json
    ├── gmaps_harga.json
    ├── traveloka_review.json
    ├── gdelt.json
    ├── gmaps_review/         # Staging parts per run (per hotel)
    │   └── 2026-10-07_2026-10/
    │       ├── HTL0001_abc123.parquet
    │       └── ...
    └── trends.json
```

### Grain Penamaan File (lihat `config/sources.yaml`)
| Grain | Pola File | Contoh Sumber |
|-------|-----------|---------------|
| `year` | `{dataset}/{YYYY}.parquet` | bps, kurs, gdelt, cuaca, hari_libur, trends, gmaps, event |
| `month` | `{dataset}/{YYYY-MM}.parquet` | traveloka_review |
| `snapshot` | `{dataset}/data.parquet` | transport, poi, gmaps/hotel_master |

---

## 🔧 Troubleshooting

### 1. `BPS_API_KEY kosong`
```bash
# Isi di config/.env
BPS_API_KEY=your_key_from_bps_web_api
```

### 2. Traveloka Review: `Cookie tidak ditemukan`
```bash
# Opsi 1: Force login via browser (interaktif)
python main.py traveloka_review --force-login

# Opsi 2: Simpan cookie manual ke ~/.traveloka_cookies.json
# Format: JSON array Playwright cookies ATAU string "name=val; name2=val2"
```

### 3. Playwright: `Browser closed / Page crashed`
- Sudah di-handle: `browser_args` include `--disable-dev-shm-usage`, `--renderer-process-limit=1`
- Jika tetap error: kurangi `harga_workers` di `config/sources.yaml` (default 2)

### 4. GDELT Backfill Lambat
- 1 hari = 96 window × 15 menit = ~1400 request
- Gunakan `--backfill` + `--lanjut` untuk resume bertahap
- Atur `max_window` di `config/sources.yaml` (default 96)

### 5. Checkpoint Corrupt / Ingin Reset
```bash
# Hapus checkpoint sumber tertentu
python main.py checkpoint-clear gmaps
python main.py checkpoint-clear traveloka_review

# Atau hapus manual
rm data_lake/_state/gmaps.json data_lake/_state/gmaps_harga.json
```

### 6. Mode Uji Tidak Menulis Parquet
- `[N]` / `--test` / `--dry-run` / `--check` → bronze **TIDAK** ditulis ke disk
- Log akan menampilkan: `(mode check: bronze TIDAK ditulis)`
- Untuk menulis parquet di mode uji: gunakan `[N]` **tanpa** `--test`/`--dry-run`

### 7. Windows: `SIGTERM` Not Available
- Normal: `hasattr(signal, 'SIGTERM')` check di `main.py` & `parallel.py`
- Hanya `SIGINT` (Ctrl+C) yang aktif di Windows

---

## 📚 Referensi Cepat: Sumber Data & Schedule

| Sumber | Schedule | Grain | Auth | Key Config |
|--------|----------|-------|------|------------|
| **bps** | Bulanan | Year | API Key | `BPS_API_KEY`, 6 dataset, `start_year` per dataset |
| **kurs** | Harian | Year | - | `start_year: 2014`, fallback 7 hari |
| **gdelt** | Harian | Year | - | `start_year: 2018`, 96 window/hari, checkpoint per hari |
| **cuaca** | Harian | Year | - | 5 wilayah DKI, Open-Meteo Archive |
| **hari_libur** | Tahunan | Year | - | `start_year: 2016`, ICS parsing |
| **trends** | Harian | Year | Playwright | 44 destinasi, `jeda_antar_destinasi: 20s` |
| **gmaps** | Harian | Year | Playwright | `lead_times: [1..7,30,60]`, `harga_workers: 2` |
| **traveloka_review** | Bulanan | Month | Cookie + API | `max_pages_per_hotel: 50`, butuh `traveloka_object_id` di DB |
| **transport** | Bulanan | Snapshot | Playwright | Moovit Jakarta |
| **poi** | Bulanan | Snapshot | - | Overpass API, `overpass_workers: 1` |
| **event** | Harian | Year | - | Event API |

---

## 🤝 Kontribusi & Development

```bash
# Branch develop untuk fitur baru
git checkout develop

# Buat feature branch
git checkout -b feature/nama-fitur

# Commit dengan conventional commits
git commit -m "feat(gmaps): add new OTA parser for Agoda"

# Push & buat PR ke develop
git push origin feature/nama-fitur
```

### Code Style
- Python 3.11+ type hints mandatory
- `ruff` / `black` untuk formatting (belum dikonfigurasi)
- Log messages dalam Bahasa Indonesia (konsisten dengan tim)

---

## 📄 Lisensi
MIT License - lihat file [LICENSE](LICENSE) untuk detail.

---

**Maintainer**: Tim Data Engineering JXB  
**Last Updated**: Oktober 2026 (branch `develop`)