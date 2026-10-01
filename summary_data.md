# Summary Data Bronze — `data_lake/bronze`

Ringkasan keadaan data pada tahap **bronze** (mentah) per 28 September 2026.
Dibuat otomatis dari isi `data_lake/bronze/`. Struktur: `bronze/{sumber}/{dataset}/{periode}.parquet`.

---

## 1. Gambaran Umum

- **Jumlah sumber:** 10 (bps, kurs, gdelt, cuaca, hari_libur, event, poi, trends, transport, gmaps)
- **Jumlah file parquet:** 26
- **Total ukuran:** ± 1,67 MB (didominasi `kurs/kurs/2026.parquet` = 1,19 MB)
- **Rentang waktu keseluruhan:** **2014 – 2026** (2014 dari BPS wisman, 2026 dari BPS/event/kurs)
- **Catatan penting:** sebagian besar file berisi **artefak mode uji** — hasil `main.py [5]` / `main.py [1]` yang memangkas baris via `truncate_df` (`src/shared/storage.py`). Jumlah baris parquet **bukan** volume data sebenarnya, terutama untuk BPS yang menyimpan ribuan nilai dalam 1 kolom JSON.

Baris kolom terpisah:

- `baris parquet` = jumlah baris fisik file (mungkin terpangkas mode uji)
- `nilai seri` (BPS) = jumlah nilai seri mentah di kolom `datacontent_json` (volume nyata)
- `baris terurai` (BPS) = jumlah baris hasil parsing per kategori (setara silver)

---

## 2. Rincian per Sumber

### BPS (12 file: 6 dataset × 2 periode)

BPS menyimpan **1 baris per tahun**; data asli ada di `datacontent_json` (dict kunci→nilai), metadata di `payload_meta_json`. Kolom: `tahun, domain, var, datacontent_json, payload_meta_json`.

| Dataset | Periode file | Tahun di file | Nilai seri | Baris terurai | Dimensi kategori |
|---|---|---|---|---|---|
| asal_turis | 2023 | 2023 | 3.224 | 2.880 | 240 negara asal × 12 bulan |
| asal_turis | 2026 | 2020–2024 | 16.120 | 14.400 | 240 negara asal × 12 bulan |
| inflasi | 2023 | 2023 | 1.183 | 1.092 | ±90 kota + Indonesia × 12 bulan |
| inflasi | 2026 | 2018–2022 | 5.707 | 5.268 | ±90 kota + Indonesia × 12 bulan |
| lama_menginap | 2023 | 2023 | 24 | 24 | Tamu Asing + Domestik × 12 bulan |
| lama_menginap | 2026 | 2016–2021 | 120 | 120 | Tamu Asing + Domestik × 12 bulan |
| tpk_bintang | 2023 | 2023 | 72 | 60 | Hotel bintang 1–5 × 12 bulan |
| tpk_bintang | 2026 | 2018–2022 | 360 | 300 | Hotel bintang 1–5 × 12 bulan |
| wisman_pintu_masuk | 2023 | 2023 | 52 | 36 | 3 pintu masuk (Soekarno-Hatta, Halim, Priok) × 12 bulan |
| wisman_pintu_masuk | 2026 | 2014–2018 | 250 | 170 | 3 pintu masuk × 12 bulan |
| wisnus_tujuan | 2023 | 2023 | 91 | 72 | 6 wilayah DKI Jakarta × 12 bulan |
| wisnus_tujuan | 2026 | 2021–2025 | 448 | 360 | 6 wilayah DKI Jakarta × 12 bulan |

### Kurs (1 file) — data riil paling lengkap

`kurs/kurs/2026.parquet`

- **65.245 baris**, ukuran 1,19 MB
- Rentang **2016-01-04 s.d. 2026-09-25**, 26 kode mata uang
- Kolom: `tanggal, kode_mata_uang, nilai_satuan, kurs_beli, kurs_jual, kurs_tengah, tanggal_ekstraksi` — tanpa nilai null

### GDELT (4 file)

`gdelt/berita/{tanggal}.parquet` — sentimen & peristiwa berita (skor dampak ±, skor sentimen −12 s.d. +8)

| Periode | Baris | Status |
|---|---|---|
| 2026-09-22 | 475 | Riil penuh |
| 2026-09-23 | 5 | Artefak uji `[5]` |
| 2026-09-24 | 611 | Riil penuh |
| 2026-09-27 | 5 | Artefak uji `[5]` |

### GMAPS (3 file)

`gmaps/hotel_master/latest.parquet` (5 hotel master), `gmaps/harga/latest.parquet` (5 baris harga, 1 hotel: Grand Hyatt), `gmaps/review/latest.parquet` (2 review). Semua artefak/parsial mode uji.

### Sumber lain (artefak uji, 5 baris)

| Sumber | Dataset | Baris | Rentang | Catatan |
|---|---|---|---|---|
| cuaca | cuaca/2026 | 5 | jam 2016-01-01 00–04 | Hanya Jakarta Pusat |
| event | events/latest | 5 | 2026-09-16 s.d. 10-03 | Kategori & 5 kolom lain kosong (jiexpo) |
| hari_libur | hari_libur/2026 | 5 | 2021-05 s.d. 2022-05 | — |
| poi | poi/latest | 5 | — | — |
| transport | rute/latest | 5 | — | — |
| trends | search_intent/latest | 5 | 2016-01 s.d. 2016-05 | — |

---

## 3. Status Kualitas per Sumber

| Sumber | Volume parquet | Volume JSON | Status |
|---|---|---|---|
| kurs | 65.245 baris | — | ✅ **Riil penuh** (2016–2026) |
| bps | 6–12 tahun per dataset | ribuan nilai/dataset | ⚠️ Parsial (file `2026` terpangkas jadi 5 tahun; `2023` hanya 1 tahun) |
| gdelt | 2 hari penuh + 2 terpangkas | — | ⚠️ Parsial |
| cuaca / event / hari_libur / poi / transport / trends / gmaps | 5 baris | — | ⚠️ Artefak mode uji `[5]` |

---

## 4. Insight

1. **`kurs` adalah satu-satunya sumber produksi lengkap** — cocok untuk basis analisis makro (kurs tengah harian 2016–2026, 26 mata uang).
2. **BPS menyimpan volume nyata yang besar meski file-nya kecil**: mis. `asal_turis/2026` file hanya 128 KB tapi menampung 16.120 nilai kunjungan wisman per negara×bulan untuk 2020–2024. Parser di `src/transform/bps_transform.py` sudah mampu mengurai ke baris silver (contoh: 14.400 baris untuk asal_turis 2026).
3. **File `bps/*/2026.parquet` hanya sampai tahun 2024–2025** (terpangkas 5 baris karena `[5]`), padahal konfigurasi `start_year` di `config/sources.yaml` menargetkan sejak 2014–2021. Rentang penuh (mis. 2020–2026 untuk asal_turis) baru akan terisi setelah run tanpa limit.
4. **GDELT hanya punya 2 hari penuh** (22 & 24 Sep 2026) — sensor sentimen berita belum mewakili tren jangka panjang.
5. **gmaps** masih tahap uji: hanya 1–5 hotel dari 13 yang terdaftar di `config/sources.yaml`; `hotel_master` kebetulan berisi 5 hotel pertama.
6. **event** berisi data 1 venue (jiexpo) dengan banyak kolom null (`kategori`, `url_gambar`, `tanggal_unggah`, `latitude/longitude`).
7. **cuaca** dan **trends** hanya beberapa jam/bulan pertama dari riwayat (terpangkas di awal).

---

## 5. Rekomendasi

- **Isi ulang bronze secara penuh**: jalankan `python main.py` (tanpa `[N]`) agar `truncate_df` tidak memangkas. Prioritas: BPS (untuk melengkapi tahun ≤ 2026), gdelt, dan seluruh sumber 5-baris.
- Untuk hanya BPS: setiap dataset akan memuat riwayat `start_year..tahun-berjalan` (lihat `config/sources.yaml`).
- Setelah bronze lengkap, jalankan kembali **transform** agar silver ikut terisi penuh.
- Cek `config/.env` untuk `BPS_API_KEY` sebelum mengeksekusi BPS.