# PANDUAN Menjalankan Pipeline `hotel_data_platform`

Panduan singkat baris perintah. Jalankan semua perintah dari folder proyek
(tempat `main.py` berada).

## Perintah Utama

| Perintah (PowerShell) | Arti |
| --- | --- |
| `python main.py` | Jalankan SEMUA sumber data (bps, kurs, hari_libur, gdelt, cuaca, event, poi, trends, transport, gmaps) lengkap |
| `python main.py [5]` | Jalankan semua sumber, maks **5 baris per dataset** (mode uji) |
| `python main.py "(poi)"` | Jalankan **poi saja** |
| `python main.py "(hari_libur[5])"` | Jalankan **hari_libur saja**, maks 5 baris |
| `python main.py "gmaps[5]"` | Bentuk lain tanpa kurung (PS-native): gmaps saja, maks 5 baris |
| `python main.py cuaca` | Jalankan cuaca saja (tanpa batas) |
| `python main.py run bps` | Perintah typer asli: satu sumber dengan pilihan stage/period (lihat di bawah) |
| `python main.py list` | Daftar sumber yang terdaftar |
| `python main.py db-init` | Buat schema `gold` di PostgreSQL |

## Catatan Pemakaian di PowerShell

- **Kurung `( )` wajib diberi tanda kutip** di PowerShell. Tanpa kutip,
  PowerShell menganggap `(poi)` sebagai perintah dan gagal:
  `python main.py (poi)`  ❌ → `python main.py "(poi)"` ✅
- **Kurung siku `[ ]` aman tanpa kutip** di PowerShell: `python main.py [5]` ✅
- Bentuk **tanpa kurung** (`poi`, `gmaps[5]`) juga aman tanpa kutip.
- Di **cmd.exe / Command Prompt** kurung tidak perlu dikutip, tetapi menambahkan
  kutip selalu lebih aman.

## Arti `[N]` — Batas Baris (Mode Uji)

Menjalankan dengan `[N]` (mis. `python main.py [5]` atau `"(cuaca[5])"`) adalah
**mode uji cepat**:

- **Extract** → potong hasil menjadi maks N baris; sumber berat berhenti lebih
  dini (lihat bawah) agar tes tidak menunggu lama.
- **Transform** → membaca bronze terpotong, sehingga silver juga maks N baris.
- **Load → TIDAK menulis ke database sama sekali.** Tidak ada INSERT, tidak ada
  DELETE. Tabel `gold` tidak tersentuh (aman walau sudah berisi data penuh).
- Run penuh (tanpa `[N]`) tetap: bronze → silver → **upsert ke `gold`**.

> Saran: gunakan mode `[N]` untuk memastikan pipeline (extract→transform)
> bekerja dengan cepat dan aman sebelum menjalankan run penuh.

## Perilaku per Sumber di Mode `[N]`

| Sumber | Perilaku mode uji |
| --- | --- |
| poi | Berhenti setelah N POI terkumpul (kategori tersisa dibatalkan) |
| trends | Berhenti setelah N baris; destinasi tersisa dilewati |
| gmaps | N hotel pertama saja; 1 skenario tanggal; ulasan dibatasi |
| transport | N rute pertama saja |
| bps/kurs/hari_libur/gdelt/cuaca/event | Ekstraksi penuh, hasil di-potong saat ditulis (cepat) |

## Tampilan Terminal

Tampilan program **tidak pernah menulis ulang di tempat**: tidak ada alternate
screen, tidak ada `clear` layar, tidak ada penulisan ulang baris. Semua keluaran
mengalir ke bawah seperti perintah biasa, sehingga **isi terminal dan log perintah
sebelumnya tetap terlihat** dan bisa di-scroll seperti biasa, dan mengubah ukuran
terminal tidak mungkin merusak tampilan.

Saat menjalankan beberapa tahap sekaligus (`python main.py`, `python main.py [10]`,
atau `python main.py bps`), program mencetak **blok rencana** sekali di awal —
dikelompokkan per tahap, lalu satu baris per script:

```
 EXTRACT
 · src/extract/bps_scraper.py  menunggu
 · src/extract/kurs_scraper.py  menunggu
 TRANSFORM
 · src/transform/bps_transform.py  menunggu
```

Baris `menunggu` di atas tidak pernah ditulis ulang. Setiap script yang selesai
menambah **satu baris baru di bawahnya**:

```
 ✔ src/extract/bps_scraper.py  selesai — 5 dataset 120 baris (3.2 dtk)
 ✖ src/extract/kurs_scraper.py  error — HTTPError: 500 (1.1 dtk)
 ✔ src/transform/bps_transform.py  selesai — 2 dataset 40 baris (0.4 dtk)
```

Hanya status final yang jadi baris baru; status sedang berjalan diam saja supaya
keluaran tidak membanjiri. Warna: hijau = selesai, merah = error, redup = menunggu.

Peringatan dan error mengalir ke bawah berurutan dengan progres, memakai cap waktu:

```
 ⚠ 15:10:38 WARNING  src.extract.kurs_scraper request error untuk 2016-04-04
 ⚠ 15:10:38 ERROR    src.extract.gdelt_scraper HTTPError: 500
```

Semua pesan ini juga tersimpan penuh di `log/<stage>/<sumber>.log`, termasuk
detail DEBUG/INFO yang tidak tampil di terminal.

Menjalankan seluruh pipeline (3 tahap) menampilkan tiga kelompok baris
(`EXTRACT`/`TRANSFORM`/`LOAD`); menjalankan `--bronze` hanya menampilkan kelompok
`EXTRACT`. Setelah semua tahap selesai, `RINGKASAN RUN` dicetak di bawah blok
rencana.

Simbol memakai Unicode (`· ✔ ✖ ⚠`) bila encoding terminal mendukung UTF-8; di
terminal ber-encoding lain (mis. CP-437) diganti ASCII (`. + x !`) agar tidak
berantakan. Tampilan ini memakai `rich` dan membutuhkan output berupa terminal
sungguhan. Di terminal lain — atau saat output di-pipe/di-redirect ke file —
otomatis kembali ke cetak ringkas per baris:

```
| extract  | bps | OK | 120 baris | 3.2 dtk
```

Kendali lewat env:

| Env | Efek |
| --- | --- |
| `HOTEL_LIVE=0` | Matikan tampilan, pakai cetak ringkas per baris |
| `HOTEL_LIVE=print` | Sama seperti default (blok rencana + baris hasil); nilai ini dulu menunjuk mode cetak saat masih ada alternate screen, sekarang diterima sebagai alias |

## Perintah `run` (typer) — Opsi Lengkap

```
python main.py run <sumber> [--stage all|extract|transform|load] [--period latest|YYYY|YYYY-YYYY] [--dry-run]
```

Contoh:
- `python main.py run bps`
- `python main.py run cuaca --stage extract`
- `python main.py run gdelt --period 20250101 --dry-run`