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

Saat menjalankan beberapa tahap sekaligus (`python main.py`, `python main.py [10]`,
atau `python main.py bps`), terminal menampilkan **blok rencana** yang dicetak
**satu kali** di awal: kelompok `EXTRACT`/`TRANSFORM`/`LOAD` dengan satu baris per
script, semuanya berstatus `menunggu`:

```
 EXTRACT
· src/extract/bps_scraper.py  menunggu
· src/extract/kurs_scraper.py  menunggu
```

Sudah tidak ada lagi redraw in-place. Sesudah blok itu, setiap script yang
selesai atau gagal menambah **satu baris baru** di bawahnya, dan setiap pesan
peringatan langsung tercetak seperti log biasa:

```
✔ src/extract/bps_scraper.py  selesai — 9 dataset 1.204 baris (2.4 dtk)
⚠ 11:30:23 WARNING  src.extract.kurs_scraper kurs request error untuk 2016-04-04 ...
✖ src/extract/gdelt_scraper.py  error — HTTPError: 500 (12.1 dtk)
```

Karena tidak ada baris yang ditulis ulang, mengubah ukuran terminal kapan pun
tidak merusak tampilan dan tidak pernah menyisakan duplikat. Warna: hijau =
selesai, merah = gagal, kuning = peringatan. Status yang dipakai: `menunggu`,
`dijalankan`, `selesai`, `error`.

Blok rencana ini memakai `rich` untuk pewarnaan dan membutuhkan output berupa
terminal sungguhan. Di terminal lain — atau saat output di-pipe/di-redirect ke
file — otomatis kembali ke cetak ringkas per baris. Matikan paksa dengan env
`HOTEL_LIVE=0`.

## Perintah `run` (typer) — Opsi Lengkap

```
python main.py run <sumber> [--stage all|extract|transform|load] [--period latest|YYYY|YYYY-YYYY] [--dry-run]
```

Contoh:
- `python main.py run bps`
- `python main.py run cuaca --stage extract`
- `python main.py run gdelt --period 20250101 --dry-run`