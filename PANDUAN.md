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
atau `python main.py bps`), terminal menampilkan **header tabel 3 kolom** yang
statusnya berubah di tempat:

```
 Script                        Status      Peringatan/Error
 ──────────────────────────────────────────────────────────
 src/extract/bps_scraper.py    ✔ selesai              0
 src/extract/kurs_scraper.py   ▶ jalan              2W 1E
 src/extract/gdelt_scraper.py  · menunggu              0
```

Kolom pertama menampilkan path modul, kolom kedua menunjukkan status
`menunggu` → `jalan` → `selesai` atau `error`, dan kolom ketiga menampilkan
jumlah peringatan (W) dan error (E) selama script itu berjalan — `0` bila
script berjalan bersih. Di bawah header ada blok pesan yang isinya **hanya**
peringatan dan error:

```
 ⚠ 15:10:38 WARNING  src.extract.kurs_scraper kurs request error untuk 2016-04-04
 ⚠ 15:10:38 ERROR    src.extract.gdelt_scraper HTTPError: 500
```

Header memakai **alternate screen**: tiap pembaruan menggambar ulang seluruh layar
dari atas (`home` + `clear`), sehingga tidak bergantung pada tinggi frame
sebelumnya. Ini disengaja — `rich.live.Live` menggeser kursor naik sebesar tinggi
frame sebelumnya, dan begitu terminal di-resize angka itu tidak lagi valid sehingga
frame lama tertinggal sebagai baris duplikat (kejadian nyata di terminal VSCode).
Akibatnya **mengubah ukuran terminal kapan pun aman**: memperkecil terminal hanya
memotong tampilan, memperbesar memulihkannya, dan tidak pernah menyisakan baris
ganda karena frame selalu digambar dari nol.

### Menyesuaikan ukuran terminal

Frame tidak pernah lebih tinggi dari terminal. Saat ruang tidak cukup, yang dipangkas
adalah isi — bukan judul kolom:

| Ruang kurang | Yang dikurangi |
| --- | --- |
|(script tidak muat seluruhnya) | baris script dari **atas**, urut tetap |
| pesan tidak muat seluruhnya | pesan **terbaru** dulu |
| terminal sangat sempit | garis pemisah `────` dilepas, penanda dipersingkat |

Baris penanda memberitahu apa yang disembunyikan, dan tidak pernah hilang diam-diam:

```
 … 6 script · 12 pesan belum tampil (detail di log/)
 … 6 skrip · 12 pesan                      ← terminal sempit, bentuk ringkas
```

Jadi kalau header terasa terpotong setelah terminal dikecilkan, **lebarkan lagi** —
bukan berarti ada yang rusak. Semua pesan tetap utuh di `log/`, dan saat program
selesai tabel final beserta **seluruh** pesan dicetak ke layar utama. Kalau pesan
menumpuk melebihi tinggi terminal, pesan terbaru yang terlihat. Terminal 1 baris
ditampilkan sebagai `terminal terlalu kecil`.

Menjalankan seluruh pipeline (3 tahap) menampilkan tiga kelompok baris
(`EXTRACT`/`TRANSFORM`/`LOAD`); menjalankan `--bronze` hanya menampilkan 10 baris
`EXTRACT`.

Warna: hijau = selesai, merah = error, kuning = sedang berjalan, redup = menunggu.
Tampilan ini memakai `rich` dan membutuhkan output berupa terminal sungguhan. Di
terminal lain — atau saat output di-pipe/di-redirect ke file — otomatis kembali ke
cetak ringkas per baris. Kendali lewat env:

| Env | Efek |
| --- | --- |
| `HOTEL_LIVE=0` | Matikan tampilan live, pakai cetak ringkas |
| `HOTEL_LIVE=print` | Pakai mode cetak, tanpa alternate screen: rencana dicetak sekali lalu tiap script yang selesai menambah satu baris |

> Alternate screen dibiarkan aktif bila program dibunuh paksa (`kill -9`,
> `taskkill`), sehingga terminal tampak kosong sampai Anda tekan Enter atau
> `/reset`. `HOTEL_LIVE=print` menghindari hal ini.

## Perintah `run` (typer) — Opsi Lengkap

```
python main.py run <sumber> [--stage all|extract|transform|load] [--period latest|YYYY|YYYY-YYYY] [--dry-run]
```

Contoh:
- `python main.py run bps`
- `python main.py run cuaca --stage extract`
- `python main.py run gdelt --period 20250101 --dry-run`