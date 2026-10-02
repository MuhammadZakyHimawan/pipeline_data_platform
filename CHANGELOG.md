# CHANGELOG - Catatan Perubahan `hotel_data_platform`

Berisi catatan perubahan **kode & konfigurasi**, ditulis manual.
Bukan log runtime eksekusi pipeline (yang ada di folder `log/`).

|fix| File : src/shared/live_view.py [02/10/2026 14:45:00]
         Mode alternate screen dibongkar total: Control.alt_screen(enable=True) di mulai() dan redraw home+clear di _gambar() dihapus, begitu juga dukung_layar(), _MODE_CETAK, mesin frame (_gambar, _render, _tinggi_tabel, _atur, _potong_pesan, _tinggi, _pilih_penanda, _penanda_panjang, _penanda_ringkas), tabel 3 kolom yang hanya hidup di frame (_tabel, _sel_status, _sel_masalah, _simbol_status), _hitung() beserta field RencanaBaris.n_warning/.n_error/.n_masalah, konstanta _LEVEL_PERINGATAN/_LEVEL_ERROR/_GAYA_STATUS, kunci simbol "jalan", dan impor rich box/Table/Group/RenderableType/Control serta sumber_dari_logger; TampilanPipeline kini hanya cetak: blok rencana sekali di mulai(), satu baris hasil per stage final di muat(), pesan WARNING+/ERROR berurutan ke bawah di pesan(); parameter langsung dihapus dari __init__; main.py dan src/parallel.py tidak berubah karena API publik (dukung_live, RencanaBaris, mulai, muat, pesan, berhenti) tetap sama
         Alasan : Alternate screen adalah buffer layar terpisah sehingga selama program berjalan seluruh isi terminal sebelumnya (log perintah sebelumnya + scrollback) tidak terlihat, dan clear() menghapus layar — keluhan "hanya header yang terlihat, log perintah sebelumnya tidak bisa dilihat". Setelah pembongkaran ini tidak ada penulisan absolut sama sekali: semua keluaran mengalir ke bawah seperti perintah biasa, isi sebelumnya tetap terlihat dan bisa di-scroll, resize terminal tidak mungkin merusak tampilan, dan warning/error ikut tersimpan di scrollback

|update| File : src/shared/logging_utils.py [02/10/2026 14:45:00]
         Docstring sumber_dari_logger() diperbarui: tidak lagi menyebut pemakaian di src/shared/live_view.py untuk menempelkan jumlah peringatan/error, karena hitungan itu ikut terhapus bersama frame-nya; fungsi ini kini hanya dipakai _record_target()
         Alasan : Deskripsi ini mendokumentasikan pemakaian yang sudah tidak ada sehingga pembacaan kode bisa meleset

|update| File : PANDUAN.md [02/10/2026 14:45:00]
         Bagian "Tampilan Terminal" ditulis ulang: blok rencana sekali per tahap, satu baris hasil per script yang selesai, pesan WARNING/ERROR berurutan dengan progres, catatan simbol Unicode vs ASCII, cetak ringkas saat output bukan terminal, tabel env HOTEL_LIVE=0/print; bagian "Menyesuaikan ukuran terminal", tabel 3 kolom status/peringatan, penjelasan alternate screen & rich.live, dan peringatan terminal kosong saat program dibunuh paksa dihapus karena tidak berlaku lagi
         Alasan : Dokumentasi harus mengikuti perilaku tampilan terbaru; pemangkasan frame dan alternate screen tidak ada lagi sehingga pengguna tidak lagi mencari batas terminal atau reset terminal yang tak pernah dibutuhkan

|fix| File : src/shared/live_view.py [01/10/2026 16:05:00]
         _render() dibatasi tinggi terminal: tinggi tabel diukur nyata dengan render_lines(), prefix script terpanjang dicari lewat pencarian biner (_atur()), pesan dipotong dari yang terbaru dan setinggi-tingginya diukur satu per satu (_potong_pesan()), tinggi tabel dicache per (lebar, pakai_garis, jumlah baris) di _tinggi_tabel(); _render(penuh=True) dipakai berhenti() supaya frame akhir ke layar utama tidak terpangkas; overflow="ellipsis" pada kolom Script; tinggi < 2 baris -> "terminal terlalu kecil"; baris penanda _penanda_panjang/_penanda_ringkas dipilih lewat _pilih_penanda(); fungsi _jumlah_baris() dihapus karena menghitung baris dengan asumsi "satu script satu baris" dan kelewatan saat teks membungkus
         Alasan : Memperkecil terminal membuat frame lebih tinggi dari terminal, sehingga terminal menggulir dan judul kolom terpotong ke luar layar (baris script terlihat, judulnya hilang). Judul kolom tidak boleh hilang, jadi yang dipangkas adalah isi; baris penanda membuat pemangkasan selalu terlihat oleh pengguna

|update| File : src/shared/live_view.py [01/10/2026 15:20:00]
         Tampilan live dikembalikan sebagai header tabel 3 kolom (nama script, status menunggu/jalan/selesai/error, jumlah peringatan+error) yang diperbarui realtime di alternate screen, dengan blok pesan WARNING+/error di bawahnya; redraw stateless memakai Control.home()+Control.clear() tiap frame dan TampilanPipeline tak lagi memakai rich.live sama sekali; STATUS_JALAN diubah dari "dijalankan" menjadi "jalan"; tambahan field RencanaBaris.n_warning/.n_error/.n_masalah, _hitung(), _render(), _tabel(), _sel_status(), _sel_masalah(), _simbol_status(), _normalisasi_status(), dukung_layar(), TampilanLogHandler, pasang_tampilan_log(); saat berhenti() seluruh pesan dicetak ke layar utama; mode cetak lama dipertahankan sebagai HOTEL_LIVE=print
         Alasan : Versi in-place sebelumnya (01/10 11:30) dibongkar karena rich.live menggeser kursor naik sebesar tinggi frame sebelumnya (LiveRender.position_cursor) sehingga frame lama tertinggal sebagai duplikat di terminal VSCode; menggambar ulang stateless dari home+clear menghapus ketergantungan pada tinggi frame itu, jadi resize aman. Permintaan: status "jalan" yang menandai script sedang berjalan + jumlah warning/error per script

|add| File : src/shared/logging_utils.py [01/10/2026 15:20:00]
         Fungsi sumber_dari_logger(nama) -> (stage, sumber) | None; _record_target() kini memakainya sehingga pemetaan nama logger ke stage+sumber hanya ada di satu tempat
         Alasan : live_view butuh memetakan logger (src.extract.bps_scraper) ke baris tabel (bps, extract) untuk menghitung peringatan/error per script tanpa menduplikasi regex yang sama

|fix| File : main.py [01/10/2026 15:20:00]
         Jalur serial (_run_sources) memanggil pasang_tampilan_log(tampilan) sehingga record WARNING+ dialihkan ke TampilanPipeline.pesan() alih-alih ditulis StreamHandler ke stdout; impor TampilanPipeline/dukung_live/pasang_tampilan_log disesuaikan
         Alasan : Di mode live, StreamHandler bawaan setup_logging() menulis langsung ke stdout dan menimpa frame alternate screen; jalur paralel sudah aman karena AntreanLogHandler, jalur serial belum

|update| File : PANDUAN.md [01/10/2026 16:05:00]
         Bagian "Tampilan Terminal" ditambah sub-bagian "Menyesuaikan ukuran terminal": tabel apa yang dipangkas saat ruang kurang (script dari atas urut tetap, pesan terbaru dulu, garis pemisah dilepas), contoh baris penanda bentuk panjang dan ringkas, serta perilaku terminal 1 baris
         Alasan : Pengguna perlu tahu bahwa memperkecil terminal memangkas isi, bukan merusak tampilan, dan tidak perlu panik saat judul kolom sempat hilang

|update| File : PANDUAN.md [01/10/2026 15:20:00]
         Bagian "Tampilan Terminal" ditulis ulang: header tabel 3 kolom dengan status & jumlah peringatan/error, blok pesan di bawahnya, penjelasan kenapa redraw stateless (bukan rich.live), perilaku saat pesan menumpuk, tabel env HOTEL_LIVE=0/print, dan catatan alternate screen saat program dibunuh paksa
         Alasan : Dokumentasi mengikuti perilaku tampilan live terbaru

|fix| File : src/shared/live_view.py [01/10/2026 11:30:27]
         Tampilan live in-place dibongkar total: rich.live (Live), Group, RenderableType, cell_len, _tinggi, _kunci_ukuran(), _tinggi_region(), _render(), _pilih_tampil(), _pilih_baris(), _baris_file(), _baris_pesan(), _alihkan_log_konsol(), _pulihkan_log_konsol(), _urutan_selesai, kolom jumlah peringatan, serta field RencanaBaris.waktu_jalan/.n_warning semuanya dihapus; main.py dan src/parallel.py tidak berubah karena API publik (dukung_live, RencanaBaris, mulai, muat, pesan, berhenti) tetap sama
         Alasan : Redraw in-place di terminal VSCode menyisakan baris frame lama sebagai duplikat meski tinggi sudah dikunci dan re-anchor sudah dipasang - kursor tetap digeser naik oleh LiveRender.position_cursor sebanyak tinggi frame sebelumnya yang tidak lagi valid. Sekarang blok rencana dicetak sekali di awal tanpa penulisan ulang baris, tiap script yang selesai menambah satu baris ke bawah, dan resize terminal tidak bisa merusak apa pun

|fix| File : src/shared/live_view.py [01/10/2026 10:22:09]
         Tinggi frame dibekukan satu kali di mulai() lewat _kunci_ukuran() sehingga _tinggi_region() tidak lagi mengikuti ukuran terminal; ditambahkan _ukuran_berubah_locked() yang menyisipkan Control.clear() + Control.move_to(0, 0) di awal _render() saat ukuran berubah (re-anchor frame transisi); padding dihitung dari len(bagian) saja karena list kontrol dirender 0 baris
         Alasan : Resize terminal saat pipeline berjalan menyisakan baris frame lama sebagai duplikat - rich menimpa frame dengan menggeser kursor naik sebanyak tinggi frame sebelumnya (LiveRender.position_cursor), dan saat terminal mengecil geseran ter-clamp di baris 0 sehingga erase tidak menutup frame lama

|fix| File : src/shared/live_view.py [28/09/2026 11:14:00]
         muat() menormalkan sumber & stage ke lowercase sebelum lookup baris; event paralel memakai stage uppercase (EXTRACT/TRANSFORM/LOAD) sehingga sebelumnya lookup tak pernah cocok dan status selalu menunggu (warning tetap muncul karena pesan() memakai nama logger lowercase)
         Alasan : Status paralel tidak berubah dari 'menunggu' padahal proses sudah dijalankan / selesai / error

|update| File : src/shared/live_view.py [28/09/2026 11:02:44]
         Header absolut (jumlah baris tetap) berisi kelompok EXTRACT/TRANSFORM/LOAD + 1 baris per script dgn kolom status (menunggu/dijalankan/selesai/error) dan kolom jumlah peringatan; baris judul & bar progres dihapus; tinggi render dipatok ke tinggi terminal + vertical_overflow=crop + padding untuk menghilangkan duplikasi frame di terminal pendek; spasi nama logger di blok pesan diperbaiki
         Alasan : Header boleh 30 baris (10/proses) tapi tidak boleh bertambah; hanya status & jumlah peringatan yang berubah in-place

|update| File : src/parallel.py [28/09/2026 11:02:44]
         Catatan mode uji dicetak sebagai baris biasa persis sebelum tampilan live (bukan bagian dari area live); TampilanPipeline tak lagi menerima argumen catatan
         Alasan : Area live dibebaskan dari baris acak agar tinggi frame bisa dipatok presisi

|update| File : main.py [28/09/2026 11:02:44]
         Catatan mode uji pada jalur serial dicetak sebelum tampilan live, sama seperti jalur paralel; TampilanPipeline tak lagi menerima argumen catatan
         Alasan : Konsistensi dengan jalur paralel dan area live yang bersih

|update| File : PANDUAN.md [28/09/2026 11:02:44]
         Bagian "Tampilan Terminal" diperbarui: header absolut (status menunggu/dijalankan/selesai/error + kolom jumlah peringatan), contoh baris files, blok log peringatan di bawah, dan daftar terminal yang mendukung
         Alasan : Dokumentasi mengikuti perilaku tampilan live terbaru

|update| File : src/shared/live_view.py [28/09/2026 10:15:53]
         Menambahkan modul tampilan live berbasis rich: status tiap script (extract/transform/load) diperbarui di tempat tanpa baris baru, mode adaptif untuk terminal pendek, blok pesan WARNING+/error di bawah tabel, fallback otomatis ke cetak baris per baris bila terminal tidak mendukung
         Alasan : Terminal lama menumpuk baris sehingga sulit melihat script mana yang selesai / sedang berjalan

|update| File : src/parallel.py [28/09/2026 10:15:53]
         Mengganti cetak baris per event dengan tampilan live in-place; WARNING+ dari worker dialihkan lewat antrian ke blok pesan bawah, bukan ditulis langsung ke terminal
         Alasan : Menjaga tampilan in-place tetap bersih dan pesan error tetap terlihat

|update| File : main.py [28/09/2026 10:15:53]
         Jalur serial (_run_sources) memakai tampilan live yang sama dengan jalur paralel; memakai get_stage_path untuk path file
         Alasan : Konsistensi cara membaca status script di semua mode

|update| File : requirements.txt [28/09/2026 10:15:53]
         Menambahkan rich>=13.0 sebagai dependensi langsung
         Alasan : rich dipakai langsung oleh src/shared/live_view.py

## Aturan Penulisan

- Entri terbaru diletakkan di **paling atas**.
- Satu entri = 3 baris:

  ```
  |update| File : <path> [DD/MM/YYYY HH:MM:SS]
           <apa yang diubah>
           Alasan : <alasan perubahan>
  ```

- Baris ke-2 dan ke-3 di-indentasi **9 spasi** (selaras dengan `|update| `).
- `<path>` relatif dari root proyek, memakai `/`:
  `src/extract/bps_scraper.py`, `main.py`, `config/sources.yaml`.
- Tanggal format `DD/MM/YYYY HH:MM:SS` (contoh: `28/09/2026 08:58:02`).
- Satu entri per file. Bila satu file berubah banyak hal sekaligus dalam satu
  pekerjaan, tetap satu entri dengan semua butir ditulis di baris ke-2.
- Tulis entri **setelah** perubahan selesai, bukan sebelum.
- Varian tag:
  - `|update|` = penambahan fitur / perubahan perilaku
  - `|fix|` = perbaikan bug
  - `|refactor|` = perubahan struktur tanpa mengubah perilaku
  - `|config|` = perubahan konfigurasi