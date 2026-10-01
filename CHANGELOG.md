# CHANGELOG - Catatan Perubahan `hotel_data_platform`

Berisi catatan perubahan **kode & konfigurasi**, ditulis manual.
Bukan log runtime eksekusi pipeline (yang ada di folder `log/`).

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