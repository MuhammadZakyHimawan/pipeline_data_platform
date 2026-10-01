"""Tampilan rencana pipeline: header 3 kolom yang diperbarui realtime.

Ada dua mode, dipilih otomatis:

* **langsung** (default di terminal sungguhan) — header berupa tabel 3 kolom
  (nama script, status, jumlah peringatan/error) yang statusnya berubah di
  tempat pada alternate screen, dengan blok pesan WARNING+ di bawahnya.
  Redraw-nya **stateless**: tiap frame digambar ulang dari `home + clear`,
  jadi tinggi frame sebelumnya tidak pernah perlu diingat.
  Inilah alasan mode ini menggantikan `rich.live.Live`: Live menggeser kursor naik
  sebesar tinggi frame sebelumnya (`LiveRender.position_cursor`), dan begitu
  terminal di-resize angka itu tidak lagi valid sehingga frame lama tertinggal
  sebagai baris duplikat — kejadian nyata di terminal VSCode. Mode langsung
  tidak memakai Live sama sekali, jadi kelas bug itu tidak mungkin muncul dan
  resize aman kapan pun. `@berhenti()` mencetak frame final beserta seluruh
  pesan ke layar utama, jadi tidak ada yang hilang saat keluar alternate screen.
  Tinggi frame selalu dibatasi tinggi terminal: kalau ruang kurang, isi yang
  dipangkas (script dari atas, pesan terbaru dulu) sementara judul kolom tidak
  pernah hilang, dan baris penanda menyebutkan apa yang disembunyikan.

* **cetak** — blok rencana dicetak satu kali di awal, lalu tiap script yang
  selesai menambah satu baris ke bawah tanpa penulisan ulang baris. Dipakai
  bila alternate screen tidak dikehendaki (`HOTEL_LIVE=print`).

Bila rich tidak tersedia atau output bukan terminal, pemanggil memakai jalur
cetak ringkas sendiri — lihat `dukung_live()`.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass, replace
from typing import Any, Sequence

try:
    from rich import box
    from rich.console import Console, Group, RenderableType
    from rich.control import Control
    from rich.table import Table
    from rich.text import Text

    HAS_RICH = True
except ImportError:  # pragma: no cover
    HAS_RICH = False
    Console = Any  # type: ignore
    Group = Any  # type: ignore
    RenderableType = Any  # type: ignore
    Control = Any  # type: ignore
    Table = Any  # type: ignore
    Text = Any  # type: ignore
    box = Any  # type: ignore

from .logging_utils import sumber_dari_logger

STATUS_MENUNGGU = "menunggu"
STATUS_JALAN = "jalan"
STATUS_OK = "selesai"
STATUS_ERROR = "error"

_ENV_LIVE = "HOTEL_LIVE"

# Nilai _ENV_LIVE yang memaksa mode cetak walau output berupa terminal.
_MODE_CETAK = "print"

_LEVEL_PERINGATAN = {"WARNING"}
_LEVEL_ERROR = {"ERROR", "CRITICAL"}

_GAYA_PESAN = {
    "WARNING": "yellow",
    "ERROR": "red",
    "CRITICAL": "bold red",
}

_GAYA_STATUS = {
    STATUS_MENUNGGU: "dim",
    STATUS_JALAN: "bold yellow",
    STATUS_OK: "green",
    STATUS_ERROR: "bold red",
}

# Simbol Unicode bila encoding terminal mendukung UTF-8; ASCII sebagai pengaman.
_SET_UNICODE = {
    "menunggu": "·",
    "jalan": "▶",
    "ok": "✔",
    "gagal": "✖",
    "warn": "⚠",
}
_SET_ASCII = {
    "menunggu": ".",
    "jalan": ">",
    "ok": "+",
    "gagal": "x",
    "warn": "!",
}


@dataclass
class RencanaBaris:
    """Satu baris tampilan = satu script yang dijalankan."""

    sumber: str
    stage: str
    path: str
    status: str = STATUS_MENUNGGU
    info: str = ""
    detik: float = 0.0
    n_warning: int = 0
    n_error: int = 0

    @property
    def kunci(self) -> tuple[str, str]:
        return (self.sumber, self.stage)

    @property
    def n_masalah(self) -> int:
        """Total peringatan + error; 0 berarti script ini bersih."""
        return self.n_warning + self.n_error


def dukung_live() -> bool:
    """True bila rich tersedia dan output berupa terminal sungguhan.

    Kalau tidak, pemanggil memakai jalur cetak ringkas yang lebih sederhana.
    """
    if not HAS_RICH:
        return False
    if os.environ.get(_ENV_LIVE, "").strip() == "0":
        return False
    konsol = Console()
    return bool(konsol.is_terminal and not konsol.is_dumb_terminal)


def dukung_layar() -> bool:
    """True bila mode langsung (alternate screen) yang dikehendaki."""
    return dukung_live() and os.environ.get(_ENV_LIVE, "").strip().lower() != _MODE_CETAK


class TampilanPipeline:
    """Header tabel yang diperbarui realtime, atau cetak sekali + append.

    Dipakai di jalur paralel (src/parallel.py) dan serial (main.py). Pemanggil
    memanggil `mulai()`, lalu `muat(...)`/`pesan(...)` untuk melaporkan progres,
    dan menutup dengan `berhenti()`. Antarmuka publik tidak berubah antar mode.
    """

    def __init__(self, rencana: Sequence[RencanaBaris], langsung: bool | None = None) -> None:
        self._rencana: list[RencanaBaris] = [replace(r) for r in rencana]
        self._baris: dict[tuple[str, str], RencanaBaris] = {
            r.kunci: r for r in self._rencana
        }
        self._lock = threading.Lock()
        self._konsol = Console()
        self._sim = _pilih_simbol(self._konsol)
        self._langsung = dukung_layar() if langsung is None else bool(langsung)
        self._pesan_tampil: list[Any] = []
        self._cache_tinggi: list[tuple[tuple[int, bool, int], int]] = []

    # ------------------------------------------------------------------ kontrol

    def mulai(self) -> None:
        """Buka tampilan: masuk alternate screen & gambar, atau cetak blok rencana."""
        with self._lock:
            if self._langsung:
                self._konsol.control(
                    Control.alt_screen(enable=True), Control.clear()
                )
                self._gambar()
                return
            stage_lalu: str | None = None
            for r in self._rencana:
                if r.stage != stage_lalu:
                    stage_lalu = r.stage
                    self._cetak(Text(f" {r.stage.upper()}", style="bold dim"))
                self._cetak(self._baris_rencana(r))
            self._cetak(Text(""))

    def berhenti(self) -> None:
        """Tutup tampilan: cetak frame final ke layar utama, atau pemisah saja."""
        with self._lock:
            if self._langsung:
                self._konsol.control(Control.alt_screen(enable=False))
                self._konsol.print(self._render(penuh=True))
                self._konsol.print(Text(""))
                return
            self._cetak(Text(""))

    def muat(self, sumber: str, stage: str, status: str, info: Any, detik: Any) -> None:
        """Catat status satu baris; di mode langsung langsung digambar ulang."""
        status = _normalisasi_status(status)
        # Event paralel memakai stage uppercase ("EXTRACT"); kunci baris lowercase.
        sumber = (sumber or "").lower()
        stage = (stage or "").lower()

        with self._lock:
            r = self._baris.get((sumber, stage))
            if r is None:
                return
            lama = r.status
            r.status = status
            r.info = str(info) if info is not None else ""
            r.detik = float(detik) if detik is not None else 0.0
            if self._langsung:
                self._gambar()
                return
            # Mode cetak: hanya status final yang jadi baris baru; status berjalan
            # diam saja supaya tidak membanjiri keluaran.
            if status in (STATUS_OK, STATUS_ERROR) and lama not in (STATUS_OK, STATUS_ERROR):
                self._cetak(self._baris_hasil(r))

    def pesan(self, level: str, nama: str, isi: str) -> None:
        """Catat satu pesan WARNING+ dan perbarui jumlah peringatan/error baris."""
        isi = (isi or "").replace("\n", " | ").strip()
        if not isi:
            return
        stempel = time.strftime("%H:%M:%S")
        with self._lock:
            self._hitung(nama, level)
            baris = self._baris_pesan(stempel, level, nama, isi)
            if not self._langsung:
                self._cetak(baris)
                return
            self._pesan_tampil.append(baris)
            self._gambar()

    # ------------------------------------------------------------- Mechanism

    def _hitung(self, nama: str, level: str) -> None:
        """Tambah penghitung peringatan/error pada baris script asal pesan."""
        kunci = sumber_dari_logger(nama or "")
        if kunci is None:
            return
        stage, sumber = kunci
        r = self._baris.get((sumber.lower(), stage.lower()))
        if r is None:
            return
        if level in _LEVEL_PERINGATAN:
            r.n_warning += 1
        elif level in _LEVEL_ERROR:
            r.n_error += 1

    def _gambar(self) -> None:
        """Gambar ulang frame penuh dari nol: home + clear, lalu render.

        Sengaja tidak memakai rich.live: Live bergantung pada tinggi frame
        sebelumnya untuk menggeser kursor, dan ketergantungan itulah yang
        menyisakan baris duplikat saat terminal di-resize.
        """
        self._konsol.control(Control.home(), Control.clear())
        self._konsol.print(self._render(), end="")

    def _render(self, penuh: bool = False) -> RenderableType:
        """Susun frame yang muat di tinggi terminal saat ini.

        `penuh=True` dipakai saat menutup tampilan: di layar utama yang bisa
        menggulir, seluruh baris dan seluruh pesan dicetak tanpa pemangkasan.
        """
        semua = self._rencana
        if penuh:
            return Group(self._tabel(semua, pakai_garis=True), *self._pesan_tampil)
        tinggi = self._konsol.size.height
        if tinggi < 2:
            return Text("terminal terlalu kecil", style="dim")

        # Garis pemisah hanya digambar bila tabel utuh masih muat; kalau tidak,
        # satu baris itu dikembalikan ke script.
        pakai_garis = tinggi >= self._tinggi_tabel(semua, True) + 2

        # Ruang penanda dibatasi oleh tabel kosong (hanya judul kolom) supaya
        # penanda tidak pernah menyingkirkan judul kolom itu sendiri.
        maks_penanda = max(0, tinggi - self._tinggi_tabel((), pakai_garis))
        reservasi = min(1, maks_penanda)

        penanda = None
        for _ in range(3):
            baris, sisa = self._atur(semua, pakai_garis, tinggi, reservasi)
            pesan = self._potong_pesan(sisa)
            penanda = self._pilih_penanda(
                len(semua) - len(baris),
                len(self._pesan_tampil) - len(pesan),
                maks_penanda,
            )
            if penanda is None:
                if reservasi:
                    # Tak ada yang disembunyikan; baris penanda dikembalikan ke pesan.
                    pesan = self._potong_pesan(sisa + 1)
                break
            perlu = self._tinggi(penanda)
            if perlu <= reservasi:
                break
            reservasi = perlu  # penanda membungkus -> hitung ulang anggaran

        bagian: list[Any] = [self._tabel(baris, pakai_garis)]
        if penanda is not None:
            bagian.append(penanda)
        bagian.extend(pesan)
        return Group(*bagian)

    def _tinggi_tabel(self, semua: Sequence[RencanaBaris], pakai_garis: bool) -> int:
        """Tinggi tabel untuk `semua`, diukur lalu disimpan per lebar terminal.

        Path tidak pernah berubah dan semua kolom `no_wrap`, jadi tinggi tabel
        hanya bergantung pada jumlah baris dan lebar terminal -- bukan pada status
        atau jumlah peringatan. Tanpa cache ini tiap frame mengulang ~log2(n) render.
        """
        lebar = self._konsol.size.width
        for kunci, nilai in self._cache_tinggi:
            if kunci == (lebar, pakai_garis, len(semua)):
                return nilai
        nilai = self._tinggi(self._tabel(semua, pakai_garis))
        self._cache_tinggi.append(((lebar, pakai_garis, len(semua)), nilai))
        return nilai

    def _pilih_penanda(self, n_script: int, n_pesan: int, maks: int) -> Text | None:
        """Baris penanda: bentuk panjang bila muat, kalau tidak bentuk ringkas."""
        if n_script <= 0 and n_pesan <= 0:
            return None
        for pembuat in (_penanda_panjang, _penanda_ringkas):
            teks = pembuat(n_script, n_pesan)
            if teks is not None and self._tinggi(teks) <= maks:
                return teks
        return None

    def _atur(self, semua: Sequence[RencanaBaris], pakai_garis: bool, tinggi: int, reservasi: int):
        """Pilih prefix script yang muat dan sisa baris untuk pesan.

        Prefix terpanjang dicari dengan pencarian biner: tinggi tabel diukur nyata
        dan bersifat monoton terhadap jumlah baris, jadi cukup ~log2(n) render.
        """
        lo, hi = 0, len(semua)
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if self._tinggi_tabel(semua[:mid], pakai_garis) + reservasi <= tinggi:
                lo = mid
            else:
                hi = mid - 1
        baris = semua[:lo]
        sisa = max(0, tinggi - self._tinggi_tabel(baris, pakai_garis) - reservasi)
        return baris, sisa

    def _potong_pesan(self, batas: int) -> list[Any]:
        """Ambil pesan paling baru yang muat dalam `batas` baris.

        Tinggi tiap pesan diukur nyata karena pesan panjang membungkus jadi 2-3
        baris di terminal sempit; menghitung satu baris per pesan akan meluber.
        Loop berhenti begitu budget habis, jadi biaya sebanding dengan jumlah
        pesan yang benar-benar tampil, bukan seluruh riwayat.
        """
        dipilih: list[Any] = []
        terpakai = 0
        for teks in reversed(self._pesan_tampil):
            tinggi = self._tinggi(teks)
            if terpakai + tinggi > batas:
                break
            dipilih.append(teks)
            terpakai += tinggi
        dipilih.reverse()
        return dipilih

    def _tinggi(self, renderable: Any) -> int:
        """Jumlah baris nyata sebuah renderable pada lebar konsol saat ini."""
        return len(self._konsol.render_lines(renderable, self._konsol.options, pad=False))

    def _tabel(self, baris: Sequence[RencanaBaris], pakai_garis: bool = True) -> Table:
        """Header 3 kolom: nama script, status, jumlah peringatan/error."""
        tabel = Table(
            box=box.SIMPLE_HEAD if pakai_garis else None,
            show_edge=False,
            pad_edge=False,
            header_style="bold",
            expand=False,
        )
        tabel.add_column("Script", no_wrap=True, overflow="ellipsis")
        tabel.add_column("Status", no_wrap=True)
        tabel.add_column("Peringatan/Error", justify="right", no_wrap=True)

        banyak_stage = len({r.stage for r in baris}) > 1
        stage_lalu: str | None = None
        for r in baris:
            if r.stage != stage_lalu:
                stage_lalu = r.stage
                if banyak_stage:
                    tabel.add_row(Text(f" {r.stage.upper()}", style="bold dim"), "", "")
            tabel.add_row(Text(r.path), self._sel_status(r), self._sel_masalah(r))
        return tabel

    def _sel_status(self, r: RencanaBaris) -> Text:
        gaya = _GAYA_STATUS.get(r.status, "dim")
        teks = Text()
        teks.append(f"{self._simbol_status(r)} ", style=gaya)
        teks.append(r.status, style=gaya)
        return teks

    def _sel_masalah(self, r: RencanaBaris) -> Text:
        """Sel jumlah: '0' redup bila bersih, kalau tidak '2W 1E'."""
        teks = Text()
        if not r.n_masalah:
            teks.append("0", style="dim")
            return teks
        if r.n_warning:
            teks.append(f"{r.n_warning}W", style="yellow")
        if r.n_error:
            if r.n_warning:
                teks.append(" ")
            teks.append(f"{r.n_error}E", style="bold red")
        return teks

    def _simbol_status(self, r: RencanaBaris) -> str:
        if r.status == STATUS_MENUNGGU:
            return self._sim["menunggu"]
        if r.status == STATUS_JALAN:
            return self._sim["jalan"]
        if r.status == STATUS_ERROR:
            return self._sim["gagal"]
        return self._sim["ok"]

    # ------------------------------------------------------------- cetak mode

    def _cetak(self, teks: Any) -> None:
        if HAS_RICH:
            self._konsol.print(teks)
        else:
            print(str(teks))

    def _baris_rencana(self, r: RencanaBaris) -> Text:
        teks = Text()
        teks.append(f"{self._sim['menunggu']} ", style="dim")
        teks.append(r.path, style="dim")
        teks.append(f"  {STATUS_MENUNGGU}", style="dim")
        return teks

    def _baris_hasil(self, r: RencanaBaris) -> Text:
        gaya = "red" if r.status == STATUS_ERROR else "green"
        ikon = self._sim["gagal"] if r.status == STATUS_ERROR else self._sim["ok"]
        teks = Text()
        teks.append(f"{ikon} ", style=gaya)
        teks.append(r.path, style=gaya)
        teks.append(f"  {r.status}", style=gaya)
        if r.info:
            teks.append(f" — {r.info}", style="dim")
        teks.append(f" ({r.detik} dtk)", style="dim")
        return teks

    def _baris_pesan(self, stempel: str, level: str, nama: str, isi: str) -> Text:
        gaya = _GAYA_PESAN.get(level, "dim")
        teks = Text()
        teks.append(f" {self._sim['warn']} {stempel} ", style="dim")
        teks.append(f"{level:8s} ", style=gaya)
        teks.append(f"{nama} ", style="dim")
        teks.append(isi, style=gaya)
        return teks


def _penanda_panjang(n_script: int, n_pesan: int) -> Text | None:
    """Penanda bentuk panjang; dipakai kalau masih muat dalam satu baris."""
    bagian: list[str] = []
    if n_script > 0:
        bagian.append(f"{n_script} script")
    if n_pesan > 0:
        bagian.append(f"{n_pesan} pesan belum tampil")
    if not bagian:
        return None
    teks = Text()
    teks.append("… ", style="dim")
    teks.append(" · ".join(bagian), style="dim")
    if n_pesan > 0:
        teks.append(" (detail di log/)", style="dim")
    return teks


def _penanda_ringkas(n_script: int, n_pesan: int) -> Text | None:
    """Penanda bentuk pendek untuk terminal sempit, tetap satu baris."""
    bagian: list[str] = []
    if n_script > 0:
        bagian.append(f"{n_script} skrip")
    if n_pesan > 0:
        bagian.append(f"{n_pesan} pesan")
    if not bagian:
        return None
    teks = Text()
    teks.append("… ", style="dim")
    teks.append(" · ".join(bagian), style="dim")
    return teks


def _normalisasi_status(status: str) -> str:
    """Petakan nama status dari event paralel/serial ke empat status baku."""
    if status in ("mulai", "jalan", STATUS_JALAN):
        return STATUS_JALAN
    if status in ("selesai", "ok", STATUS_OK):
        return STATUS_OK
    if status in ("gagal", "error", STATUS_ERROR):
        return STATUS_ERROR
    return STATUS_MENUNGGU


class TampilanLogHandler(logging.Handler):
    """Teruskan record WARNING+ ke TampilanPipeline.pesan().

    Dipakai di jalur serial (main.py) yang tidak punya antrian antar-proses
    seperti worker paralel. Tanpa handler ini, StreamHandler bawaan
    setup_logging() menulis langsung ke stdout dan merusak frame live.
    """

    def __init__(self, tampilan: TampilanPipeline, level: int = logging.WARNING) -> None:
        super().__init__(level)
        self._tampilan = tampilan

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._tampilan.pesan(record.levelname, record.name, record.getMessage())
        except Exception:
            self.handleError(record)


def pasang_tampilan_log(tampilan: TampilanPipeline) -> None:
    """Cabut handler StreamHandler konsol, arahkan WARNING+ ke tampilan live.

    Handler file per stage tetap aktif sehingga log/ tidak kehilangan apa pun.
    """
    root = logging.getLogger()
    for h in root.handlers[:]:
        if isinstance(h, logging.StreamHandler):
            root.removeHandler(h)
    root.addHandler(TampilanLogHandler(tampilan))


def _pilih_simbol(konsol: Any) -> dict[str, str]:
    enc = (konsol.encoding or "").lower()
    return _SET_UNICODE if "utf" in enc else _SET_ASCII