"""Tampilan rencana pipeline: blok rencana dicetak sekali, lalu tiap script
yang selesai menambah satu baris di bawahnya.

Prinsip: tampilan ini tidak pernah menulis ulang di tempat. Tidak ada alternate
screen, tidak ada `home + clear`, tidak ada penulisan ulang baris — semua
keluaran mengalir ke bawah seperti perintah biasa. Efeknya isi terminal
sebelum program dijalankan tetap terlihat dan bisa di-scroll seperti biasa,
dan mengubah ukuran terminal tidak mungkin merusak tampilan.

Dulu (28/09 - 01/10/2026) header tabel diperbarui in-place di alternate screen
dengan redraw `home + clear`. Alternate screen adalah buffer layar terpisah,
jadi selama program berjalan seluruh isi terminal sebelumnya tidak terlihat;
ditambah `clear()` menghapus layar. Itu persis keluhan "hanya header yang
terlihat, log perintah sebelumnya tidak bisa dilihat".

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
    from rich.console import Console, Text

    HAS_RICH = True
except ImportError:  # pragma: no cover
    HAS_RICH = False
    Console = Any  # type: ignore
    Text = Any  # type: ignore

STATUS_MENUNGGU = "menunggu"
STATUS_JALAN = "jalan"
STATUS_OK = "selesai"
STATUS_ERROR = "error"

_ENV_LIVE = "HOTEL_LIVE"

_GAYA_PESAN = {
    "WARNING": "yellow",
    "ERROR": "red",
    "CRITICAL": "bold red",
}

# Simbol Unicode bila encoding terminal mendukung UTF-8; ASCII sebagai pengaman.
_SET_UNICODE = {
    "menunggu": "·",
    "ok": "✔",
    "gagal": "✖",
    "warn": "⚠",
}
_SET_ASCII = {
    "menunggu": ".",
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

    @property
    def kunci(self) -> tuple[str, str]:
        return (self.sumber, self.stage)


def dukung_live() -> bool:
    """True bila rich tersedia dan output berupa terminal sungguhan.

    Kalau tidak, pemanggil memakai jalur cetak ringkas yang lebih sederhana
    (satu baris per event, tanpa blok rencana).
    """
    if not HAS_RICH:
        return False
    if os.environ.get(_ENV_LIVE, "").strip() == "0":
        return False
    konsol = Console()
    return bool(konsol.is_terminal and not konsol.is_dumb_terminal)


class TampilanPipeline:
    """Blok rencana yang dicetak sekali, lalu satu baris per script yang selesai.

    Dipakai di jalur paralel (src/parallel.py) dan serial (main.py). Pemanggil
    memanggil `mulai()`, lalu `muat(...)`/`pesan(...)` untuk melaporkan progres,
    dan menutup dengan `berhenti()`.
    """

    def __init__(self, rencana: Sequence[RencanaBaris]) -> None:
        self._rencana: list[RencanaBaris] = [replace(r) for r in rencana]
        self._baris: dict[tuple[str, str], RencanaBaris] = {
            r.kunci: r for r in self._rencana
        }
        self._lock = threading.Lock()
        self._konsol = Console()
        self._sim = _pilih_simbol(self._konsol)

    # ------------------------------------------------------------------ kontrol

    def mulai(self) -> None:
        """Cetak blok rencana: judul stage lalu satu baris menunggu per script."""
        with self._lock:
            stage_lalu: str | None = None
            for r in self._rencana:
                if r.stage != stage_lalu:
                    stage_lalu = r.stage
                    self._cetak(Text(f" {r.stage.upper()}", style="bold dim"))
                self._cetak(self._baris_rencana(r))
            self._cetak(Text(""))

    def berhenti(self) -> None:
        """Tutup tampilan: sisakan satu baris kosong sebelum ringkasan."""
        with self._lock:
            self._cetak(Text(""))

    def muat(self, sumber: str, stage: str, status: str, info: Any, detik: Any) -> None:
        """Catat status satu baris; status final menambah satu baris di bawah."""
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
            # Hanya status final yang jadi baris baru; status berjalan diam saja
            # supaya tidak membanjiri keluaran.
            if status in (STATUS_OK, STATUS_ERROR) and lama not in (STATUS_OK, STATUS_ERROR):
                self._cetak(self._baris_hasil(r))

    def pesan(self, level: str, nama: str, isi: str) -> None:
        """Cetak satu pesan WARNING+ sebagai baris biasa, berurutan dengan progres."""
        isi = (isi or "").replace("\n", " | ").strip()
        if not isi:
            return
        stempel = time.strftime("%H:%M:%S")
        with self._lock:
            self._cetak(self._baris_pesan(stempel, level, nama, isi))

    # ----------------------------------------------------------------- cetak

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
    setup_logging() menulis langsung ke stdout dengan format berbeda sehingga
    pesan WARNING+ tidak seragam dengan baris progres.
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
    """Cabut handler StreamHandler konsol, arahkan WARNING+ ke tampilan.

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