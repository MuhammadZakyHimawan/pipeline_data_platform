"""Tampilan daftar file ETL yang dicetak sekali di awal lalu bertambah per hasil.

Di awal program, blok rencana dicetak satu kali: kelompok EXTRACT/TRANSFORM/
LOAD dan satu baris per script dengan status `menunggu`. Sesudah itu tidak ada
redraw in-place sama sekali — setiap script yang selesai (atau gagal) menambah
satu baris baru ke bawah, dan pesan WARNING+ langsung tercetak seperti log
biasa. Tidak ada penulisan ulang baris, tidak ada penggeseran kursor, jadi
tampilan ini aman di terminal mana pun termasuk yang di-resize saat berjalan.

Bila rich tidak tersedia atau output bukan terminal, pemanggil memakai jalur
cetak ringkas sendiri — lihat `dukung_live()`.
"""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass, replace
from typing import Any, Sequence

try:
    from rich.console import Console
    from rich.text import Text

    HAS_RICH = True
except ImportError:  # pragma: no cover
    HAS_RICH = False
    Console = Any  # type: ignore
    Text = Any  # type: ignore

STATUS_MENUNGGU = "menunggu"
STATUS_JALAN = "dijalankan"
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

    Kalau tidak, pemanggil memakai jalur cetak ringkas yang lebih sederhana.
    """
    if not HAS_RICH:
        return False
    if os.environ.get(_ENV_LIVE, "").strip() == "0":
        return False
    konsol = Console()
    return bool(konsol.is_terminal and not konsol.is_dumb_terminal)


class TampilanPipeline:
    """Cetak blok rencana sekali, lalu satu baris per script yang selesai.

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
        """Cetak blok rencana tepat satu kali."""
        with self._lock:
            stage_lalu: str | None = None
            for r in self._rencana:
                if r.stage != stage_lalu:
                    stage_lalu = r.stage
                    self._cetak(Text(f" {r.stage.upper()}", style="bold dim"))
                self._cetak(self._baris_rencana(r))
            self._cetak(Text(""))

    def berhenti(self) -> None:
        """Pemisah sebelum ringkasan akhir."""
        self._cetak(Text(""))

    def muat(self, sumber: str, stage: str, status: str, info: Any, detik: Any) -> None:
        """Catat status satu baris; cetak satu baris bila statusnya final."""
        # Event paralel memakai stage uppercase ("EXTRACT"); kunci baris lowercase.
        sumber = (sumber or "").lower()
        stage = (stage or "").lower()
        if status in ("mulai", "jalan", STATUS_JALAN):
            status = STATUS_JALAN
        elif status in ("selesai", "ok", STATUS_OK):
            status = STATUS_OK
        elif status in ("gagal", "error", STATUS_ERROR):
            status = STATUS_ERROR
        else:
            status = STATUS_MENUNGGU

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
            if status in (STATUS_OK, STATUS_ERROR) and lama not in (
                STATUS_OK,
                STATUS_ERROR,
            ):
                self._cetak(self._baris_hasil(r))

    def pesan(self, level: str, nama: str, isi: str) -> None:
        """Cetak satu pesan log (WARNING+) sebagai baris biasa."""
        isi = (isi or "").replace("\n", " | ").strip()
        if not isi:
            return
        stempel = time.strftime("%H:%M:%S")
        with self._lock:
            self._cetak(self._baris_pesan(stempel, level, nama, isi))

    # ------------------------------------------------------------- pembuatan isi

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
        gaya = _gaya(r)
        if r.status == STATUS_ERROR:
            ikon = self._sim["gagal"]
        else:
            ikon = self._sim["ok"]
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


def _gaya(r: RencanaBaris) -> str:
    return "red" if r.status == STATUS_ERROR else "green"


def _pilih_simbol(konsol: Any) -> dict[str, str]:
    enc = (konsol.encoding or "").lower()
    return _SET_UNICODE if "utf" in enc else _SET_ASCII
