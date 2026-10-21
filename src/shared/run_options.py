from __future__ import annotations


# Opsi run yang diset dari CLI pintasan dan dibaca modul extract.
# Dipisah dari signature extract() karena opsi ini memengaruhi perilaku di
# dalam modul (mis. headful untuk melihat selector), bukan hanya output.

_dry_run: bool = False
_headful: bool = False
_resume: bool = False
_backfill: bool = False
_historis: bool = False


def set_dry_run(nilai: bool) -> None:
    """Aktifkan mode dry-run global (jalankan tapi jangan tulis)."""
    global _dry_run
    _dry_run = bool(nilai)


def get_dry_run() -> bool:
    return _dry_run


def set_headful(nilai: bool) -> None:
    """Paksa browser tampil (headful) untuk melihat selector saat debugging."""
    global _headful
    _headful = bool(nilai)


def get_headful() -> bool:
    return _headful


def set_resume(nilai: bool) -> None:
    """Lanjutkan run dari checkpoint (hotel yang sudah diproses dilewati)."""
    global _resume
    _resume = bool(nilai)


def get_resume() -> bool:
    return _resume


def set_backfill(nilai: bool) -> None:
    """Aktifkan mode backfill historis (abaikan validasi tanggal checkpoint)."""
    global _backfill
    _backfill = bool(nilai)


def get_backfill() -> bool:
    return _backfill


def set_historis(nilai: bool) -> None:
    """Aktifkan mode historis (backfill penuh dari start_year config)."""
    global _historis
    _historis = bool(nilai)


def get_historis() -> bool:
    return _historis