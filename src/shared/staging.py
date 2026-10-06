from __future__ import annotations

import hashlib
import logging
import os
import re
import shutil
from datetime import date
from pathlib import Path

import pandas as pd

from .config import DATA_LAKE_DIR

log = logging.getLogger(__name__)

# Hasil antara scraper panjang dan parquet bronze. Disimpan di _state/ supaya
# tidak pernah ikut terpetakan sebagai periode lake (`list_periods` hanya
# membaca data_lake/{layer}/{source}/{dataset}), jadi transform/load tidak
# mungkin membaca data setengah jadi.
DIR_STAGING = DATA_LAKE_DIR / "_state"

_BURUK = re.compile(r"[^A-Za-z0-9._-]+")
_PANJANG_NAMA = 100
_SISA_BATAS = 8


def label_run(periode: str) -> str:
    """Label satu run: tanggal eksekusi + periode data.

    Tanggal masuk label supaya state dari run kemarin tidak ikut terbaca saat
    hotel yang sama di-scrape ulang pada hari berikutnya.
    """
    return f"{date.today()}_{periode}"


def path_run(source: str, label: str) -> Path:
    return DIR_STAGING / source / label


def _nama_part(kunci: str) -> str:
    """Nama file unik dari kunci hotel.

    Nama bersih dibaca manusia, hash singkat menutup kemungkinan dua kunci
    berbeda jadi nama sama setelah disanitasi (mis. `a/b` dan `a_b`).
    """
    mentah = str(kunci)
    teks = _BURUK.sub("_", mentah).strip("._-")
    if not teks:
        teks = "hotel"
    if len(teks) > _PANJANG_NAMA:
        teks = teks[:_PANJANG_NAMA]
    sidik = hashlib.sha1(mentah.encode("utf-8")).hexdigest()[:_SISA_BATAS]
    return f"{teks}_{sidik}"


def daftar_part(source: str, label: str) -> list[Path]:
    run = path_run(source, label)
    return sorted(run.glob("*.parquet")) if run.is_dir() else []


def tulis_part(source: str, label: str, kunci: str, rows: list[dict]) -> Path | None:
    """Simpan hasil satu hotel sebagai part parquet, atomik (tmp + replace).

    Part ditulis ulang kalau hotel yang sama di-scrape lagi, jadi hotel
    tidak pernah punya dua part dan barisnya tidak terduplikasi.

    Mengembalikan None kalau memang tidak ada yang perlu ditulis (hotel nol
    ulasan).
    Kegagalan tulis sengaja dilempar sebagai exception: pemanggil wajib
    tidak menandai hotel sebagai selesai kalau partnya tidak tersimpan.
    """
    if not rows:
        return None

    run = path_run(source, label)
    run.mkdir(parents=True, exist_ok=True)
    part = run / f"{_nama_part(kunci)}.parquet"
    tmp = part.with_suffix(".parquet.tmp")
    try:
        pd.DataFrame(rows).to_parquet(tmp, index=False)
        os.replace(tmp, part)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise
    return part


def baca_parts(
    source: str,
    label: str,
    dedup_cols: str | list[str] | None = None,
) -> pd.DataFrame | None:
    """Gabung seluruh part satu run; None kalau run belum punya part.

    Part yang rusak tidak digagalkan: dilewati dengan warning supaya satu file
    cacat tidak membuat seluruh hasil run lenyap.
    """
    frames = []
    for part in daftar_part(source, label):
        try:
            frames.append(pd.read_parquet(part))
        except Exception as exc:
            log.warning(
                "staging %s: part %s tidak bisa dibaca (%s: %s); dilewati",
                source, part.name, type(exc).__name__, exc,
            )
    if not frames:
        return None

    df = pd.concat(frames, ignore_index=True)
    if dedup_cols:
        kolom = [dedup_cols] if isinstance(dedup_cols, str) else list(dedup_cols)
        kolom = [c for c in kolom if c in df.columns]
        if kolom:
            df = df.drop_duplicates(subset=kolom, keep="last")
    return df


def hapus_staging(source: str, label: str) -> None:
    """Buang staging satu run; dipanggil setelah parquet final berhasil ditulis."""
    run = path_run(source, label)
    if not run.is_dir():
        return
    try:
        shutil.rmtree(run)
        run.parent.rmdir()  # hanya berhasil kalau tidak ada run lain
    except OSError as exc:
        log.warning("staging %s: %s tidak bisa dihapus: %s", source, run, exc)
