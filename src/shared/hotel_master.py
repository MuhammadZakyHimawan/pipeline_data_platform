from __future__ import annotations

import logging
import re

import pandas as pd

from .config import get_source_config
from .db import fetch_df, table_exists

log = logging.getLogger(__name__)

# Registry hotel TIDAK berasal dari file config atau parquet data_lake: nama,
# alamat, dan koordinat hotel pembanding adalah sumber kebenaran di database
# (public.hotel_master). Tambah/kurangi hotel di tabel itu.
DEFAULT_SCHEMA = "public"
DEFAULT_TABLE = "hotel_master"
KOLOM = "id_kompetitor, nama_hotel, alamat, kota, latitude, longitude"

# Nama tabel/skema masuk ke SQL sebagai identifier, bukan parameter, jadi harus
# pola identifier PostgreSQL agar tidak bisa menyisipkan SQL lewat config.
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def kosong(nilai) -> bool:
    """True bila nilai hilang (None / NaN / pd.NA) atau cuma spasi.

    Penting: hasil baca DB lewat cursor+psycopg2 menghasilkan NaN (float) untuk
    kolom kosong, bukan None. Tanpa cek ini `str(nan or '')` menjadi string
    "nan" dan hotel dengan nama kosong akan ikut ter-scrape.
    """
    if nilai is None or nilai is pd.NA:
        return True
    try:
        if pd.isna(nilai):
            return True
    except (TypeError, ValueError):
        pass
    return not str(nilai).strip()


def angka(nilai) -> float | None:
    """Ubah nilai DB/konfigurasi menjadi float, atau None bila tidak valid."""
    if kosong(nilai):
        return None
    try:
        hasil = float(nilai)
    except (TypeError, ValueError):
        return None
    return hasil if hasil == hasil else None  # buang NaN/inf


def _teks(nilai) -> str | None:
    return None if kosong(nilai) else str(nilai).strip()


def _pecah_kota(kota) -> tuple[str | None, str | None]:
    """Pecah kolom `kota` jadi (wilayah, kota).

    Data hotel_master mengisi `kota` sebagai "kecamatan, kota" (mis.
    "Tanah Abang, Jakarta Pusat"). Untuk query pencarian Google yang andal
    bagian kedua (kota sebenarnya) yang dipakai, karena `alamat` sudah memuat
    nama jalan/kecamatan.
    """
    penuh = _teks(kota)
    if not penuh:
        return None, None
    bagian = [b.strip() for b in penuh.split(",") if b.strip()]
    if len(bagian) < 2:
        return penuh, penuh
    return penuh, bagian[-1]


def _konfigurasi_tabel(cfg: dict | None) -> tuple[str, str]:
    sumber = cfg if cfg is not None else get_source_config("gmaps_harga")
    return (
        str(sumber.get("hotel_master_schema", DEFAULT_SCHEMA)),
        str(sumber.get("hotel_master_table", DEFAULT_TABLE)),
    )


def daftar_hotel(cfg: dict | None = None) -> list[dict]:
    """Baca daftar hotel pembanding dari database.

    Kembalikan [] bila tabel belum ada / kosong / tidak bisa dibaca. Tidak ada
    fallback ke file: kalau DB tidak terbaca, lebih baik run berhenti daripada
    scraping hotel yang salah.
    """
    schema, table = _konfigurasi_tabel(cfg)
    for nama, nilai in (("skema", schema), ("tabel", table)):
        if not _IDENTIFIER.match(nilai):
            log.error("hotel_master: %s '%s' bukan identifier SQL yang valid", nama, nilai)
            return []

    try:
        if not table_exists(schema, table):
            log.error(
                "hotel_master: tabel %s.%s belum ada; isi registry hotel di database itu",
                schema, table,
            )
            return []
        sql = f'SELECT {KOLOM} FROM "{schema}"."{table}" ORDER BY id_kompetitor'
        log.info("hotel_master: executing query: %s", sql)
        df = fetch_df(sql)
        log.info("hotel_master: fetch_df returned %s", "None" if df is None else f"DataFrame with {len(df)} rows, columns={list(df.columns)}")
    except Exception as exc:
        log.error("hotel_master: gagal baca %s.%s (%s)", schema, table, exc, exc_info=True)
        return []

    if df is None or df.empty:
        log.error("hotel_master: tabel %s.%s kosong", schema, table)
        return []

    out: list[dict] = []
    skipped_no_id = 0
    skipped_no_nama = 0
    skipped_no_coord = 0
    for _, baris in df.iterrows():
        nama = _teks(baris["nama_hotel"])
        id_kompetitor = _teks(baris["id_kompetitor"])
        if not nama:
            skipped_no_nama += 1
            log.debug("hotel_master: baris id_kompetitor=%s dilewati: nama_hotel kosong", id_kompetitor)
            continue
        if not id_kompetitor:
            skipped_no_id += 1
            log.debug("hotel_master: baris nama_hotel=%s dilewati: id_kompetitor kosong", nama)
            continue
        wilayah, kota = _pecah_kota(baris["kota"])
        lat = angka(baris["latitude"])
        lon = angka(baris["longitude"])
        if lat is None or lon is None:
            skipped_no_coord += 1
            log.debug("hotel_master: baris id=%s nama=%s dilewati: koordinat kosong (lat=%s, lon=%s)", id_kompetitor, nama, lat, lon)
            continue
        out.append({
            "id_kompetitor": id_kompetitor,
            "nama_hotel": nama,
            "alamat": _teks(baris["alamat"]),
            "kota": kota,
            "wilayah": wilayah,
            "latitude": lat,
            "longitude": lon,
        })

    tanpa_koord = sum(1 for h in out if h["latitude"] is None or h["longitude"] is None)
    log.info(
        "hotel_master: %d hotel dibaca dari %s.%s%s (dilewati: %s tanpa id, %s tanpa nama, %s tanpa koordinat)",
        len(out), schema, table,
        f" ({tanpa_koord} tanpa koordinat)" if tanpa_koord else "",
        skipped_no_id, skipped_no_nama, skipped_no_coord,
    )
    if out:
        log.info("hotel_master: sample hotels: %s", [{"id": h["id_kompetitor"], "nama": h["nama_hotel"], "kota": h["kota"]} for h in out[:3]])
    return out