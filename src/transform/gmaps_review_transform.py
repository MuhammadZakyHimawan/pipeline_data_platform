from __future__ import annotations

import hashlib
import logging
import re

import pandas as pd

from ..shared.periods import resolve_months
from ..shared.storage import list_periods, read_parquet, write_parquet

log = logging.getLogger(__name__)

SOURCE = "gmaps_review"
DATASET = "review"

KOLOM_SILVER = [
    "id",
    "id_kompetitor",
    "nama_hotel",
    "nama_place",
    "url_maps",
    "nama_reviewer",
    "rating_bintang",
    "bahasa_ulasan",
    "isi_review",
    "waktu_relatif",
    "tanggal_ulasan",
    "periode",
    "bulan",
    "tahun",
    "scraped_at",
]


def _teks_bersih(nilai) -> str:
    """Rapatkan teks ulasan: spasi berulang jadi satu, baris kosong dibuang."""
    teks = re.sub(r"[ \t\u00a0]+", " ", str(nilai if nilai is not None else ""))
    return re.sub(r"\s*\n\s*", "\n", teks).strip()


def _id_baris(row) -> str:
    """Kunci idempoten satu ulasan.

    Ulasan yang sama bisa muncul di beberapa run (scroll memuat ulang kartu,
    atau bulan yang sama di-scrape dua kali), jadi kunci memakai hotel +
    tanggal + nama + awalan teks, bukan urutan baris.
    """
    dasar = "|".join(str(row.get(k) or "") for k in (
        "id_kompetitor", "tanggal_ulasan", "nama_reviewer",
    ))
    cuplikan = str(row.get("isi_review") or "")[:160]
    return f"{dasar}|{hashlib.sha1(cuplikan.encode('utf-8')).hexdigest()[:12]}"


def _clean(df: pd.DataFrame, periode: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()

    for kolom in ("nama_hotel", "nama_place", "nama_reviewer", "waktu_relatif", "url_maps"):
        if kolom in out:
            out[kolom] = out[kolom].astype("string").str.strip()

    out["isi_review"] = out["isi_review"].map(_teks_bersih)
    out = out[out["isi_review"].str.len() > 0]

    if "bahasa_ulasan" not in out:
        out["bahasa_ulasan"] = ""
    out["bahasa_ulasan"] = out["bahasa_ulasan"].astype("string").str.strip().fillna("")

    out["rating_bintang"] = pd.to_numeric(out.get("rating_bintang"), errors="coerce")
    out["rating_bintang"] = out["rating_bintang"].clip(1, 5).astype("Int64")

    out["tanggal_ulasan"] = pd.to_datetime(out.get("tanggal_ulasan"), errors="coerce")
    out = out[out["tanggal_ulasan"].notna()]
    out["tanggal_ulasan"] = out["tanggal_ulasan"].dt.date.astype("string")

    out["tahun"] = out["tanggal_ulasan"].str.slice(0, 4).astype("Int64")
    out["bulan"] = out["tanggal_ulasan"].str.slice(5, 7).astype("Int64")
    out["periode"] = periode

    # Hanya ulasan pada bulan yang sedang diproses agar file bulanan tidak
    # tercampur bulan lain.
    tahun, bulan = periode.split("-")
    out = out[(out["tahun"] == int(tahun)) & (out["bulan"] == int(bulan))]

    out["id"] = [_id_baris(r) for r in out.to_dict("records")]
    kolom = [k for k in KOLOM_SILVER if k in out.columns]
    return (out[kolom]
            .drop_duplicates(subset=["id"])
            .sort_values(["id_kompetitor", "tanggal_ulasan", "id"])
            .reset_index(drop=True))


def transform(period: str | None = None, dry_run: bool = False) -> dict[str, pd.DataFrame]:
    """TRANSFORM (silver): file bronze ulasan per bulan -> silver per bulan."""
    periods = ([str(p) for p in resolve_months(period)]
               if period
               else list_periods("bronze", SOURCE, DATASET))
    if not periods:
        log.warning("gmaps_review: tidak ada file bronze untuk diproses")
        return {}

    frames: list[pd.DataFrame] = []
    for p in periods:
        try:
            bronze = read_parquet("bronze", SOURCE, DATASET, p)
        except FileNotFoundError:
            log.warning("gmaps_review: bronze tidak ditemukan untuk periode %s", p)
            continue
        df = _clean(bronze, p)
        if df.empty:
            log.info("gmaps_review silver %s: tidak ada ulasan pada bulan ini", p)
            continue
        if dry_run:
            log.info("[dry-run] gmaps_review silver %s: %d baris (tidak ditulis)", p, len(df))
        else:
            path = write_parquet(df, "silver", SOURCE, DATASET, p)
            log.info("gmaps_review silver ditulis: %s (%d baris)", path, len(df))
        frames.append(df)

    if not frames:
        return {}
    return {DATASET: pd.concat(frames, ignore_index=True)}
