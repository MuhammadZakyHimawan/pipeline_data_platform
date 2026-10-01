from __future__ import annotations

import hashlib
import logging
import re

import pandas as pd

from ..shared.config import get_source_config
from ..shared.storage import read_parquet, write_parquet

log = logging.getLogger(__name__)

SOURCE = "gmaps"
LABEL_DEFAULT = "latest"

# `hotel_master` tidak lagi diproses: registry hotel kini dibaca langsung dari
# gold.gmaps_hotel_master oleh gmaps_scraper, jadi tidak ada bronze/silver
# hotel_master yang perlu dibangun (dan tidak ada yang menimpa DB).
DATASETS = ("harga", "review")

KOLOM_SILVER = {
    "harga": [
        "id_harga", "id_kompetitor", "checkin_date", "lead_time", "sumber",
        "harga", "scrape_date",
    ],
    "review": [
        "id_review", "id_kompetitor", "nama_reviewer", "rating_bintang",
        "isi_review", "bulan", "tahun",
    ],
}


def _label(cfg: dict, period: str | None) -> str:
    return (period or cfg.get("period", LABEL_DEFAULT)).strip().lower()


def _id_harga(id_kompetitor, checkin_date, sumber, scrape_date) -> str:
    raw = f"{str(id_kompetitor).strip()}|{str(checkin_date).strip()}|"
    raw += f"{re.sub(r'\\s+', ' ', str(sumber or '')).strip().lower()}|"
    raw += f"{str(scrape_date).strip()}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _id_review(id_kompetitor, nama_reviewer, isi_review) -> str:
    raw = f"{str(id_kompetitor).strip()}|{str(nama_reviewer).strip()}|"
    raw += f"{str(isi_review).strip()}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _bersihkan_harga(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["harga"] = pd.to_numeric(out["harga"], errors="coerce")
    out = out[out["harga"] > 0].copy()
    for kol in ("id_kompetitor", "sumber"):
        out[kol] = out[kol].astype("string").str.strip().replace({"": None})
    out["checkin_date"] = pd.to_datetime(out["checkin_date"], errors="coerce").dt.date
    out["scrape_date"] = pd.to_datetime(out["scrape_date"], errors="coerce").dt.date
    out["lead_time"] = pd.to_numeric(out["lead_time"], errors="coerce").astype("Int64")
    out = out.dropna(subset=["id_kompetitor", "sumber", "checkin_date", "scrape_date"]).copy()

    if out.empty:
        return pd.DataFrame(columns=KOLOM_SILVER["harga"])

    out["id_harga"] = [
        _id_harga(r["id_kompetitor"], r["checkin_date"], r["sumber"], r["scrape_date"])
        for _, r in out.iterrows()
    ]
    out = out.drop_duplicates(subset=["id_harga"]).reset_index(drop=True)
    return out[KOLOM_SILVER["harga"]]


def _bersihkan_review(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for kol in ("nama_reviewer", "isi_review"):
        out[kol] = out[kol].astype("string").str.strip()
    out = out[out["isi_review"].notna() & (out["isi_review"] != "")].copy()
    for kol in ("id_kompetitor", "nama_reviewer"):
        out[kol] = out[kol].astype("string").str.strip()
    out = out.dropna(subset=["id_kompetitor", "nama_reviewer"]).copy()
    out["rating_bintang"] = pd.to_numeric(out["rating_bintang"], errors="coerce").fillna(0).astype(int)
    out["bulan"] = pd.to_numeric(out["bulan"], errors="coerce").fillna(0).astype(int)
    out["tahun"] = pd.to_numeric(out["tahun"], errors="coerce").fillna(0).astype(int)
    out = out[(out["bulan"] >= 1) & (out["bulan"] <= 12) & (out["tahun"] >= 2000)].copy()

    if out.empty:
        return pd.DataFrame(columns=KOLOM_SILVER["review"])

    out["id_review"] = [
        _id_review(r["id_kompetitor"], r["nama_reviewer"], r["isi_review"])
        for _, r in out.iterrows()
    ]
    out = out.drop_duplicates(subset=["id_review"]).reset_index(drop=True)
    return out[KOLOM_SILVER["review"]]


def transform(period: str | None = None, dry_run: bool = False) -> dict[str, pd.DataFrame]:
    cfg = get_source_config(SOURCE)
    label = _label(cfg, period)

    result: dict[str, pd.DataFrame] = {}
    pembersih = {
        "harga": _bersihkan_harga,
        "review": _bersihkan_review,
    }
    for dataset in DATASETS:
        try:
            bronze = read_parquet("bronze", SOURCE, dataset, label)
        except FileNotFoundError:
            log.warning("gmaps: bronze %s tidak ada utk %s", dataset, label)
            continue

        df = pembersih[dataset](bronze)
        if df.empty:
            log.warning("gmaps silver %s kosong setelah cleaning", dataset)
            continue

        if dry_run:
            log.info("[dry-run] gmaps silver %s: %d baris (tidak ditulis)", dataset, len(df))
        else:
            path = write_parquet(df, "silver", SOURCE, dataset, label)
            log.info("gmaps silver %s ditulis: %s (%d baris)", dataset, path, len(df))
        result[dataset] = df
    return result