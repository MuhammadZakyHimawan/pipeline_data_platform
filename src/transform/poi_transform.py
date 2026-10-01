from __future__ import annotations

import logging

import pandas as pd

from ..shared.periods import resolve_period
from ..shared.storage import read_parquet, write_parquet

log = logging.getLogger(__name__)

SOURCE = "poi"

KOLOM_SILVER = [
    "id_osm",
    "nama_poi",
    "kategori_utama",
    "tag_osm",
    "latitude",
    "longitude",
    "metadata",
    "tanggal_tarik",
]


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()
    out = out[out["latitude"].notna() & out["longitude"].notna()].copy()
    out["nama_poi"] = out["nama_poi"].astype("string").str.strip().replace({"": None})
    out["metadata"] = out["metadata"].astype("string").fillna("{}")

    out = out.drop_duplicates(subset=["id_osm"], keep="first").reset_index(drop=True)
    return out[KOLOM_SILVER]


def transform(period: str | None = None, dry_run: bool = False) -> dict[str, pd.DataFrame]:
    _, _ = resolve_period(period)
    label = (period or "latest").strip().lower()

    try:
        bronze = read_parquet("bronze", SOURCE, "poi", label)
    except FileNotFoundError:
        log.warning("poi: bronze tidak ada utk %s", label)
        return {}

    df = _clean(bronze)
    if df.empty:
        log.warning("poi: tidak ada baris valid setelah cleaning")
        return {}

    if dry_run:
        log.info("[dry-run] poi silver: %d baris (tidak ditulis)", len(df))
        return {"poi": df}

    path = write_parquet(df, "silver", SOURCE, "poi", label)
    log.info("poi silver ditulis: %s (%d baris)", path, len(df))
    return {"poi": df}