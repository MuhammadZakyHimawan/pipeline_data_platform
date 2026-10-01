from __future__ import annotations

import logging

import pandas as pd

from ..shared.config import get_source_config
from ..shared.storage import read_parquet, write_parquet

log = logging.getLogger(__name__)

SOURCE = "trends"
DATASET = "search_intent"
LABEL_DEFAULT = "latest"

KOLOM_SILVER = [
    "id_tren",
    "tanggal",
    "indeks",
    "kategori",
    "search_term",
    "geo",
]


def _label(cfg: dict, period: str | None) -> str:
    return (period or cfg.get("period", LABEL_DEFAULT)).strip().lower()


def _id_tren(tanggal, search_term, geo) -> str:
    import hashlib
    bahan = f"{tanggal}|{search_term}|{geo}".encode("utf-8")
    return hashlib.md5(bahan).hexdigest()


def _bersihkan(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()
    out["tanggal"] = pd.to_datetime(out["tanggal"], errors="coerce").dt.date
    out["indeks"] = pd.to_numeric(out["indeks"], errors="coerce")
    for kol in ("kategori", "search_term", "geo"):
        out[kol] = out[kol].astype("string").str.strip().replace({"": None})

    out = out.dropna(subset=["tanggal", "search_term", "geo"]).copy()
    out["kategori"] = out["kategori"].fillna("Destinasi")

    if out.empty:
        log.warning("trends: tidak ada baris valid setelah cleaning")
        return pd.DataFrame()

    out["id_tren"] = [
        _id_tren(r["tanggal"], r["search_term"], r["geo"]) for _, r in out.iterrows()
    ]
    out = out.drop_duplicates(subset=["id_tren"]).reset_index(drop=True)
    return out[KOLOM_SILVER]


def transform(period: str | None = None, dry_run: bool = False) -> dict[str, pd.DataFrame]:
    cfg = get_source_config(SOURCE)
    label = _label(cfg, period)

    try:
        bronze = read_parquet("bronze", SOURCE, DATASET, label)
    except FileNotFoundError:
        log.warning("trends: bronze tidak ada utk %s", label)
        return {}

    df = _bersihkan(bronze)
    if df.empty:
        return {}

    if dry_run:
        log.info("[dry-run] trends silver: %d baris (tidak ditulis)", len(df))
        return {"search_intent": df}

    path = write_parquet(df, "silver", SOURCE, DATASET, label)
    log.info("trends silver ditulis: %s (%d baris)", path, len(df))
    return {"search_intent": df}