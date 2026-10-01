from __future__ import annotations

import logging

import pandas as pd

from ..shared.config import get_db_config, get_source_config
from ..shared.db import upsert_df
from ..shared.row_limit import is_limited
from ..shared.storage import read_parquet

log = logging.getLogger(__name__)

SOURCE = "gmaps"
LABEL_DEFAULT = "latest"

# gold.gmaps_hotel_master TIDAK ditulis di sini: tabel itu adalah registry
# hotel sumber kebenaran, dikelola manual. Kalau tabel ini ikut di-upsert dari
# parquet basi, daftar hotel bisa tertimpa data lama.
TABLES = {
    "harga": ("gmaps_hotel_harga", ["id_harga"]),
    "review": ("gmaps_review", ["id_review"]),
}


def _label(cfg: dict, period: str | None) -> str:
    return (period or cfg.get("period", LABEL_DEFAULT)).strip().lower()


def load(period: str | None = None, dry_run: bool = False) -> dict[str, pd.DataFrame]:
    cfg = get_source_config(SOURCE)
    label = _label(cfg, period)

    frames: dict[str, pd.DataFrame] = {}
    for dataset, (table, _) in TABLES.items():
        try:
            df = read_parquet("silver", SOURCE, dataset, label)
        except FileNotFoundError:
            log.warning("gmaps: silver %s tidak ada utk %s", dataset, label)
            continue
        if df.empty:
            continue
        frames[dataset] = df

    if not frames:
        log.warning("gmaps: tidak ada silver yang dimuat")
        return {}

    schema = get_db_config()["schema_gold"]
    if dry_run:
        for dataset, df in frames.items():
            table = TABLES[dataset][0]
            log.info("[dry-run] gmaps -> %s.%s (%d baris, tidak dimuat)", schema, table, len(df))
        return frames

    for dataset, df in frames.items():
        table, keys = TABLES[dataset]
        if is_limited():
            log.info("%d data %s berhasil didapatkan", len(df), dataset)
        else:
            upsert_df(df, schema=schema, table=table, conflict_keys=keys)
            log.info("%d data %s berhasil dimasukkan ke database dalam tabel %s.%s",
                     len(df), dataset, schema, table)
    return frames