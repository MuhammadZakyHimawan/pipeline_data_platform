from __future__ import annotations

import logging

import pandas as pd

from ..shared.config import get_db_config
from ..shared.db import upsert_df
from ..shared.periods import resolve_years
from ..shared.storage import list_periods, read_parquet

log = logging.getLogger(__name__)

SOURCE = "bps"
PREFIX = "bps"

# dataset -> {gold_table, conflict_keys, period_delete_col?}
LOAD_SPEC = {
    "asal_turis": {"table": "bps_asal_turis", "conflict_keys": ["id"]},
    "inflasi": {"table": "bps_inflasi", "conflict_keys": ["id"]},
    "lama_menginap": {"table": "bps_lama_menginap", "conflict_keys": ["id"]},
    "tpk_bintang": {"table": "bps_tpk_bintang", "conflict_keys": ["id"]},
    "wisman_pintu_masuk": {"table": "bps_wisman_pintu_masuk", "conflict_keys": ["tahun", "bulan", "tempat"]},
    "wisnus_tujuan": {"table": "bps_wisnus_tujuan", "conflict_keys": ["tahun", "bulan", "tempat"]},
}


def load_dataset(dataset: str, period_label: str, dry_run: bool = False) -> pd.DataFrame | None:
    """LOAD (gold): silver parquet -> upsert ke PostgreSQL schema gold."""
    spec = LOAD_SPEC[dataset]
    try:
        df = read_parquet("silver", SOURCE, dataset, period_label)
    except FileNotFoundError:
        log.warning("bps/%s: silver tidak ada utk periode %s", dataset, period_label)
        return None

    if df.empty:
        log.warning("bps/%s: silver kosong utk periode %s", dataset, period_label)
        return None

    schema = get_db_config()["schema_gold"]
    if dry_run:
        log.info("[dry-run] bps/%s -> %s.%s (%d baris, tidak dimuat)",
                 dataset, schema, spec["table"], len(df))
        return df

    upsert_df(df, schema=schema, table=spec["table"], conflict_keys=spec["conflict_keys"])
    log.info("%d data %s berhasil dimasukkan ke database dalam tabel %s.%s",
             len(df), dataset, schema, spec["table"])
    return df


def load(period: str | None = None, dry_run: bool = False) -> dict[str, pd.DataFrame]:
    """LOAD semua dataset BPS silver -> gold PostgreSQL.

    Silver BPS satu file per tahun; setiap file yang ada diproses (upsert
    berbasis conflict_keys sehingga aman dijalankan berulang).
    """
    hasil: dict[str, pd.DataFrame] = {}
    for dataset in LOAD_SPEC:
        periods = (
            [str(y) for y in resolve_years(period)]
            if period
            else list_periods("silver", SOURCE, dataset)
        )
        if not periods:
            log.warning("bps/%s: tidak ada file silver untuk dimuat", dataset)
            continue
        frames: list[pd.DataFrame] = []
        for p in periods:
            df = load_dataset(dataset, p, dry_run=dry_run)
            if df is not None:
                frames.append(df)
        if frames:
            hasil[dataset] = pd.concat(frames, ignore_index=True)
    return hasil