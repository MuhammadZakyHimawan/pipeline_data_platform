from __future__ import annotations

import logging

import pandas as pd

from ..shared.config import get_db_config, get_source_config
from ..shared.db import upsert_df
from ..shared.storage import read_parquet

log = logging.getLogger(__name__)

SOURCE = "trends"
DATASET = "search_intent"
TABLE = "trends_search_intent"
CONFLICT_KEYS = ["id_tren"]
LABEL_DEFAULT = "latest"


def _label(cfg: dict, period: str | None) -> str:
    return (period or cfg.get("period", LABEL_DEFAULT)).strip().lower()


def load(period: str | None = None, dry_run: bool = False) -> dict[str, pd.DataFrame]:
    cfg = get_source_config(SOURCE)
    label = _label(cfg, period)

    try:
        df = read_parquet("silver", SOURCE, DATASET, label)
    except FileNotFoundError:
        log.warning("trends: silver tidak ada utk %s", label)
        return {}

    if df.empty:
        return {}

    schema = get_db_config()["schema_gold"]
    if dry_run:
        log.info("[dry-run] trends -> %s.%s (%d baris, tidak dimuat)", schema, TABLE, len(df))
        return {"search_intent": df}

    upsert_df(df, schema=schema, table=TABLE, conflict_keys=CONFLICT_KEYS)
    log.info("%d data %s berhasil dimasukkan ke database dalam tabel %s.%s",
             len(df), SOURCE, schema, TABLE)
    return {"search_intent": df}