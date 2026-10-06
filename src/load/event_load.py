from __future__ import annotations

import logging

import pandas as pd

from ..shared.config import get_db_config
from ..shared.db import upsert_df
from ..shared.periods import resolve_period
from ..shared.storage import read_parquet

log = logging.getLogger(__name__)

SOURCE = "event"
DATASET = "agenda"
TABLE = "event_agenda"
CONFLICT_KEYS = ["id_event"]


def load(period: str | None = None, dry_run: bool = False) -> dict[str, pd.DataFrame]:
    _, _ = resolve_period(period)
    label = (period or "latest").strip().lower()

    try:
        df = read_parquet("silver", SOURCE, DATASET, label)
    except FileNotFoundError:
        log.warning("event: silver tidak ada utk %s", label)
        return {}

    if df.empty:
        return {}

    schema = get_db_config()["schema_gold"]
    if dry_run:
        log.info("[dry-run] event -> %s.%s (%d baris, tidak dimuat)", schema, TABLE, len(df))
        return {"agenda": df}

    upsert_df(df, schema=schema, table=TABLE, conflict_keys=CONFLICT_KEYS)
    log.info("%d data %s berhasil dimasukkan ke database dalam tabel %s.%s",
             len(df), SOURCE, schema, TABLE)
    return {"agenda": df}