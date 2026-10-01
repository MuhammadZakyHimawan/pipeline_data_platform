from __future__ import annotations

import logging
from datetime import date, timedelta

import pandas as pd

from ..shared.config import get_db_config, get_source_config
from ..shared.db import upsert_df
from ..shared.row_limit import is_limited
from ..shared.storage import read_parquet

log = logging.getLogger(__name__)

SOURCE = "gdelt"
TABLE = "gdelt_berita"
CONFLICT_KEYS = ["global_event_id"]


def _resolusi_tanggal(cfg: dict, period: str | None) -> date:
    s = (period or cfg.get("period", "latest")).strip()
    if len(s) == 8 and s.isdigit():
        import datetime
        return datetime.datetime.strptime(s, "%Y%m%d").date()
    return date.today() - timedelta(days=1)


def load(period: str | None = None, dry_run: bool = False) -> dict[str, pd.DataFrame]:
    cfg = get_source_config(SOURCE)
    period_label = _resolusi_tanggal(cfg, period).isoformat()

    try:
        df = read_parquet("silver", SOURCE, "berita", period_label)
    except FileNotFoundError:
        log.warning("gdelt: silver tidak ada utk %s", period_label)
        return {}

    if df.empty:
        return {}

    schema = get_db_config()["schema_gold"]
    if dry_run:
        log.info("[dry-run] gdelt -> %s.%s (%d baris, tidak dimuat)", schema, TABLE, len(df))
        return {"berita": df}

    if is_limited():
        log.info("%d data %s berhasil didapatkan", len(df), SOURCE)
    else:
        upsert_df(df, schema=schema, table=TABLE, conflict_keys=CONFLICT_KEYS)
        log.info("%d data %s berhasil dimasukkan ke database dalam tabel %s.%s",
                 len(df), SOURCE, schema, TABLE)
    return {"berita": df}