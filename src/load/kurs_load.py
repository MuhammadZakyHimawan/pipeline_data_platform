from __future__ import annotations

import logging

import pandas as pd

from ..shared.config import get_db_config
from ..shared.db import upsert_df
from ..shared.periods import resolve_years
from ..shared.row_limit import is_limited
from ..shared.storage import list_periods, read_parquet

log = logging.getLogger(__name__)

SOURCE = "kurs"
TABLE = "kurs_harian"
CONFLICT_KEYS = ["id"]


def load(period: str | None = None, dry_run: bool = False) -> dict[str, pd.DataFrame]:
    """LOAD (gold): seluruh file silver kurs per tahun -> upsert PostgreSQL."""
    periods = (
        [str(y) for y in resolve_years(period)]
        if period
        else list_periods("silver", SOURCE, "kurs")
    )
    if not periods:
        log.warning("kurs: tidak ada file silver untuk dimuat")
        return {}

    frames: list[pd.DataFrame] = []
    for p in periods:
        try:
            df = read_parquet("silver", SOURCE, "kurs", p)
        except FileNotFoundError:
            log.warning("kurs: silver tidak ada utk periode %s", p)
            continue
        if df.empty:
            log.warning("kurs: silver kosong utk periode %s", p)
            continue

        schema = get_db_config()["schema_gold"]
        if dry_run:
            log.info("[dry-run] kurs -> %s.%s (%d baris, tidak dimuat)", schema, TABLE, len(df))
        elif is_limited():
            log.info("%d data %s berhasil didapatkan", len(df), SOURCE)
        else:
            upsert_df(df, schema=schema, table=TABLE, conflict_keys=CONFLICT_KEYS)
            log.info("%d data %s berhasil dimasukkan ke database dalam tabel %s.%s",
                     len(df), SOURCE, schema, TABLE)
        frames.append(df)

    if not frames:
        return {}
    return {"kurs": pd.concat(frames, ignore_index=True)}