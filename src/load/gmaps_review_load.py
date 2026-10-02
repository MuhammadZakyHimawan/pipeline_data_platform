from __future__ import annotations

import logging

import pandas as pd

from ..shared.periods import resolve_months
from ..shared.storage import list_periods, read_parquet

log = logging.getLogger(__name__)

SOURCE = "gmaps_review"
DATASET = "review"


def load(period: str | None = None, dry_run: bool = False) -> dict[str, pd.DataFrame]:
    """LOAD: sengaja tanpa aksi.

    Ulasan Google Maps hanya bermakna sebagai data lake (dan sumber umpan
    model), bukan tabel gold. Tidak ada tabel gold untuk sumber ini, jadi
    tahap load hanya memvalidasi bahwa silver terbaca dan melaporkannya.
    """
    periods = ([str(p) for p in resolve_months(period)]
               if period
               else list_periods("silver", SOURCE, DATASET))
    if not periods:
        log.warning("gmaps_review: tidak ada file silver untuk diperiksa")
        return {}

    frames: list[pd.DataFrame] = []
    for p in periods:
        try:
            df = read_parquet("silver", SOURCE, DATASET, p)
        except FileNotFoundError:
            log.warning("gmaps_review: silver tidak ditemukan untuk periode %s", p)
            continue
        if df.empty:
            continue
        hotel = df["id_kompetitor"].nunique() if "id_kompetitor" in df else 0
        log.info("gmaps_review %s siap dipakai: %d ulasan dari %d hotel "
                 "(tidak dimuat ke gold)", p, len(df), hotel)
        frames.append(df)

    if not frames:
        return {}
    return {DATASET: pd.concat(frames, ignore_index=True)}
