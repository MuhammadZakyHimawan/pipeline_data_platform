"""LOAD harga OTA Google Travel: sengaja tanpa aksi.

Harga Google Travel hanya bermakna sebagai data lake (umpan model), bukan tabel
gold. Tidak ada tabel gold untuk sumber ini, jadi tahap load hanya memvalidasi
bahwa silver terbaca dan melaporkannya.
"""

from __future__ import annotations

import logging

import pandas as pd

from ..shared.periods import resolve_years
from ..shared.storage import list_periods, read_parquet

log = logging.getLogger(__name__)

SOURCE = "gmaps_harga"
DATASET = "harga"


def load(period: str | None = None, dry_run: bool = False) -> dict[str, pd.DataFrame]:
    """VALIDASI: baca silver per tahun, laporkan, tanpa menulis gold."""
    years = (
        [str(y) for y in resolve_years(period)]
        if period
        else list_periods("silver", SOURCE, DATASET)
    )
    if not years:
        log.warning("gmaps_harga: tidak ada file silver untuk diperiksa")
        return {}

    frames: list[pd.DataFrame] = []
    for y in years:
        try:
            df = read_parquet("silver", SOURCE, DATASET, y)
        except FileNotFoundError:
            log.warning("gmaps_harga: silver tidak ditemukan untuk periode %s", y)
            continue
        if df.empty:
            continue
        hotel = df["id_kompetitor"].nunique() if "id_kompetitor" in df else 0
        sumber = df["sumber"].nunique() if "sumber" in df else 0
        log.info("gmaps_harga %s siap dipakai: %d harga dari %d hotel / %d OTA "
                 "(tidak dimuat ke gold)", y, len(df), hotel, sumber)
        frames.append(df)

    if not frames:
        return {}
    return {DATASET: pd.concat(frames, ignore_index=True)}
