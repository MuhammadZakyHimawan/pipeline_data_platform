"""Transform silver harga OTA Google Travel dari bronze per tahun.

Bronze harga dipecah per tahun (kolom `scrape_date`, mode append karena run
harian), jadi transform membangun satu file silver per tahun juga. Cleaning
membuang harga tidak valid, menyeragamkan tipe, dan memberi id unik idempoten.
"""

from __future__ import annotations

import hashlib
import logging

import pandas as pd

from ..shared.periods import resolve_years
from ..shared.storage import list_periods, read_parquet, write_parquet

log = logging.getLogger(__name__)

SOURCE = "gmaps_harga"
DATASET = "harga"

KOLOM_SILVER = [
    "id_harga",
    "id_kompetitor",
    "checkin_date",
    "lead_time",
    "sumber",
    "harga",
    "scrape_date",
]


def _id_harga(id_kompetitor, checkin_date, sumber, scrape_date) -> str:
    sumber_bersih = " ".join(str(sumber or "").split()).strip().lower()
    raw = (
        f"{str(id_kompetitor).strip()}|{str(checkin_date).strip()}|"
        f"{sumber_bersih}|{str(scrape_date).strip()}"
    )
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _bersihkan_harga(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=KOLOM_SILVER)

    out = df.copy()
    out["harga"] = pd.to_numeric(out["harga"], errors="coerce")
    out = out[out["harga"] > 0].copy()
    for kol in ("id_kompetitor", "sumber"):
        if kol in out:
            out[kol] = out[kol].astype("string").str.strip().replace({"": None})
    out["checkin_date"] = pd.to_datetime(out["checkin_date"], errors="coerce").dt.date
    out["scrape_date"] = pd.to_datetime(out["scrape_date"], errors="coerce").dt.date
    out["lead_time"] = pd.to_numeric(out["lead_time"], errors="coerce").astype("Int64")
    out = out.dropna(
        subset=["id_kompetitor", "sumber", "checkin_date", "scrape_date", "harga"]
    ).copy()

    if out.empty:
        return pd.DataFrame(columns=KOLOM_SILVER)

    out["id_harga"] = [
        _id_harga(r["id_kompetitor"], r["checkin_date"], r["sumber"], r["scrape_date"])
        for _, r in out.iterrows()
    ]
    out = out.drop_duplicates(subset=["id_harga"]).reset_index(drop=True)
    return out[[k for k in KOLOM_SILVER if k in out.columns]]


def transform(period: str | None = None, dry_run: bool = False) -> dict[str, pd.DataFrame]:
    """TRANSFORM (silver): file bronze harga per tahun -> silver per tahun."""
    years = (
        [str(y) for y in resolve_years(period)]
        if period
        else list_periods("bronze", SOURCE, DATASET)
    )
    if not years:
        log.warning("gmaps_harga: tidak ada file bronze untuk diproses")
        return {}

    frames: list[pd.DataFrame] = []
    for y in years:
        try:
            bronze = read_parquet("bronze", SOURCE, DATASET, y)
        except FileNotFoundError:
            log.warning("gmaps_harga: bronze tidak ditemukan untuk periode %s", y)
            continue
        df = _bersihkan_harga(bronze)
        if df.empty:
            log.info("gmaps_harga silver %s: tidak ada harga valid", y)
            continue
        if dry_run:
            log.info("[dry-run] gmaps_harga silver %s: %d baris (tidak ditulis)", y, len(df))
        else:
            path = write_parquet(df, "silver", SOURCE, DATASET, y)
            log.info("gmaps_harga silver ditulis: %s (%d baris)", path, len(df))
        frames.append(df)

    if not frames:
        return {}
    return {DATASET: pd.concat(frames, ignore_index=True)}
