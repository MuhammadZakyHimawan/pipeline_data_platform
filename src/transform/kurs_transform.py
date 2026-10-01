from __future__ import annotations

import logging

import pandas as pd

from ..shared.periods import resolve_years
from ..shared.storage import list_periods, read_parquet, write_parquet

log = logging.getLogger(__name__)

SOURCE = "kurs"

MATA_UANG_LABEL = {
    "USD": "Dolar Amerika Serikat",
    "AUD": "Dolar Australia",
    "SGD": "Dolar Singapura",
    "EUR": "Euro",
    "GBP": "Poundsterling Inggris",
    "JPY": "Yen Jepang",
    "MYR": "Ringgit Malaysia",
    "CNY": "Yuan Cina",
}


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()
    out["tanggal"] = pd.to_datetime(out["tanggal"]).dt.date
    out["kode_mata_uang"] = out["kode_mata_uang"].astype(str).str.upper().str.strip()
    out["nama_mata_uang"] = out["kode_mata_uang"].map(MATA_UANG_LABEL).fillna("Lainnya")
    for col in ("nilai_satuan", "kurs_beli", "kurs_jual", "kurs_tengah"):
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out["tanggal_ekstraksi"] = pd.to_datetime(out["tanggal_ekstraksi"]).dt.date

    # kunci deterministik: tanggal + kode mata uang
    out["id"] = out["tanggal"].astype(str) + "-" + out["kode_mata_uang"]

    cols = [
        "id",
        "tanggal",
        "kode_mata_uang",
        "nama_mata_uang",
        "nilai_satuan",
        "kurs_beli",
        "kurs_jual",
        "kurs_tengah",
        "tanggal_ekstraksi",
    ]
    return out[cols].drop_duplicates(subset=["id"]).reset_index(drop=True)


def transform_dataset(period_label: str, dry_run: bool = False) -> pd.DataFrame | None:
    try:
        bronze = read_parquet("bronze", SOURCE, "kurs", period_label)
    except FileNotFoundError:
        log.warning("kurs: bronze tidak ada utk periode %s", period_label)
        return None

    df = _clean(bronze)
    if df.empty:
        log.warning("kurs: hasil clean kosong utk periode %s", period_label)
        return None

    if dry_run:
        log.info("[dry-run] kurs silver: %d baris (tidak ditulis)", len(df))
        return df

    path = write_parquet(df, "silver", SOURCE, "kurs", period_label)
    log.info("kurs silver ditulis: %s (%d baris)", path, len(df))
    return df


def transform(period: str | None = None, dry_run: bool = False) -> dict[str, pd.DataFrame]:
    """TRANSFORM (silver): seluruh file tahun kurs yang ada -> silver per tahun."""
    periods = (
        [str(y) for y in resolve_years(period)]
        if period
        else list_periods("bronze", SOURCE, "kurs")
    )
    if not periods:
        log.warning("kurs: tidak ada file bronze untuk diproses")
        return {}

    frames: list[pd.DataFrame] = []
    for p in periods:
        df = transform_dataset(p, dry_run=dry_run)
        if df is not None:
            frames.append(df)
    if not frames:
        return {}
    return {"kurs": pd.concat(frames, ignore_index=True)}