from __future__ import annotations

import logging

import pandas as pd

from ..shared.periods import resolve_years
from ..shared.storage import list_periods, read_parquet, write_parquet

log = logging.getLogger(__name__)

SOURCE = "cuaca"

# Mapping kode WMO -> kategori cuaca (Bahasa Indonesia) — dari script nyoba_jxb
WMO_WEATHER_CATEGORY = {
    0: "Cerah",
    1: "Umumnya Cerah",
    2: "Sebagian Berawan",
    3: "Berawan",
    45: "Kabut",
    48: "Kabut Membeku",
    51: "Gerimis Ringan",
    53: "Gerimis Sedang",
    55: "Gerimis Deras",
    56: "Gerimis Beku Ringan",
    57: "Gerimis Beku Deras",
    61: "Hujan",
    63: "Hujan Sedang",
    65: "Hujan Deras",
    66: "Hujan Beku Ringan",
    67: "Hujan Beku Deras",
    71: "Salju Ringan",
    73: "Salju Sedang",
    75: "Salju Deras",
    77: "Butiran Salju",
    80: "Hujan Lokal Ringan",
    81: "Hujan Lokal Sedang",
    82: "Hujan Lokal Deras",
    85: "Salju Lokal Ringan",
    86: "Salju Lokal Deras",
    95: "Badai Petir",
    96: "Badai Petir dengan Hujan Es Ringan",
    99: "Badai Petir dengan Hujan Es Deras",
}
DEFAULT_WEATHER_CATEGORY = "Lainnya"

KOLOM_SILVER = [
    "id",
    "tanggal",
    "jam",
    "nama_wilayah",
    "suhu_c",
    "suhu_terasa_c",
    "curah_hujan_mm",
    "kode_cuaca",
    "kategori_cuaca",
    "cuaca_ekstrem",
]


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()
    dt = pd.to_datetime(out["time"])
    out["tanggal"] = dt.dt.date
    out["jam"] = dt.dt.hour

    out["suhu_c"] = pd.to_numeric(out["temperature_2m"], errors="coerce")
    out["suhu_terasa_c"] = pd.to_numeric(out["apparent_temperature"], errors="coerce")
    out["curah_hujan_mm"] = pd.to_numeric(out["precipitation"], errors="coerce")
    out["kode_cuaca"] = pd.to_numeric(out["weather_code"], errors="coerce")
    out["nama_wilayah"] = out["region_name"]

    def _map(kode):
        try:
            return WMO_WEATHER_CATEGORY.get(int(kode), DEFAULT_WEATHER_CATEGORY)
        except (TypeError, ValueError):
            return DEFAULT_WEATHER_CATEGORY

    out["kategori_cuaca"] = out["kode_cuaca"].map(_map)
    out["cuaca_ekstrem"] = (out["suhu_terasa_c"] >= 41.0) | (out["curah_hujan_mm"] >= 10.0)

    out["id"] = (
        out["tanggal"].astype(str)
        + "-"
        + out["jam"].astype(str).str.zfill(2)
        + "-"
        + out["nama_wilayah"].str.replace(" ", "_", regex=False)
    )
    return out[KOLOM_SILVER].drop_duplicates(subset=["id"]).reset_index(drop=True)


def transform(period: str | None = None, dry_run: bool = False) -> dict[str, pd.DataFrame]:
    """TRANSFORM (silver): seluruh file tahun cuaca yang ada -> silver per tahun."""
    periods = (
        [str(y) for y in resolve_years(period)]
        if period
        else list_periods("bronze", SOURCE, "cuaca")
    )
    if not periods:
        log.warning("cuaca: tidak ada file bronze untuk diproses")
        return {}

    frames: list[pd.DataFrame] = []
    for p in periods:
        try:
            found = read_parquet("bronze", SOURCE, "cuaca", p)
        except FileNotFoundError:
            log.warning("cuaca: bronze tidak ditemukan untuk periode %s", p)
            continue
        df = _clean(found)
        if df.empty:
            continue
        if dry_run:
            log.info("[dry-run] cuaca silver %s: %d baris (tidak ditulis)", p, len(df))
        else:
            path = write_parquet(df, "silver", SOURCE, "cuaca", p)
            log.info("cuaca silver ditulis: %s (%d baris)", path, len(df))
        frames.append(df)

    if not frames:
        return {}
    return {"cuaca": pd.concat(frames, ignore_index=True)}