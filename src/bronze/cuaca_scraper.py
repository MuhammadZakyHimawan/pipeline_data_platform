from __future__ import annotations

import logging
import re
from datetime import date, timedelta
from typing import Any

import pandas as pd
import requests

from ..shared.config import get_source_config
from ..shared.row_limit import truncate_df
from ..shared.storage import write_parquet_by_year
from ..shared.periods import resolve_daily_period, resolve_historical_period
from ..shared.run_options import get_historis

log = logging.getLogger(__name__)

SOURCE = "cuaca"

API_URL = "https://archive-api.open-meteo.com/v1/archive"
TIMEZONE = "Asia/Jakarta"

# Koordinat 5 wilayah DKI Jakarta (bersumber dari script nyoba_jxb)
REGION_KOORDINAT = {
    "Jakarta Pusat": (-6.1931, 106.8216),
    "Jakarta Selatan": (-6.2444, 106.8006),
    "Jakarta Utara": (-6.1264, 106.8329),
    "Jakarta Barat": (-6.1683, 106.7588),
    "Jakarta Timur": (-6.2250, 106.9004),
}

VARIABEL_HOURLY = [
    "temperature_2m",
    "apparent_temperature",
    "precipitation",
    "weather_code",
]


def _resolve_rentang(period: str | None, cfg: dict) -> tuple[date, date, str]:
    """Tentukan rentang tanggal & label parquet.

    - 'latest' (default)        -> 10 tahun terakhir (1 Jan tahun-10) s.d. hari ini, label = tahun berjalan
    - 'YYYY-MM-DD'              -> satu tanggal spesifik (mode harian)
    - 'YYYY'                    -> 1 Jan s.d. 31 Des tahun tersebut (dibatasi hari ini)
    - 'YYYY-YYYY'               -> rentang tahun (label gabungan)
    """
    from datetime import datetime
    today = date.today()
    s = (period or cfg.get("period", "latest")).strip().lower()

    if s in ("", "latest", "terbaru"):
        mulai = date(today.year - 10, 1, 1)
        return mulai, today, str(today.year)

    # Handle YYYY-MM-DD (single day from daily mode)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        target_date = datetime.strptime(s, "%Y-%m-%d").date()
        return target_date, target_date, s

    if s.isdigit() and len(s) == 4:  # tahun
        y = int(s)
        return date(y, 1, 1), min(date(y, 12, 31), today), s

    if len(s.split("-")) == 2 and all(len(x) == 4 for x in s.split("-")):
        a, b = (int(x) for x in s.split("-"))
        return date(a, 1, 1), min(date(b, 12, 31), today), f"{a}-{b}"

    raise ValueError(f"periode cuaca tidak dikenal: '{period}'. Gunakan latest | YYYY-MM-DD | YYYY | YYYY-YYYY")


def _ekstrak_wilayah(
    region_name: str,
    lat: float,
    lon: float,
    tgl_mulai: date,
    tgl_akhir: date,
    retries: int = 3,
) -> pd.DataFrame | None:
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": tgl_mulai.isoformat(),
        "end_date": tgl_akhir.isoformat(),
        "hourly": ",".join(VARIABEL_HOURLY),
        "timezone": TIMEZONE,
    }
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(API_URL, params=params, timeout=120)
            if resp.status_code != 200:
                log.warning("cuaca HTTP %s utk %s (attempt %s)", resp.status_code, region_name, attempt)
                if attempt < retries:
                    continue
                return None
            data = resp.json()
            hourly = data.get("hourly")
            if not hourly:
                return None
            return pd.DataFrame(
                {
                    "time": hourly.get("time", []),
                    "region_name": region_name,
                    "temperature_2m": hourly.get("temperature_2m", []),
                    "apparent_temperature": hourly.get("apparent_temperature", []),
                    "precipitation": hourly.get("precipitation", []),
                    "weather_code": hourly.get("weather_code", []),
                }
            )
        except requests.RequestException as exc:
            log.warning("cuaca request error utk %s (attempt %s): %s", region_name, attempt, exc)
            if attempt == retries:
                return None
    return None





def extract(period: str | None = None, dry_run: bool = False, historis: bool = False) -> dict[str, pd.DataFrame]:
    """EXTRACT (bronze): cuaca hourly 5 wilayah -> data_lake/bronze/cuaca/cuaca/{YYYY}.parquet
    
    Mode Harian (default): hanya hari ini (date.today())
    Mode Historis (--historis): full backfill dari start_year s.d. hari ini"""
    cfg = get_source_config(SOURCE)
    
    # Determine period based on mode
    if historis or get_historis():
        period = resolve_historical_period(SOURCE)
        log.info("cuaca: MODE HISTORIS aktif, periode=%s", period)
    else:
        period = resolve_daily_period(SOURCE)
        log.info("cuaca: MODE HARIAN aktif, periode=%s", period)
    
    mulai, akhir, label = _resolve_rentang(period, cfg)
    log.info("cuaca extract: rentang %s s.d. %s (label %s)", mulai, akhir, label)

    frames: list[pd.DataFrame] = []
    for region, (lat, lon) in REGION_KOORDINAT.items():
        df = _ekstrak_wilayah(region, lat, lon, mulai, akhir)
        if df is None or df.empty:
            log.warning("cuaca: wilayah %s gagal/kosong", region)
            continue
        frames.append(df)
        log.info("cuaca %s: %d baris", region, len(df))

    if not frames:
        log.warning("cuaca: tidak ada wilayah yang berhasil")
        return {}

    df = pd.concat(frames, ignore_index=True)
    df = truncate_df(df)
    if dry_run:
        log.info("[dry-run] cuaca bronze: %d baris (tidak ditulis)", len(df))
        return {"cuaca": df}

    paths = write_parquet_by_year(df, "bronze", SOURCE, "cuaca", year_col="time")
    log.info("cuaca bronze ditulis: %d file per tahun (%d baris total)", len(paths), len(df))
    return {"cuaca": df}