from __future__ import annotations

import logging
import re
from datetime import date
from typing import Any

import pandas as pd
import requests

from ..shared.periods import resolve_years, resolve_daily_period, resolve_historical_period
from ..shared.row_limit import truncate_df
from ..shared.storage import write_parquet_by_year
from ..shared.run_options import get_historis

log = logging.getLogger(__name__)

SOURCE = "hari_libur"

# Kalender libur nasional Indonesia (Google Calendar public ICS)
ICS_URL = (
    "https://calendar.google.com/calendar/ical/"
    "id.indonesian%23holiday%40group.v.calendar.google.com/public/full.ics"
)
# Ambil 10 tahun terakhir (tersedia 2016 s.d. hari ini; tergantung feed ICS menyediakannya)
TAHUN_MULAI = 2016

_HEADERS = {"User-Agent": "Mozilla/5.0"}


def _parse_ics(body: str) -> list[dict[str, Any]]:
    """Ambil semua VEVENT mentah (brone: belum difilter nasional)."""
    events: list[dict[str, Any]] = []
    for blok in re.split(r"BEGIN:VEVENT", body):
        if "END:VEVENT" not in blok:
            continue
        blok = blok.split("END:VEVENT")[0]

        def get(field: str) -> str | None:
            m = re.search(r"^%s[;:](.*)$" % field, blok, re.M)
            return m.group(1).strip() if m else None

        dtstart = get("DTSTART")
        if not dtstart:
            continue
        m = re.search(r"(\d{4})-?(\d{2})-?(\d{2})", dtstart)
        if not m:
            continue
        d = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        events.append(
            {
                "tanggal": d,
                "nama": (get("SUMMARY") or "").replace("\r", "").strip(),
                "deskripsi": (get("DESCRIPTION") or "").replace("\r", "").strip(),
                "uid": (get("UID") or "").strip(),
            }
        )
    return events


def _ambil_kalender(url: str, retries: int = 3) -> str:
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=60)
            if resp.status_code == 200:
                resp.encoding = "utf-8"
                return resp.text
            log.warning("hari_libur HTTP %s (attempt %s)", resp.status_code, attempt)
        except requests.RequestException as exc:
            log.warning("hari_libur request error (attempt %s): %s", attempt, exc)
    raise RuntimeError(f"Gagal mengambil kalender libur dari {url}")


def extract(period: str | None = None, dry_run: bool = False, historis: bool = False) -> dict[str, pd.DataFrame]:
    """EXTRACT (bronze): semua VEVENT dari kalender libur Indonesia.

    Ditulis satu file per tahun: data_lake/bronze/hari_libur/hari_libur/{YYYY}.parquet
    
    Mode Tahunan/Harian (default): hanya tahun berjalan
    Mode Historis (--historis): full kalender arsip"""
    # Determine period based on mode
    if historis or get_historis():
        period = resolve_historical_period(SOURCE)
        log.info("hari_libur: MODE HISTORIS aktif, periode=%s", period)
    else:
        period = resolve_daily_period(SOURCE)
        log.info("hari_libur: MODE TAHUNAN/HARIAN aktif, periode=%s", period)

    if period and period.strip().lower() not in ("", "latest", "terbaru"):
        log.info("hari_libur: periode=%s (filter tahun dipakai)", period)

    body = _ambil_kalender(ICS_URL)
    events = _parse_ics(body)
    events = [e for e in events if e["tanggal"].year >= TAHUN_MULAI]
    if period and period.strip().lower() not in ("", "latest", "terbaru"):
        tahun_pilihan = {int(y) for y in resolve_years(period)}
        events = [e for e in events if e["tanggal"].year in tahun_pilihan]
    if not events:
        log.warning("hari_libur: tidak ada event ter-parse")
        return {}

    df = pd.DataFrame(events)
    df["tanggal"] = pd.to_datetime(df["tanggal"])
    df = truncate_df(df)

    if dry_run:
        log.info("[dry-run] hari_libur bronze: %d event (tidak ditulis)", len(df))
        return {"hari_libur": df}

    paths = write_parquet_by_year(df, "bronze", SOURCE, "hari_libur", year_col="tanggal")
    log.info("hari_libur bronze ditulis: %d file per tahun (%d event)", len(paths), len(df))
    return {"hari_libur": df}