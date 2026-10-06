from __future__ import annotations

import logging
import time
import xml.etree.ElementTree as ET
from datetime import date, timedelta
from typing import Any

import pandas as pd
import requests

from ..shared.config import get_source_config
from ..shared.periods import resolve_period
from ..shared.row_limit import truncate_df
from ..shared.storage import write_parquet_by_year

log = logging.getLogger(__name__)

SOURCE = "kurs"

# API BI Kurs: parameter startdate (YYYY-MM-DD), respons XML berisi <Table>.
BASE_URL = (
    "https://www.bi.go.id/biwebservice/wskursbi.asmx/getSubKursLokal4?startdate={tanggal}"
)
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}

MATA_UANG_LABEL = {
    "USD": "Dolar Amerika Serikat",
    "AUD": "Dolar Australia",
    "SGK": "Dolar Singapura",
    "EUR": "Euro",
    "GBP": "Poundsterling Inggris",
    "JPY": "Yen Jepang",
    "MYR": "Ringgit Malaysia",
    "CNY": "Yuan Cina",
}


def _ambil_per_tanggal(tanggal: date, retries: int = 3) -> list[dict[str, Any]]:
    """Ambil kurs satu tanggal; kosong bila hari libur/API tidak balas."""
    url = BASE_URL.format(tanggal=tanggal.isoformat())
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=30)
            if resp.status_code != 200:
                log.warning("kurs HTTP %s untuk %s (attempt %s)",
                            resp.status_code, tanggal, attempt)
                time.sleep(1.5 * attempt)
                continue
            root = ET.fromstring(resp.content)
            rows: list[dict[str, Any]] = []
            for table in root.iter("Table"):
                try:
                    tgl_xml = table.find("tgl_subkurslokal").text[:10]
                    kode = table.find("mts_subkurslokal").text.strip()
                    satuan = float(table.find("nil_subkurslokal").text)
                    beli = float(table.find("beli_subkurslokal").text)
                    jual = float(table.find("jual_subkurslokal").text)
                except (AttributeError, ValueError, TypeError) as exc:
                    log.warning("kurs: parse baris gagal untuk %s: %s", tanggal, exc)
                    continue
                rows.append(
                    {
                        "tanggal": tgl_xml,
                        "kode_mata_uang": kode,
                        "nilai_satuan": satuan,
                        "kurs_beli": beli,
                        "kurs_jual": jual,
                        "kurs_tengah": (beli + jual) / 2,
                    }
                )
            return rows
        except (requests.RequestException, ET.ParseError) as exc:
            log.warning("kurs request error untuk %s (attempt %s): %s", tanggal, attempt, exc)
            time.sleep(1.5 * attempt)
    return []


def _resolve_tanggal(period: str | None, years: list[int], period_label: str) -> list[date]:
    """Tentukan daftar tanggal yang akan di-extract.

    - 'latest'/None           -> seluruh hari 10 tahun terakhir (1 Jan tahun-10 s.d. hari ini)
    - 'YYYY' atau 'YYYY-YYYY' -> seluruh hari dalam tahun (backfill)
    """
    today = date.today()
    if period is None or period.strip().lower() in ("", "latest", "terbaru"):
        mulai = date(today.year - 10, 1, 1)
        return [mulai + timedelta(days=i) for i in range((today - mulai).days + 1)]

    tanggal_list: list[date] = []
    for year in years:
        start = date(year, 1, 1)
        end = min(date(year, 12, 31), today)
        d = start
        while d <= end:
            tanggal_list.append(d)
            d += timedelta(days=1)
    return tanggal_list


def extract(period: str | None = None, dry_run: bool = False) -> dict[str, pd.DataFrame]:
    """EXTRACT (bronze): kurs BI per hari -> data_lake/bronze/kurs/kurs/{YYYY}.parquet

    Satu file per tahun (kolom `tanggal` menentukan filenya)."""
    cfg = get_source_config(SOURCE)
    period_label, years = resolve_period(period or cfg.get("period"))
    tanggal_list = _resolve_tanggal(period or cfg.get("period"), years, period_label)

    rows: list[dict[str, Any]] = []
    today_iso = date.today().isoformat()
    for tanggal in tanggal_list:
        data = _ambil_per_tanggal(tanggal)
        if data:
            for r in data:
                r["tanggal_ekstraksi"] = today_iso
            rows.extend(data)
            log.info("kurs %s: %d mata uang", tanggal, len(data))
        else:
            log.info("kurs %s: kosong (libur/tidak ada data)", tanggal)
        time.sleep(0.8)

    if not rows:
        # coba fallback mundur hingga 7 hari bila mode "latest" (hanya tanggal terbaru)
        if period is None or period.strip().lower() in ("", "latest", "terbaru"):
            for i in range(1, 8):
                d = date.today() - timedelta(days=i)
                data = _ambil_per_tanggal(d)
                if data:
                    for r in data:
                        r["tanggal_ekstraksi"] = today_iso
                    rows.extend(data)
                    log.info("kurs fallback %s: %d mata uang", d, len(data))
                    break
                time.sleep(0.8)
        if not rows:
            log.warning("kurs: tidak ada data sama sekali utk periode %s", period_label)
            return {}

    df = pd.DataFrame(rows)
    # normalisasi tanggal string -> date
    df["tanggal"] = pd.to_datetime(df["tanggal"])
    df = truncate_df(df)

    if dry_run:
        log.info("[dry-run] kurs bronze: %d baris (tidak ditulis)", len(df))
        return {"kurs": df}

    paths = write_parquet_by_year(df, "bronze", SOURCE, "kurs", year_col="tanggal")
    log.info("kurs bronze ditulis: %d file per tahun (%d baris total)", len(paths), len(df))
    return {"kurs": df}