from __future__ import annotations

import io
import logging
import re
import time
import zipfile
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
import requests

from ..shared.config import get_source_config
from ..shared.row_limit import truncate_df
from ..shared.storage import write_parquet_by_year
from ..shared.checkpoint import Checkpoint
from ..shared.run_options import get_backfill, get_historis
from ..shared.periods import resolve_daily_period, resolve_historical_period

log = logging.getLogger(__name__)

SOURCE = "gdelt"

BASE_GDELT = "https://data.gdeltproject.org/gdeltv2"
FILE_LASTUPDATE = f"{BASE_GDELT}/lastupdate.txt"
USER_AGENT = "JXB_DSS_Pipeline/1.0"

# Historis script lama: 96 window (1 hari x 15 menit). Dapat dikonfigurasi.
MAKS_WINDOW = 96
LANGKAH_MENIT = 15


def _sasar_hari(period: str | None) -> list[date]:
    """Tentukan tanggal mana yang ditarik pada run ini.

    - None / 'latest'        -> kemarin (hari terakhir yang lengkap)
    - 'YYYYMMDD'             -> satu hari tersebut (debug/backfill harian)
    - 'YYYY' / 'YYYY-YYYY'   -> seluruh hari dalam rentang (backfill tahunan)
    """
    p = (period or "").strip()
    if not p or p.lower() in ("latest", "terbaru"):
        return [date.today() - timedelta(days=1)]
    if len(p) == 8 and p.isdigit():
        return [datetime.strptime(p, "%Y%m%d").date()]
    if re.fullmatch(r"\d{4}", p):
        return _seluruh_hari(int(p))
    if re.fullmatch(r"\d{4}-\d{4}", p):
        a, b = (int(x) for x in p.split("-"))
        if a > b:
            raise ValueError(f"rentang periode tidak valid: {p}")
        hari: list[date] = []
        for y in range(a, b + 1):
            hari.extend(_seluruh_hari(y))
        return hari
    raise ValueError(
        f"periode gdelt tidak dikenal: '{period}'. "
        "Gunakan 'latest', 'YYYYMMDD', 'YYYY', atau 'YYYY-YYYY'."
    )


def _seluruh_hari(tahun: int) -> list[date]:
    mulai = date(tahun, 1, 1)
    akhir = min(date(tahun, 12, 31), date.today())
    if akhir < mulai:
        return []
    return [mulai + timedelta(days=i) for i in range((akhir - mulai).days + 1)]


def _ambil_lastupdate(retries: int = 3) -> str | None:
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(FILE_LASTUPDATE, headers={"User-Agent": USER_AGENT}, timeout=60)
            if resp.status_code == 200:
                for baris in resp.text.strip().splitlines():
                    if "export.CSV.zip" in baris:
                        return baris.split()[-1]
                return None
            log.warning("gdelt lastupdate HTTP %s (attempt %s)", resp.status_code, attempt)
        except requests.RequestException as exc:
            log.warning("gdelt lastupdate error (attempt %s): %s", attempt, exc)
        time.sleep(5 * attempt)
    return None


def _unduh_export(url: str, retries: int = 3) -> bytes | None:
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=120)
            if resp.status_code == 200:
                return resp.content
            if resp.status_code == 404:
                return None  # window belum/tidak dipublikasikan
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(5 * attempt)
                continue
            return None
        except requests.RequestException as exc:
            log.warning("gdelt download error (attempt %s): %s", attempt, exc)
            time.sleep(5 * attempt)
    return None


def _undur_15_menit(timestamp: str) -> str:
    detik = datetime.strptime(timestamp, "%Y%m%d%H%M%S")
    return (detik - timedelta(minutes=LANGKAH_MENIT)).strftime("%Y%m%d%H%M%S")


def _parse_zip(data: bytes) -> list[list[str]]:
    rows: list[list[str]] = []
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        nama_entry = z.namelist()[0]
        with z.open(nama_entry) as fh:
            for baris in io.TextIOWrapper(fh, encoding="utf-8", errors="replace"):
                kolom = baris.rstrip("\n").split("\t")
                if len(kolom) >= 61:
                    rows.append(kolom)
    return rows


def _lokasi_seleksi(kolom: list[str]) -> tuple[str | None, str, str] | None:
    """Prioritas blok geo: ActionGeo > Actor1Geo > Actor2Geo; filter negara Indonesia (ID)."""
    blok = (
        (53, 52, 56, 57),  # ActionGeo
        (37, 36, 40, 41),  # Actor1Geo
        (45, 44, 48, 49),  # Actor2Geo
    )
    for i_neg, i_nama, i_lat, i_lon in blok:
        if kolom[i_neg] == "ID":
            return kolom[i_nama] or None, kolom[i_lat], kolom[i_lon]
    return None


def _float(nilai: str) -> float | None:
    try:
        return float(nilai) if nilai else None
    except (ValueError, TypeError):
        return None


def _transformasi(kolom: list[str], window: str) -> dict[str, Any] | None:
    if not kolom[0].isdigit():
        return None
    try:
        tanggal = datetime.strptime(kolom[1], "%Y%m%d").date()
    except ValueError:
        return None

    lokasi = _lokasi_seleksi(kolom)
    if not lokasi:
        return None
    nama_lokasi, lat_str, lon_str = lokasi

    return {
        "global_event_id": int(kolom[0]),
        "window": window,
        "tanggal_berita": tanggal,
        "aktor_utama": kolom[6] or None,
        "url_sumber": kolom[60] or None,  # disimpan mentah untuk scoring di silver
        "kode_event_cameo": kolom[26] or None,
        "skor_dampak": _float(kolom[30]),
        "skor_sentimen": _float(kolom[34]),
        "nama_lokasi": nama_lokasi,
        "latitude": _float(lat_str),
        "longitude": _float(lon_str),
        "waktu_ekstraksi": f"{pd.Timestamp.now():%Y-%m-%d %H:%M:%S}",
    }


def _ambil_satu_hari(target: date, max_window: int) -> list[dict[str, Any]]:
    """Tarik seluruh window 15 menit untuk satu tanggal, filter event Indonesia."""
    timestamp = target.strftime("%Y%m%d") + "234500"
    rows: list[dict[str, Any]] = []
    ids_dilihat: set[int] = set()

    for nomor in range(max_window):
        url = f"{BASE_GDELT}/{timestamp}.export.CSV.zip"
        data = _unduh_export(url)
        if data is None:
            log.info("gdelt skip %s (404/0)", timestamp)
        else:
            for kolom in _parse_zip(data):
                r = _transformasi(kolom, timestamp)
                if (
                    r is None
                    or r["global_event_id"] in ids_dilihat
                    or r["tanggal_berita"] != target
                ):
                    continue
                ids_dilihat.add(r["global_event_id"])
                rows.append(r)
        timestamp = _undur_15_menit(timestamp)
    return rows


def extract(period: str | None = None, dry_run: bool = False, backfill: bool = False, historis: bool = False) -> dict[str, pd.DataFrame]:
    """EXTRACT (bronze): GDELT 2.0 per window, hanya event lokasi Indonesia.

    Bronze ditulis **satu file per tahun**: {berita}/{YYYY}.parquet, mode append
    dengan dedupe `global_event_id`, sehingga run harian menumpuk ke file tahun
    yang sama tanpa menghasilkan baris duplikat.

    Mode Harian (default): hanya kemarin (date.today() - 1)
    Mode Historis (--historis): full backfill dari start_year s.d. hari ini
    """
    cfg = get_source_config(SOURCE)
    max_window = int(cfg.get("max_window", MAKS_WINDOW))
    
    # Determine period based on mode
    if historis or get_historis():
        period = resolve_historical_period(SOURCE)
        log.info("gdelt: MODE HISTORIS aktif, periode=%s", period)
    else:
        period = resolve_daily_period(SOURCE)
        log.info("gdelt: MODE HARIAN aktif, periode=%s", period)
    
    targets = _sasar_hari(period)

    # Checkpoint per tanggal (key: YYYY-MM-DD)
    checkpoint = Checkpoint(SOURCE, backfill=backfill, save_every=1)
    if get_backfill() or backfill:
        sudah = checkpoint.muat()
        targets = [d for d in targets if not checkpoint.sudah(d.isoformat())]
        log.info("gdelt: resume dari checkpoint, %d hari tersisa", len(targets))

    if dry_run:
        log.info("[dry-run] gdelt bronze: %d hari akan diproses (tidak di-download)", len(targets))
        return {"berita": pd.DataFrame()}

    if len(targets) > 1:
        log.warning(
            "gdelt: backfill %d hari (%s..%s) x %d window = ~%d permintaan; ini akan lama",
            len(targets),
            targets[0],
            targets[-1],
            max_window,
            len(targets) * max_window,
        )

    semua_rows: list[dict[str, Any]] = []
    for target in targets:
        log.info("gdelt extract: target=%s window_max=%s", target, max_window)
        rows = _ambil_satu_hari(target, max_window)
        if not rows:
            log.warning("gdelt: tidak ada event Indonesia utk %s", target)
            continue
        log.info("gdelt %s: %d event Indonesia", target, len(rows))
        semua_rows.extend(rows)
        
        # Tandai checkpoint setelah hari selesai diproses
        checkpoint.tandai(target.isoformat())
        checkpoint.simpan()

    if not semua_rows:
        log.warning("gdelt: tidak ada data untuk ditulis")
        return {}

    df = pd.DataFrame(semua_rows)
    df = truncate_df(df)
    if dry_run:
        log.info("[dry-run] gdelt bronze: %d baris (tidak ditulis)", len(df))
        return {"berita": df}

    paths = write_parquet_by_year(
        df,
        "bronze",
        SOURCE,
        "berita",
        year_col="tanggal_berita",
        mode="append",
        dedup_cols=["global_event_id"],
    )
    log.info("gdelt bronze ditulis: %d file per tahun (%d baris baru)", len(paths), len(df))
    return {"berita": df}