from __future__ import annotations

import logging
from datetime import date, timedelta

import pandas as pd
import requests

from ..shared.config import get_source_config
from ..shared.storage import read_parquet, write_parquet

log = logging.getLogger(__name__)

SOURCE = "gdelt"

USER_AGENT = "JXB_DSS_Pipeline/1.0"

KATA_KUNCI_PARIWISATA = [
    "tourism", "tourist", "tourists", "travel", "travellers", "travelers",
    "holiday", "vacation", "vacations", "resort", "resorts", "hotel",
    "hotels", "hospitality", "destination", "beach", "island", "wisata",
    "pariwisata", "agoda", "booking.com", "traveloka", "cruise",
    "flight", "airport", "trip", "tour",
]

_MAX_TITLES = 50  # maks judul artikel yang diambil (jaringan)
_BULAN = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
          "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12}


def _lokal_label(cfg: dict, period: str | None) -> str:
    return (period or cfg.get("period", "latest")).strip()


def _resolusi_tanggal(cfg: dict, period: str | None) -> date:
    s = (period or cfg.get("period", "latest")).strip()
    if len(s) == 8 and s.isdigit():
        import datetime
        return datetime.datetime.strptime(s, "%Y%m%d").date()
    return date.today() - timedelta(days=1)


def _ambil_judul(url: str) -> str | None:
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=15)
        if resp.status_code != 200:
            return None
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, "lxml")
        judul = soup.title.get_text(strip=True) if soup.title else None
        return judul or None
    except Exception:
        return None


def _skor_pariwisata(teks: str) -> int:
    return sum(1 for kata in KATA_KUNCI_PARIWISATA if kata.lower() in teks.lower())


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()
    out["relevansi"] = out.apply(
        lambda r: _skor_pariwisata(f"{r.get('url_sumber', '') or ''} {r.get('aktor_utama', '') or ''}"),
        axis=1,
    )
    out["tanggal_berita"] = pd.to_datetime(out["tanggal_berita"]).dt.date

    # prioritas: relevansi turun, lalu relevansi>0 di depan. Hanya ambil yang relevan utk produksi.
    out = out.sort_values(["relevansi", "tanggal_berita"], ascending=[False, False]).reset_index(drop=True)
    out = out[out["relevansi"] > 0].reset_index(drop=True)

    if out.empty:
        log.warning("gdelt: tidak ada baris relevan pariwisata")
        return pd.DataFrame()

    # judul artikel (opsional, jaringan)
    cnt = 0
    judul: list[str | None] = []
    for _, r in out.iterrows():
        if cnt < _MAX_TITLES:
            j = _ambil_judul(r["url_sumber"]) if r["url_sumber"] else None
            judul.append(j)
            cnt += 1
        else:
            judul.append(None)
    out["judul_berita"] = judul

    cols = [
        "global_event_id",
        "tanggal_berita",
        "window",
        "judul_berita",
        "url_sumber",
        "aktor_utama",
        "kode_event_cameo",
        "skor_dampak",
        "skor_sentimen",
        "nama_lokasi",
        "latitude",
        "longitude",
        "relevansi",
    ]
    return out[cols].drop_duplicates(subset=["global_event_id"]).reset_index(drop=True)


def transform(period: str | None = None, dry_run: bool = False) -> dict[str, pd.DataFrame]:
    cfg = get_source_config(SOURCE)
    tanggal = _resolusi_tanggal(cfg, period)
    period_label = tanggal.isoformat()

    try:
        bronze = read_parquet("bronze", SOURCE, "berita", period_label)
    except FileNotFoundError:
        log.warning("gdelt: bronze tidak ada utk %s", period_label)
        return {}

    df = _clean(bronze)
    if df.empty:
        return {}

    if dry_run:
        log.info("[dry-run] gdelt silver: %d baris (tidak ditulis)", len(df))
        return {"berita": df}

    path = write_parquet(df, "silver", SOURCE, "berita", period_label)
    log.info("gdelt silver ditulis: %s (%d baris)", path, len(df))
    return {"berita": df}