"""Extract ulasan Traveloka via API direct request -> bronze Parquet (grain: month)."""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any

import pandas as pd
import requests

from ..shared.config import get_source_config
from ..shared.db import fetch_df, table_exists
from ..shared.periods import resolve_months
from ..shared.row_limit import get_row_limit, truncate_df
from ..shared.storage import write_parquet

log = logging.getLogger(__name__)

SOURCE = "traveloka_review"
DATASET = "review"

API_URL = "https://www.traveloka.com/api/v2/ugc/review/consumption/v2/getReviews"

HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "content-type": "application/json",
    "origin": "https://www.traveloka.com",
    "referer": "https://www.traveloka.com/",
}


def _parse_timestamp(ts: Any) -> str | None:
    """Parse Traveloka epoch milliseconds timestamp to ISO date string (YYYY-MM-DD)."""
    if ts is None:
        return None
    try:
        if isinstance(ts, (int, float)):
            dt = datetime.fromtimestamp(ts / 1000)
        elif isinstance(ts, str):
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        else:
            return None
        return dt.date().isoformat()
    except (ValueError, OSError, OverflowError):
        return None


def _extract_review_fields(reviews: list[dict], hotel_info: dict) -> list[dict]:
    """Extract standardized fields from Traveloka review objects."""
    out = []
    for r in reviews:
        reviewer = r.get("reviewer", {}) or {}
        text = r.get("reviewOriginalText") or r.get("reviewContentText") or ""
        tanggal = _parse_timestamp(r.get("reviewTimestamp"))

        out.append({
            "id_ulasan": r.get("reviewId"),
            "id_kompetitor": hotel_info.get("id_kompetitor"),
            "nama_hotel": hotel_info.get("hotel_name"),
            "traveloka_object_id": hotel_info.get("objectId"),
            "nama_reviewer": reviewer.get("name", "Anonim"),
            "rating_bintang": r.get("reviewScore"),
            "isi_review": text,
            "tanggal_ulasan": tanggal,
            "review_timestamp_raw": r.get("reviewTimestamp"),
            "review_reply": r.get("reviewReply", ""),
            "review_media": r.get("reviewMedia", []),
            "scraped_at": datetime.now().isoformat(timespec="seconds"),
        })
    return out


def _fetch_all_reviews(object_id: str, max_pages: int, delay: float, timeout: int) -> list[dict]:
    """Fetch all review pages using sentinel-based pagination."""
    all_reviews = []
    sentinel: dict = {}
    page = 0

    while page < max_pages:
        payload = {
            "clientInterface": "desktop",
            "data": {
                "objectId": object_id,
                "productType": "HOTEL",
                "configId": "REV_CONSV2_HOTEL_GENERAL_V1"
            },
            "fields": [],
            "sentinel": sentinel
        }

        try:
            resp = requests.post(API_URL, json=payload, headers=HEADERS, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as exc:
            log.warning("traveloka_review: request error for %s (page %d): %s", object_id, page + 1, exc)
            break
        except ValueError as exc:
            log.warning("traveloka_review: JSON parse error for %s (page %d): %s", object_id, page + 1, exc)
            break

        d = data.get("data", {}) or {}
        reviews = d.get("reviews", []) or []
        if not reviews:
            break

        all_reviews.extend(reviews)

        has_next = d.get("hasNext", False)
        if not has_next:
            break

        sentinel = d.get("sentinel", {}) or {}
        page += 1
        time.sleep(delay)

    return all_reviews


def _get_traveloka_hotels(cfg: dict) -> list[dict]:
    """Ambil hotel yang punya traveloka_object_id dari hotel_master."""
    schema = cfg.get("hotel_master_schema", "public")
    table = cfg.get("hotel_master_table", "hotel_master")

    if not table_exists(schema, table):
        log.error("traveloka_review: tabel %s.%s tidak ditemukan", schema, table)
        return []

    sql = f'''
        SELECT id_kompetitor, nama_hotel, traveloka_object_id
        FROM "{schema}"."{table}"
        WHERE traveloka_object_id IS NOT NULL
          AND traveloka_object_id != ''
        ORDER BY id_kompetitor
    '''
    try:
        df = fetch_df(sql)
    except Exception as exc:
        log.error("traveloka_review: gagal query hotel_master: kolom traveloka_object_id mungkin belum ada. Error: %s", exc, exc_info=True)
        return []

    if df is None or df.empty:
        log.warning("traveloka_review: tidak ada hotel dengan traveloka_object_id di %s.%s", schema, table)
        return []

    return [
        {"id_kompetitor": row["id_kompetitor"],
         "hotel_name": row["nama_hotel"],
         "objectId": row["traveloka_object_id"]}
        for _, row in df.iterrows()
    ]


def extract(period: str | None = None, dry_run: bool = False) -> dict[str, pd.DataFrame]:
    """
    EXTRACT (bronze): Traveloka reviews per hotel -> data_lake/bronze/traveloka_review/review/{YYYY-MM}.parquet

    Grain: month (one file per YYYY-MM)
    """
    cfg = get_source_config(SOURCE)
    bulan_ini = resolve_months(period)[0]

    target_hotels = _get_traveloka_hotels(cfg)
    if not target_hotels:
        return {}

    max_pages = int(cfg.get("max_pages_per_hotel", 50))
    delay = float(cfg.get("request_delay_seconds", 1.0))
    timeout = int(cfg.get("timeout_seconds", 30))

    row_limit = get_row_limit()
    if row_limit is not None:
        # Test mode: batasi 3 hotel pertama
        target_hotels = target_hotels[:3]
        log.info("traveloka_review: test mode -> dibatasi %d hotel pertama", len(target_hotels))

    rows: list[dict] = []
    for hotel in target_hotels:
        object_id = hotel.get("objectId")
        if not object_id:
            continue

        log.info("traveloka_review: fetching reviews for %s (objectId=%s)", hotel.get("hotel_name"), object_id)
        raw_reviews = _fetch_all_reviews(object_id, max_pages, delay, timeout)

        if raw_reviews:
            extracted = _extract_review_fields(raw_reviews, hotel)
            rows.extend(extracted)
            log.info("traveloka_review: %s -> %d ulasan", hotel.get("hotel_name"), len(extracted))
        else:
            log.info("traveloka_review: %s -> 0 ulasan", hotel.get("hotel_name"))

    if not rows:
        log.warning("traveloka_review: tidak ada ulasan untuk periode %s", bulan_ini)
        return {}

    df = pd.DataFrame(rows)

    if "tanggal_ulasan" in df.columns and df["tanggal_ulasan"].notna().any():
        df["tanggal_ulasan"] = pd.to_datetime(df["tanggal_ulasan"], errors="coerce")
        tahun, bulan = map(int, bulan_ini.split("-"))
        df = df[(df["tanggal_ulasan"].dt.year == tahun) & (df["tanggal_ulasan"].dt.month == bulan)]
        log.info("traveloka_review: %d ulasan setelah filter bulan %s", len(df), bulan_ini)

    df = truncate_df(df)

    if dry_run:
        log.info("[dry-run] traveloka_review bronze %s: %d baris (tidak ditulis)", bulan_ini, len(df))
        return {DATASET: df}

    path = write_parquet(df, "bronze", SOURCE, DATASET, bulan_ini)
    log.info("traveloka_review bronze ditulis: %s (%d baris)", path, len(df))

    return {DATASET: df}