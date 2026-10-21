"""Extract ulasan Traveloka via API direct request -> bronze Parquet (grain: year)."""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from ..shared.checkpoint import Checkpoint
from ..shared.config import get_source_config
from ..shared.db import fetch_df, table_exists
from ..shared.periods import resolve_months, resolve_daily_period, resolve_historical_period
from ..shared.row_limit import get_row_limit, truncate_df
from ..shared.storage import write_parquet, write_parquet_by_year
from ..shared.run_options import get_historis

# Playwright untuk bypass anti-bot
try:
    from playwright.sync_api import sync_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

log = logging.getLogger(__name__)

SOURCE = "traveloka_review"
DATASET = "review"

API_URL = "https://www.traveloka.com/api/v2/ugc/review/consumption/v2/getReviews"

COOKIE_FILE = Path.home() / ".traveloka_cookies.json"

AUTO_LOGIN_RETRY = True  # Retry once with fresh login on 401/404


def save_cookies(cookie_str: str) -> None:
    """Simpan cookie string ke file untuk reuse."""
    try:
        COOKIE_FILE.write_text(cookie_str, encoding="utf-8")
        log.info("Cookie disimpan ke %s", COOKIE_FILE)
    except Exception as exc:
        log.warning("Gagal simpan cookie: %s", exc)


def get_fresh_traveloka_cookies(headless: bool = False, keep_open: bool = False) -> str:
    """
    Launch browser, wait for login, extract cookies as formatted string.
    headless=False required for manual login interaction.
    keep_open=True keeps browser open after login for debugging (default False).
    """
    if not HAS_PLAYWRIGHT:
        raise RuntimeError("Playwright tidak terinstall. Jalankan: pip install playwright && playwright install chromium")

    log.info("Membuka browser untuk login Traveloka...")
    log.info("Silakan login di browser yang terbuka (timeout 5 menit)")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, args=["--disable-blink-features=AutomationControlled"])
        context = browser.new_context(
            locale="id-ID",
            viewport={"width": 1366, "height": 768}
        )
        page = context.new_page()
        
        page.goto("https://www.traveloka.com", wait_until="domcontentloaded")
        
        # Wait for login completion - detect user profile element
        # Traveloka typically shows user avatar/menu when logged in
        login_selectors = [
            '[data-testid="user-nav-dropdown"]',
            'a[href="/id-id/logout"]',
            'a[href*="/user/account"]',
            'div:has-text("Log Out")',
            'div[class*="user"]',
            'button[aria-label*="Akun"]',
            '[data-testid*="user"]',
            '.user-avatar',
            '.user-menu',
        ]
        
        logged_in = False
        for selector in login_selectors:
            try:
                page.wait_for_selector(selector, timeout=300000)  # 5 menit
                log.info("traveloka_review: Login terdeteksi via selector: %s", selector)
                logged_in = True
                break
            except Exception:
                log.debug("traveloka_review: Selector tidak ditemukan: %s", selector)
                continue
        
        if not logged_in:
            # Fallback: wait for any element that indicates logged-in state
            try:
                page.wait_for_selector('[data-testid="user-nav-dropdown"], a[href="/id-id/logout"], a[href*="/user/account"]', timeout=60000)
                logged_in = True
                log.info("traveloka_review: Login terdeteksi via fallback selector")
            except Exception:
                pass
        
        if not logged_in:
            log.warning("traveloka_review: Login tidak terdeteksi secara otomatis setelah 5 menit, lanjutkan ekstraksi cookie...")
        
        # Extract cookies
        cookies = context.cookies("https://www.traveloka.com")
        cookie_str = "; ".join(f"{c['name']}={c['value']}" for c in cookies)
        
        # Validate critical cookies
        critical_cookies = ['sen_t', 'clientSessionId', 'tv_mcc_id']
        cookie_names = {c['name'] for c in cookies}
        missing = [c for c in critical_cookies if c not in cookie_names]
        if missing:
            log.warning("traveloka_review: Kuki krusial hilang: %s, login mungkin gagal", missing)
        else:
            log.info("traveloka_review: Semua kuki krusial (sen_t, clientSessionId, tv_mcc_id) berhasil diekstrak")
        
        if keep_open:
            log.info("traveloka_review: Browser dijaga terbuka untuk debugging. Tekan Enter untuk menutup...")
            input()
        
        browser.close()
        
        if cookie_str:
            save_cookies(cookie_str)
            log.info("Berhasil extract %d cookies dari browser", len(cookies))
        else:
            log.warning("Tidak ada cookie yang diextract")
        
        return cookie_str


def _load_cookie_and_token(force_refresh: bool = False, keep_open: bool = False) -> tuple[str, str, dict[str, str]]:
    """Load cookie dari file lokal dan ekstrak token sen_t."""
    if force_refresh:
        cookie_str = get_fresh_traveloka_cookies(headless=False, keep_open=keep_open)
        # Parse the fresh cookie string
        cookie_dict = dict(item.split("=", 1) for item in cookie_str.split("; ") if "=" in item)
        sen_t_token = cookie_dict.get("sen_t", "")
        return cookie_str, sen_t_token, cookie_dict

    possible_paths = [
        os.path.expanduser("~/.traveloka_cookies.json"),
        r"C:\Users\User\.traveloka_cookies.json",
    ]

    cookie_file = next((p for p in possible_paths if os.path.exists(p)), None)
    if not cookie_file:
        log.warning("traveloka_review: File cookie tidak ditemukan di path standar.")
        return "", "", {}

    try:
        with open(cookie_file, "r", encoding="utf-8") as f:
            content = f.read().strip()

        # Try parse as JSON first (Playwright format: list of dicts)
        cookie_str = ""
        cookie_dict = {}
        try:
            cookies_data = json.loads(content)
            if isinstance(cookies_data, list):
                for c in cookies_data:
                    cookie_dict[c["name"]] = c["value"]
                cookie_str = "; ".join([f"{k}={v}" for k, v in cookie_dict.items()])
            elif isinstance(cookies_data, dict):
                cookie_dict = cookies_data
                cookie_str = "; ".join([f"{k}={v}" for k, v in cookie_dict.items()])
            else:
                raise ValueError("Unexpected JSON format")
        except (json.JSONDecodeError, ValueError):
            # Fallback: treat as plain cookie string (semicolon-separated)
            cookie_str = content
            cookie_dict = dict(item.split("=", 1) for item in cookie_str.split("; ") if "=" in item)

        sen_t_token = cookie_dict.get("sen_t", "")
        return cookie_str, sen_t_token, cookie_dict
    except Exception as exc:
        log.warning("traveloka_review: Gagal membaca file cookie: %s", exc)
        return "", "", {}


def _parse_timestamp(ts: Any) -> str | None:
    """Parse Traveloka epoch milliseconds timestamp to ISO date string (YYYY-MM-DD)."""
    if ts is None:
        return None
    try:
        if isinstance(ts, (int, float)):
            dt = datetime.fromtimestamp(ts / 1000)
        elif isinstance(ts, str):
            # Handle numeric string (milliseconds since epoch)
            if ts.isdigit():
                dt = datetime.fromtimestamp(int(ts) / 1000)
            else:
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
            "nama_hotel": hotel_info.get("nama_hotel"),
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


def _fetch_all_reviews(
    object_id: str, 
    max_pages: int, 
    delay: float, 
    timeout: int,
    cookie_str: str = "",
    sen_t_token: str = "",
    cookie_dict: dict = None,
    headless_login: bool = False,
    keep_browser_open: bool = False
) -> list[dict]:
    """Fetch all review pages using Traveloka API v2."""
    if cookie_dict is None:
        cookie_dict = {}
    
    def _build_session(c_str: str) -> requests.Session:
        session = requests.Session()
        for cookie_pair in c_str.split("; "):
            if "=" in cookie_pair:
                key, val = cookie_pair.split("=", 1)
                session.cookies.set(key.strip(), val.strip(), domain=".traveloka.com")
        return session
    
    def _build_headers(c_dict: dict) -> dict[str, str]:
        return {
            "content-type": "application/json",
            "x-client-interface": "desktop",
            "x-domain": "ugcReview",
            "x-route-prefix": "id-id",
            "tv-language": "id_ID",
            "tv-currency": "IDR",
            "tv-country": "ID",
            "tv-clientsessionid": c_dict.get("clientSessionId", ""),
            "tv-mcc-id": c_dict.get("tv_mcc_id", ""),
            "origin": "https://www.traveloka.com",
            "referer": "https://www.traveloka.com/id-id/hotel",
        }
    
    def _do_fetch(c_str: str, s_token: str, c_dict: dict) -> list[dict]:
        session = _build_session(c_str)
        headers = _build_headers(c_dict)
        all_reviews = []
        skip = 0
        limit = 20
        target_year = 2026
        
        for page in range(max_pages):
            payload = {
                "clientInterface": "desktop",
                "data": {
                    "objectId": object_id,
                    "productType": "HOTEL",
                    "configId": "REV_CONSV2_HOTEL_GENERAL_V1",
                    "origin": "TRAVELOKA",
                    "sort": "SORT_CREATED_DESCENDING",
                    "filter": {
                        "format": "FORMAT_VALUE_TEXT",
                        "language": "LANGUAGE_VALUE_ALL",
                        "rating": "RATING_VALUE_ALL",
                        "travelPurpose": "TRAVEL_PURPOSE_VALUE_ALL"
                    },
                    "ratingTagSet": [],
                    "skip": str(skip),
                    "limit": str(limit)
                },
                "fields": [],
                "sentinel": {
                    "signals": [],
                    "token": s_token
                }
            }
            
            try:
                resp = session.post(API_URL, json=payload, headers=headers, timeout=timeout)
                
                if resp.status_code in (401, 403):
                    raise requests.HTTPError(f"{resp.status_code} Forbidden", response=resp)
                    
                resp.raise_for_status()
                data = resp.json()
                
                reviews = data.get("data", {}).get("reviews", [])
                has_next = data.get("data", {}).get("hasNext", False)
                
                if not reviews:
                    break
                
                filtered_reviews = []
                for r in reviews:
                    ts = r.get("reviewTimestamp")
                    if ts is not None:
                        try:
                            ts_num = int(ts) if isinstance(ts, str) else ts
                            dt = datetime.fromtimestamp(ts_num / 1000)
                            if dt.year == target_year:
                                filtered_reviews.append(r)
                            elif dt.year < target_year:
                                has_next = False
                                break
                        except (ValueError, OSError, OverflowError):
                            pass
                
                all_reviews.extend(filtered_reviews)
                log.info(f"  Page {page+1}: +{len(filtered_reviews)} reviews from 2026 (total: {len(all_reviews)})")
                
                if not has_next:
                    break
                    
                skip += limit
                time.sleep(delay)
                
            except requests.RequestException as exc:
                log.warning(f"  API error on page {page+1}: {exc}")
                raise
                
        return all_reviews
    
    # Try with current cookies first
    try:
        return _do_fetch(cookie_str, sen_t_token, cookie_dict)
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code in (401, 403):
            log.warning("traveloka_review: Cookie expired (401/403), attempting fresh login...")
            if HAS_PLAYWRIGHT:
                fresh_cookie_str = get_fresh_traveloka_cookies(headless=headless_login, keep_open=keep_browser_open)
                fresh_cookie_dict = dict(item.split("=", 1) for item in fresh_cookie_str.split("; ") if "=" in item)
                fresh_sen_t = fresh_cookie_dict.get("sen_t", "")
                log.info("traveloka_review: Fresh login completed, retrying with new cookies")
                return _do_fetch(fresh_cookie_str, fresh_sen_t, fresh_cookie_dict)
            else:
                log.error("traveloka_review: Playwright not available for auto-login. Install: pip install playwright && playwright install chromium")
        raise
    except requests.RequestException as exc:
        log.warning(f"  API error: {exc}")
        return []


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
        log.error("traveloka_review: gagal query hotel_master: %s", exc, exc_info=True)
        return []

    if df is None or df.empty:
        log.warning("traveloka_review: tidak ada hotel dengan traveloka_object_id di %s.%s", schema, table)
        return []

    return [
        {
            "id_kompetitor": row["id_kompetitor"],
            "nama_hotel": row["nama_hotel"],
            "objectId": str(row["traveloka_object_id"]).strip()
        }
        for _, row in df.iterrows()
    ]


def extract_year(
    year: int = 2026,
    dry_run: bool = False,
    force_login: bool = False,
    headless_login: bool = False,
    keep_browser_open: bool = False,
    resume: bool = False,
) -> dict[str, pd.DataFrame]:
    """EXTRACT (bronze): Traveloka reviews per hotel -> data_lake/bronze/traveloka_review/review/{YYYY}.parquet
    
    Fetches ALL reviews for the given year, writes incrementally per hotel to yearly Parquet files.
    Supports resume via checkpoint (--lanjut flag).
    """
    cfg = get_source_config(SOURCE)

    target_hotels = _get_traveloka_hotels(cfg)
    if not target_hotels:
        return {}

    max_pages = int(cfg.get("max_pages_per_hotel", 50))
    delay = float(cfg.get("request_delay_seconds", 1.0))
    timeout = int(cfg.get("timeout_seconds", 30))

    # Checkpoint for resume
    checkpoint = Checkpoint(SOURCE, save_every=1)
    if resume or get_resume():
        sudah = checkpoint.muat()
        target_hotels = [h for h in target_hotels if not checkpoint.sudah(h.get("id_kompetitor"))]
        if not target_hotels:
            log.info("traveloka_review: semua hotel sudah diproses hari ini, tidak ada yang diulang")
            checkpoint.bersihkan()
            return {}
        log.info("traveloka_review: melanjutkan, %d hotel tersisa", len(target_hotels))
    else:
        checkpoint.bersihkan()

    # Prepare cookies once
    if force_login:
        log.info("traveloka_review: Force login enabled, performing fresh login once...")
        cookie_str = get_fresh_traveloka_cookies(headless=headless_login, keep_open=keep_browser_open)
        cookie_dict = dict(item.split("=", 1) for item in cookie_str.split("; ") if "=" in item)
        sen_t_token = cookie_dict.get("sen_t", "")
        log.info("traveloka_review: Fresh login completed, got %d cookies", len(cookie_dict))
    else:
        cookie_str, sen_t_token, cookie_dict = _load_cookie_and_token()
        if not cookie_str:
            log.warning("traveloka_review: Cookie tidak ditemukan, tidak bisa fetch review.")
            return {}

    for hotel in target_hotels:
        object_id = hotel.get("objectId")
        if not object_id:
            continue

        log.info("traveloka_review: fetching reviews for %s (objectId=%s)", hotel.get("nama_hotel"), object_id)
        raw_reviews = _fetch_all_reviews(
            object_id, 
            max_pages, 
            delay, 
            timeout,
            cookie_str=cookie_str,
            sen_t_token=sen_t_token,
            cookie_dict=cookie_dict,
            headless_login=headless_login,
            keep_browser_open=keep_browser_open
        )

        if raw_reviews:
            extracted = _extract_review_fields(raw_reviews, hotel)
            if extracted:
                hotel_df = pd.DataFrame(extracted)
                hotel_df["tanggal_ulasan"] = pd.to_datetime(hotel_df["tanggal_ulasan"], errors="coerce")
                
                if not dry_run:
                    write_parquet_by_year(
                        hotel_df,
                        "bronze",
                        SOURCE,
                        DATASET,
                        year_col="tanggal_ulasan",
                        mode="append",
                        dedup_cols=["id_ulasan"],
                    )
                log.info("traveloka_review: %s -> %d ulasan (tertulis per tahun)", hotel.get("nama_hotel"), len(extracted))
        else:
            log.info("traveloka_review: %s -> 0 ulasan", hotel.get("nama_hotel"))

        # Mark hotel done AFTER successful write
        checkpoint.tandai(hotel.get("id_kompetitor"))

    checkpoint.simpan()

    if dry_run:
        log.info("[dry-run] traveloka_review bronze %d: selesai (tidak ditulis)", year)
    else:
        log.info("traveloka_review bronze %d: selesai", year)

    return {DATASET: pd.DataFrame()}


def extract(
    period: str | None = None, 
    dry_run: bool = False,
    force_login: bool = False,
    headless_login: bool = False,
    keep_browser_open: bool = False,
    historis: bool = False
) -> dict[str, pd.DataFrame]:
    """EXTRACT (bronze): Traveloka reviews per hotel -> data_lake/bronze/traveloka_review/review/{YYYY-MM}.parquet
    
    Mode Bulanan/Harian (default): hanya bulan berjalan
    Mode Historis (--historis): semua bulan dari start_year s.d. hari ini"""
    cfg = get_source_config(SOURCE)
    
    # Determine period based on mode
    if historis or get_historis():
        period = resolve_historical_period(SOURCE)
        log.info("traveloka_review: MODE HISTORIS aktif, periode=%s", period)
    else:
        period = resolve_daily_period(SOURCE)
        log.info("traveloka_review: MODE BULANAN/HARIAN aktif, periode=%s", period)
    
    bulan_ini = resolve_months(period)[0]

    target_hotels = _get_traveloka_hotels(cfg)
    if not target_hotels:
        return {}

    max_pages = int(cfg.get("max_pages_per_hotel", 50))
    delay = float(cfg.get("request_delay_seconds", 1.0))
    timeout = int(cfg.get("timeout_seconds", 30))

    row_limit = get_row_limit()
    if row_limit is not None:
        target_hotels = target_hotels[:3]
        log.info("traveloka_review: test mode -> dibatasi %d hotel pertama", len(target_hotels))

    # Prepare cookies once
    if force_login:
        log.info("traveloka_review: Force login enabled, performing fresh login once...")
        cookie_str = get_fresh_traveloka_cookies(headless=headless_login, keep_open=keep_browser_open)
        cookie_dict = dict(item.split("=", 1) for item in cookie_str.split("; ") if "=" in item)
        sen_t_token = cookie_dict.get("sen_t", "")
        log.info("traveloka_review: Fresh login completed, got %d cookies", len(cookie_dict))
    else:
        cookie_str, sen_t_token, cookie_dict = _load_cookie_and_token()
        if not cookie_str:
            log.warning("traveloka_review: Cookie tidak ditemukan, tidak bisa fetch review.")
            return {}

    rows: list[dict] = []
    for hotel in target_hotels:
        object_id = hotel.get("objectId")
        if not object_id:
            continue

        log.info("traveloka_review: fetching reviews for %s (objectId=%s)", hotel.get("nama_hotel"), object_id)
        raw_reviews = _fetch_all_reviews(
            object_id, 
            max_pages, 
            delay, 
            timeout,
            cookie_str=cookie_str,
            sen_t_token=sen_t_token,
            cookie_dict=cookie_dict,
            headless_login=headless_login,
            keep_browser_open=keep_browser_open
        )

        if raw_reviews:
            extracted = _extract_review_fields(raw_reviews, hotel)
            rows.extend(extracted)
            log.info("traveloka_review: %s -> %d ulasan", hotel.get("nama_hotel"), len(extracted))
        else:
            log.info("traveloka_review: %s -> 0 ulasan", hotel.get("nama_hotel"))

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