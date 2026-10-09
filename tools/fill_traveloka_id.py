#!/usr/bin/env python
"""Isi traveloka_object_id di hotel_master via Traveloka autocomplete API.

Usage:
    python tools/fill_traveloka_id.py
    python tools/fill_traveloka_id.py --dry-run    # hanya tampilkan yang akan diupdate
    python tools/fill_traveloka_id.py --limit 10   # batasi jumlah hotel
    python tools/fill_traveloka_id.py --auto-login # buka browser untuk login jika cookie expired
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.shared.db import db_connection, execute_sql, fetch_df

# Playwright untuk auto-login
try:
    from playwright.sync_api import sync_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

API_URL = "https://www.traveloka.com/api/v1/hotel/autocomplete"

DEFAULT_COOKIE = (
    "_fwb=150VcIGFpYrU7Cmd5BAytHP.1782122835050; _yjsu_yjad=1782122835.70a10868-979d-4236-9e17-f1d3ca07a300; "
    "__lt__cid=cbc02500-968d-4244-ad25-8c322d55465e; _ly_su=1782122835.70a10868-979d-4236-9e17-f1d3ca07a300; "
    "_cs_c=1; _tt_enable_cookie=1; _ttp=01KVQCRX9EBDFFMJGZBNXK0S8R_.tt.1; _fbp=fb.1.1782122839283.62221532613974181; "
    "_pin_unauth=dWlkPU5USmpNalppTkRVdFpEZ3hNUzAwT1ROakxUazVNekl0TVdWa01qYzVZVEF4T0Rkaw; "
    'g_state={"i_l":0,"i_ll":1782122901035,"i_e":{"enable_itp_optimization":24},"i_et":1782122901035}; '
    "_gac_UA-29776811-12=1.1789124050.Cj0KCQjwzY7VBhDwARIsAFtPvBRIL0vU8u3dAydogHOUbo4Ed8aVynWB1-BOVKq3DXbFyMpQpjoXbogaAi85EALw_wcB; "
    "_kmpid=km|[www.traveloka.com](https://www.traveloka.com)|1789957544592|de37b7f1-0de2-4bd7-a62c-1734fb5e1a5c; _kmpid=km|traveloka.com|1789957544592|de37b7f1-0de2-4bd7-a62c-1734fb5e1a5c; "
    'tv_cs=1; tv-repeat-visit=true; countryCode=ID; tv_user={"authorizationLevel":"400","id":"417137294"}; '
    "_gcl_gs=2.1.k1$i1791342189$u66658923; _gid=GA1.2.522278459.1791342204; AF_SYNC=1791342207164; "
    "_gcl_aw=GCL.1791342208.Cj0KCQjwuJLWBhD_ARIsAIBcRUwwAAaXca8U1HaKyfDB0e9MxMx2QADnjv8KOYvDOKFZqtOAz_EHX4QaAldyEALw_wcB; "
    "wcs_bt=s_2cb982ada97c:1791342901|undefined:1785831524; __rtbh.lid=%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%22bQCb3wLJJTHEelXFb2RI%22%2C%22expiryDate%22%22%2027-10-07T03%3A15%3A01.515Z%22%7D; "
    "_ga_RSRSMMBH0X=GS2.1.s1791342198$o16$g1$t1791342901$j30$l1$h1666236802; _ga=GA1.2.359313662.1782122835; "
    "_rdt_uuid=1782122837243.13938964-ac69-4367-b8b0-cabae3a03f85; _gcl_au=1.1.1247413928.1789957537.-.-.1789957539.621934230.1791342203.1791342905; "
    "_derived_epik=dj0yJnU9TmNsbUVTWm5NbTRmRThuek9abC16VV9sVjEtSTIxSUQmbj05aTFIODAzQ1pRUnBMZFZ6WlZUcDJRJm09MSZ0PUFBQUFBR3JGdVRnJnJtPTEmcnQ9QUFBQUFHckZ1VGcmc3A9Mg; "
    "cto_bundle=NDsyFF96YUNEZ0wzMTVmM1hxM2xORDRSeDQxdkZqcTJvQm91VzJmRXlybDlKUU8zSmM3aGFJaURZUE9zRXVmdllPcHZSRUJNT2lMclBYcFZnNkZkY3oxYVZGN0pzSFYxeDc5a1ElMkZnTmhadHVhcGZ5ZmlzcWVybEE2QnNFUHIyY2V6akdVZSUyQnpRSmtqT1NEM2pxeVNJOUFNREl3JTNEJTNE; "
    "AMP_f4354cda97=JTdCJTIyZGV2aWNlSWQlMjIlM0ElMjJhMWEyOGIzYy0yZGYwLTQ4NjgtYmIyMy00ODRlODlkZTE5NDYlMjIlMkMlMjJzZXNzaW9uSWQlMjIlM0ExNzkxMzQyOTI1NTk3JTJDJTIyb3B0T3V0JTIyJTNBZmFsc2UlMkMlMjJsYXN0RXZlbnRUaW1lJTIyJTNBMTc5MTM0MjkyNTgwMCUyQyUyMmxhc3RFdmVudElkJTIyJTNBMSUyQyUyMnBhZ2VDb3VudGVyJTIyJTNBMCUyQyUyMmNvb2tpZURvbWFpbiUyMiUzQSUyMi50cmF2ZWxva2EuY29tJTIyJTdE; "
    "AMP_1a5adb65ae=JTdCJTIyZGV2aWNlSWQlMjIlM0ElMjJhMWEyOGIzYy0yZGYwLTQ4NjgtYmIyMy00ODRlODlkZTE5NDYlMjIlMkMlMjJzZXNzaW9uSWQlMjIlM0ExNzkxMzQyOTI1NTk3JTJDJTIyb3B0T3V0JTIyJTNBZmFsc2UlMkMlMjJsYXN0RXZlbnRUaW1lJTIyJTNBMTc5MTM0MjkyNTkwNSUyQyUyMmxhc3RFdmVudElkJTIyJTNBNSUyQyUyMnBhZ2VDb3VudGVyJTIyJTNBMCUyQyUyMmNvb2tpZURvbWFpbiUyMiUzQSUyMi50cmF2ZWxva2EuY29tJTIyJTdE; "
    "ttcsid_CUM82PBC77U4QKJNCRL0=1791342207635::CsGF-F3jCN0TrU8nNF0y.13.1791342934476.1; ttcsid_CFNI0BRC77UEUGLEG00G=1791342240540::yqj5yTXqD-F4vOnMz_Bz.11.1791342934734.1; "
    "datadome=IGfPPIk5KOXA0GO3Xd3lu0pcSPR5t91V14kZglXnpg0SdGcSb2ogsXLTwsf6h7FcJ4AIOfnfh~7o_sNTvTNa7nxQV_vrareFk~921GHzH0Ym9ZZLHGLISgFyzJ6162OC; "
    "_cs_id=5cab8d91-7d6f-a3c7-a5b2-a6ec30c72ced.1784602522.13.1791355452.1791355452.1783401236.1818766522464.0.x; clientSessionId=T1-web.01M4AHP0Q69XQ4CSPHACTQGTCS; "
    "tv_lt=1791355454182; _cs_s=1.5.U.9.1791357252689; "
    "aws-waf-token=01872ca6-fc15-4035-8a0f-486148733c68:GgoAgydpu6z0IAAA:FW37NzySRSMCiVtSE9mWX1cUwO/uiwIvLW+FOjPsXpaJSPAZDwHmPBmLfz2uiNKqmvyWKyfD781UzH4tpOFanuU3iy2fSskohJbRhXpmgy2HXqjHvqo5ySjR+6rGDfy1sZt2pyZfCu6DjaSGiAiHF3/MtqTVWxskJTT+13rj+a6kfJbJfDDbiF4OOON4HHhG9wosR4/fduh5WzL/eBMPnHBQPpLzfl/bkTyoN4NSEVJSmdv4QWB2IOyl5cofESaz2erYyojcFL0IllTuwv8qrdYM3HAtkHKNF/w9ctmzsTNRvvR5bsELZF7M5mGiHA==; "
    "ttcsid=1791355456186::sNmtjMyx6qzKfygJcs6J.15.1791355456186.0::1.673767.727086::11334.7.246.138::610658.59.1377; tvo=L2FwaS92MS90dmxrL2V2ZW50cw==; "
    "_dd_s=rum=0&expire=1791356369676&logs=1&id=2550d287-3517-4013-8bab-e156672a6d52&created=1791355456171"
)

HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "content-type": "application/json",
    "origin": "https://www.traveloka.com",
    "referer": "https://www.traveloka.com/id-id/hotel",
    "cookie": DEFAULT_COOKIE,
    "x-traveloka-app": "desktop",
    "x-traveloka-locale": "id-ID",
    "x-client-interface": "desktop",
    "x-domain": "accomContent",
    "x-route-prefix": "id-id",
    "tv-language": "id_ID",
    "tv-currency": "IDR",
    "tv-country": "ID",
}

COOKIE_FILE = Path.home() / ".traveloka_cookies.json"


def save_cookies(cookie_str: str) -> None:
    """Simpan cookie string ke file untuk reuse."""
    try:
        COOKIE_FILE.write_text(cookie_str, encoding="utf-8")
        log.info("Cookie disimpan ke %s", COOKIE_FILE)
    except Exception as exc:
        log.warning("Gagal simpan cookie: %s", exc)


def load_cookies() -> str | None:
    """Load cookie string dari file jika ada."""
    if COOKIE_FILE.exists():
        try:
            cookie_str = COOKIE_FILE.read_text(encoding="utf-8").strip()
            if cookie_str:
                log.info("Cookie dimuat dari %s", COOKIE_FILE)
                return cookie_str
        except Exception:
            pass
    return None


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
                log.info("fill_traveloka_id: Login terdeteksi via selector: %s", selector)
                logged_in = True
                break
            except Exception:
                log.debug("fill_traveloka_id: Selector tidak ditemukan: %s", selector)
                continue
        
        if not logged_in:
            # Fallback: wait for any element that indicates logged-in state
            try:
                page.wait_for_selector('[data-testid="user-nav-dropdown"], a[href="/id-id/logout"], a[href*="/user/account"]', timeout=60000)
                logged_in = True
                log.info("fill_traveloka_id: Login terdeteksi via fallback selector")
            except Exception:
                pass
        
        if not logged_in:
            log.warning("fill_traveloka_id: Login tidak terdeteksi secara otomatis setelah 5 menit, lanjutkan ekstraksi cookie...")
        
        # Extract cookies
        cookies = context.cookies("https://www.traveloka.com")
        cookie_str = "; ".join(f"{c['name']}={c['value']}" for c in cookies)
        
        # Validate critical cookies
        critical_cookies = ['sen_t', 'clientSessionId', 'tv_mcc_id']
        cookie_names = {c['name'] for c in cookies}
        missing = [c for c in critical_cookies if c not in cookie_names]
        if missing:
            log.warning("fill_traveloka_id: Kuki krusial hilang: %s, login mungkin gagal", missing)
        else:
            log.info("fill_traveloka_id: Semua kuki krusial (sen_t, clientSessionId, tv_mcc_id) berhasil diekstrak")
        
        if keep_open:
            log.info("fill_traveloka_id: Browser dijaga terbuka untuk debugging. Tekan Enter untuk menutup...")
            input()
        
        browser.close()
        
        if cookie_str:
            save_cookies(cookie_str)
            log.info("Berhasil extract %d cookies dari browser", len(cookies))
        else:
            log.warning("Tidak ada cookie yang diextract")
        
        return cookie_str


def get_effective_cookies(auto_login: bool, headless_login: bool) -> str:
    """Dapatkan cookie yang valid: dari file, atau auto-login jika diminta."""
    # Coba load dari file dulu
    cookie_str = load_cookies()
    if cookie_str:
        return cookie_str
    
    # Fallback ke DEFAULT_COOKIE
    if DEFAULT_COOKIE:
        log.info("Menggunakan DEFAULT_COOKIE bawaan")
        return DEFAULT_COOKIE
    
    # Jika auto-login diminta
    if auto_login and HAS_PLAYWRIGHT:
        return get_fresh_traveloka_cookies(headless=headless_login)
    
    raise RuntimeError("Tidak ada cookie valid. Jalankan dengan --auto-login untuk login via browser.")


def get_hotels_without_traveloka_id() -> list[dict]:
    """Ambil hotel yang traveloka_object_id-nya NULL atau kosong."""
    sql = '''
        SELECT id_kompetitor, nama_hotel
        FROM "public"."hotel_master"
        WHERE traveloka_object_id IS NULL OR traveloka_object_id = ''
        ORDER BY id_kompetitor
    '''
    df = fetch_df(sql)
    if df is None or df.empty:
        return []
    return df.to_dict("records")


def _similarity(a: str, b: str) -> float:
    """Hitung similarity score antara dua string (0.0 - 1.0)."""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def _has_key_words(original: str, candidate: str) -> bool:
    """Check if candidate contains key identifying words from original name."""
    # Extract key words (length > 3, not common words)
    common_words = {'hotel', 'residence', 'apartment', 'hotel', 'inn', 'guesthouse', 'hostel', 'motel', 'syariah', 'redpartner', 'reddoorz', 'oyo', 'zen', 'collection', 'premiere', 'premier', 'express', 'international', 'expo', 'citygate', 'kota', 'harapan', 'indah', 'near', 'airport', 'by', 'and', 'the', 'by', 'of', 'in', 'at', 'on', 'for', 'with', 'from', 'to', 'a', 'an', 'the'}
    
    original_words = set(w.lower() for w in original.split() if len(w) > 3 and w.lower() not in common_words)
    candidate_lower = candidate.lower()
    
    # Check if at least one key word from original appears in candidate
    for word in original_words:
        if word in candidate_lower:
            return True
    return False


def extract_hotel_id_from_response(data: dict, query: str) -> tuple[str | None, str | None]:
    """
    Parsing response Traveloka autocomplete v1 dengan fuzzy matching.
    Return (objectId, found_name) atau (None, None) jika tidak ditemukan.
    """
    THRESHOLD = 0.65  # Minimum similarity score (lowered from 0.75)
    STRONG_THRESHOLD = 0.75  # Strong match threshold
    try:
        rows = data.get("data", {}).get("hotelContent", {}).get("rows", [])
        candidates = []
        
        for item in rows:
            if not isinstance(item, dict):
                continue
            
            # Cek tipe hotel
            if item.get("type") != "HOTEL" or item.get("accommodationType") != "HOTEL":
                continue
            
            object_id = item.get("id")
            if not object_id:
                continue
            
            found_name = item.get("displayName") or item.get("name") or "Unknown"
            score = _similarity(query, found_name)
            candidates.append((score, object_id, found_name))
        
        if not candidates:
            return None, None
        
        # Sort by score descending
        candidates.sort(key=lambda x: x[0], reverse=True)
        
        # Log top 3 candidates untuk debugging
        for score, oid, name in candidates[:3]:
            log.debug(f"  Kandidat: {name} (id: {oid}) - score: {score:.3f}")
        
        # Ambil yang score tertinggi di atas threshold
        best_score, best_oid, best_name = candidates[0]
        
        # Check if key words from original query are present in matched name
        has_key_words = _has_key_words(query, best_name)
        
        if best_score >= STRONG_THRESHOLD and has_key_words:
            log.info(f"  Match (strong): {best_name} (score: {best_score:.3f})")
            return str(best_oid), str(best_name)
        elif best_score >= THRESHOLD and has_key_words:
            log.info(f"  Match (moderate): {best_name} (score: {best_score:.3f})")
            return str(best_oid), str(best_name)
        elif best_score >= THRESHOLD:
            log.warning(f"  Match tanpa kata kunci: {best_name} (score: {best_score:.3f}) - review manual")
            return str(best_oid), str(best_name)
        else:
            log.warning(f"  Tidak ada match di atas threshold {THRESHOLD}. Terbaik: {best_name} (score: {best_score:.3f})")
            return None, None
            
    except (AttributeError, TypeError):
        return None, None


def build_headers(cookie_str: str) -> dict:
    """Build headers dengan cookie yang diberikan."""
    return {
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "content-type": "application/json",
        "origin": "https://www.traveloka.com",
        "referer": "https://www.traveloka.com/id-id/hotel",
        "cookie": cookie_str,
        "x-traveloka-app": "desktop",
        "x-traveloka-locale": "id-ID",
        "x-client-interface": "desktop",
        "x-domain": "accomContent",
        "x-route-prefix": "id-id",
        "tv-language": "id_ID",
        "tv-currency": "IDR",
        "tv-country": "ID",
    }


def _generate_query_variations(nama_hotel: str) -> list[str]:
    """
    Generate query variations from hotel name, from most specific to most general.
    Returns list of queries to try in order.
    """
    # Common suffixes/prefixes to remove for simplification
    # Each pattern replaces with a single space to avoid word concatenation
    remove_patterns = [
        (r'\s+by\s+\w+', ' '),           # "by Wyndham", "by IHG"
        (r'\s*&\s*Suites?', ' '),        # "& Suites", "& Suite"
        (r'\s*Hotel\s+', ' '),           # "Hotel "
        (r'\s*Residence\s+', ' '),       # "Residence "
        (r'\s*Apartment\s+', ' '),       # "Apartment "
        (r'\s*Guest\s+House\s+', ' '),   # "Guest House "
        (r'\s*Hostel\s+', ' '),          # "Hostel "
        (r'\s*Motel\s+', ' '),           # "Motel "
        (r'\s*Inn\s+', ' '),             # "Inn "
        (r'\s*Guesthouse\s+', ' '),      # "Guesthouse "
        (r'\s*Syariah\s+', ' '),         # "Syariah "
        (r'\s*RedPartner\s+', ' '),      # "RedPartner"
        (r'\s*RedDoorz\s+', ' '),        # "RedDoorz "
        (r'\s*OYO\s+', ' '),             # "OYO "
        (r'\s*ZEN\s+', ' '),             # "ZEN "
        (r'\s*Collection\s+', ' '),      # "Collection "
        (r'\s*Premiere\s+', ' '),        # "Premiere "
        (r'\s*Premier\s+', ' '),         # "Premier "
        (r'\s*Express\s+', ' '),         # "Express "
        (r'\s*International\s+', ' '),   # "International "
        (r'\s*Expo\s+', ' '),            # "Expo "
        (r'\s*Citygate\s+', ' '),        # "Citygate "
        (r'\s*Kota\s+Harapan\s+Indah', ' '),  # "Kota Harapan Indah"
        (r'\s*near\s+\w+(?:\s+\w+)?', ' '),   # "near Soekarno Hatta" / "Airport"
        (r'\s*@\s*\w+', ' '),            # "@ Aeropolis"
        (r'\s+[A-Z]{2,}\s*$', ''),       # Trailing airport codes
    ]
    
    queries = [nama_hotel]  # Start with full name
    
    # Generate simplified versions by removing patterns
    simplified = nama_hotel
    import re
    for pattern, replacement in remove_patterns:
        simplified = re.sub(pattern, replacement, simplified, flags=re.IGNORECASE)
        # Normalize multiple spaces to single space
        simplified = re.sub(r'\s+', ' ', simplified).strip()
        if simplified != nama_hotel and simplified not in queries:
            queries.append(simplified)
    
    # Also try just the first 2-3 words (core brand/name)
    words = nama_hotel.split()
    if len(words) >= 2:
        queries.append(' '.join(words[:2]))  # First 2 words
    if len(words) >= 3:
        queries.append(' '.join(words[:3]))  # First 3 words
    if len(words) >= 4:
        queries.append(' '.join(words[:4]))  # First 4 words
    
    # Remove duplicates while preserving order
    seen = set()
    unique_queries = []
    for q in queries:
        q_clean = q.strip()
        if q_clean and q_clean not in seen:
            seen.add(q_clean)
            unique_queries.append(q_clean)
    
    return unique_queries


def search_traveloka_hotel_id(nama_hotel: str, cookie_str: str | None = None, 
                              auto_login: bool = False, headless_login: bool = False) -> tuple[str | None, str | None]:
    """Cari objectId hotel pertama dari autocomplete Traveloka v1."""
    
    # Get effective cookies
    if cookie_str is None:
        try:
            cookie_str = get_effective_cookies(auto_login, headless_login)
        except RuntimeError as exc:
            log.warning("  %s", exc)
            return None, None
    
    # Extract sen_t token from cookie
    cookie_dict = dict(item.split("=", 1) for item in cookie_str.split("; ") if "=" in item)
    sen_t_token = cookie_dict.get("sen_t", "")
    
    # Generate query variations to try
    query_variations = _generate_query_variations(nama_hotel)
    log.debug("  Query variations: %s", query_variations)
    
    max_retries = 2 if auto_login else 1
    
    for attempt in range(max_retries):
        try:
            # Use session for proper cookie handling
            session = requests.Session()
            for cookie_pair in cookie_str.split("; "):
                if "=" in cookie_pair:
                    key, val = cookie_pair.split("=", 1)
                    session.cookies.set(key.strip(), val.strip(), domain=".traveloka.com")
            
            headers = build_headers(cookie_str)
            
            # Try each query variation until we get results
            for query_idx, query in enumerate(query_variations):
                payload = {
                    "clientInterface": "desktop",
                    "data": {
                        "query": query,
                        "experimentContext": {
                            "mapParamKeyToVariant": {"varAutocompleteLogic": "control"}
                        }
                    },
                    "fields": [],
                    "sentinel": {
                        "signals": [],
                        "token": sen_t_token
                    }
                }
                
                resp = session.post(API_URL, json=payload, headers=headers, timeout=15)
                log.debug("  Query '%s': status=%d, results=%d", query, resp.status_code, len(resp.json().get("data", {}).get("hotelContent", {}).get("rows", [])))
                
                if resp.status_code == 401 and attempt < max_retries - 1 and auto_login and HAS_PLAYWRIGHT:
                    log.info("  Cookie expired (401), mencoba auto-login ulang...")
                    cookie_str = get_fresh_traveloka_cookies(headless=headless_login)
                    cookie_dict = dict(item.split("=", 1) for item in cookie_str.split("; ") if "=" in item)
                    sen_t_token = cookie_dict.get("sen_t", "")
                    continue
                
                resp.raise_for_status()
                data = resp.json()
                rows = data.get("data", {}).get("hotelContent", {}).get("rows", [])
                
                if rows:
                    log.debug("  Query '%s' returned %d results", query, len(rows))
                    return extract_hotel_id_from_response(data, query)
                elif query_idx < len(query_variations) - 1:
                    log.debug("  Query '%s' returned 0 results, trying next variation...", query)
                    continue
                else:
                    log.warning("  All query variations returned 0 results")
                    return None, None
            
        except requests.RequestException as exc:
            log.warning("  API error untuk '%s': %s", nama_hotel, exc)
            return None, None
        except ValueError as exc:
            log.warning("  JSON parse error untuk '%s': %s", nama_hotel, exc)
            return None, None
    
    return None, None


def update_traveloka_id(id_kompetitor: str, object_id: str) -> bool:
    """Update traveloka_object_id di hotel_master."""
    sql = '''
        UPDATE "public"."hotel_master"
        SET traveloka_object_id = %s
        WHERE id_kompetitor = %s
    '''
    try:
        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (object_id, id_kompetitor))
                affected = cur.rowcount
            conn.commit()
        return affected > 0
    except Exception as exc:
        log.error("  DB update error untuk %s: %s", id_kompetitor, exc)
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Isi traveloka_object_id di hotel_master")
    parser.add_argument("--dry-run", action="store_true", help="Hanya tampilkan tanpa update DB")
    parser.add_argument("--limit", type=int, default=0, help="Batas jumlah hotel (0 = semua)")
    parser.add_argument("--delay", type=float, default=1.0, help="Jeda antar request (detik)")
    parser.add_argument("--auto-login", action="store_true", 
                        help="Buka browser untuk login otomatis jika cookie expired")
    parser.add_argument("--headless-login", action="store_true",
                        help="Coba login headless (butuh cookie tersimpan/session valid)")
    args = parser.parse_args()

    if args.auto_login and not HAS_PLAYWRIGHT:
        log.error("--auto-login butuh Playwright. Install: pip install playwright && playwright install chromium")
        sys.exit(1)

    hotels = get_hotels_without_traveloka_id()
    if not hotels:
        log.info("Tidak ada hotel yang perlu diupdate (semua sudah punya traveloka_object_id)")
        return

    if args.limit > 0:
        hotels = hotels[:args.limit]

    log.info("Ditemukan %d hotel tanpa traveloka_object_id", len(hotels))
    if args.dry_run:
        log.info("=== DRY RUN - tidak ada update DB ===")

    success = 0
    failed = 0
    for i, hotel in enumerate(hotels, 1):
        id_komp = hotel["id_kompetitor"]
        nama = hotel["nama_hotel"]
        log.info("[%d/%d] Mencari: %s (id_kompetitor=%s)", i, len(hotels), nama, id_komp)

        object_id, found_name = search_traveloka_hotel_id(
            nama, 
            auto_login=args.auto_login, 
            headless_login=args.headless_login
        )
        if object_id:
            log.info("  -> Ditemukan: %s (objectId=%s)", found_name, object_id)
            if not args.dry_run:
                if update_traveloka_id(id_komp, object_id):
                    log.info("  -> DB updated OK")
                    success += 1
                else:
                    log.warning("  -> DB update GAGAL")
                    failed += 1
            else:
                success += 1
        else:
            log.warning("  -> Tidak ditemukan di Traveloka")
            failed += 1

        if i < len(hotels):
            time.sleep(args.delay)

    log.info("=== SELESAI ===")
    log.info("Berhasil: %d | Gagal: %d | Total: %d", success, failed, len(hotels))


if __name__ == "__main__":
    main()