"""Scrape harga OTA (Google Travel) per hotel -> bronze.

Port dari bagian Google Travel di `gmaps_scraper.py` lama. Alur: rakit URL
`/travel/search` dengan parameter `ts=` (protobuf check-in/check-out), buka
kartu hotel yang cocok, lalu baca daftar OTA (`#prices`) beserta harganya.
Bila parameter `ts=` diabaikan Google, jatuh ke fallback klik kalender.

Berbeda dengan modul lama, di sini:
- registry hotel dibaca dari database (`hotel_master`), bukan config;
- tiap worker punya browser sendiri lewat `jalankan_per_hotel`;
- hasil ditulis ke data_lake bronze per tahun (mode append), bukan ke DB.

Tidak ada penulisan gold di sumber ini.
"""

from __future__ import annotations

import base64
import logging
import re
import time
from datetime import date, timedelta
from difflib import SequenceMatcher

import pandas as pd

from ..shared.checkpoint import Checkpoint
from ..shared.config import get_source_config
from ..shared.hotel_master import daftar_hotel
from ..shared.periods import resolve_years
from ..shared.row_limit import get_row_limit, is_limited, is_check
from ..shared.run_options import get_resume, get_dry_run
from ..shared.storage import write_parquet_by_year
from .gmaps_browser import (
    build_search_query,
    clean_price,
    is_similar,
    jalankan_per_hotel,
    normalize_text,
    timeout_dari_cfg,
)

log = logging.getLogger(__name__)

SOURCE = "gmaps"
DATASET = "harga"

# OTA yang diakui saat mem-parse teks panel (cadangan bila baris `#prices`
# tidak ter-render). Urutan tidak penting; nama dicocokkan case-insensitive.
OTA_TERKENAL = [
    "Traveloka", "Agoda", "Tiket.com", "Booking.com", "Expedia",
    "Trip.com", "Klook", "Pegipegi", "RedDoorz", "OYO", "Zen Rooms",
    "Nusatrip", "HotelTravel", "MyTours", "Dailytrips", "Mister Aladin",
]

SEL_SCOPE_PRICES = [
    '#prices > c-wiz.K1smNd > c-wiz.tuyxUe > div > section > '
    'div.A5WLXb.G3p8qb.q3c9pf > c-wiz > div',
    '#prices',
    '#prices c-wiz',
]
SEL_BARIS_OTA = 'div.ADs2Tc:has(span.FjC1We):has(span.nDkDDb)'
SEL_NAMA_OTA = 'span.FjC1We'
SEL_HARGA_OTA = 'span.nDkDDb'


# ============================================================
# ENCODER 'ts=' & URL GOOGLE TRAVEL
# ============================================================

def _varint(n: int) -> bytes:
    out = []
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)


def encode_ts_param(checkin: date, checkout: date) -> str:
    """Rakit parameter `ts=` (protobuf base64) untuk check-in/check-out."""
    nights = (checkout - checkin).days

    def _date_bytes(d):
        return (b'\x08' + _varint(d.year) +
                b'\x10' + _varint(d.month) +
                b'\x18' + _varint(d.day))

    payload = (
        b'\x08\x01'
        b'\x1a\x20'
        b'\x0a\x02\x1a\x00'
        b'\x12\x1a'
        b'\x12\x14'
        b'\x0a\x07' + _date_bytes(checkin) +
        b'\x12\x07' + _date_bytes(checkout) +
        b'\x18' + _varint(nights) +
        b'\x32\x02\x08\x01'
    )
    return base64.b64encode(payload).decode('ascii').rstrip('=')


def build_hotels_url(search_query: str, checkin: date, checkout: date) -> str:
    from urllib.parse import quote_plus

    q = quote_plus(search_query)
    ts = encode_ts_param(checkin, checkout)
    return f"https://www.google.com/travel/search?q={q}&hl=id&qs=CAAgACgA&ts={ts}&ap=MAE"


def _navigate_to_dates(page, search_query: str, checkin: date, checkout: date,
                       cfg: dict | None, nama_hotel: str) -> bool:
    """Navigate to new dates by updating URL, then re-click hotel card & Harga tab."""
    url = build_hotels_url(search_query, checkin, checkout)
    tunggu_ms = timeout_dari_cfg(cfg, "wait_load_ms", 2000)
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=5000)
        from .gmaps_browser import bypass_cookie
        bypass_cookie(page)
        time.sleep(0.5)
        # Re-click hotel card & Harga tab since navigation closes detail panel
        kartu = _pilih_kartu_organik_best_match(page, nama_hotel)
        if kartu is not None:
            _klik_kartu(page, kartu)
            _klik_tab_harga(page)
            try:
                page.locator('#prices').first.wait_for(state="visible", timeout=1500)
            except Exception:
                pass
        return True
    except Exception as exc:
        log.debug("[DEBUG] navigate_to_dates gagal: %s", exc)
        return False
    time.sleep(0.3)
    if not _pastikan_bulan_terlihat(page, checkin):
        return False
    if not _klik_hari(page, checkin):
        return False
    time.sleep(0.2)
    if not _pastikan_bulan_terlihat(page, checkout):
        return False
    if not _klik_hari(page, checkout):
        return False
    time.sleep(0.2)
    # Klik Done/Selesai
    for nama in ("Done", "Selesai", "Simpan", "Terapkan", "Apply"):
        try:
            btn = page.get_by_role("button", name=re.compile(re.escape(nama), re.I))
            if btn.count() > 0:
                btn.first.click(timeout=3000)
                time.sleep(0.5)
                return True
        except Exception:
            continue
    return True


def open_hotel_search(page, search_query: str, checkin: date, checkout: date,
                      cfg: dict | None = None) -> bool:
    """Buka Google Travel untuk satu skenario tanggal.

    Timeout `goto` dua tingkat dari config: percobaan pertama `goto_timeout_ms`
    (default 2s), percobaan kedua `goto_timeout_retry_ms` (default 4s). Halaman
    travel berat sehingga `goto` yang lama hanya menghasilkan log timeout tanpa
    memblokir progres run.
    """
    url = build_hotels_url(search_query, checkin, checkout)
    ts_val = url.split("ts=", 1)[1].split("&", 1)[0]
    cepat_ms = timeout_dari_cfg(cfg, "goto_timeout_ms", 2000)
    lambat_ms = timeout_dari_cfg(cfg, "goto_timeout_retry_ms", 4000)
    tunggu_ms = timeout_dari_cfg(cfg, "wait_load_ms", 2000)
    for percobaan, batas_ms in enumerate((cepat_ms, lambat_ms)):
        try:
            page.goto(url, timeout=batas_ms)
        except Exception as exc:
            log.debug("[DEBUG] goto %s gagal (timeout %dms): %s",
                      type(exc).__name__, batas_ms, exc)
            continue
        try:
            page.wait_for_load_state("domcontentloaded", timeout=tunggu_ms)
        except Exception:
            time.sleep(0.75)
        from .gmaps_browser import bypass_cookie

        bypass_cookie(page)
        if f"ts={ts_val}" in page.url:
            return True
    log.info("[DEBUG] ts= hilang; tidak ada fallback kalender")
    return False


# ============================================================
# EKSTRAKSI HARGA OTA
# ============================================================

def _tunggu_hasil_hotel(page, timeout_ms: int = 2000) -> bool:
    for sel in ('div.uaTTDe',
                'a[href^="https://www.google.com/travel/hotels/"]'):
        try:
            page.locator(sel).first.wait_for(state="attached", timeout=timeout_ms)
            return True
        except Exception:
            continue
    return False


def _penanda_sponsor():
    return re.compile(r"bersponsor|sponsor|iklan|sponsored|\bads?\b", re.I)


def _nama_dari_kartu(kartu) -> str:
    for sel in ('h2', 'h3', 'a[href*="/hotels/"]'):
        try:
            els = kartu.locator(sel)
            for i in range(min(els.count(), 3)):
                teks = (els.nth(i).inner_text() or "").strip()
                if teks:
                    return teks
        except Exception:
            continue
    return ""


def _pilih_kartu_organik_best_match(page, nama_hotel: str):
    kartu = page.locator('div.uaTTDe')
    if kartu.count() == 0:
        return None
    penanda = _penanda_sponsor()
    batas = kartu.count()
    kandidat_terbaik = None
    skor_terbaik = -1
    for i in range(batas):
        el = kartu.nth(i)
        try:
            teks_kartu = el.inner_text() or ""
        except Exception:
            continue
        if penanda.search(teks_kartu):
            continue
        nama = _nama_dari_kartu(el)
        if not nama:
            continue
        nama_lower = normalize_text(nama_hotel)
        nama_card = normalize_text(nama)
        if nama_lower == nama_card or nama_lower in nama_card or nama_card in nama_lower:
            return el
        skor = SequenceMatcher(None, nama_lower, nama_card).ratio()
        if skor > skor_terbaik:
            skor_terbaik = skor
            kandidat_terbaik = el
    if kandidat_terbaik and skor_terbaik >= 0.5:
        return kandidat_terbaik
    for i in range(batas):
        try:
            el = kartu.nth(i)
            if not penanda.search(el.inner_text() or ""):
                return el
        except Exception:
            continue
    return None


def _parse_ota_dari_teks(teks) -> list[dict]:
    """Cari pasangan OTA-harga dari teks bebas panel harga."""
    if not teks:
        return []
    teks = re.sub(r"\s+", " ", str(teks))
    spots_utama = [(m.start(), clean_price(m.group(0)))
                   for m in re.finditer(r"(?:Rp|IDR)\s*[\d.,]+", teks)]
    spots_cadangan = [(m.start(), clean_price(m.group(0)))
                      for m in re.finditer(r"\b\d{1,3}(?:[.,]\d{3})+\b", teks)]
    spots = spots_utama or spots_cadangan
    hasil: dict[str, int] = {}
    for ota in OTA_TERKENAL:
        for m in re.finditer(re.escape(ota), teks, re.I):
            pos = m.start()
            jarak, harga_terdekat = None, None
            for ppos, h in spots:
                if h <= 0:
                    continue
                d = abs(ppos - pos)
                if d <= 250 and (jarak is None or d < jarak):
                    jarak, harga_terdekat = d, h
            if harga_terdekat is not None:
                hasil[ota] = min(hasil.get(ota, harga_terdekat), harga_terdekat)
    return [{"sumber": ota, "harga": h} for ota, h in hasil.items()]


def _dialog_hotel(page, nama_hotel: str):
    try:
        dialogs = page.locator('div[role="dialog"]')
        if dialogs.count() == 0:
            return None
        target = normalize_text(nama_hotel)
        for i in range(dialogs.count()):
            try:
                teks = normalize_text(dialogs.nth(i).inner_text())
            except Exception:
                continue
            if target and teks and target in teks:
                return dialogs.nth(i)
        return dialogs.last
    except Exception:
        return None


def _teks_panel_lengkap(page, nama_hotel: str | None = None) -> str:
    bagian = []
    es = []
    dialog = None
    try:
        dialog = _dialog_hotel(page, nama_hotel) if nama_hotel else None
    except Exception:
        dialog = None
    if dialog is not None:
        try:
            teks = dialog.inner_text()
            if teks:
                bagian.append(teks)
        except Exception:
            pass
        es = [dialog]
    else:
        try:
            dl = page.locator('div[role="dialog"]')
            if dl.count() > 0:
                dlg = dl.last
                teks = dlg.inner_text()
                if teks:
                    bagian.append(teks)
                es = [dlg]
        except Exception:
            pass
    for scope_h in es:
        for sel, atr in (('img[alt]', 'alt'), ('[aria-label]', 'aria-label')):
            try:
                els = scope_h.locator(sel)
                for i in range(min(els.count(), 150)):
                    try:
                        val = (els.nth(i).get_attribute(atr) or "").strip()
                    except Exception:
                        continue
                    if val:
                        bagian.append(val)
            except Exception:
                continue
    teks = "\n".join(bagian).strip()
    if len(teks) > 20:
        return teks
    try:
        return page.inner_text("body")
    except Exception:
        return teks


def _klik_kartu(page, kartu) -> bool:
    klik_ok = False
    for sel in ('a[href*="/hotels/"]', 'h2', 'h3'):
        try:
            el = kartu.locator(sel).first
            if el.count() > 0 and el.is_visible():
                el.click(timeout=3000)
                klik_ok = True
                break
        except Exception:
            continue
    if not klik_ok:
        try:
            kartu.click(timeout=3000)
            klik_ok = True
        except Exception:
            return False
    try:
        page.locator('#prices, div[role="dialog"]').first.wait_for(
            state="attached", timeout=2000)
    except Exception:
        time.sleep(0.4)
    return True


def _klik_tab_harga(page) -> bool:
    """Klik tab 'Harga' jika belum aktif. Return True jika klik/aktif, False jika gagal."""
    # Check if already active (aria-selected="true")
    try:
        active_tab = page.locator('[role="tab"][aria-selected="true"]').first
        if active_tab.count() > 0:
            aria_label = (active_tab.get_attribute("aria-label") or "").strip().lower()
            if "harga" in aria_label:
                return True  # already on Harga tab
    except Exception:
        pass

    # Click Harga tab - actual element is <div role="tab" aria-label="Harga" aria-controls="prices">
    selectors = [
        '[role="tab"][aria-label="Harga"]',           # exact match via aria-label
        '[role="tab"][aria-controls="prices"]',       # via aria-controls
        '#prices[role="tab"]',                        # by id (same as panel)
        '[role="tab"]:has-text("Harga")',             # div with text "Harga" in children
    ]
    for sel in selectors:
        try:
            el = page.locator(sel).first
            if el.count() > 0:
                # Wait for tab to be clickable (not just visible)
                el.wait_for(state="visible", timeout=2000)
                # Ensure not disabled
                if el.is_disabled():
                    continue
                el.click(timeout=3000)
                time.sleep(0.3)
                # Verify click succeeded: check aria-selected="true"
                try:
                    updated = page.locator('[role="tab"][aria-selected="true"]').first
                    if updated.count() > 0:
                        aria_label = (updated.get_attribute("aria-label") or "").strip().lower()
                        if "harga" in aria_label:
                            return True
                except Exception:
                    pass
                return True  # clicked, assume success
        except Exception:
            continue
    return False


def _scroll_di_detail(page, langkah: int = 1, jeda: float = 0.15) -> bool:
    for _ in range(langkah):
        try:
            page.evaluate("window.scrollBy(0, 600)")
        except Exception:
            pass
        time.sleep(jeda)
    return True


def _cari_scope_prices(page):
    for sel in SEL_SCOPE_PRICES:
        try:
            scope = page.locator(sel)
            if scope.count() > 0:
                return scope
        except Exception:
            continue
    return None


def _nama_dari_baris(baris, nama_hotel: str) -> str:
    try:
        el = baris.locator(SEL_NAMA_OTA).first
        nama = (el.inner_text() or "").strip()
    except Exception:
        nama = ""
    if not nama:
        try:
            nama = (baris.inner_text().splitlines()[0] or "").strip()
        except Exception:
            nama = ""
    if nama and nama_hotel and is_similar(nama, nama_hotel, threshold=0.6):
        return "Official site"
    return nama


def _ekstrak_ota_all_options(page, nama_hotel: str | None = None) -> list[dict]:
    hasil: dict[str, int] = {}
    scope = _cari_scope_prices(page)
    if scope is None:
        return []
    try:
        baris_all = scope.locator(SEL_BARIS_OTA)
        jumlah = baris_all.count()
    except Exception:
        return []
    for i in range(min(jumlah, 400)):
        try:
            baris = baris_all.nth(i)
            sumber = _nama_dari_baris(baris, nama_hotel)
            if not sumber:
                continue
            harga = None
            harga_el = baris.locator(SEL_HARGA_OTA)
            for j in range(min(harga_el.count(), 20)):
                try:
                    nilai = clean_price(harga_el.nth(j).inner_text())
                except Exception:
                    continue
                if nilai > 0:
                    harga = nilai if harga is None else min(harga, nilai)
            if not harga or harga <= 0:
                continue
            hasil[sumber] = min(hasil.get(sumber, harga), harga)
        except Exception:
            continue
    return [{"sumber": s, "harga": h} for s, h in hasil.items()]


def _klik_opsi_lainnya(page) -> bool:
    pola_list = (
        "Lihat opsi lainnya mulai dari",
        "opsi lainnya",
        "lihat opsi",
        "lihat semua",
        "semua opsi",
        "more options",
    )

    def _coba_klik(el):
        try:
            if el.count() > 0 and el.is_visible():
                el.click(timeout=2000)
                try:
                    page.locator(SEL_BARIS_OTA).first.wait_for(
                        state="visible", timeout=2000)
                except Exception:
                    time.sleep(0.15)
                return True
        except Exception:
            pass
        return False

    for sel in ('span.bbRZy[role="button"]', 'span.bbRZy',
                '[role="button"][data-id="tBvzvc"]'):
        if _coba_klik(page.locator(sel).first):
            return True
    for scope_sel in ('#prices', None):
        for pola in pola_list:
            for role in ('button', '[role="button"]', 'a', 'span'):
                base = page.locator(scope_sel) if scope_sel else page
                sel = f'{role}:has-text("{pola}")'
                try:
                    if _coba_klik(base.locator(sel).first):
                        return True
                except Exception:
                    continue
    return False


def extract_ota_prices(page, nama_hotel: str, cfg: dict | None = None) -> list[dict]:
    """Baca semua harga OTA pada halaman detail hotel saat ini."""
    hasil_gabungan: dict[str, int] = {}

    def _agregasi(pasangan):
        for p in pasangan or []:
            ota = str(p.get('sumber') or '').strip()
            h = int(p.get('harga') or 0)
            if not ota or h <= 0:
                continue
            hasil_gabungan[ota] = min(hasil_gabungan.get(ota, h), h)

    if not _tunggu_hasil_hotel(page, timeout_dari_cfg(cfg, "wait_load_ms", 2000)):
        log.debug("[DEBUG] hasil hotel tidak muncul; anggap detail terbuka.")
        _scroll_di_detail(page)
        _klik_opsi_lainnya(page)
        pasangan = _ekstrak_ota_all_options(page, nama_hotel)
        if not pasangan:
            pasangan = _parse_ota_dari_teks(_teks_panel_lengkap(page, nama_hotel))
        _agregasi(pasangan)
        return [{"sumber": o, "harga": h} for o, h in hasil_gabungan.items()]

    kartu = _pilih_kartu_organik_best_match(page, nama_hotel)
    if kartu is not None:
        try:
            _agregasi(_parse_ota_dari_teks(kartu.inner_text()))
        except Exception:
            pass
        _klik_kartu(page, kartu)
        _klik_tab_harga(page)
        try:
            page.locator('#prices').first.wait_for(state="visible", timeout=1500)
        except Exception:
            pass
    _scroll_di_detail(page)
    _klik_opsi_lainnya(page)
    pasangan = _ekstrak_ota_all_options(page, nama_hotel)
    if not pasangan:
        pasangan = _parse_ota_dari_teks(_teks_panel_lengkap(page, nama_hotel))
    _agregasi(pasangan)
    log.info("gmaps harga %s -> %d sumber OTA", nama_hotel, len(hasil_gabungan))
    return [{"sumber": o, "harga": h} for o, h in hasil_gabungan.items()]


# ============================================================
# PER HOTEL & ORKESTRATOR
# ============================================================

def _lead_time_dates(cfg: dict, today: date) -> list[dict]:
    """Skenario tanggal check-in dari config `lead_times` (mis. [1..7, 30, 60])."""
    daftar = cfg.get("lead_times")
    if daftar:
        lead_times = sorted({int(x) for x in daftar})
    else:
        min_lt = int(cfg.get("lead_time_min", 1))
        max_lt = int(cfg.get("lead_time_max", 7))
        lead_times = list(range(min_lt, max_lt + 1))
    return [
        {"lead_time": lt, "checkin": today + timedelta(days=lt),
         "checkout": today + timedelta(days=lt + 1)}
        for lt in lead_times
    ]


def scrape_hotel_prices(context, hotel: dict, cfg: dict) -> list[dict]:
    """Ambil harga OTA untuk semua lead time satu hotel. Batas waktu dihormati.

    Hybrid approach:
    - First lead time: full page load via ts= URL (reliable)
    - Subsequent lead times: change dates via calendar picker on same page (no reload)
    """
    nama = str(hotel.get("nama_hotel") or "").strip()
    id_komp = str(hotel.get("id_kompetitor") or "").strip()
    hasil: list[dict] = []
    if not nama or not id_komp:
        return hasil

    search_query = build_search_query(
        nama, hotel.get("alamat"), hotel.get("kota"))
    today = date.today()
    skenario = _lead_time_dates(cfg, today)
    batas_detik = float(cfg.get("batas_detik_per_hotel", 420) or 420)
    tenggat = time.monotonic() + batas_detik

    page = context.new_page()
    try:
        first_sk = skenario[0] if skenario else None
        if not first_sk:
            return hasil

        # ========== FIRST LEAD TIME: full page load via ts= URL ==========
        checkin, checkout = first_sk["checkin"], first_sk["checkout"]
        try:
            if open_hotel_search(page, search_query, checkin, checkout, cfg):
                # Click hotel card & Harga tab ONCE
                harga_list = extract_ota_prices(page, nama, cfg)
                for item in harga_list or []:
                    harga = int(item.get("harga") or 0)
                    sumber = str(item.get("sumber") or "").strip()
                    if harga > 0 and sumber:
                        hasil.append({
                            "id_kompetitor": id_komp,
                            "nama_hotel": nama,
                            "checkin_date": checkin,
                            "lead_time": first_sk["lead_time"],
                            "sumber": sumber,
                            "harga": harga,
                            "scrape_date": today,
                        })
        except Exception as exc:
            log.warning("  [%s] H-%s gagal (first): %s", nama[:40], first_sk["lead_time"], exc)

        # ========== REMAINING LEAD TIMES: calendar picker (no reload) ==========
        for sk in skenario[1:]:
            if time.monotonic() > tenggat:
                log.info("  [%s] batas waktu %s dtk tercapai; sisa lead time dilewati",
                         nama[:40], int(batas_detik))
                break
            checkin, checkout = sk["checkin"], sk["checkout"]
            try:
                # Navigate to new dates via URL, then re-click hotel card & Harga tab
                if _navigate_to_dates(page, search_query, checkin, checkout, cfg, nama):
                    harga_list = extract_ota_prices(page, nama, cfg)
                    for item in harga_list or []:
                        harga = int(item.get("harga") or 0)
                        sumber = str(item.get("sumber") or "").strip()
                        if harga > 0 and sumber:
                            hasil.append({
                                "id_kompetitor": id_komp,
                                "nama_hotel": nama,
                                "checkin_date": checkin,
                                "lead_time": sk["lead_time"],
                                "sumber": sumber,
                                "harga": harga,
                                "scrape_date": today,
                            })
                else:
                    log.warning("  [%s] H-%s: navigasi URL gagal", nama[:40], sk["lead_time"])
            except Exception as exc:
                log.warning("  [%s] H-%s gagal: %s", nama[:40], sk["lead_time"], exc)
    finally:
        try:
            page.close()
        except Exception:
            pass
    return hasil


def _simpan_hotel_ke_parquet(baris_hotel: list[dict], hotel: dict, cfg: dict) -> None:
    """Tulis data satu hotel ke parquet (mode append per tahun, thread-safe).

    Dipanggil via `titik_selesai` callback di `jalankan_per_hotel()`.
    Jika gagal, exception naik supaya hotel tidak ditandai di checkpoint.
    """
    if not baris_hotel:
        return
    if get_dry_run():
        log.info("  [%s] %d baris (dry-run: tidak ditulis)", hotel.get("nama_hotel")[:40], len(baris_hotel))
        return
    
    df = pd.DataFrame(baris_hotel)
    write_parquet_by_year(
        df,
        "bronze",
        SOURCE,
        DATASET,
        year_col="scrape_date",
        mode="append",
        dedup_cols=["id_kompetitor", "checkin_date", "sumber", "scrape_date"],
    )
    log.info("  [%s] %d baris ditulis ke parquet", hotel.get("nama_hotel")[:40], len(df))


def extract(period: str | None = None, dry_run: bool = False) -> dict[str, pd.DataFrame]:
    """Harga OTA Google Travel semua hotel -> bronze per tahun (append).

    Bronze dipecah per tahun berdasarkan `scrape_date` dengan mode append:
    run harian berikutnya tidak menimpa harga hari-hari sebelumnya. Lead time
    yang diambil ditentukan `lead_times` di config; tidak ada penulisan gold.
    """
    cfg = get_source_config(SOURCE)
    tahun = resolve_years(period)
    hotel_list = daftar_hotel(cfg)

    batas = get_row_limit()
    if batas is not None:
        hotel_list = hotel_list[:batas]
        log.info("%s: mode uji, dibatasi %d hotel pertama", SOURCE, batas)

    if not hotel_list:
        log.warning("%s: tidak ada hotel di registry (hotel_master)", SOURCE)
        return {}

    log.info("%s: %d hotel, lead_times=%s",
             SOURCE, len(hotel_list), [s["lead_time"] for s in _lead_time_dates(cfg, date.today())])

    checkpoint = Checkpoint(SOURCE)
    if get_resume():
        sudah = checkpoint.muat()
        hotel_list = [h for h in hotel_list if not checkpoint.sudah(h.get("id_kompetitor"))]
        if not hotel_list:
            log.info("%s: %d hotel sudah diproses hari ini, tidak ada yang diulang",
                     SOURCE, len(sudah))
            checkpoint.bersihkan()
            return {}
        log.info("%s: melanjutkan, %d hotel tersisa", SOURCE, len(hotel_list))
    else:
        checkpoint.bersihkan()

    hasil = jalankan_per_hotel(
        hotel_list,
        lambda ctx, h: scrape_hotel_prices(ctx, h, cfg),
        cfg,
        "harga_workers",
        checkpoint=checkpoint,
        titik_selesai=lambda baris, hotel: _simpan_hotel_ke_parquet(baris, hotel, cfg),
    )
    checkpoint.simpan()

    log.info("%s: %d baris harga dari %d hotel (tahun %s)",
             SOURCE, len(hasil), len(hotel_list), tahun)
    if not hasil:
        return {}

    df = pd.DataFrame(hasil)
    if dry_run:
        log.info("[dry-run] %s bronze %s: %d baris (tidak ditulis)",
                 SOURCE, DATASET, len(df))
    elif is_check():
        # Mode check tidak boleh menyentuh parquet riil; laporkan apa adanya
        # supaya "OK + ditulis" tidak terbaca padahal nol byte tersimpan.
        log.info("%s: %d baris (mode check: bronze TIDAK ditulis)", SOURCE, len(df))
    # else: per-hotel write sudah dilakukan via titik_selesai callback

    return {DATASET: df}
