from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from urllib.parse import quote_plus

import pandas as pd
from playwright.sync_api import sync_playwright

from ..shared.config import get_db_config, get_source_config
from ..shared.dataset_filter import dataset_terpilih, wants
from ..shared.db import fetch_df, table_exists
from ..shared.row_limit import get_row_limit
from ..shared.storage import write_parquet, write_parquet_by_year

log = logging.getLogger(__name__)

SOURCE = "gmaps"
LABEL_DEFAULT = "latest"

NAMA_BULAN = {
    1: "Januari", 2: "Februari", 3: "Maret", 4: "April", 5: "Mei", 6: "Juni",
    7: "Juli", 8: "Agustus", 9: "September", 10: "Oktober", 11: "November",
    12: "Desember",
}
NAMA_BULAN_EN = {
    1: "January", 2: "February", 3: "March", 4: "April", 5: "May", 6: "June",
    7: "July", 8: "August", 9: "September", 10: "October", 11: "November",
    12: "December",
}

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


def _label(cfg: dict, period: str | None) -> str:
    return (period or cfg.get("period", LABEL_DEFAULT)).strip().lower()


# ============================================================
# UTILITAS
# ============================================================

def clean_price(text):
    s = str(text or '')
    digits = re.sub(r'[^\d]', '', s)
    try:
        return int(digits) if digits else 0
    except (ValueError, TypeError):
        return 0


def normalize_text(text):
    return re.sub(r'\s+', ' ', str(text or '')).strip().lower()


def is_similar(teks1, teks2, threshold=0.7):
    a, b = normalize_text(teks1), normalize_text(teks2)
    if not a or not b:
        return False
    if a in b or b in a:
        return True
    return SequenceMatcher(None, a, b).ratio() >= threshold


def build_search_query(nama_hotel, alamat=None, kota=None):
    query = str(nama_hotel).strip()
    bagian = []
    if alamat and str(alamat).strip().lower() not in ('nan', 'none', ''):
        bagian.append(str(alamat).strip())
    if kota and str(kota).strip().lower() not in ('nan', 'none', ''):
        bagian.append(str(kota).strip())
    if bagian:
        query = f"{query}, {', '.join(bagian)}"
    return query


def convert_relative_time(relative_str):
    current_date = datetime.now()
    text = str(relative_str).lower().strip()
    if not text:
        return None
    if "kemarin" in text or "hari ini" in text or "baru saja" in text:
        return f"{current_date.year}-{current_date.month:02d}"
    text = text.replace("sebulan", "1 bulan").replace("setahun", "1 tahun")
    text = text.replace("seminggu", "1 minggu").replace("sehari", "1 hari")
    match = re.search(r'(\d+)\s+(hari|minggu|bulan|tahun)', text)
    if not match:
        return None
    amount = int(match.group(1))
    unit = match.group(2)
    result_date = current_date
    if unit == 'hari':
        result_date -= timedelta(days=amount)
    elif unit == 'minggu':
        result_date -= timedelta(weeks=amount)
    elif unit == 'bulan':
        result_date -= timedelta(days=round(amount * 30.44))
    elif unit == 'tahun':
        result_date -= timedelta(days=round(amount * 365.25))
    return f"{result_date.year}-{result_date.month:02d}"


def extract_review_count(text):
    text = str(text or '')
    match = re.search(r'([\d.,]+)\s*(ulasan|reviews|review)', text.lower())
    if not match:
        match = re.search(r'\((\d{1,3}(?:[.,]\d{3})+)\)', text)
    if not match:
        return 0
    try:
        return int(re.sub(r'[.,]', '', match.group(1)))
    except Exception:
        return 0


# ============================================================
# GOOGLE TRAVEL: ENCODER 'ts=' & NAVIGASI
# ============================================================

def _varint(n):
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


def encode_ts_param(checkin, checkout):
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
    return __import__("base64").b64encode(payload).decode('ascii').rstrip('=')


def build_hotels_url(search_query, checkin, checkout):
    q = quote_plus(search_query)
    ts = encode_ts_param(checkin, checkout)
    return f"https://www.google.com/travel/search?q={q}&hl=id&qs=CAAgACgA&ts={ts}&ap=MAE"


def konfigurasi_browser(playwright, headless=False, blokir_gambar=False):
    args = [] if headless else ["--start-maximized"]
    browser = playwright.chromium.launch(headless=headless, args=args)
    if headless:
        context = browser.new_context(
            viewport={"width": 1366, "height": 768}, locale="id-ID")
    else:
        context = browser.new_context(no_viewport=True, locale="id-ID")
    page = context.new_page()

    blokir = ["media", "font"]
    if blokir_gambar:
        blokir.append("image")

    def _route(route):
        if route.request.resource_type in blokir:
            route.abort()
        else:
            route.continue_()

    page.route("**/*", _route)
    return browser, context, page


def bypass_cookie(page):
    for teks in ("Accept all", "Setuju", "Saya setuju", "Terima semua",
                 "Agree", "Saya menyetujui"):
        try:
            btn = page.locator(f'button:has-text("{teks}")')
            if btn.count() > 0:
                btn.first.click(timeout=3000)
                time.sleep(0.15)
                return
        except Exception:
            continue


def _buka_field_kalender(page):
    for nama in ("Check-in", "Check in", "Checkin", "Tanggal masuk", "Masuk"):
        for role in ("textbox", "button", "combobox"):
            try:
                el = page.get_by_role(role, name=re.compile(re.escape(nama), re.I))
                if el.count() > 0:
                    el.first.click(timeout=3000)
                    return True
            except Exception:
                continue
    for sel in ('input[placeholder*="heck"]',
                '[aria-label*="heck-in"]',
                '[aria-label*="heck in"]'):
        try:
            el = page.locator(sel)
            if el.count() > 0:
                el.first.click(timeout=3000)
                return True
        except Exception:
            continue
    return False


def _label_bulan(tgl):
    return {
        "id": f"{NAMA_BULAN[tgl.month]} {tgl.year}",
        "en": f"{NAMA_BULAN_EN[tgl.month]} {tgl.year}",
        "pendek_id": f"{NAMA_BULAN[tgl.month][:3]} {tgl.year}",
        "pendek_en": f"{NAMA_BULAN_EN[tgl.month][:3]} {tgl.year}",
    }


def _teks_kalender(page):
    for sel in ('div[role="dialog"]',
                'div[aria-label*="alender" i]',
                'div[aria-label*="tanggal" i]'):
        try:
            els = page.locator(sel)
            if els.count() > 0:
                teks = els.last.inner_text()
                if teks:
                    return teks
        except Exception:
            continue
    try:
        return page.inner_text("body")
    except Exception:
        return ""


def _tekan_arah(page, maju=True):
    istilah = (("next", "berikut", "selanjutnya", "lanjut", "kanan", "depan")
               if maju else
               ("previous", "sebelum", "kembali", "kiri", "belakang"))
    for role in ("button", "img"):
        els = page.get_by_role(
            role,
            name=re.compile(r"(next|prev|previous|sebelum|berikut|"
                            r"selanjutnya|lanjut|kanan|kiri)", re.IGNORECASE),
        )
        for i in range(els.count()):
            el = els.nth(i)
            try:
                lbl = (el.get_attribute("aria-label") or "") + " " + (el.inner_text() or "")
            except Exception:
                continue
            if any(k in lbl.lower() for k in istilah):
                try:
                    el.click(timeout=3000)
                    return True
                except Exception:
                    continue
    try:
        els = page.locator('button')
        for i in range(els.count()):
            el = els.nth(i)
            try:
                teks = (el.inner_text() or "").strip()
            except Exception:
                continue
            if (maju and teks in ("›", ">", "»", "❯")) or \
                    (not maju and teks in ("‹", "<", "«", "❮")):
                try:
                    el.click(timeout=3000)
                    return True
                except Exception:
                    continue
    except Exception:
        pass
    return False


def _pastikan_bulan_terlihat(page, tgl, max_percobaan=36):
    target = set(_label_bulan(tgl).values())
    for _ in range(max_percobaan):
        teks = _teks_kalender(page)
        if any(b.lower() in teks.lower() for b in target):
            return True
        if not _tekan_arah(page, maju=True):
            return False
        time.sleep(0.15)
    return False


def _klik_hari(page, tgl):
    hari = str(tgl.day)
    label = _label_bulan(tgl)
    peluang = ('div[role="gridcell"]', 'td[role="gridcell"]', 'td[class*="day"]',
               'button[class*="day"]', 'table td', 'div[role="button"]',
               '[role="gridcell"]')
    for sel in peluang:
        try:
            els = page.locator(sel)
            for i in range(els.count()):
                el = els.nth(i)
                try:
                    if el.is_disabled():
                        continue
                    teks = (el.inner_text() or "").strip()
                except Exception:
                    continue
                if teks != hari:
                    continue
                try:
                    aria = (el.get_attribute("aria-label") or "")
                except Exception:
                    aria = ""
                if aria:
                    potongan = [v.lower() for v in label.values()]
                    if not any(p in aria.lower() for p in potongan) and \
                            re.search(r'20\d{2}', aria):
                        continue
                try:
                    el.click(timeout=3000)
                    return True
                except Exception:
                    continue
        except Exception:
            continue
    return False


def set_dates_via_calendar(page, checkin, checkout):
    log.info("[Kalender] parameter ts= tidak dikenali; fallback klik kalender...")
    if not _buka_field_kalender(page):
        return False
    time.sleep(0.25)
    if not _pastikan_bulan_terlihat(page, checkin):
        return False
    if not _klik_hari(page, checkin):
        return False
    time.sleep(0.15)
    if not _pastikan_bulan_terlihat(page, checkout):
        return False
    if not _klik_hari(page, checkout):
        return False
    time.sleep(0.15)
    for nama in ("Done", "Selesai", "Simpan", "Terapkan"):
        try:
            btn = page.get_by_role("button", name=re.compile(re.escape(nama), re.I))
            if btn.count() > 0:
                btn.first.click(timeout=3000)
                time.sleep(0.25)
                return True
        except Exception:
            continue
    return True


def _ambil_teks_field_tanggal(page, pola):
    for role in ("textbox", "button", "combobox"):
        els = page.get_by_role(role, name=re.compile(pola, re.I))
        for i in range(min(els.count(), 6)):
            el = els.nth(i)
            teks = ""
            try:
                teks = el.get_attribute("value") or ""
            except Exception:
                pass
            if not teks:
                try:
                    teks = (el.inner_text() or "").strip()
                except Exception:
                    continue
            if teks:
                return teks.strip()
    return ""


def dates_are_set(page, checkin, checkout):
    teks_ci = _ambil_teks_field_tanggal(page, r".*check.?in.*|.*masuk.*")
    teks_co = _ambil_teks_field_tanggal(page, r".*check.?out.*|.*keluar.*")
    if not teks_ci and not teks_co:
        return False

    def _cocok(teks, tgl):
        if not teks:
            return False
        if not re.search(rf"(?<!\d)0?{tgl.day}(?!\d)", teks):
            return False
        bulan_id = NAMA_BULAN[tgl.month]
        bulan_en = NAMA_BULAN_EN[tgl.month]
        pola_bulan = [bulan_id, bulan_id[:3], bulan_en, bulan_en[:3], str(tgl.month)]
        if any(re.search(re.escape(b), teks, re.IGNORECASE) for b in pola_bulan):
            return True
        return str(tgl.year) in teks

    return _cocok(teks_ci, checkin) and _cocok(teks_co, checkout)


def open_hotel_search(page, search_query, checkin, checkout):
    url = build_hotels_url(search_query, checkin, checkout)
    ts_val = url.split("ts=", 1)[1].split("&", 1)[0]
    for _ in range(2):
        page.goto(url, timeout=30000)
        try:
            page.wait_for_load_state("domcontentloaded", timeout=15000)
        except Exception:
            time.sleep(0.75)
        bypass_cookie(page)
        if f"ts={ts_val}" in page.url:
            return True
    log.info("[DEBUG] ts= hilang; fallback kalender...")
    if set_dates_via_calendar(page, checkin, checkout):
        time.sleep(0.5)
        return True
    return False


# ============================================================
# GOOGLE TRAVEL: EKSTRAKSI HARGA OTA
# ============================================================

def _tunggu_hasil_hotel(page, timeout_ms=8000):
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


def _nama_dari_kartu(kartu):
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


def _pilih_kartu_organik_best_match(page, nama_hotel):
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
            if not _penanda_sponsor().search(el.inner_text() or ""):
                return el
        except Exception:
            continue
    return None


def _parse_ota_dari_teks(teks):
    if not teks:
        return []
    teks = re.sub(r"\s+", " ", str(teks))
    spots_utama = [(m.start(), clean_price(m.group(0)))
                   for m in re.finditer(r"(?:Rp|IDR)\s*[\d.,]+", teks)]
    spots_cadangan = [(m.start(), clean_price(m.group(0)))
                      for m in re.finditer(r"\b\d{1,3}(?:[.,]\d{3})+\b", teks)]
    spots = spots_utama or spots_cadangan
    hasil = {}
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


def _dialog_hotel(page, nama_hotel):
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


def _container_hotel(scope, nama_hotel):
    target = normalize_text(nama_hotel)
    if not target:
        return None
    for sel in ('div[role="dialog"]', '[jsname]', 'div[data-hotel]',
                'li', '[role="listitem"]', 'div'):
        try:
            els = scope.locator(sel)
            for i in range(min(els.count(), 500)):
                try:
                    el = els.nth(i)
                    teks = normalize_text(el.inner_text())
                except Exception:
                    continue
                if target and teks and target in teks and len(teks) <= 600:
                    return el
        except Exception:
            continue
    return None


def _teks_panel_lengkap(page, nama_hotel=None):
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


def _klik_kartu(page, kartu):
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
            state="attached", timeout=4000)
    except Exception:
        time.sleep(0.4)
    return True


def _scroll_di_detail(page, langkah=1, jeda=0.15):
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


def _nama_dari_baris(baris, nama_hotel):
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


def _ekstrak_ota_all_options(page, nama_hotel=None):
    hasil = {}
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


def _klik_opsi_lainnya(page):
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


def extract_ota_prices(page, nama_hotel):
    hasil_gabungan = {}

    def _agregasi(pasangan):
        for p in pasangan or []:
            ota = str(p.get('sumber') or '').strip()
            h = int(p.get('harga') or 0)
            if not ota or h <= 0:
                continue
            hasil_gabungan[ota] = min(hasil_gabungan.get(ota, h), h)

    if not _tunggu_hasil_hotel(page):
        log.info("[DEBUG] hasil hotel tidak muncul; anggap detail terbuka.")
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
    _scroll_di_detail(page)
    _klik_opsi_lainnya(page)
    pasangan = _ekstrak_ota_all_options(page, nama_hotel)
    if not pasangan:
        pasangan = _parse_ota_dari_teks(_teks_panel_lengkap(page, nama_hotel))
    _agregasi(pasangan)
    log.info("gmaps harga %s -> %d sumber OTA", nama_hotel, len(hasil_gabungan))
    return [{"sumber": o, "harga": h} for o, h in hasil_gabungan.items()]


# ============================================================
# GOOGLE MAPS: ULASAN HOTEL
# ============================================================

def ambil_bintang(block):
    pola_pecahan = re.compile(
        r'([1-5])(?:\s*[,.]\d*)?\s*(?:/|dari|of|out\s*of)\s*5',
        re.IGNORECASE)
    pola_kata = re.compile(
        r'(\d)\s*(dari|out)?\s*\d?\s*(bintang|star)', re.IGNORECASE)

    def ekstrak_rating(teks):
        if not teks:
            return 0
        m = pola_pecahan.search(teks)
        if m:
            return int(m.group(1))
        m2 = pola_kata.search(teks)
        if m2:
            return int(m2.group(1))
        return 0

    selectors = [
        'span[role="img"][aria-label*="bintang"], span[role="img"][aria-label*="star"], span[role="img"][aria-label*="/5"]',
        '[role="img"][aria-label*="bintang"], [role="img"][aria-label*="star"], [role="img"][aria-label*="/5"]',
        'span.kvMYJc svg use, span.kvMYJc svg, span.kvMYJc [aria-label]',
    ]
    for sel in selectors:
        el = block.locator(sel)
        if el.count() == 0:
            continue
        for j in range(el.count()):
            aria = el.nth(j).get_attribute("aria-label") or ""
            rating = ekstrak_rating(aria)
            if 1 <= rating <= 5:
                return rating

    for sel_star in ('span.kvMYJc', 'span[class*="kvMYJc"]', '[role="img"]'):
        star_el = block.locator(sel_star)
        if star_el.count() > 0:
            try:
                teks = star_el.first.inner_text().strip()
            except Exception:
                teks = ""
            rating = ekstrak_rating(teks)
            if 1 <= rating <= 5:
                return rating

    warna_kuning = ("fbbc04", "rgb(251, 188, 4)", "rgb(251,188,4)",
                    "#fbbc04", "rgb(255, 200, 0)", "goldenrod")

    try:
        filled = block.evaluate("""
            (el, kuning) => {
                const svgs = el.querySelectorAll('span.kvMYJc svg, span[jsaction] svg');
                let filledCount = 0;
                let used = false;
                for (const svg of svgs) {
                    const paths = svg.querySelectorAll('path');
                    if (paths.length === 0) continue;
                    if (used) continue;
                    for (const p of paths) {
                        const fill = (p.getAttribute('fill') || '').toLowerCase().replace(/\\s/g, '');
                        if (kuning.some(k => fill.includes(k.toLowerCase().replace(/\\s/g, '')))) {
                            filledCount++;
                            used = true;
                            break;
                        }
                    }
                }
                return { total: svgs.length, filledCount };
            }
        """, list(warna_kuning))
        if filled and filled.get('total', 0) == 5 and 1 <= filled.get('filledCount', 0) <= 5:
            return filled['filledCount']
    except Exception:
        pass

    try:
        star_container = block.locator('span.kvMYJc, span[jsaction][role="img"]')
        if star_container.count() > 0:
            filled_count = 0
            svg_els = star_container.first.locator('svg')
            for k in range(svg_els.count()):
                paths = svg_els.nth(k).locator('path')
                for p in range(paths.count()):
                    fill = paths.nth(p).get_attribute('fill') or ''
                    if not fill or fill.lower() in ('none', 'inherit'):
                        continue
                    fl = fill.lower().replace(' ', '')
                    if any(w in fl for w in warna_kuning):
                        filled_count += 1
                        break
            if 1 <= filled_count <= 5:
                return filled_count
    except Exception:
        pass

    try:
        el = block.locator('[data-rating]')
        if el.count() > 0:
            rating = int(el.first.get_attribute('data-rating') or '0')
            if 1 <= rating <= 5:
                return rating
    except Exception:
        pass
    return 0


def ambil_waktu_relatif(block):
    star = block.locator(
        'span[role="img"][aria-label*="bintang"], span[role="img"][aria-label*="star"]')
    if star.count() > 0:
        sibling = star.first.locator('xpath=following-sibling::span')
        if sibling.count() > 0:
            t = sibling.first.inner_text().strip()
            if t:
                return t
    for sel in ('span.rsqaWe', 'span[class*="rsqaWe"]',
                'div[class*="status"], span[class*="status"]'):
        try:
            el = block.locator(sel)
            if el.count() > 0:
                t = el.first.inner_text().strip()
                if t:
                    return t
        except Exception:
            continue
    try:
        seluruh = block.inner_text()
        m = re.search(r'\d+\s*(hari|minggu|bulan|tahun)\s*(lalu|yang lalu)?', seluruh)
        if m:
            return m.group(0)
    except Exception:
        pass
    return ""


def scrape_hotel_reviews(page, nama_hotel, latitude, longitude,
                         max_reviews=200, alamat=None, kota=None):
    hasil_reviews = []
    search_query = build_search_query(nama_hotel, alamat, kota)
    nama_url = search_query.replace(" ", "+")
    url_absolut = f"https://www.google.com/maps/search/{nama_url}/@{latitude},{longitude},19z"

    try:
        page.goto(url_absolut, timeout=60000)
        time.sleep(4)
        btn_cookie = page.locator('button:has-text("Accept all"), button:has-text("Setuju")')
        if btn_cookie.count() > 0:
            btn_cookie.first.click()
            time.sleep(1)

        sudah_detail = page.locator(
            'button[role="tab"]:has-text("Ulasan"), button[role="tab"]:has-text("Reviews")').count() > 0

        if not sudah_detail:
            sudah_dicoba_urls = set()
            for _ in range(6):
                place_links = page.locator('div[role="feed"] a[href*="/maps/place/"]')
                if place_links.count() == 0:
                    break
                kandidat = []
                seen_urls = set()
                for i in range(place_links.count()):
                    link = place_links.nth(i)
                    try:
                        href = link.get_attribute("href") or ""
                        if not href or href in seen_urls or href in sudah_dicoba_urls:
                            continue
                        seen_urls.add(href)
                        parent = link.locator('xpath=ancestor::div[@role="article"]')
                        if parent.count() == 0:
                            parent = link.locator('xpath=ancestor::div[contains(@class,"Nv2PK")]')
                        if parent.count() == 0:
                            continue
                        parent = parent.first
                        teks_kartu = parent.inner_text()
                        teks_lower = teks_kartu.lower()
                        if ('iklan' in teks_lower or 'bersponsor' in teks_lower
                                or 'disponsori' in teks_lower or 'sponsored' in teks_lower
                                or parent.locator('[aria-label="Iklan"], [aria-label="Ad"], '
                                                  '[aria-label="Bersponsor"], [aria-label="Sponsored"], '
                                                  '.G7qWGe').count() > 0):
                            continue
                        nama_place = link.get_attribute("aria-label") or "Unknown"
                        skor = SequenceMatcher(
                            None, normalize_text(nama_place), normalize_text(nama_hotel)).ratio()
                        jumlah_review = extract_review_count(teks_kartu)
                        kandidat.append({
                            'index': i, 'href': href, 'link': link, 'parent': parent,
                            'nama': nama_place, 'skor': skor,
                            'punya_review': jumlah_review > 0,
                            'jumlah_review': jumlah_review,
                        })
                    except Exception:
                        continue
                if not kandidat:
                    break
                kandidat.sort(key=lambda x: (
                    x['skor'] < 0.55, not x['punya_review'], -x['jumlah_review'],
                    -x['skor'], x['index']))
                k = kandidat[0]
                try:
                    k['link'].click(timeout=6000)
                except Exception:
                    try:
                        k['parent'].click(timeout=6000)
                    except Exception:
                        sudah_dicoba_urls.add(k['href'])
                        page.goto(url_absolut, timeout=60000)
                        time.sleep(3)
                        continue
                try:
                    page.wait_for_load_state('domcontentloaded', timeout=8000)
                except Exception:
                    pass
                try:
                    page.wait_for_selector(
                        'button[role="tab"]:has-text("Ulasan"), '
                        'button[role="tab"]:has-text("Reviews"), '
                        'div.jftiEf, div[data-review-id]', timeout=8000)
                except Exception:
                    pass
                time.sleep(0.5)
                tab_cek = page.locator(
                    'button[role="tab"]:has-text("Ulasan"), '
                    'button[role="tab"]:has-text("Reviews")')
                section_cek = page.locator(
                    'div.jftiEf, div[data-review-id], '
                    'button:has-text("Semua ulasan")')
                if tab_cek.count() > 0 or section_cek.count() > 0:
                    break
                sudah_dicoba_urls.add(k['href'])
                page.goto(url_absolut, timeout=60000)
                time.sleep(3)

        tab_ulasan = page.locator(
            'button[role="tab"]:has-text("Ulasan"), button[role="tab"]:has-text("Reviews")')
        if tab_ulasan.count() > 0:
            tab_ulasan.first.click()
            time.sleep(3)

        btn_semua = page.locator(
            'button:has-text("Semua ulasan"), button:has-text("Lihat semua ulasan"), '
            'button:has-text("More reviews")')
        if btn_semua.count() > 0:
            btn_semua.first.click()
            time.sleep(3)

        btn_urutkan = page.locator(
            'button[data-value="Sort"], button[aria-label*="Urutkan"], '
            'button[aria-label*="Sort"], button:has-text("Paling relevan"), '
            'button:has-text("Most relevant")')
        if btn_urutkan.count() > 0:
            btn_urutkan.first.click()
            time.sleep(1.5)
            menu_terbaru = page.locator(
                'div[role="menuitemradio"][data-index="1"], '
                'div[role="menuitemradio"]:has-text("Terbaru")')
            if menu_terbaru.count() > 0:
                menu_terbaru.first.click()
                time.sleep(3.5)

        scroll_js = """
            let pool = document.querySelectorAll('div[role="dialog"] div.m6QErb');
            if (pool.length === 0) {
                pool = document.querySelectorAll('div.m6QErb');
            }
            let containers = Array.from(pool)
                .filter(c => c.scrollHeight > c.clientHeight)
                .sort((a, b) => (b.scrollHeight - b.clientHeight) - (a.scrollHeight - a.clientHeight));
            if (containers.length === 0 && pool.length > 0) {
                containers = [pool[pool.length - 1]];
            }
            containers.forEach(c => c.scrollBy(0, 1000));
        """
        for _ in range(2):
            page.evaluate(scroll_js)
            time.sleep(1.5)

        max_scroll = 150
        patience = 12
        prev_count = 0
        patience_counter = 0
        for _ in range(max_scroll):
            page.evaluate(scroll_js)
            time.sleep(4)
            btn_muat = page.locator(
                'button:has-text("Muat lainnya"), button:has-text("Load more"), '
                'button:has-text("Tampilkan lebih banyak")')
            if btn_muat.count() > 0:
                try:
                    btn_muat.first.click(timeout=2000)
                    time.sleep(2)
                except Exception:
                    pass
            curr_count = page.locator('div.jftiEf, div[data-review-id]').count()
            if curr_count == prev_count:
                patience_counter += 1
                if patience_counter >= patience:
                    break
            else:
                patience_counter = 0
            prev_count = curr_count

        btn_muat = page.locator(
            'button:has-text("Muat lainnya"), button:has-text("Load more"), '
            'button:has-text("Tampilkan lebih banyak")')
        if btn_muat.count() > 0:
            try:
                btn_muat.first.click(timeout=3000)
                time.sleep(3)
            except Exception:
                pass

        btn_selengkapnya = page.locator('button:has-text("Selengkapnya"), button:has-text("More")')
        if btn_selengkapnya.count() > 0:
            for i in range(btn_selengkapnya.count()):
                try:
                    btn_selengkapnya.nth(i).click(timeout=1000)
                except Exception:
                    pass
            time.sleep(1)

        review_blocks = page.locator('div[data-review-id]')
        if review_blocks.count() == 0:
            review_blocks = page.locator('div.jftiEf')
        total_ditemukan = review_blocks.count()

        for i in range(min(total_ditemukan, max_reviews or total_ditemukan)):
            block = review_blocks.nth(i)
            try:
                nama_el = block.locator('div.d4r55')
                if nama_el.count() == 0:
                    nama_el = block.locator('button[data-href*="/maps/contrib/"] div')
                nama = nama_el.first.inner_text().strip() if nama_el.count() > 0 else "Anonim"

                teks_el = block.locator('div[tabindex="-1"][id][lang]')
                if teks_el.count() == 0:
                    teks_el = block.locator('span.wiI7pd')
                teks = teks_el.first.inner_text().strip() if teks_el.count() > 0 else ""

                waktu_relatif = ambil_waktu_relatif(block)
                rating = ambil_bintang(block)
                tanggal_absolut = convert_relative_time(waktu_relatif)
                if tanggal_absolut is None or teks == "":
                    continue
                bulan = int(tanggal_absolut.split('-')[1])
                tahun = int(tanggal_absolut.split('-')[0])
                hasil_reviews.append({
                    'nama_reviewer': nama,
                    'rating_bintang': rating,
                    'isi_review': teks,
                    'waktu_relatif': waktu_relatif,
                    'bulan': bulan,
                    'tahun': tahun,
                })
            except Exception:
                continue
    except Exception as e:
        log.warning("gmaps review gagal utk %s: %s", nama_hotel, e)
    return hasil_reviews


# ============================================================
# ORKESTRATOR
# ============================================================

def _kosong(nilai) -> bool:
    """True bila nilai hilang (None / NaN / pd.NA) atau cuma spasi.

    Penting: hasil baca DB lewat cursor+psycopg2 menghasilkan NaN (float) untuk
    kolom kosong, bukan None. Tanpa cek ini `str(nan or '')` menjadi string
    "nan" dan hotel dengan nama kosong akan ikut ter-scrape.
    """
    if nilai is None or nilai is pd.NA:
        return True
    try:
        if pd.isna(nilai):
            return True
    except (TypeError, ValueError):
        pass
    return not str(nilai).strip()


def _angka(nilai) -> float | None:
    """Ubah nilai DB/konfigurasi menjadi float, atau None bila tidak valid."""
    if _kosong(nilai):
        return None
    try:
        hasil = float(nilai)
    except (TypeError, ValueError):
        return None
    return hasil if hasil == hasil else None  # buang NaN/inf


def _daftar_hotel_dari_db() -> list[dict]:
    """Baca daftar hotel dari gold.gmaps_hotel_master.

    Registry hotel dikelola di database, bukan di config. Kembalikan [] bila
    tabel belum ada / kosong / tidak bisa dibaca supaya pemanggil bisa jatuh
    ke daftar cadangan di config.
    """
    schema = get_db_config()["schema_gold"]
    kolom = "id_kompetitor, nama_hotel, alamat, kota, latitude, longitude"
    try:
        if not table_exists(schema, "gmaps_hotel_master"):
            log.warning(
                "gmaps: tabel %s.gmaps_hotel_master belum ada; pakai daftar_hotel dari config",
                schema,
            )
            return []
        df = fetch_df(
            f'SELECT {kolom} FROM "{schema}"."gmaps_hotel_master" ORDER BY id_kompetitor'
        )
    except Exception as exc:
        log.warning("gmaps: gagal baca %s.gmaps_hotel_master (%s); pakai config", schema, exc)
        return []
    if df is None or df.empty:
        log.warning("gmaps: %s.gmaps_hotel_master kosong; pakai daftar_hotel dari config", schema)
        return []

    out: list[dict] = []
    for _, baris in df.iterrows():
        nama = None if _kosong(baris["nama_hotel"]) else str(baris["nama_hotel"]).strip()
        id_kompetitor = (
            None if _kosong(baris["id_kompetitor"]) else str(baris["id_kompetitor"]).strip()
        )
        if not nama or not id_kompetitor:
            log.warning("gmaps: baris hotel tanpa id_kompetitor/nama_hotel dilewati")
            continue
        out.append({
            "id_kompetitor": id_kompetitor,
            "nama_hotel": nama,
            "alamat": None if _kosong(baris["alamat"]) else str(baris["alamat"]).strip(),
            "kota": None if _kosong(baris["kota"]) else str(baris["kota"]).strip(),
            "latitude": _angka(baris["latitude"]),
            "longitude": _angka(baris["longitude"]),
        })
    log.info("gmaps: %d hotel dibaca dari %s.gmaps_hotel_master", len(out), schema)
    return out


def _daftar_hotel(cfg: dict) -> list[dict]:
    """Daftar hotel: dari DB dulu, lalu daftar_hotel di config sebagai cadangan."""
    hotel_list = _daftar_hotel_dari_db()
    if hotel_list:
        return hotel_list

    items = cfg.get("daftar_hotel") or []
    out: list[dict] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        if _kosong(it.get("nama_hotel")):
            continue
        out.append(it)
    if out:
        log.warning("gmaps: %d hotel diambil dari daftar_hotel config (DB kosong)", len(out))
    return out


def _lead_time_dates(cfg: dict, today):
    """Skenario tanggal check-in dari config `lead_times` (mis. [1..7, 30, 60]).

    Fallback ke rentang `lead_time_min`..`lead_time_max` bila `lead_times`
    belum diisi di config.
    """
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


def extract(period: str | None = None, dry_run: bool = False) -> dict[str, pd.DataFrame]:
    """EXTRACT (bronze): harga OTA (Google Travel) + ulasan (Google Maps) per hotel.

    Daftar hotel dibaca dari gold.gmaps_hotel_master (fallback ke
    daftar_hotel di config bila tabel kosong/tidak terbaca).

    Bronze datasets: harga, review. `hotel_master` tidak ditulis lagi karena
    registry hotel kini menjadi milik database.
    Harga ditulis per tahun (tahun scrape_date, mode append) agar run harian
    tidak menimpa harga sebelumnya; review cukup satu label.

    Filter dataset dari CLI (mis. `--harga`) membatasi apa yang di-scrape:
    bila review tidak diminta, pemanggilan browser untuk ulasan dilewati.
    """
    cfg = get_source_config(SOURCE)
    label = _label(cfg, period)
    headless = bool(cfg.get("headless", True))
    blokir_gambar = bool(cfg.get("blokir_gambar", False))
    max_reviews = int(cfg.get("max_reviews_per_hotel", 200))
    today = datetime.now().date()

    mau_harga = wants("harga")
    mau_review = wants("review")
    log.info("gmaps: dataset yang diambil = %s", dataset_terpilih())
    if not (mau_harga or mau_review):
        log.warning("gmaps: tidak ada dataset gmaps yang diminta; dilewati")
        return {}

    # mode uji: batasi jumlah hotel, 1 skenario tanggal, dan ulasan secukupnya
    limit = get_row_limit()
    if limit:
        max_reviews = min(max_reviews, limit)

    hotel_list = _daftar_hotel(cfg)
    if not hotel_list:
        log.warning(
            "gmaps: tidak ada daftar hotel (DB gold.gmaps_hotel_master kosong & "
            "config/sources.yaml tidak punya daftar_hotel)"
        )
        return {}

    skenario = _lead_time_dates(cfg, today) if mau_harga else []
    if limit and skenario:
        skenario = skenario[:1]
    rentang = (
        f"{skenario[0]['checkin']}..{skenario[-1]['checkin']}" if skenario else "tidak ada skenario"
    )
    log.info("gmaps: %d hotel, skenario %s, headless=%s, max_reviews=%s%s",
             len(hotel_list), rentang,
             headless, max_reviews, " (mode uji)" if limit else "")

    rows_harga: list[dict] = []
    rows_review: list[dict] = []
    hotel_terpakai = 0

    with sync_playwright() as p:
        browser, context, page = konfigurasi_browser(p, headless=headless, blokir_gambar=blokir_gambar)
        try:
            for idx, h in enumerate(hotel_list, start=1):
                if limit and hotel_terpakai >= limit:
                    log.info("gmaps: cap %d hotel tercapai; sisa hotel dilewati", limit)
                    break
                id_komp = str(h.get("id_kompetitor") or f"hotel-{idx}").strip()
                nama = str(h.get("nama_hotel") or "").strip()
                alamat = h.get("alamat")
                kota = h.get("kota")
                log.info("gmaps [%d/%d] %s", idx, len(hotel_list), nama)

                lat_f, lon_f = _angka(h.get("latitude")), _angka(h.get("longitude"))
                if (mau_review and (lat_f is None or lon_f is None)):
                    log.warning("gmaps skip %s (koordinat kosong/tidak valid)", nama)
                    continue

                hotel_terpakai += 1

                search_query = build_search_query(nama, None)

                for sk in skenario:
                    checkin, checkout = sk["checkin"], sk["checkout"]
                    try:
                        ok = open_hotel_search(page, search_query, checkin, checkout)
                        if not ok:
                            continue
                        harga_list = extract_ota_prices(page, nama)
                        for item in harga_list or []:
                            harga = int(item.get("harga") or 0)
                            sumber = str(item.get("sumber") or "").strip()
                            if harga > 0 and sumber:
                                rows_harga.append({
                                    "id_kompetitor": id_komp,
                                    "nama_hotel": nama,
                                    "checkin_date": checkin,
                                    "lead_time": sk["lead_time"],
                                    "sumber": sumber,
                                    "harga": harga,
                                    "scrape_date": today,
                                })
                                if limit and len(rows_harga) >= limit:
                                    break
                        if limit and len(rows_harga) >= limit:
                            break
                    except Exception as e:
                        log.warning("gmaps harga H-%s %s gagal: %s", sk["lead_time"], nama, e)
                        continue

                if mau_review:
                    try:
                        rev = scrape_hotel_reviews(
                            page, nama, lat_f, lon_f,
                            max_reviews=max_reviews, alamat=alamat, kota=kota,
                        )
                        for r in rev or []:
                            rows_review.append({
                                "id_kompetitor": id_komp,
                                "nama_hotel": nama,
                                "nama_reviewer": r["nama_reviewer"],
                                "rating_bintang": r["rating_bintang"],
                                "isi_review": r["isi_review"],
                                "bulan": r["bulan"],
                                "tahun": r["tahun"],
                            })
                    except Exception as e:
                        log.warning("gmaps review %s gagal: %s", nama, e)
        finally:
            browser.close()

    result: dict[str, pd.DataFrame] = {}
    kolom_dataset = {
        "harga": ["id_kompetitor", "nama_hotel", "checkin_date", "lead_time",
                  "sumber", "harga", "scrape_date"],
        "review": ["id_kompetitor", "nama_hotel", "nama_reviewer",
                   "rating_bintang", "isi_review", "bulan", "tahun"],
    }
    ds = {
        "harga": pd.DataFrame(rows_harga),
        "review": pd.DataFrame(rows_review),
    }
    for dataset, df in ds.items():
        if not wants(dataset):
            continue
        if df.empty:
            log.warning("gmaps %s kosong; dilewati", dataset)
            continue

        df = df[kolom_dataset[dataset]]
        if dry_run:
            log.info("[dry-run] gmaps bronze %s: %d baris (tidak ditulis)", dataset, len(df))
        elif dataset == "harga":
            # Harga per tanggal run selalu unik (scrape_date ikut jadi kunci),
            # jadi ditulis per tahun dengan append: run harian berikutnya tidak
            # boleh menimpa harga hari-hari sebelumnya.
            paths = write_parquet_by_year(
                df,
                "bronze",
                SOURCE,
                dataset,
                year_col="scrape_date",
                mode="append",
                dedup_cols=["id_kompetitor", "checkin_date", "sumber", "scrape_date"],
            )
            log.info("gmaps bronze harga ditulis: %d file per tahun (%d baris baru)",
                     len(paths), len(df))
        else:
            path = write_parquet(df, "bronze", SOURCE, dataset, label)
            log.info("gmaps bronze %s ditulis: %s (%d baris)", dataset, path, len(df))
        result[dataset] = df
    return result