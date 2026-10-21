from __future__ import annotations

import html
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import Any, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

from ..shared.config import get_source_config
from ..shared.row_limit import truncate_df
from ..shared.storage import write_parquet_by_year
from ..shared.periods import resolve_daily_period, resolve_historical_period
from ..shared.run_options import get_historis

log = logging.getLogger(__name__)

SOURCE = "event"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7",
}

NAMA_BULAN = {
    "januari": 1, "februari": 2, "maret": 3, "april": 4, "mei": 5,
    "juni": 6, "juli": 7, "agustus": 8, "september": 9, "oktober": 10,
    "november": 11, "desember": 12,
    "january": 1, "february": 2, "march": 3, "may": 5,
    "june": 6, "july": 7, "august": 8, "october": 10, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6, "jul": 7,
    "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}

KOLOM_BRONZE = [
    "sumber", "judul", "tanggal_mulai", "tanggal_akhir", "lokasi_venue",
    "kategori", "url_detail", "url_gambar", "deskripsi", "tanggal_unggah",
    "latitude", "longitude",
]


# ============================================================
# UTILITAS FETCH & TANGGAL (murni, tanpa DB)
# ============================================================

def fetch(url: str, retries: int = 4, backoff: float = 2.0, params: dict | None = None) -> str:
    nafas = retries
    wait = backoff
    while True:
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=30)
            r.raise_for_status()
            r.encoding = r.apparent_encoding or "utf-8"
            return r.text
        except Exception as e:
            nafas -= 1
            if nafas <= 0:
                raise
            log.debug("retry %s - %s", url, e)
            time.sleep(wait)
            wait *= 2


def parse_bulan(nama: str | None) -> int | None:
    if not nama:
        return None
    return NAMA_BULAN.get(nama.strip().lower())


def parse_tanggal_tahun(bulan, tgl_1, tgl_2=None, tahun=None) -> tuple[date | None, date | None]:
    m = parse_bulan(bulan)
    if not m:
        return None, None
    th = int(tahun) if tahun else date.today().year
    try:
        awal = date(th, m, int(tgl_1))
    except ValueError:
        return None, None
    if tgl_2:
        try:
            akhir = date(th, m, int(tgl_2))
        except ValueError:
            akhir = awal
    else:
        akhir = awal
    return awal, akhir


def parse_dmy_indonesia(teks: str | None) -> date | None:
    m = re.search(r"[A-Za-z]+,\s*(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", teks or "")
    if not m:
        return None
    awal, _ = parse_tanggal_tahun(m.group(2), m.group(1), tahun=int(m.group(3)))
    return awal


def parse_list_ice(teks: str | None) -> date | None:
    m = re.search(r"([A-Za-z]{3,9})\s+(\d{1,2}),\s*(\d{4})", teks or "")
    if not m:
        return None
    awal, _ = parse_tanggal_tahun(m.group(1), m.group(2), tahun=int(m.group(3)))
    return awal


def parse_detail_ice(teks: str | None) -> tuple[date | None, date | None]:
    teks = teks or ""
    m = re.search(r"(\d{4})", teks)
    if not m:
        return None, None
    tahun = int(m.group(1))
    depan = teks[: m.start()]
    pasangan = re.findall(r"([A-Za-z]{3,9})\s+(\d{1,2})", depan)
    if not pasangan:
        return None, None
    awal, _ = parse_tanggal_tahun(pasangan[0][0], pasangan[0][1], tahun=tahun)
    if len(pasangan) > 1:
        akhir, _ = parse_tanggal_tahun(pasangan[-1][0], pasangan[-1][1], tahun=tahun)
    else:
        ekor = re.search(r"-\s*(\d{1,2})\s*,?\s*$", depan)
        if ekor:
            akhir, _ = parse_tanggal_tahun(pasangan[0][0], ekor.group(1), tahun=tahun)
        else:
            akhir = awal
    return awal, akhir


def parse_detail_nice(teks: str | None) -> tuple[date | None, date | None]:
    m = re.search(r"[A-Za-z]+day,\s*(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", teks or "")
    if not m:
        return None, None
    return parse_tanggal_tahun(m.group(2), m.group(1), tahun=int(m.group(3)))


def parse_badge_nice(teks_hari: str | None, bulan: str | None) -> tuple[date | None, date | None]:
    hari = [int(x) for x in re.findall(r"\d+", teks_hari or "")]
    if not hari:
        return None, None
    t1 = hari[0]
    t2 = hari[1] if len(hari) > 1 else t1
    return parse_tanggal_tahun(bulan, t1, t2)


def parse_list_jcc(teks: str | None) -> tuple[date | None, date | None]:
    m = re.search(r"(\d{1,2})\s*(?:-|–|—)?\s*(\d{1,2})?\s+([A-Za-z]+)\s+(\d{4})", teks or "")
    if not m:
        return None, None
    return parse_tanggal_tahun(m.group(3), m.group(1), m.group(2), int(m.group(4)))


def parse_list_eventbrite(teks: str | None) -> tuple[date | None, date | None]:
    m = re.search(
        r"([A-Za-z]{3,9})\s+(\d{1,2}),\s*(?:(\d{4}),?\s*)?\d{1,2}:\d{2}\s*[AP]M",
        teks or "",
    )
    if not m:
        return None, None
    tahun = int(m.group(3)) if m.group(3) else date.today().year
    return parse_tanggal_tahun(m.group(1), m.group(2), tahun=tahun)


RANGE_ID_RE = re.compile(
    r"(\d{1,2})\s*(?:-|–|—|s\.d\.|sampai)\s*(\d{1,2})\s+([A-Za-z]+)(?:\s+(\d{4}))?"
)
SINGLE_ID_RE = re.compile(r"(?<!\d)(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})")


def parse_body_jakarta(teks: str | None):
    teks = (teks or "").strip()
    m = RANGE_ID_RE.search(teks)
    if m:
        awal, akhir = parse_tanggal_tahun(
            m.group(3), m.group(1), m.group(2),
            int(m.group(4)) if m.group(4) else None,
        )
        if awal:
            if akhir and akhir < awal:
                akhir = awal
            venue = teks[: m.start()].strip().rstrip(",").strip()
            desk = teks[m.end():].strip()
            return awal, akhir, (venue or None), (desk or None)
    m = SINGLE_ID_RE.search(teks)
    if m:
        awal, _ = parse_tanggal_tahun(m.group(2), m.group(1), tahun=int(m.group(3)))
        if awal:
            venue = teks[: m.start()].strip().rstrip(",").strip()
            desk = teks[m.end():].strip()
            return awal, awal, (venue or None), (desk or None)
    return None, None, None, teks


# ============================================================
# HELPER ICS (JIExpo)
# ============================================================

def ics_field(blok: str, nama: str) -> str:
    nilai: list[str] = []
    ditemukan = False
    for ln in blok.splitlines():
        if ln.startswith(nama + ":"):
            nilai.append(ln[len(nama) + 1:])
            ditemukan = True
        elif ditemukan and (ln.startswith(" ") or ln.startswith("\t")):
            nilai[-1] += ln.strip()
        elif ditemukan:
            break
    return "".join(nilai)


def ics_tanggal(t_start: str, t_end: str):
    def ambil(nilai):
        m = re.search(r"(\d{4})(\d{2})(\d{2})", nilai or "")
        if not m:
            return None, False
        th, bln, tgl = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if th < 2000 or th > 2100 or not (1 <= bln <= 12) or not (1 <= tgl <= 31):
            return None, False
        return date(th, bln, tgl), "T" in (nilai or "")

    awal, waktu_awal = ambil(t_start)
    akhir, waktu_akhir = ambil(t_end)
    if awal is None:
        return None, None, None
    if akhir is None:
        akhir = awal
    if not waktu_awal and not waktu_akhir and akhir > awal:
        akhir = date.fromordinal(akhir.toordinal() - 1)
    if akhir < awal:
        akhir = awal
    return awal, akhir, (waktu_awal or waktu_akhir)


def ics_unescape(teks: str | None) -> str | None:
    if not teks:
        return teks
    teks = teks.replace(r"\,", ",").replace(r"\;", ";").replace(r"\n", " ")
    return html.unescape(teks)


# ============================================================
# EKSTRAKTOR PER SITUS (murni parsing, tanpa geocode)
# ============================================================

def ekstrak_jiexpo() -> list[dict[str, Any]]:
    url_dir = "https://exhibition.jiexpo.com/event-directory/"
    html_dir = fetch(url_dir, retries=3)
    m = re.search(r"export-events/all/\?nonce=([0-9a-f]+)", html_dir)
    if not m:
        raise RuntimeError("Nonce export-events tidak ditemukan di halaman JIExpo.")

    url_ics = f"https://exhibition.jiexpo.com/export-events/all/?nonce={m.group(1)}"
    ics = None
    for _ in range(4):
        ics = fetch(url_ics, retries=2)
        if ics and "BEGIN:VEVENT" in ics:
            break
        time.sleep(2)
    if not ics or "BEGIN:VEVENT" not in ics:
        raise RuntimeError("Ekspor ICS JIExpo tidak valid setelah beberapa kali percobaan.")

    hasil: list[dict[str, Any]] = []
    for blok in re.findall(r"BEGIN:VEVENT(.*?)END:VEVENT", ics, re.S):
        try:
            judul = ics_field(blok, "SUMMARY")
            lokasi = ics_field(blok, "LOCATION")
            desk = ics_field(blok, "DESCRIPTION")
            url_detail = ics_field(blok, "URL")
            dt_start = ics_field(blok, "DTSTART")
            dt_end = ics_field(blok, "DTEND")
            tgl_awal, tgl_akhir, _ = ics_tanggal(dt_start, dt_end)
            if not judul:
                continue
            hasil.append({
                "sumber": "jiexpo",
                "judul": ics_unescape(judul),
                "tanggal_mulai": tgl_awal,
                "tanggal_akhir": tgl_akhir,
                "lokasi_venue": ics_unescape(lokasi) or None,
                "kategori": None,
                "url_detail": (ics_unescape(url_detail) or None),
                "url_gambar": None,
                "deskripsi": ics_unescape(desk) or None,
                "tanggal_unggah": None,
            })
        except Exception as e:
            log.debug("jiexpo lewati 1 event: %s", e)
    log.info("jiexpo: %d event", len(hasil))
    return hasil


def _blok_ikon(soup, kata_kunci):
    img = soup.find("img", src=lambda s: s and kata_kunci in s)
    if not img:
        return None
    induk = img.find_parent("div")
    if not induk:
        return None
    konten = induk.find_next_sibling("div")
    return konten if konten is not None else induk


def _teks_blok(konten) -> str | None:
    if konten is None:
        return None
    return " ".join(konten.get_text(" ", strip=True).split())


def _detail_ice(url_detail: str):
    try:
        html = fetch(url_detail, retries=2)
        soup = BeautifulSoup(html, "lxml")
        blok_tgl = _blok_ikon(soup, "event_detail/calendar-icon.png")
        tgl_awal, tgl_akhir = parse_detail_ice(_teks_blok(blok_tgl))
        blok_lok = _blok_ikon(soup, "event_detail/location-icon.png")
        lokasi = _teks_blok(blok_lok)
        section_desc = soup.select_one("section.event-desc .col-12")
        desk = None
        if section_desc:
            pp = section_desc.find_all("p")
            if pp:
                desk = " ".join(" ".join(p.get_text(" ", strip=True).split()) for p in pp)
        return {
            "tanggal_mulai": tgl_awal,
            "tanggal_akhir": tgl_akhir,
            "lokasi_venue": lokasi,
            "url_detail": url_detail,
            "deskripsi": desk,
        }
    except Exception as e:
        log.debug("ice detail gagal %s - %s", url_detail, e)
        return None


def ekstrak_ice() -> list[dict[str, Any]]:
    hasil: list[dict[str, Any]] = []
    terlihat: set[str] = set()
    url_dasar = "https://ice-indonesia.com/en/event/event_list?events=all&sort=year&category=all"
    batas_halaman = 80
    halaman = 1

    while halaman <= batas_halaman:
        url = url_dasar if halaman == 1 else f"{url_dasar}&page_events={halaman}"
        html = fetch(url, retries=3)
        soup = BeautifulSoup(html, "lxml")
        if halaman == 1:
            angka_pages = [int(m) for m in re.findall(r"page_events=(\d+)", str(soup))]
            if angka_pages:
                batas_halaman = max(angka_pages)

        kartu = soup.select("div.col-md-6.col-lg-4.events")
        if not kartu:
            break
        baru = 0
        for k in kartu:
            try:
                a = k.select_one("a.view-more-button") or k.select_one('a[href*="/event/event_list/"]')
                url_detail = a["href"] if a and a.get("href") else None
                judul_el = k.select_one(".event-title h2")
                judul = " ".join(judul_el.get_text(" ", strip=True).split()) if judul_el else None
                if not judul or not url_detail:
                    continue
                if url_detail in terlihat:
                    continue
                terlihat.add(url_detail)
                baru += 1

                tgl_div = k.select_one("div.col-12.date-time-event div.col-12.mt-2")
                tgl_list = _teks_blok(tgl_div)
                tgl_awal = parse_list_ice(tgl_list)
                kat_div = k.select_one("div.col-12.date-time-event div.col-6")
                kategori = _teks_blok(kat_div)
                tipe_div = k.select_one(".event-type")
                tipe = _teks_blok(tipe_div)
                img = k.select_one("img.image")
                url_gambar = img.get("src") if img else None

                detail = _detail_ice(url_detail)
                rec = detail or {
                    "tanggal_mulai": tgl_awal,
                    "tanggal_akhir": tgl_awal,
                    "lokasi_venue": None,
                }
                rec.update({
                    "sumber": "ice",
                    "judul": judul,
                    "kategori": kategori or tipe or None,
                    "url_detail": url_detail,
                    "url_gambar": url_gambar,
                    "tanggal_unggah": None,
                })
                hasil.append(rec)
            except Exception as e:
                log.debug("ice lewati 1 kartu: %s", e)
        if baru == 0:
            break
        halaman += 1
    log.info("ice: %d event", len(hasil))
    return hasil


def _detail_jcc(url_detail: str) -> str | None:
    try:
        html = fetch(url_detail, retries=2)
        soup = BeautifulSoup(html, "lxml")
        pp = [p for p in soup.select("p") if len(p.get_text(" ", strip=True)) > 40]
        if not pp:
            return None
        return " ".join(" ".join(p.get_text(" ", strip=True).split()) for p in pp)
    except Exception as e:
        log.debug("jcc detail gagal %s - %s", url_detail, e)
        return None


def ekstrak_jcc() -> list[dict[str, Any]]:
    sesi = requests.Session()
    sesi.headers.update(HEADERS)

    r0 = sesi.get("https://www.jcc.co.id/schedule-of-events", timeout=30)
    r0.raise_for_status()
    r0.encoding = r0.apparent_encoding or "utf-8"
    soup0 = BeautifulSoup(r0.text, "lxml")
    form = soup0.select_one("form#form_filter_category")
    if not form:
        raise RuntimeError("JCC: form#form_filter_category tidak ditemukan.")
    data = {
        inp["name"]: inp.get("value", "")
        for inp in form.select('input[type="hidden"]')
        if inp.get("name")
    }
    data["tahun"] = "all"
    data["bulan"] = "all"
    data["tags"] = ""

    r1 = sesi.post("https://www.jcc.co.id/events/schedule_of_events_searching/", data=data, timeout=30)
    r1.raise_for_status()
    r1.encoding = r1.apparent_encoding or "utf-8"
    soup = BeautifulSoup(r1.text, "lxml")
    kartu = soup.select("div.list-p-horizontal-wimg")
    if not kartu:
        raise RuntimeError("JCC: tidak ada kartu event pada respons searching.")

    hasil: list[dict[str, Any]] = []
    for k in kartu:
        try:
            judul_el = k.select_one(".list-p-title h4")
            judul = " ".join(judul_el.get_text(" ", strip=True).split()) if judul_el else None
            if not judul:
                continue
            time_el = k.select_one(".list-p-date time")
            tgl_awal, tgl_akhir = parse_list_jcc(time_el.get_text(" ", strip=True) if time_el else None)
            kat_el = k.select_one(".list-p-date a")
            kategori = " ".join(kat_el.get_text(" ", strip=True).split()) if kat_el else None
            peny_el = k.select_one(".list-p-content p")
            penyelenggara = " ".join(peny_el.get_text(" ", strip=True).split()) if peny_el else None
            img = k.select_one(".list-p-thumb img")
            url_gambar = img.get("src") if img else None
            more = k.select_one('a[href*="/events/soe_detail/"]')
            url_detail = more.get("href") if more else None
            deskripsi = _detail_jcc(url_detail) if url_detail else None

            hasil.append({
                "sumber": "jcc",
                "judul": judul,
                "tanggal_mulai": tgl_awal,
                "tanggal_akhir": tgl_akhir,
                "lokasi_venue": "Jakarta Convention Center",
                "kategori": kategori,
                "url_detail": url_detail,
                "url_gambar": url_gambar,
                "deskripsi": (
                    f"Penyelenggara: {penyelenggara}. {deskripsi}" if penyelenggara else deskripsi
                ),
                "tanggal_unggah": None,
            })
        except Exception as e:
            log.debug("jcc lewati 1 kartu: %s", e)
    log.info("jcc: %d event", len(hasil))
    return hasil


def ekstrak_eventbrite() -> list[dict[str, Any]]:
    hasil: list[dict[str, Any]] = []
    terlihat: set[str] = set()
    url_dasar = "https://www.eventbrite.com/d/indonesia--jakarta-pusat/all-events/"
    for halaman in range(1, 11):
        html = fetch(url_dasar, retries=3, params={"page": halaman})
        soup = BeautifulSoup(html, "lxml")
        kartu = soup.select('div[data-testid="search-event"]')
        if not kartu:
            break
        baru = 0
        for k in kartu:
            try:
                a = k.select_one("a.event-card-link[data-event-id]")
                if not a:
                    continue
                event_id = a.get("data-event-id")
                if not event_id or event_id in terlihat:
                    continue
                terlihat.add(event_id)
                baru += 1

                details = k.select_one("section.event-card-details")
                judul_el = details.select_one("h3") if details else None
                judul = " ".join(judul_el.get_text(" ", strip=True).split()) if judul_el else None
                if not judul:
                    continue

                tgl_awal = tgl_akhir = None
                if details:
                    tgl_p = details.select_one('p[class*="Typography_body-md-bold"]')
                    tgl_awal, tgl_akhir = parse_list_eventbrite(
                        tgl_p.get_text(" ", strip=True) if tgl_p else None
                    )
                lokasi = None
                if details:
                    loc_p = details.select_one('p[class*="event-card__clamp-line--one"]')
                    lokasi = " ".join(loc_p.get_text(" ", strip=True).split()) if loc_p else None
                if not lokasi:
                    lokasi = a.get("data-event-location")
                kategori = a.get("data-event-category")
                href = a.get("href")
                url_detail = href.split("?")[0] if href else None
                img = k.select_one("a.event-card-link img.event-card-image")
                url_gambar = img.get("src") if img else None

                hasil.append({
                    "sumber": "eventbrite",
                    "judul": judul,
                    "tanggal_mulai": tgl_awal,
                    "tanggal_akhir": tgl_akhir,
                    "lokasi_venue": lokasi,
                    "kategori": kategori,
                    "url_detail": url_detail,
                    "url_gambar": url_gambar,
                    "deskripsi": None,
                    "tanggal_unggah": None,
                })
            except Exception as e:
                log.debug("eventbrite lewati 1 kartu: %s", e)
        if baru == 0:
            break
    log.info("eventbrite: %d event", len(hasil))
    return hasil


def _detail_nice(url_detail: str):
    try:
        html = fetch(url_detail, retries=2)
        soup = BeautifulSoup(html, "lxml")
        scope = soup.select_one('div[style*="box-shadow"]') or soup

        judul_el = scope.select_one("p.font-erode")
        judul = " ".join(judul_el.get_text(" ", strip=True).split()) if judul_el else None

        tgl_awal = tgl_akhir = None
        ikon_tgl = scope.select_one("i.fa-calendar")
        if ikon_tgl and ikon_tgl.find_parent("div"):
            p_tgl = ikon_tgl.find_parent("div").find("p")
            if p_tgl:
                tgl_awal, tgl_akhir = parse_detail_nice(p_tgl.get_text(strip=True))

        hall = None
        ikon_lok = scope.select_one("i.fa-location-dot")
        if ikon_lok and ikon_lok.find_parent("div"):
            p_lok = ikon_lok.find_parent("div").find("p")
            hall = _teks_blok(p_lok)

        desk = None
        blok_desc = scope.select_one("div.space-y-4 div")
        if blok_desc:
            desk = blok_desc.get_text(" ", strip=True)

        return {
            "judul": judul,
            "tanggal_mulai": tgl_awal,
            "tanggal_akhir": tgl_akhir,
            "lokasi_venue": hall,
            "url_detail": url_detail,
            "deskripsi": desk,
        }
    except Exception as e:
        log.debug("nice detail gagal %s - %s", url_detail, e)
        return None


def ekstrak_nice() -> list[dict[str, Any]]:
    hasil: list[dict[str, Any]] = []
    terlihat: set[str] = set()
    for halaman in range(1, 16):
        url = "https://nice-nusantaraconvex.com/events"
        if halaman > 1:
            url = f"https://nice-nusantaraconvex.com/events?page={halaman}"
        html = fetch(url, retries=3)
        soup = BeautifulSoup(html, "lxml")
        kartu = [
            a for a in soup.select('a[href^="https://nice-nusantaraconvex.com/events/"]')
            if re.search(r"/events/[a-z0-9%(}\-]+$", a.get("href", ""))
        ]
        if not kartu:
            break
        baru = 0
        for k in kartu:
            try:
                url_detail = k["href"]
                if url_detail in terlihat:
                    continue
                terlihat.add(url_detail)
                baru += 1

                judul_el = k.select_one("p.line-clamp-2")
                judul = " ".join(judul_el.get_text(" ", strip=True).split()) if judul_el else None
                if not judul:
                    continue

                ikon = k.select_one("i.fa-location-dot")
                hall = None
                if ikon and ikon.find_parent("div"):
                    p_hall = ikon.find_parent("div").find("p")
                    hall = _teks_blok(p_hall)

                badge = k.select_one('div[style*="108px"]')
                tgl_awal = tgl_akhir = None
                if badge:
                    pp = badge.find_all("p")
                    if len(pp) >= 2:
                        tgl_awal, tgl_akhir = parse_badge_nice(
                            pp[0].get_text(strip=True), pp[1].get_text(strip=True)
                        )

                img = k.select_one("img")
                url_gambar = img.get("src") if img else None
                detail = _detail_nice(url_detail)
                if detail:
                    rec = detail
                    if not rec.get("tanggal_mulai"):
                        rec["tanggal_mulai"] = tgl_awal
                        rec["tanggal_akhir"] = tgl_akhir
                else:
                    rec = {
                        "tanggal_mulai": tgl_awal,
                        "tanggal_akhir": tgl_akhir,
                        "lokasi_venue": hall,
                    }
                rec.update({
                    "sumber": "nice",
                    "judul": judul,
                    "kategori": None,
                    "url_detail": url_detail,
                    "url_gambar": url_gambar,
                    "tanggal_unggah": None,
                })
                hasil.append(rec)
            except Exception as e:
                log.debug("nice lewati 1 kartu: %s", e)
        if baru == 0:
            break
    log.info("nice: %d event", len(hasil))
    return hasil


def ekstrak_jakarta() -> list[dict[str, Any]]:
    hasil: list[dict[str, Any]] = []
    for halaman in range(1, 16):
        params = {"q": "Produk Hukum", "section": "events", "page": halaman}
        html = fetch("https://www.jakarta.go.id/index.php/search", retries=5, backoff=2.0, params=params)
        soup = BeautifulSoup(html, "lxml")
        wadah = soup.select_one("div.search-list")
        if not wadah:
            break
        item_div = wadah.select("div.col-md-10")
        if not item_div:
            break

        for div in item_div:
            try:
                a = div.select_one("h4 a")
                if not a:
                    continue
                judul = " ".join(a.get_text(" ", strip=True).split())
                url_detail = a.get("href")
                span = div.select_one("span")
                tanggal_unggah = parse_dmy_indonesia(span.get_text(" ", strip=True) if span else None)
                p = div.select_one("p.my-2")
                teks_body = p.get_text(" ", strip=True) if p else ""
                tgl_awal, tgl_akhir, lokasi, desk = parse_body_jakarta(teks_body)

                img_src = None
                induk = div.find_previous_sibling("div")
                if induk:
                    img = induk.select_one("img")
                    img_src = img.get("src") if img else None

                hasil.append({
                    "sumber": "jakarta.go.id",
                    "judul": judul,
                    "tanggal_mulai": tgl_awal,
                    "tanggal_akhir": tgl_akhir,
                    "lokasi_venue": lokasi,
                    "kategori": None,
                    "url_detail": url_detail,
                    "url_gambar": img_src,
                    "deskripsi": desk or (teks_body or None),
                    "tanggal_unggah": tanggal_unggah,
                })
            except Exception as e:
                log.debug("jakarta lewati 1 item: %s", e)
    log.info("jakarta.go.id: %d event", len(hasil))
    return hasil


PEKERJA = {
    "jiexpo": ekstrak_jiexpo,
    "ice": ekstrak_ice,
    "nice": ekstrak_nice,
    "jcc": ekstrak_jcc,
    "eventbrite": ekstrak_eventbrite,
    "jakarta.go.id": ekstrak_jakarta,
}


def _parse_event_period(period: str | None) -> tuple[date | None, date | None]:
    """Parse period string for event scraper.
    
    Returns:
        (start_date, end_date) tuple for filtering events.
        - None, None = no filtering (historical mode)
        - target_date, target_date = single day (daily mode YYYY-MM-DD)
        - start_date, end_date = range (if extended in future)
    """
    from datetime import datetime
    if period is None:
        return None, None
    
    p = period.strip().lower()
    if p in ("", "latest", "terbaru"):
        return None, None
    
    # Handle YYYY-MM-DD (single day from daily mode)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", p):
        target_date = datetime.strptime(p, "%Y-%m-%d").date()
        return target_date, target_date
    
    # Handle YYYY-YYYY (year range) - future extension
    if re.fullmatch(r"\d{4}-\d{4}", p):
        a, b = map(int, p.split("-"))
        return date(a, 1, 1), date(b, 12, 31)
    
    # Handle YYYY (single year) - future extension
    if re.fullmatch(r"\d{4}", p):
        y = int(p)
        return date(y, 1, 1), date(y, 12, 31)
    
    return None, None


def extract(period: str | None = None, dry_run: bool = False, historis: bool = False) -> dict[str, pd.DataFrame]:
    """EXTRACT (bronze): event dari 6 situs venue (JIExpo, ICE, NICE, JCC, Eventbrite, Jakarta).

    Bronze ditulis per tahun (grain: year) berdasarkan tanggal_mulai event,
    mode append: listing situs hanya memuat event mendatang, jadi event yang
    sudah lewat harus tetap tersimpan.
    
    Mode Harian (default): ambil SEMUA event yang terdeteksi hari ini (upcoming/past),
    tidak filter by tanggal pelaksanaan. Dedup otomatis via append + dedup_cols.
    Mode Historis (--historis): semua event dari start_year s.d. hari ini.
    """
    cfg = get_source_config(SOURCE)
    
    # Determine period based on mode
    if historis or get_historis():
        period = resolve_historical_period(SOURCE)
        log.info("event: MODE HISTORIS aktif, periode=%s", period)
    else:
        period = resolve_daily_period(SOURCE)
        log.info("event: MODE HARIAN aktif, periode=%s", period)
    
    # Parse period for potential date filtering
    filter_start, filter_end = _parse_event_period(period)

    daftar_event: list[dict[str, Any]] = []
    workers = int(cfg.get("event_workers", 6))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(fn): nama for nama, fn in PEKERJA.items()}
        for fut in as_completed(futures):
            nama = futures[fut]
            try:
                daftar_event.extend(fut.result())
            except Exception as e:
                log.error("event sumber %s gagal total: %s", nama, e)

    log.info("event: total terkumpul %d dari 6 situs", len(daftar_event))
    if not daftar_event:
        log.warning("event: tidak ada event berhasil di-scrape")
        return {}

    df = pd.DataFrame(daftar_event, columns=KOLOM_BRONZE)
    
    # Daily mode: capture ALL events visible today (upcoming/past).
    # Do NOT filter by event date (tanggal_mulai) - sources return upcoming events
    # that are newly published. Deduplication via write_parquet_by_year(mode="append",
    # dedup_cols=...) handles incremental updates correctly.
    # Historical mode (--historis) also skips this filter since filter_start=None.
    
    df = truncate_df(df)
    if dry_run:
        log.info("[dry-run] event bronze: %d baris (tidak ditulis)", len(df))
        return {"events": df}

    paths = write_parquet_by_year(
        df,
        "bronze",
        SOURCE,
        "events",
        year_col="tanggal_mulai",
        mode="append",
        dedup_cols=["sumber", "judul", "tanggal_mulai", "url_detail"],
    )
    log.info("event bronze ditulis: %d file per tahun (%d baris baru)", len(paths), len(df))
    return {"events": df}