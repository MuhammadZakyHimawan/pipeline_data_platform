from __future__ import annotations

import json
import logging
import random
import time
from datetime import date
from urllib.parse import quote

import pandas as pd
from playwright.sync_api import sync_playwright

from ..shared.config import get_source_config
from ..shared.row_limit import get_row_limit, truncate_df
from ..shared.storage import write_parquet_by_year

log = logging.getLogger(__name__)

SOURCE = "trends"
DATASET = "search_intent"

TIMEOUT_MENUNGGU = 30
TIMEOUT_GOTO = 30000
MAKS_PERC0BAAN = 3
JEDA_ANTAR_DESTINASI_JITTER = 5
# Rentang default Google Trends = 10 tahun terakhir s.d. hari ini. Preset relatíf
# Trends hanya tersedia s.d. "today 5-y", jadi dipakai rentang custom (YYYY-MM-DD YYYY-MM-DD).
TAHUN_HISTORIS = 10
KATEGORI_DEFAULT = "Destinasi"

HEADERS_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

KOLOM_BRONZE = ["tanggal", "indeks", "kategori", "search_term", "geo"]


def _rentang_tren(cfg: dict) -> str:
    """Rentang tanggal request Trends; override opsional via config 'range'.

    Default = 10 tahun terakhir (1 Jan tahun-10) s.d. hari ini.
    """
    cfg_range = str(cfg.get("range") or "").strip()
    if cfg_range:
        return cfg_range
    today = date.today()
    mulai = date(today.year - TAHUN_HISTORIS, 1, 1)
    return f"{mulai.isoformat()} {today.isoformat()}"


def _daftar_destinasi(cfg: dict) -> list[tuple[str, str]]:
    """Ambil daftar (nama, kategori) dari config (pengganti dim_destinasi)."""
    items = cfg.get("daftar_destinasi") or []
    out = []
    for it in items:
        if not isinstance(it, dict):
            continue
        nama = str(it.get("nama") or "").strip()
        if not nama:
            continue
        kategori = str(it.get("kategori") or KATEGORI_DEFAULT).strip() or KATEGORI_DEFAULT
        out.append((nama, kategori))
    return out


class StatusTunggu:
    def __init__(self):
        self.rate_limited = False


def _buat_browser(playwright, headless: bool):
    browser = playwright.chromium.launch(
        headless=headless,
        slow_mo=50,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--disable-infobars",
            "--no-first-run",
        ],
    )
    context = browser.new_context(
        viewport={"width": 1366, "height": 768},
        user_agent=HEADERS_UA,
        locale="en-US",
        bypass_csp=True,
    )
    page = context.new_page()
    page.add_init_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return browser, page


def _tangkap_multiline(resp, hasil, status: StatusTunggu) -> None:
    if "trends/api/widgetdata/multiline" not in resp.url:
        return
    if resp.status == 429:
        status.rate_limited = True
        return
    if resp.status != 200:
        return
    try:
        body = resp.text()
        start = body.index("{")
        payload = json.loads(body[start:])
        timeline = payload.get("default", {}).get("timelineData", [])
        if not timeline:
            return
        hasil.clear()
        for titik in timeline:
            t = titik.get("time")
            if t is None:
                continue
            val = titik.get("value") or [0]
            hasil.append(
                {
                    "tanggal": date.fromtimestamp(int(t)),
                    "indeks": float(val[0]) if val else 0.0,
                }
            )
    except Exception:
        pass


def _tutup_consent(page) -> None:
    try:
        page.get_by_role("button", name="Reject all").click(timeout=5000)
        page.wait_for_timeout(500)
    except Exception:
        pass


def _tunggu_data(page, hasil, status: StatusTunggu, timeout=TIMEOUT_MENUNGGU) -> str:
    batas = time.time() + timeout
    langkah = 0
    while time.time() < batas:
        if hasil:
            return "data"
        if status.rate_limited:
            return "ratelimit"
        page.wait_for_timeout(1000)
        langkah += 1
        if langkah % 10 == 0:
            sisa = int(batas - time.time())
            log.info("trends masih menunggu data ... (%ss)", max(sisa, 0))
    return "timeout"


def _tunggu_backoff(detik: int) -> None:
    log.info("trends menunggu %d dtk (backoff rate-limit)...", detik)
    lewat = 0
    while lewat < detik:
        time.sleep(10)
        lewat += 10


def _buka_halaman(page, url: str) -> bool:
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=TIMEOUT_GOTO)
        return True
    except Exception as e:
        log.warning("trends goto gagal: %s", e)
        return False


def _scrap_tren(page, nama_destinasi: str, geo: str, rentang: str) -> list[dict]:
    status = StatusTunggu()
    hasil: list[dict] = []

    def handler(resp):
        _tangkap_multiline(resp, hasil, status)

    page.on("response", handler)
    try:
        url = (
            "https://trends.google.com/trends/explore"
            f"?hl=en-US&date={rentang}&geo={geo}&q={quote(nama_destinasi)}"
        )
        for percobaan in range(1, MAKS_PERC0BAAN + 1):
            if not _buka_halaman(page, url):
                continue
            _tutup_consent(page)

            alasan = _tunggu_data(page, hasil, status)
            if alasan == "data":
                return hasil
            if alasan == "ratelimit":
                status.rate_limited = False
                _tunggu_backoff(30 * percobaan)
                continue

            log.info("trends waktu habis, reload...")
            if not _buka_halaman(page, url):
                continue
            _tutup_consent(page)
            alasan = _tunggu_data(page, hasil, status)
            if alasan == "data":
                return hasil
            if alasan == "ratelimit":
                status.rate_limited = False
                _tunggu_backoff(30 * percobaan)
                continue
        return hasil
    finally:
        page.remove_listener("response", handler)


def extract(period: str | None = None, dry_run: bool = False) -> dict[str, pd.DataFrame]:
    """EXTRACT (bronze): interest-over-time Google Trends per destinasi dari config.

    Bronze ditulis per tahun (grain: year): {dataset}/{YYYY}.parquet.
    """
    cfg = get_source_config(SOURCE)
    geo = str(cfg.get("geo", "ID")).strip().upper()
    jeda = int(cfg.get("jeda_antar_destinasi", 20))
    headless = bool(cfg.get("headless", True))
    rentang = _rentang_tren(cfg)

    destinasi = _daftar_destinasi(cfg)
    if not destinasi:
        log.warning("trends: tidak ada daftar_destinasi di config/sources.yaml")
        return {}

    log.info("trends: %d destinasi, rentang=%s geo=%s headless=%s",
             len(destinasi), rentang, geo, headless)

    semua: list[dict] = []
    limit = get_row_limit()
    with sync_playwright() as p:
        browser, page = _buat_browser(p, headless)
        try:
            if _buka_halaman(page, "https://trends.google.com/trends/?geo=" + geo):
                _tutup_consent(page)
            page.wait_for_timeout(3000)

            for i, (nama, kategori) in enumerate(destinasi, start=1):
                if i > 1:
                    d = jeda + random.randint(
                        -JEDA_ANTAR_DESTINASI_JITTER, JEDA_ANTAR_DESTINASI_JITTER
                    )
                    d = max(d, 5)
                    time.sleep(d)

                log.info("trends [%d/%d] %s ...", i, len(destinasi), nama)
                try:
                    titik = _scrap_tren(page, nama, geo, rentang)
                except Exception as e:
                    log.warning("trends error utk %s: %s", nama, e)
                    titik = []

                if not titik:
                    log.warning("trends tidak ada data tertangkap utk %s", nama)
                    continue

                for h in titik:
                    semua.append({
                        "tanggal": h["tanggal"],
                        "indeks": h["indeks"],
                        "kategori": kategori,
                        "search_term": nama,
                        "geo": geo,
                    })
                log.info("trends %s -> %d titik", nama, len(titik))
                if limit and len(semua) >= limit:
                    log.info("trends: cap %d baris tercapai; destinasi tersisa dilewati", limit)
                    break
        finally:
            browser.close()

    if not semua:
        log.warning("trends: tidak ada data berhasil diekstrak")
        return {}

    df = pd.DataFrame(semua, columns=KOLOM_BRONZE)
    df = truncate_df(df)
    if dry_run:
        log.info("[dry-run] trends bronze: %d baris (tidak ditulis)", len(df))
        return {"search_intent": df}

    paths = write_parquet_by_year(df, "bronze", SOURCE, DATASET, year_col="tanggal")
    log.info("trends bronze ditulis: %d file per tahun (%d baris)", len(paths), len(df))
    return {"search_intent": df}