from __future__ import annotations

import json
import logging
import time

import pandas as pd
from playwright.sync_api import sync_playwright

from ..shared.config import get_source_config
from ..shared.row_limit import get_row_limit, truncate_df
from ..shared.storage import write_parquet_snapshot

log = logging.getLogger(__name__)

SOURCE = "transport"
DATASET = "rute"

KOLOM_BRONZE = [
    "nama_jenis",
    "kode_rute",
    "nama_rute",
    "daftar_halte_json",
]


def _buat_browser(playwright, headless: bool):
    browser = playwright.chromium.launch(
        headless=headless,
        slow_mo=500,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--incognito",
            "--disable-infobars",
        ],
    )
    context = browser.new_context(
        viewport={"width": 1366, "height": 768},
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        bypass_csp=True,
    )
    page = context.new_page()
    page.add_init_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return browser, page


def _kumpulkan_daftar_rute(page, moovit_url: str, max_scroll: int = 35, limit: int | None = None) -> list[dict]:
    """Scrolling daftar rute Moovit, menyimpan URL unik per rute."""
    page.goto(moovit_url, wait_until="domcontentloaded")
    try:
        page.wait_for_selector(".line-row", timeout=60000)
        time.sleep(2)
    except Exception:
        page.reload(wait_until="domcontentloaded")
        page.wait_for_selector(".line-row", timeout=60000)
        time.sleep(3)

    page.evaluate(
        "() => { document.querySelectorAll('iframe, ins, .adsbygoogle,"
        " div[id*=\"google_ads\"]').forEach(ad => ad.remove()); }"
    )

    rute_tersimpan: set[str] = set()
    data_koleksi: list[dict] = []
    jenis_saat_ini = "Umum/Lainnya"

    page.locator(".cdk-virtual-scroll-viewport").hover()

    for _ in range(max_scroll):
        for baris in page.locator(".line-row").all():
            try:
                if baris.locator(".agency-header").count() > 0:
                    if baris.locator(".name").count() > 0:
                        agency = baris.locator(".name").first.text_content(timeout=1000).strip()
                        tipe = ""
                        if baris.locator(".type").count() > 0:
                            tipe = baris.locator(".type").first.text_content(timeout=1000).strip()
                        jenis_saat_ini = f"{agency} ({tipe})" if tipe else agency
                elif baris.locator(".line-item").count() > 0:
                    kode_angkutan = "-"
                    nama_rute = "-"
                    if baris.locator(".line-title").count() > 0:
                        kode_angkutan = baris.locator(".line-title").first.text_content(timeout=1000).strip()
                    if baris.locator(".line-detail").count() > 0:
                        nama_rute = baris.locator(".line-detail").first.text_content(timeout=1000).strip()
                    else:
                        if kode_angkutan != "-":
                            nama_rute = kode_angkutan
                            kode_angkutan = "-"

                    tag_a = baris.locator("a")
                    if tag_a.count() > 0:
                        link_rute = tag_a.first.get_attribute("href", timeout=1000)
                        if link_rute and link_rute not in rute_tersimpan:
                            rute_tersimpan.add(link_rute)
                            data_koleksi.append({
                                "nama_jenis": jenis_saat_ini,
                                "kode_rute": kode_angkutan,
                                "nama_rute": nama_rute,
                                "url": link_rute,
                            })
                            if limit and len(data_koleksi) >= limit:
                                return data_koleksi
            except Exception:
                continue
        page.mouse.wheel(0, 500)
        time.sleep(1.5)

    return data_koleksi


def _ekstrak_halte_per_rute(page, daftar_rute: list[dict]) -> list[dict]:
    hasil_akhir: list[dict] = []
    for rute in daftar_rute:
        page.goto(rute["url"])
        daftar_halte: list[str] = []
        try:
            page.wait_for_selector(".stops-list", timeout=60000)
            time.sleep(1)
            daftar_halte = [
                h.text_content(timeout=2000).strip()
                for h in page.locator(".stop-container h3").all()
                if h.text_content(timeout=2000).strip()
            ]
        except Exception:
            daftar_halte = []
        log.info("transport rute %s -> %d halte", rute["nama_rute"], len(daftar_halte))
        hasil_akhir.append({
            "nama_jenis": rute["nama_jenis"],
            "kode_rute": rute["kode_rute"],
            "nama_rute": rute["nama_rute"],
            "daftar_halte": daftar_halte,
        })
    return hasil_akhir


def extract(period: str | None = None, dry_run: bool = False) -> dict[str, pd.DataFrame]:
    """EXTRACT (bronze): daftar rute + halte angkutan Jakarta dari Moovit.

    daftar_halte disimpan sebagai JSON string agar kompatibel parquet.
    Bronze ditulis sebagai snapshot `data.parquet` (grain: snapshot).
    """
    cfg = get_source_config(SOURCE)
    moovit_url = str(cfg.get("moovit_url", "")).strip()
    headless = bool(cfg.get("headless", True))
    max_scroll = int(cfg.get("max_scroll_moovit", 35))

    if not moovit_url:
        log.warning("transport: moovit_url kosong di config")
        return {}

    log.info("transport: moovit_url=%s headless=%s", moovit_url, headless)

    limit = get_row_limit()

    with sync_playwright() as p:
        browser, page = _buat_browser(p, headless)
        try:
            log.info("transport: mengumpulkan daftar rute ...")
            daftar_rute = _kumpulkan_daftar_rute(page, moovit_url, max_scroll, limit)
            if not daftar_rute:
                log.warning("transport: tidak ada rute berhasil diambil")
                return {}
            if limit:
                daftar_rute = daftar_rute[:limit]
            log.info("transport: %d rute terkumpul; ekstrak halte ...", len(daftar_rute))
            hasil_akhir = _ekstrak_halte_per_rute(page, daftar_rute)
        finally:
            browser.close()

    rows = [
        {
            "nama_jenis": h["nama_jenis"],
            "kode_rute": h["kode_rute"],
            "nama_rute": h["nama_rute"],
            "daftar_halte_json": json.dumps(h["daftar_halte"], ensure_ascii=False),
        }
        for h in hasil_akhir
    ]
    if not rows:
        log.warning("transport: tidak ada rute valid")
        return {}

    df = pd.DataFrame(rows, columns=KOLOM_BRONZE)
    df = truncate_df(df)
    if dry_run:
        log.info("[dry-run] transport bronze: %d baris (tidak ditulis)", len(df))
        return {"rute": df}

    path = write_parquet_snapshot(df, "bronze", SOURCE, DATASET)
    log.info("transport bronze ditulis: %s (%d baris)", path, len(df))
    return {"rute": df}