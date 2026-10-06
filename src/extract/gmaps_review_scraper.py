"""Scrape ulasan Google Maps per bulan -> bronze.

Alur di port ini mengikuti `Review_hotel.py` (D:\\PKL (JXB)\\nyoba_jxb) persis,
termasuk selector, urutan klik, dan jaring pengamannya. Deviasi dari referensi
hanya tiga, semuanya disebut di tempatnya: cara merakit URL, tahun target
mengikuti config, dan nilai balik memakai dict bertipe kolom.

Selector teks ulasan sengaja TIDAK memakai `div[lang]`. Elemen itu milik
`tanggapan dari pemilik` (`.CDe7pd > div.wiI7pd`, "Kepada Tamu Terhormat, ...")
bukan tulisan tamu, sehingga memakainya mengisi bronze dengan balasan hotel.
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import date, datetime, timezone

import pandas as pd

from ..shared import staging
from ..shared.checkpoint import Checkpoint
from ..shared.config import get_source_config
from ..shared.hotel_master import daftar_hotel
from ..shared.periods import resolve_months
from ..shared.row_limit import get_row_limit
from ..shared.run_options import get_resume
from ..shared.storage import write_parquet
from .gmaps_browser import (
    ambil_bintang,
    ambil_waktu_relatif,
    build_search_query,
    convert_relative_time,
    extract_review_count,
    is_similar,
    jalankan_per_hotel,
    normalize_text,
    timeout_dari_cfg,
)

log = logging.getLogger(__name__)

SOURCE = "gmaps_review"


def _goto_ms(cfg: dict | None, percobaan: int, total: int) -> int:
    """Timeout `page.goto` untuk percobaan ke-`percobaan` dari `total`.

    Percobaan pertama pendek (default 2s) karena halaman Maps sering menggantung
    puluhan detik; percobaan terakhir diberi kelonggaran (default 4s).
    """
    cepat = timeout_dari_cfg(cfg, "goto_timeout_ms", 2000)
    lambat = timeout_dari_cfg(cfg, "goto_timeout_retry_ms", 4000)
    return lambat if percobaan >= total - 1 else cepat
DATASET = "review"

# ============================================================
# KONSTANTA (dari Review_hotel.py)
# ============================================================

BATAS_KATA_IKLAN = ("iklan", "bersponsor", "disponsori", "sponsored", "advertisement")

# `h1` sponsor/hasil bukan nama hotel. Judul sponsor sengaja TIDAK memblokir
# verifikasi: halaman sponsor tetap memuat ulasan asli hotel di bawah banner.
KATA_NON_TEMPAT = (
    "bersponsor",
    "sponsored",
    "disponsori",
    "iklan",
    "hasil",
    "results",
    "pencarian",
    "search",
)

SEL_TAB_ULASAN = (
    'button[role="tab"]:has-text("Ulasan"), button[role="tab"]:has-text("Reviews")'
)
SEL_TAB_ULASAN_LEBAR = (
    'button[role="tab"]:has-text("Ulasan"), '
    'button[role="tab"]:has-text("Reviews"), '
    'button[aria-label*="Ulasan"], '
    'button[aria-label*="Reviews"]'
)
SEL_SECTION_ULASAN = (
    "div.jftiEf, div[data-review-id], "
    'button:has-text("Semua ulasan"), '
    'button:has-text("Lihat semua ulasan")'
)
SEL_BTN_SEMUA = (
    'button:has-text("Semua ulasan"), '
    'button:has-text("Lihat semua ulasan"), '
    'button:has-text("More reviews")'
)
SEL_BTN_URUTKAN = (
    'button[data-value="Sort"], button[aria-label*="Urutkan"], '
    'button[aria-label*="Sort"], button:has-text("Paling relevan"), '
    'button:has-text("Most relevant")'
)
SEL_MENU_TERBARU = (
    'div[role="menuitemradio"][data-index="1"], '
    'div[role="menuitemradio"]:has-text("Terbaru")'
)
SEL_BTN_MUAT = (
    'button:has-text("Muat lainnya"), button:has-text("Load more"), '
    'button:has-text("Tampilkan lebih banyak")'
)
SEL_BTN_SELEMPENG = 'button:has-text("Selengkapnya"), button:has-text("More")'
SEL_BTN_COOKIE = 'button:has-text("Accept all"), button:has-text("Setuju")'
SEL_KARTU_ULASAN = "div[data-review-id]"
SEL_KARTU_ULASAN_ALT = "div.jftiEf"

MAX_PERCOBAAN_KANDIDAT = 6
BATAS_SCROLL = 150
SABAR_SCROLL_KOSONG = 12
AMBANG_SKOR_NAMA = 0.55

# Jumlah kartu di luar tahun target yang berturut-turut dianggap "sudah selesai
# dibaca". Kartu pertama keluar tahun tidak boleh langsung menghentikan blok:
# urutan kartu Google tidak selalu benar-benar terbaru-ke-terlama, dan satu
# kartu nyasar (mis. ulasan lama yang muncul di atas) bikin 7 dari 41 hotel
# menghasilkan 0 baris padahal kartu setelahnya masih tahun target.
KELUAR_TAHUN_BERTURUT = 5
# Ambang serupa untuk menghentikan scroll: tanggal terakhir di luar tahun harus
# muncul beberapa kali berturut-turut, bukan sekali.
SCROLL_KELUAR_TAHUN_BERTURUT = 3

# Kontainer scroll yang dipakai referensi: scroll SEMUA `div.m6QErb` yang
# bisa di-scroll (yang terbesar dulu). `div[role="main"]` sengaja tidak
# dipakai karena men-scrollnya tidak memicu lazy-load ulasan. Dibungkus arrow
# function supaya `page.evaluate` pasti memanggilnya.
JS_SCROLL = """
() => {
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
    containers.forEach(c => c.scrollBy(0, 1500));
    return document.querySelectorAll('div[data-review-id]').length;
}
"""


def _jumlah_kartu_js(page) -> int:
    """Coba baca jumlah kartu langsung dari DOM (tanpa fallback locator)."""
    try:
        return int(page.evaluate("() => document.querySelectorAll('div[data-review-id]').length"))
    except Exception:
        return -1


# ============================================================
# HELPER
# ============================================================

def _tahun_dari_bulan(bulan_ini) -> int:
    """Tahun target dari label bulan `YYYY-MM`."""
    try:
        return int(str(bulan_ini)[:4])
    except (TypeError, ValueError):
        return date.today().year


def _tutup_cookie(page) -> None:
    """Tutup popup cookie. Popup ini memblokir panel ulasan kalau tidak ditutup."""
    try:
        btn = page.locator(SEL_BTN_COOKIE)
        if btn.count() > 0:
            btn.first.click(timeout=4000)
            time.sleep(1)
    except Exception:
        pass


def _kartu(page):
    """Locator kartu ulasan. `data-review-id` utama, `jftiEf` cadangan."""
    blok = page.locator(SEL_KARTU_ULASAN)
    if blok.count() == 0:
        blok = page.locator(SEL_KARTU_ULASAN_ALT)
    return blok


def _buka_ulasan_penuh(page, minimal: int = 30) -> int:
    """Klik tab Ulasan lalu tombol daftar penuh; baliknya jumlah kartu ter-render.

    Panel kiri hanya memuat +-10 ulasan. Tombol "Semua ulasan" membuka dialog
    yang memuat seluruh daftar, dan hanya dialog itu yang bisa di-scroll. Tombol
    itu kadang muncul belakangan, sehingga diklik ulang sampai kartu bertambah
    atau tombolnya sudah tidak ada (berarti dialog terbuka / memang tidak ada).
    """
    for percobaan in range(3):
        try:
            tab = page.locator(SEL_TAB_ULASAN)
            if percobaan == 0 and tab.count() > 0:
                tab.first.click()
                time.sleep(3)
        except Exception:
            pass

        ada_tombol = False
        try:
            semua = page.locator(SEL_BTN_SEMUA)
            ada_tombol = semua.count() > 0
            if ada_tombol:
                semua.first.click()
                time.sleep(3)
        except Exception:
            ada_tombol = False

        jumlah = _kartu(page).count()
        if jumlah >= minimal or not ada_tombol:
            return jumlah

    return _kartu(page).count()


def _urutkan_terbaru(page) -> bool:
    """Terapkan filter 'Terbaru'. True bila menu berhasil diklik.

    Penanda baliknya penting: urutan 'Paling relevan' menaruh ulasan lama di
    depan, sehingga ekstraksi berhenti di tahun target terlalu cepat. Hasil
    hanya sah kalau pengurutan benar-benar berhasil.
    """
    try:
        tombol = page.locator(SEL_BTN_URUTKAN)
        if tombol.count() == 0:
            return False
        tombol.first.click()
        time.sleep(1.5)

        menu = page.locator(SEL_MENU_TERBARU)
        if menu.count() == 0:
            return False
        menu.first.click()
        time.sleep(3.5)
        return True
    except Exception:
        return False


def _waktu_terakhir(page) -> str:
    """Waktu relatif dari ulasan paling bawah yang sudah ter-render."""
    blok = _kartu(page)
    for i in range(blok.count() - 1, -1, -1):
        try:
            return ambil_waktu_relatif(blok.nth(i))
        except Exception:
            continue
    return ""


def _tunggu_tab_ulasan(page, url_search: str, percobaan: int = 4,
                       cfg: dict | None = None) -> bool:
    """Pastikan tab "Ulasan" benar-benar ada, dengan muat ulang bila perlu.

    Pemeriksaannya memakai `button[role="tab"]` persis referensi. Selector
    `aria-label*="Ulasan"` sengaja TIDAK dipakai: tombol "Tulis ulasan" ikut
    cocok, sehingga tab yang sebenarnya hilang dikira ada dan muat ulang
    (satu-satunya pemulih yang terbukti) jadi tidak pernah dijalankan.

    Tab ini hilang pada sekitar seperempat muat awal.
    """
    for i in range(percobaan):
        if page.locator(SEL_TAB_ULASAN).count() > 0:
            return True
        log.info("  tab Ulasan belum muncul, muat ulang (%d/%d)", i + 1, percobaan)
        try:
            page.goto(url_search, timeout=_goto_ms(cfg, i, percobaan))
        except Exception:
            pass
        time.sleep(4)
        _tutup_cookie(page)
    return page.locator(SEL_TAB_ULASAN).count() > 0


def _scroll_sampai_keluar_tahun(page, tahun_target: int,
                                tenggat: float | None = None) -> tuple[int, bool]:
    """Geser daftar ulasan selama ulasan terakhir masih satu tahun.

    Mengembalikan (jumlah iterasi, berhenti karena keluar tahun). Berhenti juga
    bila `tenggat` (monotonic) terlampaui supaya satu hotel tidak menahan run.

    Berhenti karena tahun baru setelah tanggal terakhir keluar target muncul
    SCROLL_KELUAR_TAHUN_BERTURUT kali berturut-turut. Dulu cukup satu kali, dan
    karena urutan kartu tidak selalu terbaru-ke-terlama, satu ulasan lama yang
    muncul lebih dulu membuat scroll berhenti sebelum tahun target selesai
    termuat.
    """
    for _ in range(2):
        try:
            page.evaluate(JS_SCROLL)
        except Exception:
            pass
        time.sleep(1.5)

    jumlah_sebelumnya = 0
    sabar_kosong = 0
    keluar_berturut = 0
    berhenti_karena_tahun = False

    for i in range(BATAS_SCROLL):
        if tenggat is not None and time.monotonic() > tenggat:
            break
        try:
            page.evaluate(JS_SCROLL)
        except Exception:
            pass
        time.sleep(4)

        try:
            muat = page.locator(SEL_BTN_MUAT)
            if muat.count() > 0:
                muat.first.click(timeout=2000)
                time.sleep(2)
        except Exception:
            pass

        waktu = _waktu_terakhir(page)
        if waktu:
            tanggal = convert_relative_time(waktu)
            if tanggal is not None and tanggal.year != tahun_target:
                keluar_berturut += 1
                if keluar_berturut >= SCROLL_KELUAR_TAHUN_BERTURUT:
                    berhenti_karena_tahun = True
                    break
            else:
                keluar_berturut = 0

        try:
            jumlah = page.locator(
                f"{SEL_KARTU_ULASAN}, {SEL_KARTU_ULASAN_ALT}").count()
        except Exception:
            jumlah = 0
        if jumlah == jumlah_sebelumnya:
            sabar_kosong += 1
            if sabar_kosong >= SABAR_SCROLL_KOSONG:
                break
        else:
            sabar_kosong = 0
        jumlah_sebelumnya = jumlah

    return i + 1, berhenti_karena_tahun


def _buka_teks_terpotong(page) -> None:
    """Klik 'Selengkapnya' supaya teks ulasan tidak terpotong 200 karakter."""
    try:
        tombol = page.locator(SEL_BTN_SELEMPENG)
        for i in range(tombol.count()):
            try:
                tombol.nth(i).click(timeout=1000)
            except Exception:
                pass
    except Exception:
        pass
    time.sleep(1)


# ============================================================
# PEMILIHAN PLACE
# ============================================================

def _kandidat_place(page, nama_hotel: str) -> list[dict]:
    """Kumpulkan kandidat hotel dari feed hasil pencarian, sponsor dibuang."""
    link_placeholder = page.locator('div[role="feed"] a[href*="/maps/place/"]')
    total = link_placeholder.count()
    kandidat: list[dict] = []
    sudah_dicoba: set[str] = set()
    terlihat: set[str] = set()

    for i in range(total):
        link = link_placeholder.nth(i)
        try:
            href = link.get_attribute("href") or ""
            if not href or href in terlihat or href in sudah_dicoba:
                continue
            terlihat.add(href)

            induk = link.locator('xpath=ancestor::div[@role="article"]')
            if induk.count() == 0:
                induk = link.locator('xpath=ancestor::div[contains(@class,"Nv2PK")]')
            if induk.count() == 0:
                continue
            induk = induk.first

            teks_kartu = induk.inner_text()
            teks_kartu_lower = teks_kartu.lower()
            if (
                any(kata in teks_kartu_lower for kata in BATAS_KATA_IKLAN)
                or induk.locator(
                    '[aria-label="Iklan"], [aria-label="Ad"], '
                    '[aria-label="Bersponsor"], [aria-label="Sponsored"], .G7qWGe'
                ).count() > 0
            ):
                continue

            nama = link.get_attribute("aria-label") or "Unknown"
            n_nama, n_hotel = normalize_text(nama), normalize_text(nama_hotel)
            skor = 0.0
            if n_nama and n_hotel:
                from difflib import SequenceMatcher

                skor = SequenceMatcher(None, n_nama, n_hotel).ratio()
                if n_nama in n_hotel or n_hotel in n_nama:
                    skor = max(skor, 0.95)

            jumlah = extract_review_count(teks_kartu)
            kandidat.append({
                "index": i,
                "href": href,
                "link": link,
                "induk": induk,
                "nama": nama,
                "skor": skor,
                "punya_ulasan": jumlah > 0,
                "jumlah_ulasan": jumlah,
            })
        except Exception:
            continue

    # 1) nama mirip target di atas, 2) yang punya ulasan dulu, 3) paling banyak
    # ulasan, 4) paling mirip namanya, 5) posisi paling atas.
    kandidat.sort(key=lambda k: (
        k["skor"] < AMBANG_SKOR_NAMA,
        not k["punya_ulasan"],
        -k["jumlah_ulasan"],
        -k["skor"],
        k["index"],
    ))
    return kandidat


def _judul_place(page) -> str:
    """Nama tempat yang benar-benar tampil, bukan banner sponsor."""
    try:
        for teks in page.locator("h1").all_inner_texts():
            teks = teks.strip()
            bawah = teks.lower()
            if not teks or any(kata in bawah for kata in KATA_NON_TEMPAT):
                continue
            return teks
    except Exception:
        pass

    try:
        judul = (
            page.title()
            .replace(" - Google Maps", "")
            .replace("- Google Maps", "")
            .strip()
        )
        bawah = judul.lower()
        if judul and not any(kata in bawah for kata in KATA_NON_TEMPAT):
            return judul
    except Exception:
        pass
    return ""


def _cari_place(page, nama_hotel: str, url_search: str,
                cfg: dict | None = None) -> dict | None:
    """Masuk ke halaman ulasan hotel yang benar lewat feed hasil pencarian.

    Deviasi dari referensi: pendaratan langsung dianggap berhasil bila `h1`
    sudah nama hotel dan feed hasil pencarian tidak ada. Referensi memakai
    keberadaan tab "Ulasan" sebagai penanda, padahal tab itu hilang sesaat pada
    sekitar seperempat muat awal; kalau itu dipakai sebagai syarat, hotel yang
    sebenarnya sudah terbuka jadi ikut gagal.
    """
    judul = _judul_place(page)
    ada_feed = page.locator('div[role="feed"] a[href*="/maps/place/"]').count() > 0
    if judul and not ada_feed:
        return {"judul": judul, "sumber": "langsung"}
    if page.locator(SEL_TAB_ULASAN).count() > 0:
        return {"judul": judul or nama_hotel, "sumber": "langsung"}

    sudah_dicoba: set[str] = set()

    for percobaan in range(MAX_PERCOBAAN_KANDIDAT):
        kandidat = _kandidat_place(page, nama_hotel)
        if not kandidat:
            log.info("  [%s] tidak ada kandidat place tersisa", nama_hotel[:40])
            break

        k = kandidat[0]
        label = f"{k['jumlah_ulasan']} ulasan" if k["punya_ulasan"] else "tanpa ulasan"
        log.info("  [%s] kandidat '%s' (skor %.2f, %s, percobaan %d)",
                 nama_hotel[:40], k["nama"], k["skor"], label, percobaan + 1)

        gagal_klik = False
        try:
            k["link"].click(timeout=6000)
        except Exception:
            try:
                k["induk"].click(timeout=6000)
            except Exception:
                gagal_klik = True

        if gagal_klik:
            sudah_dicoba.add(k["href"])
            try:
                page.goto(url_search, timeout=_goto_ms(cfg, percobaan, MAX_PERCOBAAN_KANDIDAT))
            except Exception:
                pass
            time.sleep(3)
            continue

        # Jangan terburu-buru menilai gagal: panel place butuh waktu merender
        # tab dan kartunya. Timeout render tetap longgar (`wait_load_ms`,
        # default 8s) karena hanya navigation yang dipendekkan.
        tunggu_ms = timeout_dari_cfg(cfg, "wait_load_ms", 8000)
        for tunggu, sel in (
            (("domcontentloaded",), None),
            (("h1",), "h1"),
        ):
            try:
                if sel is None:
                    page.wait_for_load_state(tunggu[0], timeout=tunggu_ms)
                else:
                    page.wait_for_selector(sel, timeout=tunggu_ms)
            except Exception:
                pass
        try:
            page.wait_for_selector(SEL_TAB_ULASAN_LEBAR + ", " + SEL_SECTION_ULASAN,
                                   timeout=tunggu_ms)
        except Exception:
            pass
        time.sleep(0.5)

        judul = _judul_place(page)
        verif_gagal = bool(judul) and not is_similar(judul, nama_hotel)

        ada_jalur_ulasan = (
            page.locator(SEL_TAB_ULASAN_LEBAR).count() > 0
            or page.locator(SEL_SECTION_ULASAN).count() > 0
        )

        if not verif_gagal and ada_jalur_ulasan:
            log.info("  [%s] masuk halaman ulasan '%s'", nama_hotel[:40], judul or k["nama"])
            return {"judul": judul or k["nama"], "sumber": "feed"}

        sudah_dicoba.add(k["href"])
        log.info("  [%s] '%s' gagal (judul=%r), kembali ke feed",
                 nama_hotel[:40], k["nama"], judul)
        try:
            page.goto(url_search, timeout=_goto_ms(cfg, percobaan, MAX_PERCOBAAN_KANDIDAT))
        except Exception:
            pass
        time.sleep(3)

    return None


# ============================================================
# EKSTRAKSI
# ============================================================

def _teks_ulasan_tamu(block) -> tuple[str, str]:
    """Teks ulasan tamu dan bahasanya.

    Rantai persis Review_hotel.py. `div[lang]` sengaja TIDAK dipakai: pada
    kartu yang punya tanggapan pemilik, elemen itu berisi "Kepada Tamu
    Terhormat, Terima kasih..." dan akan tersimpan sebagai ulasan.
    """
    for sel, ambil_lang in (
        ('div[tabindex="-1"][id][lang]', True),
        ("span.wiI7pd", False),
    ):
        try:
            el = block.locator(sel)
            if el.count() == 0:
                continue
            pertama = el.first
            teks = pertama.inner_text().strip()
            if not teks:
                continue
            bahasa = (pertama.get_attribute("lang") or "") if ambil_lang else ""
            return teks, bahasa
        except Exception:
            continue
    return "", ""


def _nama_pengulas(block) -> str:
    for sel in ("div.d4r55", 'button[data-href*="/maps/contrib/"] div'):
        try:
            el = block.locator(sel)
            if el.count() > 0:
                teks = el.first.inner_text().strip()
                if teks:
                    return teks
        except Exception:
            continue
    return "Anonim"


def _id_ulasan(id_kompetitor, nama_reviewer: str, isi_review: str) -> str:
    """Kunci anti-duplikat ala Review_hotel.py: md5(hotel|nama|teks)."""
    dasar = f"{id_kompetitor}|{nama_reviewer}|{isi_review}"
    return hashlib.md5(dasar.encode("utf-8")).hexdigest()


def _ekstrak_blok(blok, hotel: dict, info: dict, tahun_target: int) -> tuple[list[dict], dict]:
    """Ekstrak semua kartu yang ter-render, berhenti saat tanggal keluar tahun."""
    baris: list[dict] = []
    stat = {"tahun": 0, "teks_kosong": 0, "tanggal_gagal": 0,
            "keluar_tahun": 0, "error": 0, "bungkus": 0}
    contoh_waktu: list[str] = []
    keluar_berturut = 0
    total = blok.count()

    for i in range(total):
        try:
            satu = blok.nth(i)
            # Google membungkus tiap ulasan dua lapis `div[data-review-id]`
            # (pembungkus + isi), jadi satu ulasan terhitung dua kali. Ambil
            # hanya kartu yang tidak membungkus kartu lain.
            if satu.locator("div[data-review-id]").count() > 0:
                stat["bungkus"] += 1
                continue

            nama = _nama_pengulas(satu)
            isi, bahasa = _teks_ulasan_tamu(satu)
            waktu = ambil_waktu_relatif(satu)
            rating = ambil_bintang(satu)

            tanggal = convert_relative_time(waktu) if waktu else None
            if tanggal is None:
                # Tanggal tak terbaca -> lewati. Menulisnya dengan bulan
                # berjalan akan menggeser periode secara diam-diam.
                stat["tanggal_gagal"] += 1
                if len(contoh_waktu) < 4:
                    contoh_waktu.append(waktu or "<kosong>")
                continue

            if tanggal.year != tahun_target:
                # Bukan `break` lagi: satu kartu di luar tahun tidak cukup bukti
                # bahwa semua kartu berikutnya juga di luar tahun. Terus baca,
                # putuskan baru setelah KELUAR_TAHUN_BERTURUT kartu berturut-turut.
                stat["keluar_tahun"] += 1
                keluar_berturut += 1
                if keluar_berturut >= KELUAR_TAHUN_BERTURUT:
                    break
                continue
            keluar_berturut = 0

            if not isi:
                stat["teks_kosong"] += 1
                continue

            stat["tahun"] += 1
            baris.append({
                "id_ulasan": _id_ulasan(hotel.get("id_kompetitor"), nama, isi),
                "id_kompetitor": hotel.get("id_kompetitor"),
                "nama_hotel": hotel.get("nama_hotel"),
                "nama_place": info.get("judul") or hotel.get("nama_hotel"),
                "url_maps": info.get("url") or "",
                "nama_reviewer": nama,
                "rating_bintang": rating,
                "bahasa_ulasan": bahasa,
                "isi_review": isi,
                "waktu_relatif": waktu,
                "tanggal_ulasan": tanggal.isoformat(),
                "bulan": tanggal.month,
                "tahun": tanggal.year,
                "scraped_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            })
        except Exception as exc:
            stat["error"] += 1
            log.info("  [%s] error ekstrak kartu %d: %s",
                     hotel.get("nama_hotel", "?")[:40], i + 1, exc)

    return baris, stat, contoh_waktu


# ============================================================
# SCRAPER
# ============================================================

def _tutup_halaman(page) -> None:
    try:
        page.close()
    except Exception:
        pass


def _siapkan_halaman_ulasan(context, hotel: dict, url_search: str,
                            nama_hotel: str, percobaan: int = 3,
                            tenggat: float | None = None,
                            cfg: dict | None = None):
    """Muat halaman pada varian yang daftar ulasannya bisa dipaginasi.

    Google menyajikan dua varian per muat: (a) tab "Ulasan" SUDAH ada saat cek
    pertama -- daftar mentok ~10 kartu walau digulir berapa pun; (b) tab HILANG
    lalu dimuat ulang -- daftar tumbuh sampai ratusan kartu. Varian ditentukan
    per muat dan tidak bisa dipaksa, jadi halaman diulang dengan page baru
    sampai varian (b) didapat. Bila semua percobaan (a), pakai page terakhir
    sebagai upaya terakhir.

    Klasifikasi WAJIB dilakukan segera setelah muat: pada varian (a) tab muncul
    hanya 1-2 detik setelahnya, sehingga cek yang telat selalu membaca "ada".
    Karena itu `_cari_place` dijalankan setelah klasifikasi/reloal, bukan
    sebelumnya.

    Mengembalikan (page, info, jumlah_kartu) atau (None, None, 0).
    """
    cadangan = None
    for urutan in range(percobaan):
        if tenggat is not None and time.monotonic() > tenggat:
            log.info("  [%s] batas waktu per hotel tercapai; berhenti mencoba",
                     nama_hotel[:40])
            break
        page = context.new_page()
        try:
            page.goto(url_search, timeout=_goto_ms(cfg, urutan, percobaan))
            time.sleep(4)
            _tutup_cookie(page)
        except Exception:
            _tutup_halaman(page)
            continue

        if page.locator(SEL_TAB_ULASAN).count() > 0:
            log.info("  [%s] varian buntu (tab sudah ada di awal), ulang",
                     nama_hotel[:40])
            if cadangan is not None:
                _tutup_halaman(cadangan[0])
            cadangan = (page, None)
            continue

        _tunggu_tab_ulasan(page, url_search, cfg=cfg)
        if page.locator(SEL_TAB_ULASAN).count() == 0:
            _tutup_halaman(page)
            continue

        info = _cari_place(page, nama_hotel, url_search, cfg)
        if info is None:
            _tutup_halaman(page)
            return None, None, 0
        info["url"] = page.url
        jumlah = _buka_ulasan_penuh(page)
        if cadangan is not None:
            _tutup_halaman(cadangan[0])
        return page, info, jumlah

    if cadangan is not None:
        page = cadangan[0]
        info = _cari_place(page, nama_hotel, url_search, cfg)
        if info is None:
            _tutup_halaman(page)
            return None, None, 0
        info["url"] = page.url
        return page, info, _buka_ulasan_penuh(page)
    return None, None, 0


def scrape_hotel_reviews(context, hotel: dict, cfg: dict, bulan_ini=None) -> list[dict]:
    """Mendarat di koordinat hotel, buka panel ulasan, urutkan terbaru, ekstrak."""
    nama_hotel = hotel.get("nama_hotel") or ""
    # Mode test (--test): batasi ulasan per hotel ke row_limit (default 3)
    batas_uji = get_row_limit()
    if batas_uji is not None:
        maks_ulasan = batas_uji
        log.info("  [%s] mode uji: batas ulasan per hotel = %d", nama_hotel[:40], maks_ulasan)
    else:
        maks_ulasan = int(cfg.get("max_reviews_per_hotel", 200) or 200)
    tahun_target = _tahun_dari_bulan(bulan_ini)
    hasil: list[dict] = []

    latitude = hotel.get("latitude")
    longitude = hotel.get("longitude")
    if latitude is None or longitude is None:
        log.info("  [%s] dilewati: koordinat kosong", nama_hotel[:40])
        return hasil

    # Deviasi 1 dari referensi: referensi memakai replace(" ", "+") polos,
    # bukan quote_plus yang ikut meng-encode koma koordinat.
    query = build_search_query(nama_hotel, hotel.get("alamat"), hotel.get("kota"))
    url_search = f"https://www.google.com/maps/search/{query.replace(' ', '+')}/@{latitude},{longitude},19z"

    batas_detik = float(cfg.get("batas_detik_per_hotel", 90) or 90)
    tenggat = time.monotonic() + batas_detik

    page, info, jumlah_awal = _siapkan_halaman_ulasan(
        context, hotel, url_search, nama_hotel, tenggat=tenggat, cfg=cfg)
    if page is None:
        log.info("  [%s] tidak ditemukan hotel dengan ulasan", nama_hotel[:40])
        return hasil

    try:
        log.info("  [%s] daftar ulasan terbuka: %d kartu", nama_hotel[:40], jumlah_awal)

        terurut = _urutkan_terbaru(page)
        if not terurut:
            # Warning, bukan info: hotel ini urutan kartunya tidak tepercaya,
            # jadi hasil scraping-nya perlu dicek manual.
            log.warning("  [%s] filter 'Terbaru' tidak terpasang", nama_hotel[:40])

        _scroll_sampai_keluar_tahun(page, tahun_target, tenggat=tenggat)

        # Penutup: tombol muat terakhir + buka teks yang terpotong.
        try:
            muat = page.locator(SEL_BTN_MUAT)
            if muat.count() > 0:
                muat.first.click(timeout=3000)
                time.sleep(3)
        except Exception:
            pass
        _buka_teks_terpotong(page)

        blok = _kartu(page)
        total = blok.count()
        if total == 0:
            log.info("  [%s] 0 kartu ulasan ter-render", nama_hotel[:40])
            return hasil

        baris, stat, contoh_waktu = _ekstrak_blok(blok, hotel, info, tahun_target)

        # md5(hotel|nama|teks) bisa bentrok bila satu orang menulis ulasan
        # sama persis dua kali; yang pertama saja yang disimpan.
        sebelum = len(baris)
        unik: dict[str, dict] = {}
        for b in baris:
            unik.setdefault(b["id_ulasan"], b)
        baris = list(unik.values())
        hasil.extend(baris[:maks_ulasan])

        log.info(
            "  [%s] %d kartu | %d terbungkus | %d tahun %d | %d teks kosong | "
            "%d tanggal gagal | %d keluar tahun | %d error | %d duplikat -> %d baris",
            nama_hotel[:40], total, stat["bungkus"], stat["tahun"], tahun_target,
            stat["teks_kosong"], stat["tanggal_gagal"], stat["keluar_tahun"],
            stat["error"], sebelum - len(baris), len(baris[:maks_ulasan]),
        )
        if contoh_waktu:
            log.info("  [%s] contoh waktu tak terbaca: %s",
                     nama_hotel[:40], contoh_waktu)
        return hasil

    except Exception as exc:
        log.warning("  [%s] gagal scrape: %s", nama_hotel[:40], exc)
        return hasil
    finally:
        try:
            page.close()
        except Exception:
            pass


# ============================================================
# ORKESTRATOR
# ============================================================

def extract(period: str | None = None, dry_run: bool = False) -> dict[str, pd.DataFrame]:
    """Ulasan Google Maps untuk satu bulan, semua hotel dari hotel_master.

    Hanya bulan pertama dari `period` yang dipakai. Berbeda dengan harga yang
    butuh 9 lead time, satu run ulasan hanya menghasilkan satu bulan; rentang
    bulan dijalani lewat beberapa kali jalan, bukan sekali.

    Isi bronze adalah ulasan milik tahun target yang termuat di UI, bukan
    hanya bulan ini; pemotongan ke satu bulan ada di transform silver.

    Hasil tiap hotel langsung ditulis ke staging `data_lake/_state/`, bukan
    ditahan di memori sampai run selesai: run penuh 781 hotel butuh belasan jam
    dan sempat hilang total dua kali karena proses dibunuh sebelum parquet
    ditulis. Parquet bronze disusun dari staging + hasil sesi ini saat run
    selesai, jadi `--lanjut` tidak lagi melewati hotel yang datanya tak pernah
    tersimpan.
    """
    cfg = get_source_config(SOURCE)
    bulan_ini = resolve_months(period)[0]
    hotel_list = daftar_hotel(cfg)

    batas = get_row_limit()
    if batas is not None:
        # Mode uji `sumber[N]`: N hotel pertama saja. Tanpa ini `[1]` tetap
        # scrape seluruh 781 hotel karena limit hanya memotong DataFrame.
        hotel_list = hotel_list[:batas]
        log.info("%s: mode uji, dibatasi %d hotel pertama", SOURCE, batas)

    total_hotel = len(hotel_list)
    log.info("%s: %d hotel untuk bulan %s", SOURCE, total_hotel, bulan_ini)

    # Mode uji dan dry-run tidak boleh menyentuh staging run sungguhan.
    simpan_staging = batas is None and not dry_run
    label = staging.label_run(bulan_ini)
    checkpoint = Checkpoint(SOURCE)

    dari_staging = None
    if get_resume():
        sudah = checkpoint.muat()
        hotel_list = [h for h in hotel_list if not checkpoint.sudah(h.get("id_kompetitor"))]
        if simpan_staging:
            dari_staging = staging.baca_parts(SOURCE, label, dedup_cols="id_ulasan")
        if not hotel_list:
            # Semua hotel sudah beres. Jangan berhenti di sini: hasil run ada di
            # staging, parquet bronze tetap harus disusun dari sana.
            log.info("%s: %d hotel sudah diproses hari ini, menyusun bronze dari staging",
                     SOURCE, len(sudah))
        else:
            log.info("%s: melanjutkan, %d hotel sudah beres dan %d tersisa",
                     SOURCE, len(sudah), len(hotel_list))
    else:
        # Run baru: checkpoint dan staging label ini dibuang supaya hasil run
        # lama tidak bocor masuk lewat dedup id_ulasan.
        checkpoint.bersihkan()
        if simpan_staging:
            staging.hapus_staging(SOURCE, label)

    hotel_baru = 0

    def _tandai_selesai(rows: list[dict], hotel: dict) -> None:
        nonlocal hotel_baru
        hotel_baru += 1
        if not simpan_staging:
            return
        try:
            staging.tulis_part(
                SOURCE, label,
                hotel.get("id_kompetitor") or f"{hotel.get('nama_hotel')}|{hotel.get('kota')}",
                rows,
            )
            checkpoint.simpan()
        except Exception as exc:
            # Part gagal = hotel ini belum aman. Hotel sengaja tidak ditandai
            # selesai, jadi `--lanjut` akan mengulangnya.
            log.error("  [%s] staging gagal, hotel akan diulang: %s: %s",
                      (hotel.get("nama_hotel") or "?")[:40], type(exc).__name__, exc)
            raise

    if hotel_list:
        hasil = jalankan_per_hotel(
            hotel_list,
            lambda ctx, h: scrape_hotel_reviews(ctx, h, cfg, bulan_ini),
            cfg,
            "review_workers",
            checkpoint=checkpoint,
            titik_selesai=_tandai_selesai if simpan_staging else None,
            label_progres=SOURCE,
            total_progres=total_hotel,
        )
        checkpoint.simpan()
    else:
        hasil = []
    hotel_selesai = hotel_baru or len(checkpoint.selesai)

    semua = list(hasil)
    if dari_staging is not None:
        semua = dari_staging.to_dict("records") + semua
    df = pd.DataFrame(semua)
    if not df.empty and "id_ulasan" in df.columns:
        sebelum = len(df)
        # Baris sama arise kalau satu hotel ter-scrape dua kali di run berbeda
        # lalu digabung; hasil paling baru yang dipakai.
        df = df.drop_duplicates(subset=["id_ulasan"], keep="last").reset_index(drop=True)
        if len(df) != sebelum:
            log.info("%s: %d baris kembar dibuang", SOURCE, sebelum - len(df))

    log.info("%s: %d baris dari %d hotel diproses (staging %d baris, sesi ini %d baris)",
             SOURCE, len(df), hotel_selesai,
             0 if dari_staging is None else len(dari_staging), len(hasil))

    if df.empty:
        # Parquet kosong tidak ditulis: `write_parquet` melempar ValueError, jadi
        # bulan tanpa ulasan akan menggagalkan seluruh pipeline.
        log.warning("%s: tidak ada ulasan untuk bulan %s, bronze tidak ditulis",
                    SOURCE, bulan_ini)
        if simpan_staging:
            staging.hapus_staging(SOURCE, label)
        return {}

    if dry_run:
        log.info("[dry-run] %s bronze %s: %d baris (tidak ditulis)",
                 SOURCE, bulan_ini, len(df))
    elif simpan_staging:
        path = write_parquet(df, "bronze", SOURCE, DATASET, bulan_ini)
        log.info("%s bronze ditulis: %s (%d baris)", SOURCE, path, len(df))
        staging.hapus_staging(SOURCE, label)
        checkpoint.bersihkan()
    else:
        log.info("%s: mode uji/dry-run aktif, bronze %s tidak ditulis (%d baris)",
                 SOURCE, bulan_ini, len(df))

    return {DATASET: df}
