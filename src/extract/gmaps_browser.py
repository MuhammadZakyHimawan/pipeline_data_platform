from __future__ import annotations

import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from difflib import SequenceMatcher
from threading import Lock
from typing import Callable
from urllib.parse import quote_plus

from playwright.sync_api import sync_playwright

from ..shared.checkpoint import Checkpoint
from ..shared.run_options import get_headful

log = logging.getLogger(__name__)

# Argumen Chromium untuk menjaga renderer tetap hidup selama scrape panjang.
# Tanpa --disable-dev-shm-usage renderer Chromium sering mati dengan
# "Page crashed" setelah ribuan query di satu instance.
ARG_BROWSER_STABIL = [
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--no-sandbox",
    "--disable-blink-features=AutomationControlled",
    "--renderer-process-limit=1",
    "--disable-background-timer-throttling",
    "--disable-renderer-backgrounding",
]

SEL_KARTU_ULASAN = "div[data-review-id], div.jftiEf, div[role='article'][aria-label*='Ulasan']"
SEL_TAB_ULASAN = (
    'button[role="tab"]:has-text("Ulasan"), button[role="tab"]:has-text("Reviews"), '
    'button[role="tab"][aria-label*="Ulasan"], button[role="tab"][aria-label*="Review"]'
)

BULAN_ID = {
    "januari": 1, "februari": 2, "maret": 3, "april": 4, "mei": 5, "juni": 6,
    "juli": 7, "agustus": 8, "september": 9, "oktober": 10, "november": 11, "desember": 12,
    # singkatan yang dipakai Google di Indonesia
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "mei": 5, "jun": 6, "jul": 7,
    "agu": 8, "agt": 8, "sep": 9, "okt": 10, "nop": 11, "nov": 11, "des": 12,
}
BULAN_EN = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6, "jul": 7, "aug": 8,
    "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}
# `normalize_text` mengecilkan huruf, dan singkatan Indonesian vs English
# happily bernilai sama (mar=3, aug=8, okt/oct=10, des/dec=12) sehingga cukup
# digabung dengan urutan Indonesia dulu.
BULAN = {**BULAN_EN, **BULAN_ID}


# ============================================================
# BROWSER
# ============================================================

def mau_headless(cfg: dict) -> bool:
    """Headless kecuali config bilang headful ATAU CLI --headful aktif.

    --headful untuk melihat selector di layar saat debugging.
    """
    if get_headful():
        return False
    return bool(cfg.get("headless", True))


# Ukuran/posisi jendela headful. `--start-maximized` tidak dipakai karena
# beberapa jendela fullscreen akan saling menutupi dan mustahil dipantau.
JENDELA_LEBAR, JENDELA_TINGGI = 1000, 900


def buka_browser(playwright, headless: bool, indeks: int = 0, jumlah: int = 1):
    """Buka Chromium dengan argumen yang sama persis Review_hotel.py.

    Headful memakai `--start-maximized` tanpa flag penstabil lain. Google Maps
    hanya merender tab "Ulasan" dan daftar ulasan di jendela sungguhan, dan
    penyesuaian argumen (disable-gpu, renderer-process-limit, hide automation)
    justru membuat halaman gagal memuat panel ulasan.
    """
    if headless:
        return playwright.chromium.launch(headless=True, args=list(ARG_BROWSER_STABIL))

    args = ["--start-maximized"]
    if jumlah > 1:
        # `--start-maximized` menumpuk semua jendela di layar yang sama.
        # Worker kedua digeser sedikit supaya tidak menutupi worker pertama;
        # menggeser window tidak mengubah perilaku render.
        args.append(f"--window-position={indeks * 120},0")
    return playwright.chromium.launch(headless=False, args=args)


def konteks_baru(browser, headless: bool, blokir_gambar: bool):
    """Context + page baru. Satu context per hotel: konteks browser menumpuk
    dan satu context yang dipakai 781 hotel berakhir dengan 'Page crashed'.

    Pemblokiran resource hanya saat headless. Di headful Google Maps butuh
    font + image + media untuk merender panel ulasan, dan memblokirnya membuat
    kartu ulasan tidak pernah termuat.
    """
    if headless:
        context = browser.new_context(
            viewport={"width": 1366, "height": 768}, locale="id-ID")

        blokir = ["media", "font"]
        if blokir_gambar:
            blokir.append("image")

        def _route(route):
            if route.request.resource_type in blokir:
                route.abort()
            else:
                route.continue_()

        context.route("**/*", _route)
    else:
        context = browser.new_context(no_viewport=True, locale="id-ID")

    return context


def tutup_konteks(context) -> None:
    try:
        context.close()
    except Exception:
        pass  # context sudah tertutup / browser sudah crash


# ============================================================
# PENJADWALAN PER-HOTEL
# ============================================================

_LOG_PROGRES_N_HOTEL = 10
_LOG_PROGRES_N_DETIK = 60


def _durasi(detik: float) -> str:
    """Format detik menjadi HH:MM:SS."""
    total = int(detik)
    return f"{total // 3600:02d}:{total % 3600 // 60:02d}:{total % 60:02d}"


class _Progres:
    """Rekap progres run panjang untuk log INFO, aman dipakai lintas thread.

    Run gmaps_review bisa berjalan belasan jam; tanpa rekap ini satu-satunya
    penanda kemajuan hanya baris per hotel, sehingga kelihatannya macet.
    """

    def __init__(self, total: int, label: str) -> None:
        self.total = total
        self.label = label or "gmaps"
        self.selesai = 0
        self.ulasan = 0
        self.tanpa_ulasan = 0
        self.mulai = time.monotonic()
        self._kunci = Lock()
        self._log_terakhir = 0.0

    def catat(self, jumlah_ulasan: int) -> None:
        with self._kunci:
            self.selesai += 1
            self.ulasan += jumlah_ulasan
            if jumlah_ulasan == 0:
                self.tanpa_ulasan += 1
            sekarang = time.monotonic()
            # Cetak tiap 10 hotel, tiap menit, dan di hotel terakhir supaya
            # run yang macet di tengah tidak terlihat diam.
            if not (
                self.selesai % _LOG_PROGRES_N_HOTEL == 0
                or self.selesai >= self.total
                or sekarang - self._log_terakhir >= _LOG_PROGRES_N_DETIK
            ):
                return
            self._log_terakhir = sekarang
            log.info(
                "%s: %d/%d hotel selesai, %d tanpa ulasan, %d baris terkumpul, berjalan %s",
                self.label, self.selesai, self.total, self.tanpa_ulasan,
                self.ulasan, _durasi(sekarang - self.mulai),
            )


def jalankan_per_hotel(
    hotel_list: list[dict],
    worker,
    cfg: dict,
    kunci_worker: str,
    checkpoint: Checkpoint | None = None,
    titik_selesai: Callable[[list, dict], None] | None = None,
    label_progres: str = "",
    total_progres: int | None = None,
) -> list:
    """Jalankan `worker(context, hotel)` untuk tiap hotel memakai N browser paralel.

    Sync API Playwright tidak thread-safe, jadi tiap thread membuat
    `sync_playwright()` + chromium sendiri (1 worker = 1 proses Chromium).
    Hotel dipecah round-robin supaya beban tiap worker seimbang.

    Yang diteruskan adalah context, bukan page: saat renderer Chromium crash
    (`Page.goto: Page crashed`) page tidak bisa dihidupkan lagi dan harus
    dibuat ulang dari context yang sama.

    Args:
        worker: fungsi (context, hotel) -> list[dict]; exception di dalamnya
            ditangani di sini supaya 1 hotel gagal tidak menghentikan run.
        kunci_worker: nama key config jumlah worker, mis. 'harga_workers'.
        checkpoint: dicatat sekali per hotel selesai supaya `--lanjut` bisa
            melewati hotel yang sudah beres.
        titik_selesai: dipanggil (baris_hotel, hotel) tepat setelah satu hotel
            selesai, SEBELUM hotel ditandai di checkpoint. Dipakai scraper yang
            menyimpan hasil per hotel: kalau penyimpanannya gagal, hotel tidak
            ditandai dan akan diulang pada `--lanjut`.
        label_progres: nama sumber untuk baris log progres.
        total_progres: penyebut total untuk progres; default `len(hotel_list)`.
            Pada `--lanjut` hotel yang sudah beres tidak ikut dikirim ke worker,
            jadi total run penuh perlu diteruskan supaya bar progres tidak
            terlihat mundur.
    """
    n = max(1, min(int(cfg.get(kunci_worker, 1)), len(hotel_list)))
    n = min(n, len(hotel_list))

    shard: list[list[dict]] = [hotel_list[i::n] for i in range(n)]
    log.info("gmaps: %d hotel dibagi ke %d worker%s",
             len(hotel_list), n, "" if mau_headless(cfg) else " (headful, ada jendela browser)")
    collected: list = []
    progres = _Progres(total_progres or len(hotel_list), label_progres)

    def _jalan(daftar: list[dict], indeks: int) -> list:
        keluar: list = []
        with sync_playwright() as p:
            headless = mau_headless(cfg)
            browser = buka_browser(p, headless, indeks=indeks, jumlah=n)
            try:
                for hotel in daftar:
                    context = konteks_baru(browser, headless,
                                          bool(cfg.get("blokir_gambar", False)))
                    try:
                        baris = worker(context, hotel) or []
                    except Exception as exc:
                        baris = []
                        log.warning(
                            "gmaps: hotel %s gagal: %s: %s",
                            hotel.get("nama_hotel"), type(exc).__name__, exc,
                        )
                    finally:
                        tutup_konteks(context)
                    keluar.extend(baris)
                    if titik_selesai is not None:
                        # Gagal di sini (mis. staging tidak bisa ditulis) sengaja
                        # dibiarkan naik supaya hotel tidak ditandai selesai.
                        titik_selesai(baris, hotel)
                    if checkpoint is not None:
                        checkpoint.tandai(hotel.get("id_kompetitor"))
                    progres.catat(len(baris))
            finally:
                try:
                    browser.close()
                except Exception:
                    pass
        return keluar

    if n == 1:
        return _jalan(shard[0], 0)

    with ThreadPoolExecutor(max_workers=n) as ex:
        for hasil in ex.map(_jalan, shard, range(n)):
            collected.extend(hasil)
    return collected


# ============================================================
# UTILITAS TEKS
# ============================================================

def clean_price(text) -> int:
    digits = re.sub(r"[^\d]", "", str(text or ""))
    try:
        return int(digits) if digits else 0
    except (ValueError, TypeError):
        return 0


def normalize_text(text) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip().lower()


def is_similar(teks1, teks2, threshold=0.7) -> bool:
    return similarity(teks1, teks2) >= threshold


def similarity(teks1, teks2) -> float:
    """Skor kemiripan 0..1 antara dua nama.

    Substring dihitung sebagai 1.0 karena nama hotel di DB sering lebih lengkap
    ("Hotel Indonesia Kempinski") atau lebih pendek dari yang ditulis Google
    ("Indonesia Kempinski Jakarta").
    """
    a, b = normalize_text(teks1), normalize_text(teks2)
    if not a or not b:
        return 0.0
    if a in b or b in a:
        return 1.0
    return SequenceMatcher(None, a, b).ratio()


def build_search_query(nama_hotel, alamat=None, kota=None) -> str:
    """Query pencarian Google Maps.

    `kota` dari hotel_master sudah berupa kota saja (bagian kedua dari
    "kecamatan, kota"), sedangkan `alamat` memuat nama jalan, jadi keduanya
    tidak perlu digabung dua kali.
    """
    query = str(nama_hotel or "").strip()
    bagian = [
        str(b).strip() for b in (alamat, kota)
        if b and str(b).strip().lower() not in ("nan", "none", "")
    ]
    return f"{query}, {', '.join(bagian)}" if bagian else query


def bypass_cookie(page) -> None:
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


# ============================================================
# STEMPEL WAKTU ULASAN
# ============================================================

_SATUAN_HARI = {
    "jam": 1 / 24, "hour": 1 / 24, "hr": 1 / 24,
    "menit": 1 / 1440, "minute": 1 / 1440, "min": 1 / 1440,
    "hari": 1, "day": 1,
    "minggu": 7, "pekan": 7, "week": 7,
    "bulan": 30.44, "month": 30.44,
    "tahun": 365.25, "year": 365.25, "thn": 365.25,
}


def _satuan_hari(kata: str) -> float | None:
    """Nilai hari untuk satu satuan waktu, toleran jamaran ("months" -> "month")."""
    k = kata.strip()
    if k in _SATUAN_HARI:
        return _SATUAN_HARI[k]
    if k.endswith("s") and k[:-1] in _SATUAN_HARI:
        return _SATUAN_HARI[k[:-1]]
    return None


def convert_relative_time(teks, today: date | None = None) -> date | None:
    """Ubah stempel waktu ulasan Google Maps (relatif atau absolut) jadi date.

    Kembalikan None bila formatnya tidak dikenali. API lama hanya mengenali
    pola "N hari/bulan/tahun" sehingga ulasan berformat lain dibuang diam-diam
    dan hasil run terlihat kosong.
    """
    dasar = today or datetime.now().date()
    t = normalize_text(teks)
    if not t:
        return None

    # absolut Indonesia: "12 agustus 2025", "3 agt 2025"
    m = re.search(r"(\d{1,2})\s+([a-z]{3,10})\.?\s+(\d{4})", t)
    if m and m.group(2) in BULAN:
        return _tanggal(int(m.group(3)), BULAN[m.group(2)], int(m.group(1)))

    # absolut Inggris/Indonesia: "aug 12, 2025", "12 agt 2025"
    m = re.search(r"([a-z]{3,10})\.?\s+(\d{1,2}),?\s+(\d{4})", t)
    if m and m.group(1) in BULAN:
        return _tanggal(int(m.group(3)), BULAN[m.group(1)], int(m.group(2)))

    # numerik: "12/08/2025" atau "2025-08-12"
    m = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", t)
    if m:
        return _tanggal(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", t)
    if m:
        return _tanggal(int(m.group(3)), int(m.group(2)), int(m.group(1)))

    # relatif tanpa angka ("beberapa bulan lalu")
    if "kurang dari sebulan" in t or "less than a month" in t:
        return dasar - timedelta(days=15)
    if "beberapa bulan" in t or "several months" in t or "a few months" in t:
        return dasar - timedelta(days=60)
    if "beberapa minggu" in t or "several weeks" in t or "a few weeks" in t:
        return dasar - timedelta(days=21)
    if "beberapa tahun" in t or "several years" in t:
        return dasar - timedelta(days=730)

    if "baru saja" in t or "just now" in t or "moments ago" in t:
        return dasar
    if "hari ini" in t or "today" in t:
        return dasar
    if "kemarin" in t or "yesterday" in t:
        return dasar - timedelta(days=1)

    # "a week ago" -> "1 week ago"; "sebulan lalu" -> "1 bulan lalu"
    t = re.sub(r"\b(?:a|an|one)\s+(?=jam|hour|hr|menit|minute|min|hari|day|"
               r"minggu|pekan|week|bulan|month|tahun|year)", "1 ", t)
    t = (t.replace("sebulan", "1 bulan").replace("setahun", "1 tahun")
          .replace("seminggu", "1 minggu").replace("sehari", "1 hari"))
    # Google juga menulis satuan telanjang: "minggu lalu", "bulan lalu".
    # Negative lookbehind "(?<!\d )" menjaga "2 minggu lalu" tidak jadi
    # "2 1 minggu lalu".
    t = re.sub(r"(?<!\d )\b(minggu|pekan|bulan|tahun|hari)\s+lalu", r"1 \1 lalu", t)
    t = re.sub(r"(?<!\d )\b(week|month|year|day)s?\s+ago", r"1 \1 ago", t)

    # "2 tahun 3 bulan lalu" berisi lebih dari satu satuan, jadi dijumlahkan.
    total_hari = 0.0
    ketemu = False
    for angka, satuan in re.findall(r"(\d+)\s*([a-z]+)", t):
        hari = _satuan_hari(satuan)
        if hari is None:
            continue
        total_hari += int(angka) * hari
        ketemu = True
    if ketemu:
        return dasar - timedelta(days=round(total_hari))
    return None


def _tanggal(tahun: int, bulan: int, hari: int) -> date | None:
    try:
        return date(tahun, bulan, hari)
    except ValueError:
        return None


def ambil_waktu_relatif(block) -> str:
    """Stempel waktu relatif di header kartu ulasan."""
    for sel in (
        f'{SEL_TAB_ULASAN.split(",")[0]} span',
        'span[role="img"][aria-label*="bintang"] ~ span',
        'span[data-attrid="text"]',
        'span[class*="rsqaWe"]',
        'span[class*="status"]',
    ):
        try:
            el = block.locator(sel)
            if el.count() > 0:
                teks = el.first.inner_text().strip()
                if teks and not re.fullmatch(r"[\d.,]+", teks):
                    return teks
        except Exception:
            continue
    try:
        seluruh = block.inner_text()
    except Exception:
        return ""
    m = re.search(
        r"(?:kurang dari\s*)?(?:\d+\s*)?"
        r"(?:baru saja|hari ini|kemarin|jam|menit|hari|minggu|pekan|bulan|tahun)"
        r"\s*(?:lalu|yang lalu)?",
        seluruh.lower(),
    )
    return m.group(0).strip() if m else ""


def ambil_bintang(block) -> int:
    """Rating bintang 1-5 dari kartu ulasan (0 bila tidak terbaca)."""
    for sel in (
        'span[role="img"][aria-label*="bintang"], span[role="img"][aria-label*="star"], '
        'span[role="img"][aria-label*="/5"]',
        '[role="img"][aria-label*="bintang"], [role="img"][aria-label*="star"], '
        '[role="img"][aria-label*="/5"]',
    ):
        try:
            el = block.locator(sel)
            for j in range(el.count()):
                rating = _rating_dari_teks(el.nth(j).get_attribute("aria-label") or "")
                if rating:
                    return rating
        except Exception:
            continue

    try:
        n = block.evaluate(
            """() => {
                const svgs = document.querySelectorAll('span.kvMYJc svg, span[jsaction] svg');
                let filled = 0;
                for (const svg of svgs) {
                    const paths = svg.querySelectorAll('path');
                    for (const p of paths) {
                        const fill = (p.getAttribute('fill') || '').toLowerCase()
                            .replace(/\\s/g, '');
                        if (fill && fill !== 'none' && fill !== 'inherit') { filled++; break; }
                    }
                }
                return svgs.length ? filled : 0;
            }"""
        )
        if 1 <= n <= 5:
            return int(n)
    except Exception:
        pass

    try:
        el = block.locator('[data-rating]')
        if el.count() > 0:
            rating = int(el.first.get_attribute("data-rating") or 0)
            if 1 <= rating <= 5:
                return rating
    except Exception:
        pass

    # Terakhir: rating di kartu ulasan Google ditulis sebagai TEKS polos
    # ("1/5", "4,5 dari 5"), bukan atribut aria-label. Tanpa langkah ini semua
    # rating jadi 0.
    try:
        return _rating_dari_teks(block.inner_text() or "")
    except Exception:
        return 0


def _rating_dari_teks(teks: str) -> int:
    m = re.search(r"([1-5])(?:\s*[,.]\d*)?\s*(?:/|dari|of|out\s*of)\s*5", teks or "", re.I)
    if not m:
        m = re.search(r"(\d)\s*(?:dari|out)?\s*\d?\s*(?:bintang|star)", teks or "", re.I)
    return int(m.group(1)) if m else 0


def extract_review_count(text) -> int:
    """Jumlah ulasan dari teks kartu hasil (dipakai untuk peringkat kandidat).

    Kartu hasil Google menulis jumlah ulasan dalam kurung: "(713)", "(1.208)",
    "(9.805)". Versi lama hanya menerima angka ber thousands separator ("1.208")
    sehingga "(713)" terbaca 0; karena itu `punya_ulasan` selalu False dan
    pengurutan kandidat tidak pernah membedakan kandidat ber-ulasan.
    """
    t = str(text or "").lower()
    m = re.search(r"([\d.,]+)\s*(?:ulasan|reviews|review)", t)
    if not m:
        # Angka di dalam kurung, dengan atau tanpa thousands separator.
        m = re.search(r"\((\d{1,3}(?:[.,]\d{3})*)\)", t)
    if not m:
        return 0
    return _angka(m.group(1))


def _angka(teks) -> int:
    """'1.208' / '9,805' -> 1208 / 9805. Separator ribuan dihapus apa adanya."""
    try:
        return int(re.sub(r"[.,]", "", str(teks)))
    except ValueError:
        return 0


# ============================================================
# NAVIGASI GOOGLE MAPS
# ============================================================

def url_search_maps(query: str, latitude=None, longitude=None) -> str:
    """URL pencarian Google Maps.

    Bentuk `/maps/search/{query}/@lat,lon,19z` diambil dari `Review_hotel.py`
    yang sudah terbukti menghasilkan ulasan. Bentuk `?api=1&query=` yang
    sebelumnya dipakai mendarat di daftar hasil dan tidak pernah memunculkan
    panel ulasan. `19z` adalah tingkat zoom (sangat dekat).
    """
    q = quote_plus(query)
    if latitude is not None and longitude is not None:
        return f"https://www.google.com/maps/search/{q}/@{latitude},{longitude},19z"
    return f"https://www.google.com/maps/search/{q}"


def page_crash(exc: Exception) -> bool:
    """True bila error berasal dari renderer Chromium yang mati/bClosed.

    Yang penting: bedakan crash (butuh page baru) dari error biasa (cukup
    coba ulang), supaya log tidak berbohong soal penyebab kegagalan.
    """
    pesan = str(exc).lower()
    return any(k in pesan for k in (
        "page crashed", "target closed", "target page, context or browser has been closed",
        "browser has been closed", "connection closed", "session closed",
        "target crashed", "crashed",
    ))


def goto_robust(context, url: str, tries: int = 3,
                goto_timeout_ms: int = 2000, goto_timeout_retry_ms: int = 4000):
    """Buka `url` dan kembalikan page yang masih hidup.

    Page crash tidak bisa di-recover, jadi page dibuat ulang dari context yang
    sama. Error non-crash cukup dicoba ulang di page yang sama.

    Timeout navigation dibuat dua tingkat: percobaan pertama `goto_timeout_ms`
    (default 2s) karena halaman Maps/Travel berat dan `goto` bisa menggantung
    puluhan detik; percobaan terakhir memakai `goto_timeout_retry_ms`
    (default 4s) sebagai kelonggaran agar tidak kehilangan semua data ketika
    Google sedang lambat.
    """
    percobaan = 0
    n = max(1, tries)
    while percobaan < n:
        percobaan += 1
        batas_ms = goto_timeout_retry_ms if percobaan == n else goto_timeout_ms
        page = context.new_page()
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=batas_ms)
            bypass_cookie(page)
            return page
        except Exception as exc:
            try:
                page.close()
            except Exception:
                pass
            log.debug("goto percobaan %d/%d gagal (%s: %s)",
                      percobaan, n, type(exc).__name__, exc)
            if percobaan >= n:
                raise
            time.sleep(1.5 * percobaan)
    raise RuntimeError(f"tidak bisa membuka {url}")


def timeout_dari_cfg(cfg: dict | None, kunci: str, default: int) -> int:
    """Ambil timeout (ms) dari config sumber, jatuh ke `default` bila invalid."""
    try:
        nilai = int((cfg or {}).get(kunci))
    except (TypeError, ValueError):
        return default
    return nilai if nilai > 0 else default