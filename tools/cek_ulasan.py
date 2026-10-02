"""Cek visual: apakah Google Maps masih menampilkan daftar ulasan hotel?

Dipakai saat selector gmaps_review gagal dan perlu dilihat langsung apa yang
benar-benar dirender Maps untuk satu hotel. Browser sengaja dibiarkan terbuka
sampai user menekan Enter supaya tab "Ulasan" (jika ada) bisa diperiksa mata.

Contoh:
    python tools/cek_ulasan.py
    python tools/cek_ulasan.py "Hotel Indonesia Kempinski"
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from src.extract.gmaps_browser import (  # noqa: E402
    buka_browser,
    goto_robust,
    konteks_baru,
    sync_playwright,
    url_search_maps,
    build_search_query,
    tutup_konteks,
)
from src.shared.config import get_source_config  # noqa: E402
from src.shared.hotel_master import daftar_hotel  # noqa: E402

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")

DEFAULT_HOTEL = "Hotel Indonesia Kempinski"


def main() -> None:
    nama = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_HOTEL
    cfg = get_source_config("gmaps_review")
    hotel = next((h for h in daftar_hotel(cfg) if nama.lower() in h["nama_hotel"].lower()), None)
    if hotel is None:
        raise SystemExit(f"hotel '{nama}' tidak ada di hotel_master")

    print("HOTEL :", hotel["nama_hotel"])
    print("ALAMAT:", hotel["alamat"], "|", hotel["kota"])
    print("SRC   :", url_search_maps(
        build_search_query(hotel["nama_hotel"], hotel["alamat"], hotel["kota"]),
        hotel["latitude"], hotel["longitude"]))

    with sync_playwright() as p:
        browser = buka_browser(p, headless=False)
        context = konteks_baru(browser, headless=False,
                              blokir_gambar=False)
        try:
            page = goto_robust(context, url_search_maps(
                build_search_query(hotel["nama_hotel"], hotel["alamat"], hotel["kota"]),
                hotel["latitude"], hotel["longitude"]))
            page.wait_for_timeout(6000)

            print("\n--- yang dirender Maps ---")
            print("judul   :", page.title()[:90])
            print("h1      :", [page.locator("h1").nth(i).inner_text()[:45]
                              for i in range(min(2, page.locator("h1").count()))])
            print("tab     :", page.locator('button[role="tab"]').all_inner_texts())
            print("ulasan  :", page.locator("div[data-review-id]").count(), "kartu",
                  "| jftiEf:", page.locator("div.jftiEf").count(),
                  "| tombol 'Semua ulasan':",
                  page.locator('button:has-text("Semua ulasan")').count())
            chip = page.locator('span[aria-label*="bintang"]')
            print("rating  :", [chip.nth(i).get_attribute("aria-label")
                               for i in range(min(chip.count(), 8))])

            print("\nLihat jendela browser: apakah tab Ulasan / daftar ulasan ada?")
            input("Tekan Enter untuk menutup browser... ")
        finally:
            tutup_konteks(context)
            browser.close()
    print("Selesai.")


if __name__ == "__main__":
    main()
