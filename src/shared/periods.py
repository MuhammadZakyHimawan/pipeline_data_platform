from __future__ import annotations

import re
from datetime import datetime


def resolve_period(period: str | None) -> tuple[str, list[int]]:
    """Normalisasi periode.

    Returns:
        (period_label, years)
        - period_label: label untuk nama file di data_lake (stabil)
        - years: daftar tahun untuk BPS (fetch per tahun)

    Didukung:
        - "latest" / None  -> tahun berjalan (periode terbaru)
        - "2025"           -> satu tahun
        - "2020-2026"      -> rentang tahun (backfill)
    """
    now_year = datetime.now().year

    if period is None or period.strip().lower() in ("", "latest", "terbaru"):
        return str(now_year), [now_year]

    period = period.strip()

    if re.fullmatch(r"\d{4}", period):
        y = int(period)
        return str(y), [y]

    if re.fullmatch(r"\d{4}-\d{4}", period):
        a, b = map(int, period.split("-"))
        if a > b:
            raise ValueError(f"rentang periode tidak valid: {period}")
        return f"{a}-{b}", list(range(a, b + 1))

    raise ValueError(
        f"periode tidak dikenal: '{period}'. Gunakan 'latest', 'YYYY', atau 'YYYY-YYYY'."
    )


def resolve_years(period: str | None) -> list[int]:
    """Urai spec periode menjadi daftar tahun tanpa batas atas tahun berjalan.

    Dipakai sumber yang menulis satu file per tahun dan butuh daftar tahun
    eksplisit (mis. backfill BPS '2020-2024'):
        - "2025"        -> [2025]
        - "2020-2024"   -> [2020, 2021, 2022, 2023, 2024]
        - None/latest   -> tahun berjalan
    """
    now_year = datetime.now().year

    if period is None or period.strip().lower() in ("", "latest", "terbaru"):
        return [now_year]

    period = period.strip()

    if re.fullmatch(r"\d{4}", period):
        return [int(period)]

    if re.fullmatch(r"\d{4}-\d{4}", period):
        a, b = map(int, period.split("-"))
        if a > b:
            raise ValueError(f"rentang periode tidak valid: {period}")
        return list(range(a, b + 1))

    raise ValueError(
        f"periode tidak dikenal: '{period}'. Gunakan 'latest', 'YYYY', atau 'YYYY-YYYY'."
    )


def resolve_months(period: str | None) -> list[str]:
    """Urai spec periode menjadi daftar label bulan 'YYYY-MM'.

    Dipakai sumber dengan grain per bulan (mis. gmaps_review) supaya tiap run
    menulis file terpisah dan riwayat bulan sebelumnya tidak tertimpa:

        None/latest        -> ['2026-10'] (bulan berjalan)
        '2026-10'          -> ['2026-10']
        '2026'             -> 12 bulan sepanjang 2026
        '2026-08/2026-10'  -> ['2026-08', '2026-09', '2026-10']
    """
    now = datetime.now()

    if period is None or period.strip().lower() in ("", "latest", "terbaru"):
        return [f"{now.year:04d}-{now.month:02d}"]

    period = period.strip()

    if re.fullmatch(r"\d{4}", period):
        return [f"{int(period):04d}-{m:02d}" for m in range(1, 13)]

    if re.fullmatch(r"\d{4}-\d{2}", period):
        _validasi_bulan(period)
        return [period]

    m = re.fullmatch(r"(\d{4})-(\d{2})/(\d{4})-(\d{2})", period)
    if m:
        _validasi_bulan(f"{m.group(1)}-{m.group(2)}")
        _validasi_bulan(f"{m.group(3)}-{m.group(4)}")
        awal, akhir = (int(m.group(1)), int(m.group(2))), (int(m.group(3)), int(m.group(4)))
        if awal > akhir:
            raise ValueError(f"rentang periode tidak valid: {period}")
        keluar, tahun, bulan = [], awal[0], awal[1]
        while (tahun, bulan) <= akhir:
            keluar.append(f"{tahun:04d}-{bulan:02d}")
            tahun, bulan = (tahun + 1, 1) if bulan == 12 else (tahun, bulan + 1)
        return keluar

    raise ValueError(
        f"periode tidak dikenal: '{period}'. "
        "Gunakan 'latest', 'YYYY', 'YYYY-MM', atau 'YYYY-MM/YYYY-MM'."
    )


def _validasi_bulan(periode: str) -> None:
    bulan = int(periode.split("-")[1])
    if not 1 <= bulan <= 12:
        raise ValueError(f"bulan tidak valid: '{periode}'. Bulan harus 01-12.")