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