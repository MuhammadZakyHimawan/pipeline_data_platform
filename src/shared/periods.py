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
        - "latest" / None     -> tahun berjalan (periode terbaru)
        - "2025"              -> satu tahun
        - "2020-2026"         -> rentang tahun (backfill)
        - "2026-10-21"        -> tanggal spesifik (mode harian)
    """
    now_year = datetime.now().year

    if period is None or period.strip().lower() in ("", "latest", "terbaru"):
        return str(now_year), [now_year]

    period = period.strip()

    # Handle YYYY-MM-DD (single day from daily mode)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", period):
        return period, [int(period[:4])]

    if re.fullmatch(r"\d{4}", period):
        y = int(period)
        return str(y), [y]

    if re.fullmatch(r"\d{4}-\d{4}", period):
        a, b = map(int, period.split("-"))
        if a > b:
            raise ValueError(f"rentang periode tidak valid: {period}")
        return f"{a}-{b}", list(range(a, b + 1))

    raise ValueError(
        f"periode tidak dikenal: '{period}'. Gunakan 'latest', 'YYYY', 'YYYY-YYYY', atau 'YYYY-MM-DD'."
    )


def resolve_years(period: str | None) -> list[int]:
    """Urai spec periode menjadi daftar tahun tanpa batas atas tahun berjalan.

    Dipakai sumber yang menulis satu file per tahun dan butuh daftar tahun
    eksplisit (mis. backfill BPS '2020-2024'):
        - "2025"           -> [2025]
        - "2020-2024"      -> [2020, 2021, 2022, 2023, 2024]
        - "2026-10-21"     -> [2026] (tanggal spesifik, ambil tahun nya)
        - None/latest      -> tahun berjalan
    """
    now_year = datetime.now().year

    if period is None or period.strip().lower() in ("", "latest", "terbaru"):
        return [now_year]

    period = period.strip()

    # Handle YYYY-MM-DD (single day from daily mode) - extract year
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", period):
        return [int(period[:4])]

    if re.fullmatch(r"\d{4}", period):
        return [int(period)]

    if re.fullmatch(r"\d{4}-\d{4}", period):
        a, b = map(int, period.split("-"))
        if a > b:
            raise ValueError(f"rentang periode tidak valid: {period}")
        return list(range(a, b + 1))

    raise ValueError(
        f"periode tidak dikenal: '{period}'. Gunakan 'latest', 'YYYY', 'YYYY-YYYY', atau 'YYYY-MM-DD'."
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


def get_start_year(source: str, default: int = 2014) -> int:
    """Ambil start_year dari config sources.yaml untuk sumber tertentu."""
    from .config import get_source_config
    try:
        return int(get_source_config(source).get("start_year", default))
    except (TypeError, ValueError):
        return default


def resolve_daily_period(source: str) -> str:
    """Return period string for DAILY mode based on source type.
    
    Daily mode = only process today (or yesterday for GDELT).
    """
    from datetime import date, timedelta
    from .run_options import get_historis
    
    # If historis mode is explicitly set, return None to use default/historical logic
    if get_historis():
        return None
    
    today = date.today()
    
    if source == "gdelt":
        # GDELT: yesterday's data (today's data usually incomplete)
        return (today - timedelta(days=1)).strftime("%Y%m%d")
    elif source == "kurs":
        # Kurs: single day (today)
        return today.isoformat()
    elif source == "cuaca":
        # Cuaca: single day (today)
        return today.isoformat()
    elif source == "trends":
        # Trends: single day range (today today)
        return f"{today.isoformat()} {today.isoformat()}"
    elif source == "bps":
        # BPS: current year only (no daily granularity)
        return str(today.year)
    elif source == "event":
        # Event: today only
        return today.isoformat()
    elif source in ("poi", "transport"):
        # POI/Transport: snapshot sources, skip in daily mode
        return "skip"
    elif source == "hari_libur":
        # Hari libur: current year only in daily/tahunan mode
        return str(today.year)
    elif source == "traveloka_review":
        # Traveloka review: current month only
        return f"{today.year:04d}-{today.month:02d}"
    elif source == "gmaps":
        # Gmaps: ignores period, uses date.today() internally for lead-times
        return None
    
    return None


def resolve_historical_period(source: str) -> str:
    """Return period string for HISTORICAL mode based on source type.
    
    Historical mode = full backfill from start_year to today.
    """
    from datetime import date
    from .run_options import get_historis
    
    # Only apply if historis mode is set
    if not get_historis():
        return None
    
    today = date.today()
    start_year = get_start_year(source)
    
    if source == "gdelt":
        # GDELT: full range from start_year
        return f"{start_year}-{today.year}"
    elif source == "kurs":
        return f"{start_year}-{today.year}"
    elif source == "cuaca":
        return f"{start_year}-{today.year}"
    elif source == "trends":
        # Trends: use config range or default 10 years
        from .config import get_source_config
        cfg = get_source_config(source)
        cfg_range = str(cfg.get("range") or "").strip()
        if cfg_range:
            return cfg_range
        # Default 10 years back
        return f"{today.year - 10}-{today.year}"
    elif source == "bps":
        return f"{start_year}-{today.year}"
    elif source == "event":
        return f"{start_year}-{today.year}"
    elif source in ("poi", "transport"):
        # POI/Transport: full refresh
        return "latest"
    elif source == "hari_libur":
        return "latest"  # Full ICS parse
    elif source == "traveloka_review":
        # Traveloka review: all months from start_year
        return f"{start_year}-{today.year}"
    elif source == "gmaps":
        # Gmaps: no historical concept, uses date.today() internally
        return None
    
    return "latest"