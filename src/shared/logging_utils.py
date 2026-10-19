from __future__ import annotations

import logging
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_LOG_DIR = PROJECT_ROOT / "log"

FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# konsol: hanya peringatan & di atasnya; detail (DEBUG/INFO) tersimpan di file log/
_CONSOLE_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"
_CONSOLE_DATE_FORMAT = "%H:%M:%S"

_CONSOLE_LEVEL = logging.WARNING
_FILE_LEVEL = logging.DEBUG

# src.<stage>.<modul> -> stage & sumber
_STAGE_RE = re.compile(r"^src\.(bronze|silver|gold)\.(.+)$")
_SUFFIX_RE = re.compile(r"_(scraper|transform|load)$")

# logger pihak ketiga yang bising; diamkan agar file log fokus ke kode pipeline
_NOISY_LOGGERS = ("urllib3", "requests", "http.client", "asyncio", "playwright", "websockets")

_CONFIGURED = False


def sumber_dari_logger(name: str) -> tuple[str, str] | None:
    """Petakan nama logger modul pipeline ke (stage, sumber); None bila bukan.

    - src.bronze.bps_scraper             -> ('bronze', 'bps')
    - src.silver.hari_libur_transform   -> ('silver', 'hari_libur')
    - src.gold.gmaps_harga_load          -> ('gold', 'gmaps_harga')
    - src.shared.storage / root / ''      -> None (bukan script pipeline)

    Dipakai `_record_target` untuk menentukan file log per stage & sumber.
    """
    m = _STAGE_RE.match(name or "")
    if not m:
        return None
    stage, modul = m.group(1), m.group(2)
    return stage, (_SUFFIX_RE.sub("", modul) or modul)


def _record_target(name: str, log_dir: Path) -> Path:
    """Tentukan file log target dari nama logger.

    - src.bronze.bps_scraper          -> <log_dir>/bronze/bps.log
    - src.silver.hari_libur_transform -> <log_dir>/silver/hari_libur.log
    - src.gold.gmaps_harga_load       -> <log_dir>/gold/gmaps_harga.log
    - selain itu (root, src.shared.*)  -> <log_dir>/pipeline.log
    """
    kunci = sumber_dari_logger(name)
    if kunci:
        stage, sumber = kunci
        return log_dir / stage / f"{sumber}.log"
    return log_dir / "pipeline.log"


class ConsoleFormatter(logging.Formatter):
    """Formatter konsol: tambahkan petunjuk file log-nya di akhir pesan."""

    def format(self, record: logging.LogRecord) -> str:
        msg = super().format(record)
        try:
            target = _record_target(record.name, DEFAULT_LOG_DIR).resolve()
            hint = target.relative_to(PROJECT_ROOT).as_posix()
        except (ValueError, OSError):
            hint = (DEFAULT_LOG_DIR / "pipeline.log").as_posix()
        return f"{msg} [detail: {hint}]"


class StageSourceHandler(logging.Handler):
    """Tulis record ke file per stage & per sumber berdasarkan nama logger."""

    def __init__(self, log_dir: Path, level: int = _FILE_LEVEL):
        super().__init__(level)
        self.log_dir = Path(log_dir)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            target = _record_target(record.name, self.log_dir)
            target.parent.mkdir(parents=True, exist_ok=True)
            with open(target, "a", encoding="utf-8") as f:
                f.write(self.format(record) + "\n")
        except Exception:
            self.handleError(record)


def setup_logging(
    level: int = _FILE_LEVEL,
    log_dir: Path | str | None = None,
    console_level: int = _CONSOLE_LEVEL,
) -> logging.Logger:
    """Konfigurasi root logger sekali saja.

    Konsol : hanya WARNING & di atasnya (progres dipakai jalur print/echo, bukan log).
    File   : DEBUG, terpisah per stage & per sumber di <log_dir>
    """
    global _CONFIGURED
    if _CONFIGURED:
        return logging.getLogger("hotel_data_platform")

    log_dir = Path(log_dir) if log_dir is not None else DEFAULT_LOG_DIR

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()

    fmt = logging.Formatter(FORMAT, datefmt=DATE_FORMAT)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(console_level)
    console.setFormatter(ConsoleFormatter(_CONSOLE_FORMAT, datefmt=_CONSOLE_DATE_FORMAT))
    root.addHandler(console)

    file_handler = StageSourceHandler(log_dir, level=_FILE_LEVEL)
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)

    for noisy in _NOISY_LOGGERS:
        logging.getLogger(noisy).setLevel(logging.WARNING)

    # logger aplikasi: propagasi ke root agar record terserap handler file
    app = logging.getLogger("hotel_data_platform")
    app.setLevel(level)
    app.propagate = True

    _CONFIGURED = True
    return app