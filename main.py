from __future__ import annotations

import os
import re
import sys
import time
from typing import Optional

from src.parallel import STAGES, cetak_ringkasan, jalankan_paralel
from src.pipeline import (
    get_registered_sources,
    get_stage_path,
    run_pipeline,
    run_stage,
)
from src.shared.db import ensure_schema
from src.shared.config import get_db_config, get_source_config
from src.shared.dataset_filter import get_dataset_filter, set_dataset_filter
from src.shared.live_view import RencanaBaris, TampilanPipeline, dukung_live
from src.shared.logging_utils import setup_logging
from src.shared.row_limit import get_row_limit, set_row_limit

try:
    import typer

    HAS_TYPER = True
except ImportError:
    typer = None  # type: ignore
    HAS_TYPER = False

app = None
if HAS_TYPER:
    app = typer.Typer(
        help=(
            "Data pipeline hotel_data_platform (Medallion: bronze->silver->gold). "
            "Tanpa sub-perintah: jalankan SEMUA sumber secara PARALEL (4 worker, "
            "override lewat env HOTEL_WORKERS). Gunakan 'run <sumber>' utk satu sumber."
        )
    )
else:
    import argparse


if HAS_TYPER:

    @app.command("list")
    def list_sources() -> None:
        """Daftar sumber data yang tersedia."""
        typer.echo("Tersedia: " + ", ".join(get_registered_sources()))

    @app.command("db-init")
    def db_init() -> None:
        """Buat schema gold di PostgreSQL (dari config/.env)."""
        config = get_db_config()
        ensure_schema(config["schema_gold"])
        typer.echo(f"Schema '{config['schema_gold']}' siap di {config['host']}:{config['port']}/{config['name']}")

    @app.command("run")
    def run(
        source: str = typer.Argument(..., help="Nama sumber (contoh: bps)"),
        stage: str = typer.Option("all", help="all | extract | transform | load"),
        period: Optional[str] = typer.Option(None, help="latest | YYYY | YYYY-YYYY"),
        dry_run: bool = typer.Option(False, "--dry-run", help="Tanpa menulis file/DB"),
    ) -> None:
        """Jalankan pipeline untuk satu sumber."""
        logger = setup_logging()
        try:
            if stage == "all":
                summary = run_pipeline(source, period=period, dry_run=dry_run)
                logger.info("Pipeline '%s' selesai: %s", source, summary)
            elif stage in ("extract", "transform", "load"):
                result = run_stage(source, stage, period=period, dry_run=dry_run)
                logger.info("Stage '%s' '%s' selesai: %s", stage, source, {k: len(v) for k, v in result.items()})
            else:
                raise typer.BadParameter("stage harus all|extract|transform|load", param_hint="stage")
        except Exception as exc:
            typer.secho(f"ERROR: {exc}", fg=typer.colors.RED)
            raise typer.Exit(1)


def _run_argparse() -> None:
    parser = argparse.ArgumentParser(description="Data pipeline hotel_data_platform")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list", help="daftar sumber")
    sub.add_parser("db-init", help="buat schema gold di PostgreSQL")

    p_run = sub.add_parser("run", help="jalankan pipeline satu sumber")
    p_run.add_argument("source", help="nama sumber (bps)")
    p_run.add_argument("--stage", choices=["all", "extract", "transform", "load"], default="all")
    p_run.add_argument("--period", default=None, help="latest | YYYY | YYYY-YYYY")
    p_run.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()
    logger = setup_logging()

    if args.command == "list":
        print("Tersedia:", ", ".join(get_registered_sources()))
    elif args.command == "db-init":
        config = get_db_config()
        ensure_schema(config["schema_gold"])
        print(f"Schema '{config['schema_gold']}' siap di {config['host']}:{config['port']}/{config['name']}")
    elif args.command == "run":
        try:
            if args.stage == "all":
                summary = run_pipeline(args.source, period=args.period, dry_run=args.dry_run)
                logger.info("Pipeline '%s' selesai: %s", args.source, summary)
            else:
                result = run_stage(args.source, args.stage, period=args.period, dry_run=args.dry_run)
                logger.info("Stage '%s' '%s' selesai: %s", args.stage, args.source, {k: len(v) for k, v in result.items()})
        except Exception as exc:
            logger.error("ERROR: %s", exc)
            raise SystemExit(1)


ORDERSUMBER = [
    "bps",
    "kurs",
    "hari_libur",
    "gdelt",
    "cuaca",
    "event",
    "poi",
    "trends",
    "transport",
    "gmaps",
]

WORKERS_DEFAULT = 4

_LIMIT_ONLY_RE = re.compile(r"^\[(\d+)\]$")
_PAREN_LIMIT_RE = re.compile(r"^\((\w+)\[(\d+)\]\)$")
_PAREN_SRC_RE = re.compile(r"^\((\w+)\)$")
_SRC_LIMIT_RE = re.compile(r"^(\w+)\[(\d+)\]$")
_SRC_ONLY_RE = re.compile(r"^(\w+)$")

# Flag tahap untuk jalur pintasan: '--bronze' = jalankan stage extract saja.
# Ditabulasi dengan pintasan sumber/limit, mis. `main.py gmaps[5] --bronze`.
_STAGE_FLAG = {
    "--bronze": ("extract",),
}

# Flag sumber: '--<sumber>' untuk semua sumber di ORDERSUMBER, mis. --gmaps.
_SUMBER_FLAG = {f"--{s}": s for s in ORDERSUMBER}

# Flag dataset: '--<dataset>' membatasi sumber hanya meng-scrape dataset itu,
# mis. `--harga` -> gmaps hanya ambil harga, tanpa ulasan.
_DATASET_FLAG = {
    "--harga": "harga",
    "--review": "review",
}


class OpsiCLI:
    """Hasil urai argumen pintasan CLI."""

    def __init__(self) -> None:
        self.sisa: list[str] = []
        self.stages: tuple[str, ...] | None = None
        self.sumber: str | None = None
        self.datasets: tuple[str, ...] = ()

    @property
    def pakai_pintasan(self) -> bool:
        return self.stages is not None or self.sumber is not None or bool(self.datasets)


def _urai_flag(args: list[str]) -> OpsiCLI:
    """Pisahkan flag pintasan (tahap/sumber/dataset) dari argumen lain.

    Flag yang dikenali:
      --bronze        -> stage extract saja
      --<sumber>      -> jalankan satu sumber, mis. --gmaps
      --<dataset>     -> batasi dataset, mis. --harga

    Tanpa flag yang dikenali, `sisa` dikembalikan utuh dan stages=None sehingga
    jalur typer serta pintasan lama (positional) tidak berubah.
    """
    opsi = OpsiCLI()
    terpilih: list[str] = []
    dataset_terpilih: list[str] = []
    for arg in args:
        kunci = arg.strip().lower()
        tahap = _STAGE_FLAG.get(kunci)
        if tahap is not None:
            terpilih.extend(tahap)
            continue
        sumber = _SUMBER_FLAG.get(kunci)
        if sumber is not None:
            if opsi.sumber is not None and opsi.sumber != sumber:
                sys.stderr.write(
                    f"ERROR: dua sumber sekaligus (--{opsi.sumber} dan --{sumber}). "
                    f"Pilih satu, atau jalankan tanpa flag untuk semua sumber.\n"
                )
                raise SystemExit(1)
            opsi.sumber = sumber
            continue
        dataset = _DATASET_FLAG.get(kunci)
        if dataset is not None:
            dataset_terpilih.append(dataset)
            continue
        opsi.sisa.append(arg)

    # Validasi "dataset wajib ada sumber" ditunda ke _jalankan_pintasan supaya
    # positional (mis. `main.py gmaps --harga`) ikut terperhitungkan.
    if terpilih:
        opsi.stages = tuple(s for s in STAGES if s in terpilih)
    elif dataset_terpilih:
        # Filter dataset hanya bermakna di extract; tanpa --bronze, tetapkan
        # extract supaya transform/load lama tidak ikut jalan dan gagal.
        opsi.stages = ("extract",)
    opsi.datasets = tuple(dict.fromkeys(dataset_terpilih))
    return opsi


def _parse_shortcut(args: list[str]) -> Optional[tuple[Optional[str], Optional[int]]]:
    """Urai argumen pintasan CLI; None bila bukan pintasan (diteruskan ke typer).

    Dikenali (satu argumen tunggal):
      [N]             -> semua sumber, batas N baris/dataset
      (sumber)        -> satu sumber, tanpa batas
      (sumber[N])     -> satu sumber, batas N baris
      sumber          -> satu sumber, tanpa batas (PS-native, tanpa kurung)
      sumber[N]       -> satu sumber, batas N baris
    """
    if len(args) != 1:
        return None
    arg = args[0].strip()
    m = _PAREN_LIMIT_RE.match(arg)
    if m:
        return m.group(1), int(m.group(2))
    m = _LIMIT_ONLY_RE.match(arg)
    if m:
        return None, int(m.group(1))
    m = _PAREN_SRC_RE.match(arg)
    if m:
        return m.group(1), None
    m = _SRC_LIMIT_RE.match(arg)
    if m:
        return m.group(1), int(m.group(2))
    m = _SRC_ONLY_RE.match(arg)
    if m and m.group(1) in get_registered_sources():
        return m.group(1), None
    return None


def _piih_sumber(pilihan: Optional[list[str]] = None) -> tuple[list[str], list[str]]:
    """Tentukan sumber enabled; disabled/absent tercatat sebagai SKIP."""
    himpunan_pilihan = set(pilihan) if pilihan else None
    sumber_aktif: list[str] = []
    skips: list[str] = []
    for sumber in ORDERSUMBER:
        if himpunan_pilihan is not None and sumber not in himpunan_pilihan:
            continue
        try:
            cfg = get_source_config(sumber)
        except KeyError as exc:
            skips.append(f"  [SKIP] {sumber:12s} -> tidak ada di sources.yaml ({exc})")
            continue
        if not cfg.get("enabled", True):
            skips.append(f"  [SKIP] {sumber:12s} -> disabled di config/sources.yaml")
            continue
        sumber_aktif.append(sumber)
    return sumber_aktif, skips


def _run_sources(
    pilihan: Optional[list[str]] = None,
    stages: Optional[tuple[str, ...]] = None,
) -> None:
    """Jalankan sumber enabled per stage secara serial.

    pilihan=None  -> seluruh sumber; pilihan=[...] -> hanya sumber tsb.
    stages=None   -> extract -> transform -> load; mis. ('extract',) utk --bronze.
    """
    logger = setup_logging()

    if HAS_TYPER:
        echo, secho, colors = typer.echo, typer.secho, typer.colors
    else:
        echo, colors = print, None

        def secho(text: str, **kwargs):
            print(text)

    stages = stages or STAGES
    limit = get_row_limit()
    sumber_aktif, skips = _piih_sumber(pilihan)
    filter_dataset = get_dataset_filter()
    if filter_dataset:
        echo(f"  (dataset dibatasi: {', '.join(filter_dataset)})")

    hasil_per_sumber: dict[str, dict] = {}
    gagal: list[str] = []

    # Tampilan live (konsisten dengan jalur paralel); fallback cetak baris per baris.
    rencana = [
        RencanaBaris(sumber=s, stage=st, path=get_stage_path(s, st))
        for s in sumber_aktif
        for st in stages
    ]
    tampilan = TampilanPipeline(rencana) if dukung_live() else None
    catatan_uji = None
    if limit is not None:
        catatan_uji = f"  (mode uji: maks {limit} baris per dataset; LOAD tidak menulis ke database)"
    if tampilan is not None:
        if catatan_uji:
            echo(catatan_uji)
        tampilan.mulai()
    elif catatan_uji:
        echo(catatan_uji)

    for stage in stages:
        for sumber in sumber_aktif:
            if tampilan is not None:
                tampilan.muat(sumber, stage, "jalan", None, None)
            start = time.time()
            try:
                result = run_stage(sumber, stage)
                status = "ok"
                info = f"{sum(len(v) for v in result.values())} baris"
            except Exception as exc:
                status = "gagal"
                info = f"{type(exc).__name__}: {exc}"
                logger.exception("Stage '%s' '%s' GAGAL: %s", stage, sumber, exc)
                gagal.append(f"{sumber}.{stage}")
            detik = round(time.time() - start, 1)
            if tampilan is not None:
                tampilan.muat(sumber, stage, status, info, detik)
            else:
                echo(f"| {stage:8s} | {sumber} | {status.upper()} | {info} | {detik} dtk")
            hasil_per_sumber.setdefault(sumber, {})[stage] = {
                "status": "OK" if status == "ok" else "GAGAL",
                "info": info,
                "detik": detik,
            }
    if tampilan is not None:
        tampilan.berhenti()

    echo("\n" + "=" * 60)
    echo("RINGKASAN RUN")
    echo("=" * 60)
    for sumber, stages in hasil_per_sumber.items():
        ok = all(s["status"] == "OK" for s in stages.values())
        bits = " | ".join(f"{st}:{s['info']}" for st, s in stages.items())
        total_detik = round(sum(s["detik"] for s in stages.values()), 1)
        echo(f"  [{'OK' if ok else 'GAGAL':5s}] {sumber:12s} -> {bits} ({total_detik}s)")
    for skip in skips:
        echo(skip)
    if gagal:
        secho(f"  Ada {len(gagal)} tahap gagal: {gagal}", fg=colors.RED)
        raise SystemExit(1)


def run_all(stages: Optional[tuple[str, ...]] = None) -> None:
    """Jalankan seluruh sumber enabled secara paralel (default 4 worker).

    stages=None -> extract -> transform -> load; mis. ('extract',) utk --bronze.
    """
    setup_logging()
    stages = stages or STAGES
    sumber_aktif, skips = _piih_sumber(None)
    workers = int(os.environ.get("HOTEL_WORKERS", str(WORKERS_DEFAULT)))
    per = jalankan_paralel(
        sumber_aktif,
        workers=workers,
        limit=get_row_limit(),
        stages=stages,
        datasets=get_dataset_filter(),
    )
    cetak_ringkasan(per, skips, stages=stages)


def run_single(source: str, stages: Optional[tuple[str, ...]] = None) -> None:
    """Jalankan satu sumber (extract -> transform -> load)."""
    if HAS_TYPER:
        secho, colors = typer.secho, typer.colors
    else:
        colors = None

        def secho(text: str, **kwargs):
            print(text)

    if source not in ORDERSUMBER:
        secho(
            f"ERROR: sumber '{source}' tidak dikenal. Tersedia: {', '.join(ORDERSUMBER)}",
            fg=colors.RED,
        )
        raise SystemExit(1)
    _run_sources([source], stages)


def _jalankan_pintasan(opsi: OpsiCLI) -> None:
    """Jalankan sesuai hasil urai flag pintasan."""
    set_dataset_filter(opsi.datasets)
    if not opsi.sisa:
        if opsi.sumber is None:
            set_row_limit(None)
            run_all(opsi.stages)
        else:
            run_single(opsi.sumber, opsi.stages)
        return

    pintasan = _parse_shortcut(opsi.sisa)
    if opsi.datasets and opsi.sumber is None and (pintasan is None or pintasan[0] is None):
        sys.stderr.write(
            f"ERROR: flag dataset ({' '.join(sorted(_DATASET_FLAG))}) harus dipakai "
            f"bersama satu sumber, mis. main.py --gmaps --harga\n"
        )
        raise SystemExit(1)
    if pintasan is None:
        contoh = " | ".join(
            [
                "main.py --bronze",
                "main.py --gmaps --harga",
                "main.py --bronze --gmaps --harga",
                "main.py gmaps --bronze --harga",
                "main.py gmaps[5] --bronze --harga",
            ]
        )
        sys.stderr.write(f"ERROR: {opsi.sisa[0]} bukan sumber yang dikenal\n  Contoh: {contoh}\n")
        raise SystemExit(1)
    target, limit = pintasan
    if opsi.sumber is not None and target is not None and target != opsi.sumber:
        sys.stderr.write(
            f"ERROR: sumber ganda ({opsi.sumber} dan {target}); pilih satu.\n"
        )
        raise SystemExit(1)
    if opsi.sumber is not None:
        target = opsi.sumber
    set_row_limit(limit)
    if target is None:
        run_all(opsi.stages)
    else:
        run_single(target, opsi.stages)


def main() -> None:
    opsi = _urai_flag(sys.argv[1:])
    if opsi.pakai_pintasan:
        _jalankan_pintasan(opsi)
        return
    pintasan = _parse_shortcut(sys.argv[1:])
    if pintasan is not None:
        target, limit = pintasan
        set_row_limit(limit)
        if target is None:
            run_all()
        else:
            run_single(target)
        return
    if len(sys.argv) <= 1:
        run_all()
        return
    if hasattr(app, "__call__"):
        app()  # typer
    else:
        _run_argparse()


if __name__ == "__main__":
    main()