from __future__ import annotations

import logging
import multiprocessing
import queue as queue_mod
import threading
import time
from concurrent.futures import ProcessPoolExecutor
from typing import Any

from .pipeline import get_stage_path, run_stage
from .shared.live_view import RencanaBaris, TampilanPipeline, dukung_live
from .shared.logging_utils import setup_logging
from .shared.row_limit import is_limited, set_row_limit
from .shared.run_options import get_dry_run, set_dry_run

_ANTRIAN: multiprocessing.Queue | None = None

STAGES = ("bronze",)

_KONSOLE_LOG_LEVEL = logging.WARNING


class AntreanLogHandler(logging.Handler):
    """Ganti handler konsol di worker: catatan WARNING+ dikirim lewat antrian ke parent.

    Worker tidak pernah menulis langsung ke terminal, sehingga tampilan live
    in-place tidak rusak. Detail DEBUG/INFO tetap masuk file log per tahap.
    """

    def __init__(self, antrian: multiprocessing.Queue, level: int = _KONSOLE_LOG_LEVEL) -> None:
        super().__init__(level)
        self._antrian = antrian

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._antrian.put(("pesan", (record.levelname, record.name, record.getMessage())))
        except Exception:
            self.handleError(record)


def _init_worker(
    antrian: multiprocessing.Queue,
    limit: int | None,
    antre_log: bool,
    dry_run: bool = False,
) -> None:
    """Initializer tiap proses worker: simpan antrian, siapkan logging, limit & dry-run."""
    global _ANTRIAN
    _ANTRIAN = antrian
    setup_logging()
    if antre_log:
        _pasang_antrean_log(antrian)
    if limit is not None:
        set_row_limit(limit)
    if dry_run:
        set_dry_run(True)


def _pasang_antrean_log(antrian: multiprocessing.Queue) -> None:
    """Cabut handler StreamHandler worker dan ganti dengan AntreanLogHandler."""
    root = logging.getLogger()
    for h in root.handlers[:]:
        if isinstance(h, logging.StreamHandler):
            root.removeHandler(h)
    root.addHandler(AntreanLogHandler(antrian))


def _kirim(ev: tuple) -> None:
    q = _ANTRIAN
    if q is not None:
        q.put(ev)


def jalankan_sumber(sumber: str, stages: tuple[str, ...] | None = None) -> dict[str, Any]:
    """Jalankan bronze (extract) satu sumber dalam satu worker.

    stages=None -> bronze; mis. ('bronze',) utk --bronze.
    Mengirimkan event (sumber, STAGE, status, info, detik) ke antrian utama.
    """
    stages = stages or STAGES
    status: str = "OK"
    for stage in stages:
        _kirim((sumber, stage.upper(), "mulai", None, None))
        mulai = time.time()
        try:
            result = run_stage(sumber, stage, dry_run=get_dry_run())
            n_dataset = len(result)
            n_baris = sum(len(v) for v in result.values())
            detik = round(time.time() - mulai, 1)
            info = f"{n_dataset} dataset {n_baris} baris"
            _kirim((sumber, stage.upper(), "selesai", info, detik))
        except Exception as exc:
            detik = round(time.time() - mulai, 1)
            status = "GAGAL"
            info = f"{type(exc).__name__}: {exc}"
            _kirim((sumber, stage.upper(), "gagal", info, detik))
            break
    _kirim((sumber, None, "selesai_sumber", status, None))
    return {"sumber": sumber, "status": status}


def _print_baris(stage: str, sumber: str, status: str, info: Any, detik: Any) -> None:
    if status == "mulai":
        print(f"| {stage:8s} | {sumber}")
    elif status == "selesai":
        print(f"| {stage:8s} | {sumber} | OK | {info} | {detik} dtk")
    elif status == "gagal":
        print(f"| {stage:8s} | {sumber} | GAGAL | {info} | {detik} dtk")


def _konsumsi(
    antrian: multiprocessing.Queue,
    per_sumber: dict,
    berhenti: threading.Event,
    tampilan: TampilanPipeline | None,
) -> None:
    """Konsumen antrian: perbarui tampilan live / cetak baris saat live mati."""
    while True:
        try:
            ev = antrian.get(timeout=0.5)
        except queue_mod.Empty:
            if berhenti.is_set():
                return
            continue
        if len(ev) == 2 and ev[0] == "pesan":
            _, (level, nama, isi) = ev
            if tampilan is not None:
                tampilan.pesan(level, nama, isi)
            else:
                print(f"{level:8s} | {nama} | {isi}")
            continue
        sumber, stage, status, info, detik = ev
        if stage is None and status == "selesai_sumber":
            per_sumber.setdefault(sumber, {})["status"] = info
            continue
        if tampilan is not None:
            tampilan.muat(sumber, stage, status, info, detik)
        else:
            _print_baris(stage, sumber, status, info, detik)
        if status in ("selesai", "gagal"):
            per_sumber.setdefault(sumber, {}).setdefault("stages", {})[stage.lower()] = {
                "status": "OK" if status == "selesai" else "GAGAL",
                "info": info if info else "",
                "detik": detik or 0.0,
            }


def _buat_tampilan_paralel(
    sumber_aktif: list[str], stages: tuple[str, ...] | None = None
) -> TampilanPipeline | None:
    if not dukung_live():
        return None
    stages = stages or STAGES
    rencana: list[RencanaBaris] = []
    for stage in stages:
        for sumber in sumber_aktif:
            rencana.append(RencanaBaris(sumber=sumber, stage=stage, path=get_stage_path(sumber, stage)))
    return TampilanPipeline(rencana)


def jalankan_paralel(
    sumber_aktif: list[str],
    workers: int = 4,
    limit: int | None = None,
    stages: tuple[str, ...] | None = None,
    dry_run: bool = False,
) -> dict:
    """Jalankan semua sumber aktif secara paralel dengan tampilan live in-place.

    stages=None -> bronze; mis. ('bronze',) utk --bronze.
    dry_run -> hanya baca & laporkan, tanpa menulis file/DB.
    """
    stages = stages or STAGES
    if dry_run:
        set_dry_run(True)
    antrian: multiprocessing.Queue = multiprocessing.Queue()
    per_sumber: dict = {s: {"status": "?", "stages": {}} for s in sumber_aktif}
    berhenti = threading.Event()

    tampilan = _buat_tampilan_paralel(sumber_aktif, stages)
    catatan: list[str] = []
    if is_limited():
        catatan.append(f"  (mode uji: maks {limit} baris per dataset)")
    if dry_run:
        catatan.append("  (dry-run: tidak ada file/DB yang ditulis)")

    if tampilan is not None:
        for c in catatan:
            print(c)
        tampilan.mulai()
    else:
        for c in catatan:
            print(c)

    konsumen = threading.Thread(
        target=_konsumsi, args=(antrian, per_sumber, berhenti, tampilan), daemon=True
    )
    konsumen.start()

    antre_log = tampilan is not None
    try:
        with ProcessPoolExecutor(
            max_workers=workers,
            initializer=_init_worker,
            initargs=(antrian, limit, antre_log, dry_run),
        ) as executor:
            futures = [executor.submit(jalankan_sumber, s, stages) for s in sumber_aktif]
            for f in futures:
                hasil = f.result()
                per_sumber[hasil["sumber"]]["status"] = hasil["status"]
    finally:
        berhenti.set()
        konsumen.join(timeout=10)
        if tampilan is not None:
            tampilan.berhenti()
    return per_sumber


def cetak_ringkasan(
    per_sumber: dict, skips: list[str], stages: tuple[str, ...] | None = None
) -> None:
    stages_dijalankan = stages or STAGES
    print("\n" + "=" * 60)
    print("RINGKASAN RUN")
    print("=" * 60)
    gagal: list[str] = []
    for sumber in per_sumber:
        status = per_sumber[sumber]["status"]
        ok = status == "OK"
        if not ok:
            gagal.append(sumber)
        hasil = per_sumber[sumber].get("stages", {})
        bits = " | ".join(
            f"{st}:{hasil.get(st, {}).get('status', '-')} {hasil.get(st, {}).get('info', '')}".strip()
            for st in stages_dijalankan
        )
        total = round(
            sum(hasil.get(st, {}).get("detik", 0.0) for st in stages_dijalankan), 1
        )
        print(f"  [{'OK' if ok else 'GAGAL':5s}] {sumber:12s} -> {bits} ({total}s)")
    for skip in skips:
        print(skip)
    if gagal:
        print(f"  Ada {len(gagal)} sumber gagal: {', '.join(gagal)}")
        raise SystemExit(1)