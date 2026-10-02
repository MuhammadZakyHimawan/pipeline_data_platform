from __future__ import annotations

import json
import logging
import os
from datetime import date

from .config import DATA_LAKE_DIR

log = logging.getLogger(__name__)

# State run disimpan terpisah dari parquet: parquet ditulis per periode, sedangkan
# checkpoint dipakai untuk melanjutkan daftar hotel. Folder ini ikut gitignored
# bersama data_lake/.
DIR_STATE = DATA_LAKE_DIR / "_state"
_SIMPAN_SETIAP = 10


class Checkpoint:
    """Catat hotel yang sudah diproses satu run, supaya run panjang (puluhan
    jam untuk 781 hotel) bisa dilanjutkan setelah crash dengan `--lanjut`.

    State berlaku untuk satu tanggal run: lanjut hanya masuk akal kalau
    run-nya memang hari yang sama, kalau tidak state lama akan membuat
    hotel terlewat.
    """

    def __init__(self, sumber: str, tanggal: date | None = None) -> None:
        self.sumber = sumber
        self.tanggal = str(tanggal or date.today())
        self.path = DIR_STATE / f"{sumber}.json"
        self.selesai: set[str] = set()
        self._kotor = 0

    def muat(self) -> set[str]:
        """Baca state yang sudah ada; kosongkan bila bukan milik run hari ini."""
        if not self.path.exists():
            return self.selesai
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            log.warning("checkpoint %s tidak bisa dibaca (%s); mulai ulang", self.path.name, exc)
            return self.selesai

        if str(data.get("tanggal")) != self.tanggal:
            log.info(
                "checkpoint %s milik run %s, bukan hari ini; mulai ulang",
                self.path.name, data.get("tanggal"),
            )
            return self.selesai

        selesai = data.get("selesai") or []
        self.selesai = {str(x) for x in selesai}
        log.info("checkpoint %s: %d hotel sudah diproses hari ini", self.sumber, len(self.selesai))
        return self.selesai

    def sudah(self, id_kompetitor: str) -> bool:
        return str(id_kompetitor) in self.selesai

    def tandai(self, id_kompetitor: str) -> None:
        self.selesai.add(str(id_kompetitor))
        self._kotor += 1
        if self._kotor >= _SIMPAN_SETIAP:
            self.simpan()

    def tandai_kumpulan(self, daftar) -> None:
        for x in daftar:
            self.tandai(x)
        self.simpan()

    def simpan(self) -> None:
        try:
            DIR_STATE.mkdir(parents=True, exist_ok=True)
            isi = {
                "sumber": self.sumber,
                "tanggal": self.tanggal,
                "jumlah": len(self.selesai),
                "selesai": sorted(self.selesai),
            }
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps(isi, ensure_ascii=False), encoding="utf-8")
            os.replace(tmp, self.path)
            self._kotor = 0
        except OSError as exc:
            log.warning("checkpoint %s gagal ditulis: %s", self.path.name, exc)

    def bersihkan(self) -> None:
        try:
            self.path.unlink(missing_ok=True)
        except OSError as exc:
            log.warning("checkpoint %s gagal dihapus: %s", self.path.name, exc)