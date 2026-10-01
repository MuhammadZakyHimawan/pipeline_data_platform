from __future__ import annotations

import json
import logging
import re
from typing import Any

import pandas as pd

from ..shared.config import get_source_config
from ..shared.periods import resolve_years
from ..shared.storage import list_periods, read_parquet, write_parquet

log = logging.getLogger(__name__)

SOURCE = "bps"

# --- Konstanta hasil penyalinan dari script nyoba_jxb (agar akurat) ---

VERVAR_SKIP_ASAL_TURIS = {12, 39, 56, 115, 163, 186, 247, 248}

CODE_TO_NAMA = {
    1: "Malaysia", 2: "Singapore", 3: "Timor Leste", 4: "Indonesia",
    5: "Philippines", 6: "Thailand", 7: "Vietnam", 8: "Myanmar/Burma",
    9: "Brunei Darussalam", 10: "Kamboja", 11: "Laos", 12: "ASEAN",
    13: "Hongkong, RRT", 14: "Republik Rakyat Tiongkok", 15: "India",
    16: "South Korea", 17: "Japan", 18: "Turki", 19: "Mongolia",
    20: "Kazakhstan", 21: "Pakistan", 22: "Uzbekistan", 23: "Bangladesh",
    24: "Nepal", 25: "Taiwan", 26: "Srilanka", 27: "Other Asia",
    28: "Maladewa", 29: "Kirgistan", 30: "Tajikhistan", 31: "Bhutan",
    32: "Afganistan", 33: "Turkmenistan", 34: "Stateless", 35: "Korea Utara",
    36: "Macao, RRT", 37: "British Indian Ocean Teritory", 38: "Lain-lain",
    39: "TOTAL ASIA (Excl.ASEAN)", 40: "Saudi Arabia", 41: "Oman",
    42: "Egypt", 43: "Jordan", 44: "Uni Emirat Arab", 45: "Yaman",
    46: "Other Middle East", 47: "Iran", 48: "Kuwait", 49: "Irak",
    50: "Syiria", 51: "Palestina", 52: "Libanon", 53: "Bahrain",
    54: "Qatar", 55: "Israel", 56: "TOTAL MIDDLE EAST", 57: "United Kingdom",
    58: "Rusia", 59: "France", 60: "Germany", 61: "Netherlands",
    62: "Italia", 63: "Spain", 64: "Polandia", 65: "Ukraine",
    66: "Sweden", 67: "Denmark", 68: "Switzerland", 69: "Portugal",
    70: "Ceko", 71: "Irlandia (Ireland)", 72: "Belgium", 73: "Norway",
    74: "Romania", 75: "Austria", 76: "Hongaria", 77: "Finland",
    78: "Belarus", 79: "Lithuania", 80: "Slovakia", 81: "Estonia",
    82: "Bulgaria", 83: "Yunani (Greece)", 84: "Latvia", 85: "Croasia",
    86: "Other West Europe", 87: "Iceland (Islandia)", 88: "Malta",
    89: "Luxemburg", 90: "Liechtenstein", 91: "Monako", 92: "Andorra",
    93: "San Marino", 94: "Aruba", 95: "Scotlandia", 96: "Vatikan",
    97: "Volta Atas", 98: "Hellenic", 99: "Wales", 100: "Other East Europe",
    101: "Serbia", 102: "Slovenia", 103: "Armenia", 104: "Azerbaijan",
    105: "Cyprus", 106: "Macedonia", 107: "Bosnia Herzegovina", 108: "Georgia",
    109: "Albania", 110: "Moldova", 111: "Montenegro", 112: "Republik Cosovo",
    113: "Siberia", 114: "Yugoslavia", 115: "TOTAL EUROPE",
    116: "United States of America", 117: "Canada", 118: "Brazilia",
    119: "Kolombia", 120: "Argentina", 121: "Mexiko", 122: "Chili",
    123: "Central America", 124: "Costarika", 125: "Guatemala", 126: "Panama",
    127: "Honduras", 128: "El Salvador", 129: "Nicaragua", 130: "Belize",
    131: "South America", 132: "Peru", 133: "Venezuela", 134: "Ekuador",
    135: "Uruguay", 136: "Paraguay", 137: "Bolivia", 138: "Suriname",
    139: "Guyana", 140: "Falkland Islands", 141: "Guiana", 142: "Other America",
    143: "UNO", 144: "Kuba", 145: "Dominika", 146: "Trinidad & Tobago",
    147: "St. Kitts & Nevis", 148: "Jamaika", 149: "Barbados",
    150: "Grenadines (Grenada)", 151: "St. Lucia", 152: "Antigua & Barbuda",
    153: "Bahama Islands", 154: "Haiti", 155: "Dominika Rep.",
    156: "St. Vincent", 157: "Alaska", 158: "Bermuda", 159: "Martinique",
    160: "Puertorico", 161: "St. Christopher", 162: "Guadelope",
    163: "TOTAL AMERICA", 164: "Australia", 165: "Papua New Guinea",
    166: "New Zealand", 167: "Other Oceania", 168: "Fiji Islands",
    169: "Vanuatu", 170: "Tonga", 171: "Solomon Islands", 172: "Samoa",
    173: "Nauru", 174: "Palau Rep.", 175: "Cooks Islands", 176: "Guam",
    177: "Kaledonia", 178: "Karibati (Kiribati)", 179: "Marshall Islands",
    180: "Micronesia Rep.", 181: "Niue", 182: "Polynesia", 183: "Samoa Barat",
    184: "Tuvalu", 185: "Walis & Futuna", 186: "TOTAL OCEANIA",
    187: "South Africa", 188: "Maroko", 189: "Other Africa", 190: "Kenya",
    191: "Tunisia", 192: "Aljazair / Algeria", 193: "Mauritius",
    194: "Tanzania", 195: "Libya", 196: "Nigeria", 197: "Mozambik",
    198: "Zimbabwe", 199: "Sudan", 200: "Somalia", 201: "Ghana",
    202: "Kamerun", 203: "Uganda", 204: "Malagasy (Madagascar)",
    205: "Ethiopia", 206: "Congo Rep. Dem.", 207: "Seychelles",
    208: "Burkina Faso", 209: "Rwanda", 210: "Zambia", 211: "Malawi",
    212: "Senegal", 213: "Comoros", 214: "Mali", 215: "Eritrea", 216: "Chad",
    217: "Sierra Leona", 218: "Namibia", 219: "Gambia", 220: "Bostwana",
    221: "Liberia", 222: "Swaziland", 223: "Guinea", 224: "Cape Verde",
    225: "Ivory Coast (Pantai Gading)", 226: "Congo", 227: "Burundi",
    228: "Mauritania", 229: "Togo", 230: "Gabon", 231: "Lesotho",
    232: "South Sudan", 233: "Niger", 234: "Djibouti", 235: "Guinea Equator",
    236: "Afrika Tengah Rep.", 237: "Angola", 238: "Benin", 239: "Sahara",
    240: "Sao Tome & Principe", 241: "Zaire", 242: "Burkinabe",
    243: "Cabinda", 244: "Guinea Bissau", 245: "Maseru", 246: "Zomba",
    247: "TOTAL AFRICA", 248: "GRAND TOTAL",
}

KOREKSI_BULAN = {"Pebruari": "Februari", "Nopember": "November"}

KODE_JENIS = {"Tamu Asing": 2, "Tamu Domestik": 1}

KODE_TURVAR_BINTANG = {"1816": 1, "1817": 2, "1818": 3, "1819": 4, "1820": 5}

JUMLAH_TURVAR_WISMAN = 276
JUMLAH_BULAN_TAHUNAN = 13

VERVAR_AGREGAT_WISNUS = 7


# --- helper bronze ---

def _load_bronze(dataset: str, period_label: str) -> pd.DataFrame | None:
    try:
        return read_parquet("bronze", SOURCE, dataset, period_label)
    except FileNotFoundError:
        return None


def _json_meta(meta_json: str) -> dict:
    return json.loads(meta_json)


# --- parser per dataset (logika hasil penyalinan script nyoba_jxb) ---

def _parse_asal_turis(bronze: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, row in bronze.iterrows():
        year = int(row["tahun"])
        th = year - 1900
        prefix = "0" + str(th)
        dc = json.loads(row["datacontent_json"])
        for key, value in dc.items():
            key_str = str(key)
            if "1470" not in key_str:
                continue
            idx = key_str.index("1470")
            vervar_str = key_str[:idx]
            tail = key_str[idx + 4:]
            if not tail.startswith(prefix):
                continue
            bulan_str = tail[len(prefix):]
            if not bulan_str:
                continue
            bulan = int(bulan_str)
            vervar = int(vervar_str) if vervar_str else 0
            if vervar in VERVAR_SKIP_ASAL_TURIS:
                continue
            if bulan < 1 or bulan > 12:
                continue
            rows.append(
                {
                    "id": f"{year}{bulan:02d}{vervar:03d}",
                    "tahun": year,
                    "bulan": bulan,
                    "vervar": vervar,
                    "negara_asal": CODE_TO_NAMA.get(vervar, str(vervar)),
                    "jumlah_kunjungan": int(value),
                }
            )
    return pd.DataFrame(rows)


def _parse_inflasi(bronze: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, row in bronze.iterrows():
        year = int(row["tahun"])
        th = year - 1900
        kota_map = {
            str(v.get("val")): v.get("label", "") for v in _json_meta(row["payload_meta_json"]).get("vervar", [])
        }
        dc = json.loads(row["datacontent_json"])
        for key, value in dc.items():
            k = str(key)
            vervar = k[0:4]
            if k[4:5] != str(row["var"]):
                continue
            tahun_kode = k[5:9]
            if tahun_kode != "0" + str(th):
                continue
            bulan_str = k[9:]
            if not bulan_str.isdigit():
                continue
            bulan = int(bulan_str)
            if not (1 <= bulan <= 12):
                continue
            vervar_int = int(vervar)
            rows.append(
                {
                    "id": year * 1000000 + bulan * 10000 + vervar_int,
                    "tahun": year,
                    "bulan": bulan,
                    "vervar": vervar_int,
                    "tempat": kota_map.get(vervar, vervar),
                    "nilai_inflasi": float(value),
                }
            )
    return pd.DataFrame(rows)


def _parse_lama_menginap(bronze: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, row in bronze.iterrows():
        year = int(row["tahun"])
        meta = _json_meta(row["payload_meta_json"])

        bulan_map: dict[str, str] = {}
        for v in meta.get("vervar", []):
            label = (v.get("label") or "").strip()
            label = KOREKSI_BULAN.get(label, label)
            if "tahun" in label.lower():
                continue
            bulan_map[str(v.get("val"))] = label

        jenis_map: dict[str, str] = {}
        for t in meta.get("turvar", []):
            label = (t.get("label") or "").strip()
            if label:
                jenis_map[str(t.get("val"))] = label

        if not bulan_map or not jenis_map:
            continue

        try:
            var_id = str(meta["var"][0]["val"])
            kode_tahun = str(meta["tahun"][0]["val"])
        except (KeyError, IndexError):
            continue

        turtahun_ids = [str(t.get("val")) for t in meta.get("turtahun", []) if t.get("val") is not None]

        dc = json.loads(row["datacontent_json"])
        for b_id, bulan_label in bulan_map.items():
            bulan_no = int(b_id)
            if not (1 <= bulan_no <= 12):
                continue
            for j_id, jenis_label in jenis_map.items():
                for tt_id in turtahun_ids:
                    nilai = dc.get(f"{b_id}{var_id}{j_id}{kode_tahun}{tt_id}")
                    if nilai is None:
                        continue
                    rows.append(
                        {
                            "id": int(f"{year}{bulan_no:02d}{KODE_JENIS.get(jenis_label, 0)}"),
                            "tahun": year,
                            "bulan": bulan_no,
                            "jenis_wisatawan": jenis_label,
                            "lama_menginap_hari": float(nilai),
                        }
                    )
    return pd.DataFrame(rows)


def _parse_tpk_bintang(bronze: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, row in bronze.iterrows():
        year = int(row["tahun"])
        dc = json.loads(row["datacontent_json"])
        for key, value in dc.items():
            k = str(key)
            turvar = k[-8:-4]
            var_str = k[-12:-8]
            bulan_str = k[:-12]
            if var_str != str(row["var"]):
                continue
            try:
                bulan = int(bulan_str)
            except ValueError:
                continue
            if not (1 <= bulan <= 12):
                continue
            klasifikasi = KODE_TURVAR_BINTANG.get(turvar)
            if klasifikasi is None:
                continue
            rows.append(
                {
                    "id": year * 1000 + bulan * 10 + klasifikasi,
                    "tahun": year,
                    "bulan": bulan,
                    "klasifikasi_bintang": klasifikasi,
                    "tpk_persen": float(value),
                }
            )
    return pd.DataFrame(rows)


def _parse_wisman_pintu_masuk(bronze: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, row in bronze.iterrows():
        year = int(row["tahun"])
        var = str(row["var"])
        meta = _json_meta(row["payload_meta_json"])

        turvar_map: dict[str, str] = {}
        for t in meta.get("turvar", []):
            val = t.get("val")
            label = (t.get("label") or "").strip()
            if val is None or not label or val == JUMLAH_TURVAR_WISMAN:
                continue
            turvar_map[str(val)] = label

        if not turvar_map:
            continue

        pola = re.compile(
            r"^(\d{1,2})" + var + "(" + "|".join(sorted(turvar_map, key=len, reverse=True)) + r")\d+$"
        )

        dc = json.loads(row["datacontent_json"])
        for key, value in dc.items():
            m = pola.match(str(key))
            if not m:
                continue
            bulan = int(m.group(1))
            turvar = m.group(2)
            if bulan == JUMLAH_BULAN_TAHUNAN or turvar not in turvar_map:
                continue
            rows.append(
                {
                    "tahun": year,
                    "bulan": bulan,
                    "tempat": turvar_map[turvar],
                    "jumlah_kunjungan": int(value),
                }
            )
    return pd.DataFrame(rows)


def _parse_wisnus_tujuan(bronze: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, row in bronze.iterrows():
        year = int(row["tahun"])
        th = year - 1900
        var = str(row["var"])
        meta = _json_meta(row["payload_meta_json"])

        vervar_map: dict[str, str] = {}
        for v in meta.get("vervar", []):
            val = v.get("val")
            label = (v.get("label") or "").strip()
            if val is None or not label or int(val) == VERVAR_AGREGAT_WISNUS:
                continue
            vervar_map[str(val)] = label

        turvar_codes = [str(t.get("val")) for t in meta.get("turvar", []) if t.get("val") is not None]
        if not vervar_map or not turvar_codes:
            continue

        pola = re.compile(
            r"^(\d+)" + var + "(" + "|".join(sorted(turvar_codes, key=len, reverse=True)) + ")"
            + str(th) + r"(\d{1,2})$"
        )

        dc = json.loads(row["datacontent_json"])
        for key, value in dc.items():
            m = pola.match(str(key))
            if not m:
                continue
            wilayah = m.group(1)
            bulan = int(m.group(3))
            if wilayah not in vervar_map or bulan == JUMLAH_BULAN_TAHUNAN:
                continue
            rows.append(
                {
                    "tahun": year,
                    "bulan": bulan,
                    "tempat": vervar_map[wilayah],
                    "jumlah_perjalanan": int(value),
                }
            )
    return pd.DataFrame(rows)


PARSERS = {
    "asal_turis": _parse_asal_turis,
    "inflasi": _parse_inflasi,
    "lama_menginap": _parse_lama_menginap,
    "tpk_bintang": _parse_tpk_bintang,
    "wisman_pintu_masuk": _parse_wisman_pintu_masuk,
    "wisnus_tujuan": _parse_wisnus_tujuan,
}


def transform_dataset(dataset: str, period_label: str, dry_run: bool = False) -> pd.DataFrame | None:
    """TRANSFORM (silver): bronze satu tahun -> baris bersih, satu file per tahun."""
    bronze = _load_bronze(dataset, period_label)
    if bronze is None:
        log.warning("bps/%s: bronze tidak ada utk periode %s", dataset, period_label)
        return None

    parser = PARSERS[dataset]
    df = parser(bronze)
    if df is None or df.empty:
        log.warning("bps/%s: hasil parse kosong utk periode %s", dataset, period_label)
        return None

    for col in df.columns:
        if df[col].dtype == "object" and not df[col].isna().any():
            df[col] = df[col].astype(str).str.strip()

    if dry_run:
        log.info("[dry-run] bps/%s silver: %d baris (tidak ditulis)", dataset, len(df))
        return df

    path = write_parquet(df, "silver", SOURCE, dataset, period_label)
    log.info("bps/%s silver ditulis: %s (%d baris)", dataset, path, len(df))
    return df


def transform(period: str | None = None, dry_run: bool = False) -> dict[str, pd.DataFrame]:
    """TRANSFORM semua dataset BPS bronze -> silver.

    Bronze BPS ditulis satu file per tahun, jadi transform memproses **setiap
    file tahun yang benar-benar ada** di data_lake (bukan satu label periode
    hasil tebakan). Bila `period` diisi eksplisit (mis. '2020-2024'), hanya
    tahun-years itu yang diproses.
    """
    hasil: dict[str, pd.DataFrame] = {}
    for dataset in PARSERS:
        if period:
            try:
                periods = [str(y) for y in resolve_years(period)]
            except ValueError as exc:
                log.warning("bps/%s: %s", dataset, exc)
                continue
        else:
            periods = list_periods("bronze", SOURCE, dataset)
        if not periods:
            log.warning("bps/%s: tidak ada file bronze untuk diproses", dataset)
            continue

        frames: list[pd.DataFrame] = []
        for p in periods:
            df = transform_dataset(dataset, p, dry_run=dry_run)
            if df is not None:
                frames.append(df)
        if frames:
            hasil[dataset] = pd.concat(frames, ignore_index=True)
    return hasil