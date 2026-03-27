"""
Watcher
Responsibilities: Scan the folder structure YEAR/AGREEMENT/BASE_TYPE/file
and detect new vs. processed files.
No prior knowledge of user interfaces or data processing.
"""

import json
import re
from pathlib import Path
from dataclasses import dataclass


PROCESSED_PATH = Path("config/procesados.json")

MONTHS_NAME = {
    "ENERO": "01", "FEBRERO": "02", "MARZO": "03",
    "ABRIL": "04", "MAYO": "05", "JUNIO": "06",
    "JULIO": "07", "AGOSTO": "08", "SEPTIEMBRE": "09",
    "OCTUBRE": "10", "NOVIEMBRE": "11", "DICIEMBRE": "12",
}


# ════════════════════════════════════════════════════════════
# DATA MODEL
# ════════════════════════════════════════════════════════════

@dataclass
class DetectedFile:
    ruta:        str
    nombre:      str
    convenio:    str   # name of file agreement
    tipo_carpeta:str   # name of file type_base
    tipo_base:   str   # tipo mapeado en config (puede estar vacío si no hay mapeo)
    mes:         str   # "01" a "12" detectado del nombre del archivo
    año:         int
    procesado:   bool  = False
    procesado_el:str   = ""


# ════════════════════════════════════════════════════════════
# PROCESSED RECORD
# ════════════════════════════════════════════════════════════

def _load_processed() -> dict:
    """Load the log of already processed files."""
    if PROCESSED_PATH.exists():
        try:
            return json.loads(PROCESSED_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save_processed(procesados: dict):
    """The record of those processed persists."""
    PROCESSED_PATH.parent.mkdir(parents=True, exist_ok=True)
    PROCESSED_PATH.write_text(
        json.dumps(procesados, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def _normalize_root(ruta: str) -> str:
    """Normalizes the path for consistent comparison across operating systems.
     Resolves backslash vs. slash, case sensitivity, and relative paths."""
    return str(Path(ruta).resolve())


def mark_processed(ruta: str, tipo_base: str):
    """Marks a file as processed with timestamp."""
    from datetime import datetime
    processed = _load_processed()
    processed[_normalize_root(ruta)] = {
        "tipo_base":    tipo_base,
        "procesado_el": datetime.now().strftime("%Y-%m-%d %H:%M"),
    }
    _save_processed(processed)


def unmark_processed(ruta: str):
    """Allows you to reprocess a file by removing it from the registry."""
    procesados = _load_processed()
    procesados.pop(_normalize_root(ruta), None)
    _save_processed(procesados)


# ════════════════════════════════════════════════════════════
# MONTH DETECTION IN FILE NAME
# ════════════════════════════════════════════════════════════

def _detect_month(nombre_archivo: str) -> str:

    """
    Searches for the name of a month within the file name.
    BASE_CABEZOTE_CAPITALSALUD_DICIEMBRE_2025.xlsx → "12"
    Returns "" if none is found.
    """
    nombre_upper = nombre_archivo.upper()
    for nombre_mes, numero in MONTHS_NAME.items():
        if nombre_mes in nombre_upper:
            return numero
    return ""


def _detect_year(nombre_archivo: str, carpeta_año: str) -> int:
    """
    Try to extract the year from the filename.
    If you can't find it, use the root folder name.
    """

    años = re.findall(r"20\d{2}", nombre_archivo)
    if años:
        return int(años[-1])
    try:
        return int(carpeta_año)
    except ValueError:
        return 0


# ════════════════════════════════════════════════════════════
# SCAN OF FILES
# ════════════════════════════════════════════════════════════

def scan(
        carpeta_raiz: str,
        month_filter: str,
        map_type_file: dict,
) -> list[DetectedFile]:
    """
    Scans the structure YEAR/AGREEMENT/BASE_TYPE/*.xlsx
    and returns a list of files for the specified month.
    mapping_folder_type: "base_type_folder" section of config.json
    """
    root = Path(carpeta_raiz)
    processed = _load_processed()
    detected_files = []

    if not root.exists():
        return []

    # STRUCTURE: YEAR / AGREEMENT / BASE_TYPE / file.xlsx
    for year_dir in sorted(root.iterdir()):
        if not year_dir.is_dir():
            continue

        for agreement_dir in sorted(year_dir.iterdir()):
            if not agreement_dir.is_dir():
                continue

            for type_dir in sorted(agreement_dir.iterdir()):
                if not type_dir.is_dir():
                    continue

                for file_path in sorted(type_dir.glob("*.xlsx")):
                    month_file = _detect_month(file_path.name)

                    if month_file != month_filter:
                        continue

                    route_str = _normalize_root(str(file_path))
                    type_folder_name = type_dir.name
                    base_type = map_type_file.get(type_folder_name, "")
                    already_processed = route_str in processed

                    detected_files.append(DetectedFile(
                        ruta         = route_str,
                        nombre       = file_path.name,
                        convenio     = agreement_dir.name,
                        tipo_carpeta = type_folder_name,
                        tipo_base    = base_type,
                        mes          = month_file,
                        año          = _detect_year(file_path.name, year_dir.name),
                        procesado    = already_processed,
                        procesado_el = processed.get(route_str, {}).get("procesado_el", ""),
                    ))

    return detected_files


def new_files(files_list: list[DetectedFile]) -> list[DetectedFile]:
    """Filter only the unprocessed files."""
    return [a for a in files_list if not a.procesado]

def files_processed(files_list: list[DetectedFile]) -> list[DetectedFile]:
    """Filter only the files that have already been processed."""
    return [a for a in files_list if a.procesado]