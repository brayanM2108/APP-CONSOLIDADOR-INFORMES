"""
watcher.py
Responsabilidad: escanear la estructura de carpetas AÑO/CONVENIO/TIPO_BASE/archivo
y detectar archivos nuevos vs ya procesados.
"""

import json
from pathlib import Path
from dataclasses import dataclass, field


# ── Ruta del registro de procesados ─────────────────────────
PROCESADOS_PATH = Path("config/procesados.json")

MESES_NOMBRE = {
    "ENERO": "01", "FEBRERO": "02", "MARZO": "03",
    "ABRIL": "04", "MAYO": "05", "JUNIO": "06",
    "JULIO": "07", "AGOSTO": "08", "SEPTIEMBRE": "09",
    "OCTUBRE": "10", "NOVIEMBRE": "11", "DICIEMBRE": "12",
}


# ════════════════════════════════════════════════════════════
# MODELO DE DATOS
# ════════════════════════════════════════════════════════════

@dataclass
class ArchivoDetectado:
    ruta:        str
    nombre:      str
    convenio:    str   # nombre de la carpeta convenio
    tipo_carpeta:str   # nombre de la carpeta tipo_base
    tipo_base:   str   # tipo mapeado en config (puede estar vacío si no hay mapeo)
    mes:         str   # "01" a "12" detectado del nombre del archivo
    año:         int
    procesado:   bool  = False
    procesado_el:str   = ""


# ════════════════════════════════════════════════════════════
# REGISTRO DE PROCESADOS
# ════════════════════════════════════════════════════════════

def _cargar_procesados() -> dict:
    """Carga el registro de archivos ya procesados."""
    if PROCESADOS_PATH.exists():
        try:
            return json.loads(PROCESADOS_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _guardar_procesados(procesados: dict):
    """Persiste el registro de procesados."""
    PROCESADOS_PATH.parent.mkdir(parents=True, exist_ok=True)
    PROCESADOS_PATH.write_text(
        json.dumps(procesados, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def marcar_procesado(ruta: str, tipo_base: str):
    """Marca un archivo como procesado con timestamp."""
    from datetime import datetime
    procesados = _cargar_procesados()
    procesados[ruta] = {
        "tipo_base":    tipo_base,
        "procesado_el": datetime.now().strftime("%Y-%m-%d %H:%M"),
    }
    _guardar_procesados(procesados)


def desmarcar_procesado(ruta: str):
    """Permite reprocesar un archivo quitándolo del registro."""
    procesados = _cargar_procesados()
    procesados.pop(ruta, None)
    _guardar_procesados(procesados)


# ════════════════════════════════════════════════════════════
# DETECCIÓN DE MES EN NOMBRE DE ARCHIVO
# ════════════════════════════════════════════════════════════

def _detectar_mes(nombre_archivo: str) -> str:
    """
    Busca el nombre de un mes en el nombre del archivo.
    BASE_"NOMBREBASE"_"TIPOEPS"_DICIEMBRE_2025.xlsx → "12"
    Retorna "" si no encuentra ninguno.
    """
    nombre_upper = nombre_archivo.upper()
    for nombre_mes, numero in MESES_NOMBRE.items():
        if nombre_mes in nombre_upper:
            return numero
    return ""


def _detectar_año(nombre_archivo: str, carpeta_año: str) -> int:
    """
    Intenta extraer el año del nombre del archivo.
    Si no lo encuentra, usa el nombre de la carpeta raíz.
    """
    import re
    años = re.findall(r"20\d{2}", nombre_archivo)
    if años:
        return int(años[-1])
    try:
        return int(carpeta_año)
    except ValueError:
        return 0


# ════════════════════════════════════════════════════════════
# ESCANEO DE CARPETAS
# ════════════════════════════════════════════════════════════

def escanear(
    carpeta_raiz: str,
    mes_filtro: str,          # "01" a "12", filtra por mes
    mapeo_carpeta_tipo: dict, # {"CABEZOTE": "CAPITALSALUD - Cabezote", ...}
) -> list[ArchivoDetectado]:
    """
    Escanea la estructura AÑO/CONVENIO/TIPO_BASE/*.xlsx
    y retorna lista de archivos del mes indicado.

    mapeo_carpeta_tipo: sección "carpeta_tipo_base" del config.json
    """
    raiz      = Path(carpeta_raiz)
    procesados= _cargar_procesados()
    archivos  = []

    if not raiz.exists():
        return []

    # Estructura: AÑO / CONVENIO / TIPO_BASE / archivo.xlsx
    for carpeta_año in sorted(raiz.iterdir()):
        if not carpeta_año.is_dir():
            continue

        for carpeta_convenio in sorted(carpeta_año.iterdir()):
            if not carpeta_convenio.is_dir():
                continue

            for carpeta_tipo in sorted(carpeta_convenio.iterdir()):
                if not carpeta_tipo.is_dir():
                    continue

                for archivo in sorted(carpeta_tipo.glob("*.xlsx")) :
                    mes_archivo = _detectar_mes(archivo.name)

                    # Filtrar por mes seleccionado
                    if mes_archivo != mes_filtro:
                        continue

                    ruta_str     = str(archivo)
                    tipo_carpeta = carpeta_tipo.name
                    tipo_base    = mapeo_carpeta_tipo.get(tipo_carpeta, "")
                    ya_procesado = ruta_str in procesados

                    archivos.append(ArchivoDetectado(
                        ruta         = ruta_str,
                        nombre       = archivo.name,
                        convenio     = carpeta_convenio.name,
                        tipo_carpeta = tipo_carpeta,
                        tipo_base    = tipo_base,
                        mes          = mes_archivo,
                        año          = _detectar_año(archivo.name, carpeta_año.name),
                        procesado    = ya_procesado,
                        procesado_el = procesados.get(ruta_str, {}).get("procesado_el", ""),
                    ))

    return archivos


def archivos_nuevos(archivos: list[ArchivoDetectado]) -> list[ArchivoDetectado]:
    """Filtra solo los archivos no procesados."""
    return [a for a in archivos if not a.procesado]


def archivos_procesados(archivos: list[ArchivoDetectado]) -> list[ArchivoDetectado]:
    """Filtra solo los archivos ya procesados."""
    return [a for a in archivos if a.procesado]