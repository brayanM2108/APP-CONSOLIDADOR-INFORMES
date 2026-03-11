"""
exportador.py
Responsabilidad: generar archivos de salida en 3 niveles.

Nivel 1 → General       : todos los convenios y tipos de base
Nivel 2 → Por convenio  : un reporte por cada convenio
Nivel 3 → Por tipo base : un reporte por cada tipo de base

Formatos: CSV (liviano) + Excel (múltiples hojas)
"""

import pandas as pd
import io
import json
from pathlib import Path
from .analizador import (
    resumen_por_convenio,
    pendientes_por_facturador,
)

# ════════════════════════════════════════════════════════════
# CARGA DE CONFIGURACIÓN PARA COLUMNAS EXTRA POR TIPO DE BASE
# ════════════════════════════════════════════════════════════

def _cargar_config() -> dict:
    """Carga el config.json para obtener columnas extra por tipo de base."""
    config_path = Path(__file__).parent.parent / "config" / "config.json"
    if config_path.exists():
        return json.loads(config_path.read_text(encoding="utf-8"))
    return {}


def _aliases_extra_de_tipo_base(tipo_base: str) -> list[str]:
    """
    Retorna la lista de aliases (nombres finales) de las columnas extra
    configuradas para un tipo de base específico.
    Solo devuelve los aliases del tipo_base indicado, no de otros.
    """
    config = _cargar_config()
    conf_tipo = config.get(tipo_base, {})
    columnas_extra = conf_tipo.get("columnas_extra", [])
    aliases = []
    for item in columnas_extra:
        if isinstance(item, dict):
            aliases.append(item.get("alias", item.get("col", "")))
        elif isinstance(item, str):
            aliases.append(item)
    return [a for a in aliases if a]


# ════════════════════════════════════════════════════════════
# HELPERS INTERNOS
# ════════════════════════════════════════════════════════════

def _excel(hojas: dict) -> bytes:
    """Construye un Excel con múltiples hojas."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        for nombre_hoja, df in hojas.items():
            if not df.empty:
                df.to_excel(w, sheet_name=nombre_hoja[:31], index=False)
    buf.seek(0)
    return buf.getvalue()


def _csv(df: pd.DataFrame) -> bytes:
    """CSV con encoding correcto para Excel en español."""
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")


def _nombre_seguro(texto: str) -> str:
    """Convierte texto a nombre de archivo seguro."""
    return texto.replace(" ", "_").replace("/", "-").replace("\\", "-")


# ════════════════════════════════════════════════════════════
# NIVEL 1 — GENERAL
# ════════════════════════════════════════════════════════════

def general_csv(df: pd.DataFrame) -> bytes:
    return _csv(df)


def general_excel(df: pd.DataFrame) -> bytes:
    return _excel({
        "Resumen por Convenio":      resumen_por_convenio(df),
        "Pendientes por Facturador": pendientes_por_facturador(df),
        "Detalle Pendientes":        df[df["estado"] == "Pendiente"],
        "Consolidado General":       df,
    })


# ════════════════════════════════════════════════════════════
# HELPER — limpieza de columnas
# ════════════════════════════════════════════════════════════

def _limpiar_columnas_vacias(df) -> "pd.DataFrame":
    """Elimina columnas completamente vacías o NaN."""
    cols_no_vacias = [
        c for c in df.columns
        if not df[c].isna().all() and df[c].astype(str).str.strip().ne("").any()
    ]
    return df[cols_no_vacias]


def _columnas_propias_tipo_base(df: "pd.DataFrame", tipo_base: str) -> list[str]:
    """
    Retorna la lista de columnas que pertenecen al tipo_base indicado:
    las columnas estándar (COLUMNAS) + solo las columnas extra configuradas
    para ESE tipo_base en el config.json.

    Esto evita que columnas extra de otros tipos de base aparezcan en el reporte.
    """
    from .procesador import COLUMNAS as COLS_STD
    aliases_extra = _aliases_extra_de_tipo_base(tipo_base)
    columnas_objetivo = COLS_STD + aliases_extra
    # Solo incluir las que existen en el DataFrame
    return [c for c in columnas_objetivo if c in df.columns]


# ════════════════════════════════════════════════════════════
# NIVEL 2 — POR CONVENIO
# ════════════════════════════════════════════════════════════

def convenio_csv(df: pd.DataFrame, convenio: str) -> bytes:
    return _csv(df[df["nombre_convenio"] == convenio])


# Columnas estándar que van en el detalle de pendientes del convenio
COLS_DETALLE_PENDIENTES = [
    "tipo_base",
    "documento_paciente",
    "nombre_paciente",
    "descripcion_servicio",
    "fecha_atencion",
    "facturador",
    "observacion",
    "archivo_origen",
]


def convenio_excel(df: pd.DataFrame, convenio: str) -> bytes:
    """
    Excel del convenio con esta estructura:

      - Resumen                  : KPIs del convenio
      - Pendientes por Facturador: resumen agrupado
      - Detalle Pendientes       : fila a fila, filtrable por tipo_base
      - [Tipo de base 1]         : todos los registros con sus columnas limpias
      - [Tipo de base 2]         : idem
      - ...
    """
    df_c = df[df["nombre_convenio"] == convenio]

    # Detalle pendientes — solo columnas estándar + tipo_base para filtrar
    df_pend = df_c[df_c["estado"] == "Pendiente"].copy()
    cols_detalle = [c for c in COLS_DETALLE_PENDIENTES if c in df_pend.columns]
    df_detalle = df_pend[cols_detalle].sort_values("tipo_base")

    hojas = {
        "Resumen":                   resumen_por_convenio(df_c),
        "Pendientes por Facturador": pendientes_por_facturador(df_c),
        "Detalle Pendientes":        df_detalle,
    }

    # Una hoja por tipo de base con sus columnas propias
    for tipo_base in sorted(df_c["tipo_base"].unique()):
        df_tipo = df_c[df_c["tipo_base"] == tipo_base].copy()
        cols = _columnas_propias_tipo_base(df_tipo, tipo_base)
        df_tipo = df_tipo[cols]
        nombre_hoja = tipo_base.split(" - ", 1)[-1] if " - " in tipo_base else tipo_base
        hojas[nombre_hoja] = df_tipo

    return _excel(hojas)


# ════════════════════════════════════════════════════════════
# NIVEL 3 — POR TIPO DE BASE
# ════════════════════════════════════════════════════════════

def tipo_base_csv(df: pd.DataFrame, tipo_base: str) -> bytes:
    df_t = df[df["tipo_base"] == tipo_base].copy()
    cols = _columnas_propias_tipo_base(df_t, tipo_base)
    return _csv(df_t[cols])


def tipo_base_excel(df: pd.DataFrame, tipo_base: str) -> bytes:
    from .procesador import COLUMNAS

    df_t = df[df["tipo_base"] == tipo_base].copy()
    cols = _columnas_propias_tipo_base(df_t, tipo_base)
    df_t = df_t[cols]

    # Columnas extra que pertenecen a este tipo de base
    cols_extra = [c for c in cols if c not in COLUMNAS]

    # Detalle pendientes con columnas extra incluidas
    cols_detalle = [
        "nombre_convenio", "tipo_base", "documento_paciente",
        "nombre_paciente", "descripcion_servicio",
        "facturador", "observacion", "archivo_origen",
    ] + cols_extra
    cols_detalle = [c for c in cols_detalle if c in df_t.columns]

    df_pend = df_t[df_t["estado"] == "Pendiente"]

    return _excel({
        "Resumen":             resumen_por_convenio(df_t),
        "Detalle Pendientes":  df_pend[cols_detalle],
        "Todos los registros": df_t,
    })


# ════════════════════════════════════════════════════════════
# EXPORTACIÓN COMPLETA — guarda los 3 niveles en disco
# ════════════════════════════════════════════════════════════

def exportar_todo_en_disco(
    df: pd.DataFrame,
    carpeta_reportes: str,
    mes_label: str,
) -> dict:
    """
    Guarda todos los reportes en:

    carpeta_reportes/
      mes_label/
        general/
        por_convenio/
        por_tipo_base/

    Retorna dict con rutas generadas por nivel.
    """
    base  = Path(carpeta_reportes) / _nombre_seguro(mes_label)
    rutas = {"general": [], "por_convenio": [], "por_tipo_base": []}

    # Nivel 1
    carpeta_gen = base / "general"
    carpeta_gen.mkdir(parents=True, exist_ok=True)
    for ext, datos in [("csv", general_csv(df)), ("xlsx", general_excel(df))]:
        ruta = carpeta_gen / f"general_{_nombre_seguro(mes_label)}.{ext}"
        ruta.write_bytes(datos)
        rutas["general"].append(str(ruta))

    # Nivel 2
    carpeta_conv = base / "por_convenio"
    carpeta_conv.mkdir(parents=True, exist_ok=True)
    for convenio in sorted(df["nombre_convenio"].unique()):
        n = _nombre_seguro(convenio)
        for ext, datos in [
            ("csv",  convenio_csv(df, convenio)),
            ("xlsx", convenio_excel(df, convenio)),
        ]:
            ruta = carpeta_conv / f"{n}_{_nombre_seguro(mes_label)}.{ext}"
            ruta.write_bytes(datos)
            rutas["por_convenio"].append(str(ruta))

    # Nivel 3
    carpeta_tipo = base / "por_tipo_base"
    carpeta_tipo.mkdir(parents=True, exist_ok=True)
    for tipo in sorted(df["tipo_base"].unique()):
        n = _nombre_seguro(tipo)
        for ext, datos in [
            ("csv",  tipo_base_csv(df, tipo)),
            ("xlsx", tipo_base_excel(df, tipo)),
        ]:
            ruta = carpeta_tipo / f"{n}_{_nombre_seguro(mes_label)}.{ext}"
            ruta.write_bytes(datos)
            rutas["por_tipo_base"].append(str(ruta))

    return rutas


# ════════════════════════════════════════════════════════════
# NOMBRES PARA DESCARGA EN STREAMLIT
# ════════════════════════════════════════════════════════════

def nombre_general(mes_label: str, ext: str) -> str:
    return f"general_{_nombre_seguro(mes_label)}.{ext}"

def nombre_convenio_archivo(convenio: str, mes_label: str, ext: str) -> str:
    return f"{_nombre_seguro(convenio)}_{_nombre_seguro(mes_label)}.{ext}"

def nombre_tipo_base_archivo(tipo_base: str, mes_label: str, ext: str) -> str:
    return f"{_nombre_seguro(tipo_base)}_{_nombre_seguro(mes_label)}.{ext}"


# ════════════════════════════════════════════════════════════
# PARQUET — almacenamiento interno eficiente
# ════════════════════════════════════════════════════════════

PARQUET_DIR = Path("datos/parquet")


def guardar_parquet(df: pd.DataFrame, mes_label: str) -> Path:
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    ruta = PARQUET_DIR / f"consolidado_{_nombre_seguro(mes_label)}.parquet"

    def _limpiar(df_in):
        df_out = df_in.copy()
        for col in df_out.columns:
            if df_out[col].dtype == object:
                df_out[col] = df_out[col].fillna("").astype(str)
        return df_out

    # Si ya existe el mes, combinar con lo nuevo
    if ruta.exists():
        df_existente = pd.read_parquet(ruta, engine="pyarrow")
        df_final = pd.concat([df_existente, df], ignore_index=True)
    else:
        df_final = df

    _limpiar(df_final).to_parquet(ruta, index=False, engine="pyarrow")
    return ruta

def cargar_parquet(mes_label: str) -> pd.DataFrame | None:
    """
    Carga el consolidado de un mes desde Parquet.
    Retorna None si no existe.
    """
    ruta = PARQUET_DIR / f"consolidado_{_nombre_seguro(mes_label)}.parquet"
    if ruta.exists():
        return pd.read_parquet(ruta, engine="pyarrow")
    return None


def cargar_todos_parquet() -> pd.DataFrame | None:
    """
    Carga y concatena todos los meses disponibles en Parquet.
    Útil para construir la base maestra acumulada.
    """
    archivos = sorted(PARQUET_DIR.glob("consolidado_*.parquet"))
    if not archivos:
        return None
    dfs = [pd.read_parquet(f, engine="pyarrow") for f in archivos]
    return pd.concat(dfs, ignore_index=True)


def meses_disponibles_parquet() -> list[str]:
    """
    Lista los meses que ya tienen consolidado guardado en Parquet.
    """
    archivos = sorted(PARQUET_DIR.glob("consolidado_*.parquet"))
    return [
        f.stem.replace("consolidado_", "").replace("_", " ")
        for f in archivos
    ]
def eliminar_archivo_de_parquet(nombre_archivo: str) -> dict:
    archivos_parquet = sorted(PARQUET_DIR.glob("consolidado_*.parquet"))
    total_eliminados = 0
    meses_afectados  = []

    for ruta in archivos_parquet:
        try:
            df = pd.read_parquet(ruta, engine="pyarrow")
            mask = df["archivo_origen"] == nombre_archivo
            n_filas = mask.sum()
            if n_filas == 0:
                continue
            df_limpio = df[~mask].reset_index(drop=True)
            if df_limpio.empty:
                ruta.unlink()
            else:
                for col in df_limpio.columns:
                    if df_limpio[col].dtype == object:
                        df_limpio[col] = df_limpio[col].fillna("").astype(str)
                df_limpio.to_parquet(ruta, index=False, engine="pyarrow")
            total_eliminados += n_filas
            meses_afectados.append(ruta.stem.replace("consolidado_", "").replace("_", " "))
        except Exception as e:
            return {"eliminados": 0, "meses_afectados": [], "error": str(e)}

    try:
        from .watcher import PROCESADOS_PATH
        if PROCESADOS_PATH.exists():
            procesados = json.loads(PROCESADOS_PATH.read_text(encoding="utf-8"))
            procesados_limpios = {
                ruta: datos for ruta, datos in procesados.items()
                if Path(ruta).name != nombre_archivo
            }
            PROCESADOS_PATH.write_text(
                json.dumps(procesados_limpios, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )
    except Exception:
        pass  # No bloquear si falla la limpieza del json

    return {"eliminados": total_eliminados, "meses_afectados": meses_afectados, "error": None}