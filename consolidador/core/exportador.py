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
from pathlib import Path
from .analizador import (
    resumen_por_convenio,
    pendientes_por_facturador,
)


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
# NIVEL 2 — POR CONVENIO
# ════════════════════════════════════════════════════════════

def convenio_csv(df: pd.DataFrame, convenio: str) -> bytes:
    return _csv(df[df["nombre_convenio"] == convenio])


def convenio_excel(df: pd.DataFrame, convenio: str) -> bytes:
    df_c = df[df["nombre_convenio"] == convenio]
    return _excel({
        "Resumen":                   resumen_por_convenio(df_c),
        "Pendientes por Facturador": pendientes_por_facturador(df_c),
        "Detalle Pendientes":        df_c[df_c["estado"] == "Pendiente"],
        "Todos los registros":       df_c,
    })


# ════════════════════════════════════════════════════════════
# NIVEL 3 — POR TIPO DE BASE
# ════════════════════════════════════════════════════════════

def tipo_base_csv(df: pd.DataFrame, tipo_base: str) -> bytes:
    return _csv(df[df["tipo_base"] == tipo_base])


def tipo_base_excel(df: pd.DataFrame, tipo_base: str) -> bytes:
    df_t = df[df["tipo_base"] == tipo_base]
    return _excel({
        "Resumen":             resumen_por_convenio(df_t),
        "Detalle Pendientes":  df_t[df_t["estado"] == "Pendiente"],
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