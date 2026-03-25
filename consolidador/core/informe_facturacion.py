"""
informe_facturacion.py
Responsabilidad: leer, validar y guardar el Informe de Facturación.
Una sola hoja, datos planos. Parquet separado del facturado.
"""

import pandas as pd
import datetime
from pathlib import Path

PARQUET_INFORME = Path(__file__).parent.parent / "datos" / "parquet" / "informe_facturacion.parquet"

COLUMNAS_CLAVE = [
    "concatenado doc_mes_ cups",
    "concatenado doc_mes_servicio",
    "ESTADO DE FACTURA",
    "NUMERO_IDENTIFICACION",
]

COL_CONCAT_CUPS     = "concatenado doc_mes_ cups"
COL_CONCAT_SERVICIO = "concatenado doc_mes_servicio"
COL_ESTADO          = "ESTADO DE FACTURA"
COLS_CRUCE_RESULTADO = ["VALOR TOTAL", "CUFE", "facturador"]


def _col(df: pd.DataFrame, nombre: str):
    """Busca columna por nombre exacto o con strip."""
    if nombre in df.columns:
        return nombre
    n = nombre.strip()
    for c in df.columns:
        if c.strip() == n:
            return c
    return None


def leer_informe(archivo) -> tuple:
    advertencias = []
    try:
        df = pd.read_excel(archivo, header=0)
    except Exception as e:
        return None, [f"No se pudo leer el archivo: {e}"]

    df.columns = df.columns.str.strip()

    faltantes = [c for c in COLUMNAS_CLAVE if not _col(df, c)]
    if faltantes:
        advertencias.append(f"Columnas no encontradas: {', '.join(faltantes)}")

    for col in df.columns:
        if pd.api.types.is_float_dtype(df[col]):
            col_nonan = df[col].dropna()
            if len(col_nonan) > 0 and (col_nonan % 1 == 0).all():
                df[col] = df[col].astype("Int64").astype(str).replace("<NA>", "")

    return df, advertencias


def kpis_informe(df: pd.DataFrame) -> dict:
    col_estado = _col(df, COL_ESTADO)
    col_valor  = _col(df, "VALOR TOTAL") or _col(df, " VALOR TOTAL ")

    activas = anuladas = 0
    if col_estado:
        estado_norm = df[col_estado].astype(str).str.strip().str.upper()
        activas  = int((estado_norm == "ACTIVO").sum())
        anuladas = int((estado_norm == "ANULADO").sum())

    valor_total = 0.0
    if col_valor:
        valor_total = pd.to_numeric(df[col_valor], errors="coerce").fillna(0).sum()

    convenios = []
    col_conv = _col(df, "CONVENIO")
    if col_conv:
        convenios = sorted(df[col_conv].dropna().unique().tolist())

    fecha_min = fecha_max = None
    for fc in ["FECHA_FACTURA", "FECHA PRESTACION"]:
        c = _col(df, fc)
        if c:
            fechas = pd.to_datetime(df[c], errors="coerce").dropna()
            if not fechas.empty:
                fecha_min = fechas.min()
                fecha_max = fechas.max()
                break

    return {
        "total": int(len(df)), "activas": activas, "anuladas": anuladas,
        "valor_total": float(valor_total), "convenios": convenios,
        "fecha_min": fecha_min, "fecha_max": fecha_max,
    }


def guardar_informe(df: pd.DataFrame) -> Path:
    PARQUET_INFORME.parent.mkdir(parents=True, exist_ok=True)
    df_g = df.copy()
    for col in df_g.columns:
        if df_g[col].dtype == object:
            df_g[col] = df_g[col].fillna("").astype(str)
    df_g.to_parquet(PARQUET_INFORME, index=False, engine="pyarrow")
    return PARQUET_INFORME


def cargar_informe():
    if PARQUET_INFORME.exists():
        return pd.read_parquet(PARQUET_INFORME, engine="pyarrow")
    return None


def info_informe_guardado():
    if not PARQUET_INFORME.exists():
        return None
    df    = cargar_informe()
    mtime = PARQUET_INFORME.stat().st_mtime
    fecha = datetime.datetime.fromtimestamp(mtime).strftime("%d/%m/%Y %H:%M")
    col_estado = _col(df, COL_ESTADO)
    activas = anuladas = 0
    if col_estado:
        en = df[col_estado].astype(str).str.strip().str.upper()
        activas  = int((en == "ACTIVO").sum())
        anuladas = int((en == "ANULADO").sum())
    return {"fecha_guardado": fecha, "total": len(df),
            "activas": activas, "anuladas": anuladas}