"""
billing_report.py
Responsibility: Read, validate, and save the Billing Report.
Single sheet, flat data. Separate inventory from invoiced items.
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
    """Search column by exact name or with strip."""
    if nombre in df.columns:
        return nombre
    n = nombre.strip()
    for c in df.columns:
        if c.strip() == n:
            return c
    return None


def read_report(archivo) -> tuple:
    warnings = []
    try:
        df = pd.read_excel(archivo, header=0)
    except Exception as e:
        return None, [f"No se pudo leer el archivo: {e}"]

    df.columns = df.columns.str.strip()

    missing = [c for c in COLUMNAS_CLAVE if not _col(df, c)]
    if missing:
        warnings.append(f"Columnas no encontradas: {', '.join(missing)}")

    # Convert float integer-like columns to Int64 -> str (preserve missing)
    for col in df.columns:
        if pd.api.types.is_float_dtype(df[col]):
            non_nan = df[col].dropna()
            if len(non_nan) > 0 and (non_nan % 1 == 0).all():
                df[col] = df[col].astype("Int64").astype(str).replace("<NA>", "")

    return df, warnings


def kpis_report(df: pd.DataFrame) -> dict:
    col_state = _col(df, COL_ESTADO)
    col_value = _col(df, "VALOR TOTAL") or _col(df, " VALOR TOTAL ")

    active_count = cancelled_count = 0
    if col_state:
        estado_norm = df[col_state].astype(str).str.strip().str.upper()
        active_count  = int((estado_norm == "ACTIVO").sum())
        cancelled_count = int((estado_norm == "ANULADO").sum())

    total_value = 0.0
    if col_value:
        total_value = pd.to_numeric(df[col_value], errors="coerce").fillna(0).sum()

    agreements = []
    col_conv = _col(df, "CONVENIO")
    if col_conv:
        agreements = sorted(df[col_conv].dropna().unique().tolist())

    date_min = date_max = None
    for candidate in ["FECHA_FACTURA", "FECHA PRESTACION"]:
        col_candidate = _col(df, candidate)
        if col_candidate:
            dates = pd.to_datetime(df[col_candidate], errors="coerce").dropna()
            if not dates.empty:
                date_min = dates.min()
                date_max = dates.max()
                break

    # Keep return keys in Spanish to match UI expectations
    return {
        "total": int(len(df)),
        "activas": active_count,
        "anuladas": cancelled_count,
        "valor_total": float(total_value),
        "convenios": agreements,
        "fecha_min": date_min,
        "fecha_max": date_max,
    }


def save_report(df: pd.DataFrame) -> Path:
    PARQUET_INFORME.parent.mkdir(parents=True, exist_ok=True)
    df_copy = df.copy()
    for col in df_copy.columns:
        if df_copy[col].dtype == object:
            df_copy[col] = df_copy[col].fillna("").astype(str)
    df_copy.to_parquet(PARQUET_INFORME, index=False, engine="pyarrow")
    return PARQUET_INFORME


def load_report():
    if PARQUET_INFORME.exists():
        return pd.read_parquet(PARQUET_INFORME, engine="pyarrow")
    return None


def info_save_report():
    if not PARQUET_INFORME.exists():
        return None
    df    = load_report()
    mtime = PARQUET_INFORME.stat().st_mtime
    fecha = datetime.datetime.fromtimestamp(mtime).strftime("%d/%m/%Y %H:%M")
    col_state = _col(df, COL_ESTADO)
    activas = anuladas = 0
    if col_state:
        en = df[col_state].astype(str).str.strip().str.upper()
        activas  = int((en == "ACTIVO").sum())
        anuladas = int((en == "ANULADO").sum())
    return {"fecha_guardado": fecha, "total": len(df),
            "activas": activas, "anuladas": anuladas}
