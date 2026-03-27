"""
Billing
Responsibility: Read, validate, and save the billing file.
The Excel file has 3 sheets:
- Sheet 1: Pivot table (ignore)
- Sheet 2: Invoices with Active status
- Sheet 3: Invoices with Cancelled status
It is saved in data/parquet/billing.parquet (replaces if it already exists).
"""

import pandas as pd
from pathlib import Path

PARQUET_FACTURADO = Path(__file__).parent.parent / "datos" / "parquet" / "facturado.parquet"

# Expected (normalized) columns
BILLING_COLUMNS = [
    "PREFIJO", "FACTURA", "FECHA LEGALIZACION", "FECHA FACTURA",
    "CUFE", "TIPO IDENTIFICACIÓN", "IDENTIFICACION", "PACIENTE",
    "VALOR PACIENTE", "VALOR TERCERO", "NIT", "EPS", "CONVENIO",
    "USUARIO", "Estado", "RADICADO PANACEA", "FECHA RADICADO",
    "RADICADO EXTERNO", "MES", "AÑO",
]

# Minimum key columns to validate that the file is correct
VALIDATE_COLUMNS = ["FACTURA", "IDENTIFICACION", "PACIENTE", "CONVENIO", "Estado"]


def read_billing(archivo) -> tuple[pd.DataFrame | None, list[str]]:
    """
    Lee el archivo de facturado desde las hojas 2 y 3.
    Agrega columna _estado_factura: 'Activo' o 'Anulado'.

    Retorna:
      - DataFrame combinado (Activo + Anulado)
      - Lista de advertencias
    """
    warnings: list[str] = []

    try:
        xl_file = pd.ExcelFile(archivo)
    except Exception as e:
        return None, [f"No se pudo leer el archivo: {e}"]

    sheet_names = xl_file.sheet_names
    if len(sheet_names) < 3:
        return None, [f"El archivo tiene {len(sheet_names)} hoja(s). Se esperan al menos 3."]

    dfs_list: list[pd.DataFrame] = []
    for idx, state in [(1, "Activo"), (2, "Anulado")]:
        try:
            df_sheet = xl_file.parse(idx)
            df_sheet.columns = df_sheet.columns.str.strip()

            # Validar columnas clave
            missing = [c for c in VALIDATE_COLUMNS if c not in df_sheet.columns]
            if missing:
                warnings.append(
                    f"Hoja '{sheet_names[idx]}' ({state}): columnas no encontradas: {', '.join(missing)}"
                )

            df_sheet["_estado_factura"] = state
            dfs_list.append(df_sheet)
        except Exception as e:
            warnings.append(f"Error leyendo hoja '{sheet_names[idx]}' ({state}): {e}")

    if not dfs_list:
        return None, warnings + ["No se pudo leer ninguna hoja."]

    df_total = pd.concat(dfs_list, ignore_index=True)

    for col in df_total.columns:
        if pd.api.types.is_float_dtype(df_total[col]):
            non_na = df_total[col].dropna()
            if len(non_na) > 0 and (non_na % 1 == 0).all():
                df_total[col] = (
                    df_total[col].astype("Int64").astype(str).replace("<NA>", "")
                )

    return df_total, warnings


def billing_kpis(df: pd.DataFrame) -> dict:
    """KPIs básicos del archivo de facturado."""

    def _col_series(df_in: pd.DataFrame, col: str) -> pd.Series | None:
        if col not in df_in.columns:
            return None
        series_or_df = df_in[col]

        if isinstance(series_or_df, pd.DataFrame):
            return series_or_df.iloc[:, 0]
        return series_or_df

    state_series = _col_series(df, "_estado_factura")
    if state_series is None:
        state_series = _col_series(df, "Estado")

    if state_series is None:
        active_count = 0
        cancelled_count = 0
        mask_active = pd.Series([False] * len(df), index=df.index)
    else:
        state_norm = state_series.astype(str).str.strip().str.lower()
        mask_active = state_norm.eq("activo")
        mask_cancelled = state_norm.eq("anulado")
        active_count = int(mask_active.sum())
        cancelled_count = int(mask_cancelled.sum())

    total_value = 0.0
    value_col = _col_series(df, "VALOR TERCERO")
    if value_col is not None:
        numeric_vals = pd.to_numeric(value_col, errors="coerce").fillna(0)
        total_value = float(numeric_vals[mask_active].sum())

    agreements_col = _col_series(df, "CONVENIO")
    agreements = (
        sorted(agreements_col.dropna().astype(str).str.strip().unique().tolist())
        if agreements_col is not None else []
    )

    date_min = date_max = None
    for candidate in ["FECHA FACTURA", "FECHA LEGALIZACION"]:
        date_col = _col_series(df, candidate)
        if date_col is not None:
            dates = pd.to_datetime(date_col, errors="coerce").dropna()
            if not dates.empty:
                date_min = dates.min()
                date_max = dates.max()
                break

    return {
        "activas": int(active_count),
        "anuladas": int(cancelled_count),
        "total": int(len(df)),  # Total registros (activas + anuladas)
        "valor_total": total_value,  # Solo activas
        "fecha_min": date_min,
        "fecha_max": date_max,
    }


def guardar_facturado(df: pd.DataFrame) -> Path:
    """Save the DataFrame in Parquet, replacing it if it already exists."""
    PARQUET_FACTURADO.parent.mkdir(parents=True, exist_ok=True)

    df_save = df.copy()
    for col in df_save.columns:
        if df_save[col].dtype == object:
            df_save[col] = df_save[col].fillna("").astype(str)

    df_save.to_parquet(PARQUET_FACTURADO, index=False, engine="pyarrow")
    return PARQUET_FACTURADO


def cargar_facturado() -> pd.DataFrame | None:
    """Loads the saved invoice. Returns None if it does not exist."""
    if PARQUET_FACTURADO.exists():
        return pd.read_parquet(PARQUET_FACTURADO, engine="pyarrow")
    return None


def info_facturado_guardado() -> dict | None:
    """
    Returns saved invoice metadata:
    modification date, total records, active records, voided records.
    """
    if not PARQUET_FACTURADO.exists():
        return None
    import datetime
    df = cargar_facturado()
    mtime = PARQUET_FACTURADO.stat().st_mtime
    fecha = datetime.datetime.fromtimestamp(mtime).strftime("%d/%m/%Y %H:%M")
    return {
        "fecha_guardado": fecha,
        "total":          len(df),
        "activas":        int((df["_estado_factura"] == "Activo").sum()),
        "anuladas":       int((df["_estado_factura"] == "Anulado").sum()),
    }
