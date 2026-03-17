"""
facturado.py
Responsabilidad: leer, validar y guardar el archivo de facturado.

El archivo Excel tiene 3 hojas:
  - Hoja 1: tabla dinámica (ignorar)
  - Hoja 2: facturas con estado Activo
  - Hoja 3: facturas con estado Anulado

Se guarda en datos/parquet/facturado.parquet (reemplaza si existe).
"""

import pandas as pd
from pathlib import Path

PARQUET_FACTURADO = Path(__file__).parent.parent / "datos" / "parquet" / "facturado.parquet"

# Columnas esperadas (normalizadas)
COLUMNAS_FACTURADO = [
    "PREFIJO", "FACTURA", "FECHA LEGALIZACION", "FECHA FACTURA",
    "CUFE", "TIPO IDENTIFICACIÓN", "IDENTIFICACION", "PACIENTE",
    "VALOR PACIENTE", "VALOR TERCERO", "NIT", "EPS", "CONVENIO",
    "USUARIO", "Estado", "RADICADO PANACEA", "FECHA RADICADO",
    "RADICADO EXTERNO", "MES", "AÑO",
]

# Columnas clave mínimas para validar que el archivo es correcto
COLUMNAS_CLAVE = ["FACTURA", "IDENTIFICACION", "PACIENTE", "CONVENIO", "Estado"]


def leer_facturado(archivo) -> tuple[pd.DataFrame | None, list[str]]:
    """
    Lee el archivo de facturado desde las hojas 2 y 3.
    Agrega columna _estado_factura: 'Activo' o 'Anulado'.

    Retorna:
      - DataFrame combinado (Activo + Anulado)
      - Lista de advertencias
    """
    advertencias = []

    try:
        xl = pd.ExcelFile(archivo)
    except Exception as e:
        return None, [f"No se pudo leer el archivo: {e}"]

    hojas = xl.sheet_names
    if len(hojas) < 3:
        return None, [f"El archivo tiene {len(hojas)} hoja(s). Se esperan al menos 3."]

    dfs = []
    for idx, estado in [(1, "Activo"), (2, "Anulado")]:
        try:
            df = xl.parse(idx)
            df.columns = df.columns.str.strip()

            # Validar columnas clave
            faltantes = [c for c in COLUMNAS_CLAVE if c not in df.columns]
            if faltantes:
                advertencias.append(
                    f"Hoja '{hojas[idx]}' ({estado}): columnas no encontradas: {', '.join(faltantes)}"
                )

            df["_estado_factura"] = estado
            dfs.append(df)
        except Exception as e:
            advertencias.append(f"Error leyendo hoja '{hojas[idx]}' ({estado}): {e}")

    if not dfs:
        return None, advertencias + ["No se pudo leer ninguna hoja."]

    df_total = pd.concat(dfs, ignore_index=True)

    # Normalizar números grandes
    for col in df_total.columns:
        if pd.api.types.is_float_dtype(df_total[col]):
            col_nonan = df_total[col].dropna()
            if len(col_nonan) > 0 and (col_nonan % 1 == 0).all():
                df_total[col] = (
                    df_total[col].astype("Int64").astype(str).replace("<NA>", "")
                )

    return df_total, advertencias


def kpis_facturado(df: pd.DataFrame) -> dict:
    """KPIs básicos del archivo de facturado."""

    def _col_serie(df_in: pd.DataFrame, col: str) -> pd.Series | None:
        if col not in df_in.columns:
            return None
        c = df_in[col]
        # Si hay columnas duplicadas con el mismo nombre, pandas puede devolver DataFrame
        if isinstance(c, pd.DataFrame):
            return c.iloc[:, 0]
        return c

    # 1) Resolver columna de estado de forma robusta
    estado = _col_serie(df, "_estado_factura")
    if estado is None:
        estado = _col_serie(df, "Estado")

    if estado is None:
        # Sin estado, evitar fallo y devolver KPIs mínimos
        activas = 0
        anuladas = 0
        mask_activo = pd.Series([False] * len(df), index=df.index)
    else:
        estado_norm = estado.astype(str).str.strip().str.lower()
        mask_activo = estado_norm.eq("activo")
        mask_anulado = estado_norm.eq("anulado")
        activas = int(mask_activo.sum())
        anuladas = int(mask_anulado.sum())

    # 2) Valor total: SOLO facturas activas
    valor_total = 0.0
    valor_col = _col_serie(df, "VALOR TERCERO")
    if valor_col is not None:
        vals = pd.to_numeric(valor_col, errors="coerce").fillna(0)
        valor_total = float(vals[mask_activo].sum())

    convenios_col = _col_serie(df, "CONVENIO")
    convenios = (
        sorted(convenios_col.dropna().astype(str).str.strip().unique().tolist())
        if convenios_col is not None else []
    )

    fecha_min = fecha_max = None
    for col in ["FECHA FACTURA", "FECHA LEGALIZACION"]:
        fcol = _col_serie(df, col)
        if fcol is not None:
            fechas = pd.to_datetime(fcol, errors="coerce").dropna()
            if not fechas.empty:
                fecha_min = fechas.min()
                fecha_max = fechas.max()
                break

    return {
        "activas": int(activas),
        "anuladas": int(anuladas),
        "total": int(len(df)),  # Total registros (activas + anuladas)
        "valor_total": valor_total,  # Solo activas
        "fecha_min": fecha_min,
        "fecha_max": fecha_max,
    }


def guardar_facturado(df: pd.DataFrame) -> Path:
    """Guarda el DataFrame en Parquet, reemplazando si ya existe."""
    PARQUET_FACTURADO.parent.mkdir(parents=True, exist_ok=True)

    df_guardar = df.copy()
    for col in df_guardar.columns:
        if df_guardar[col].dtype == object:
            df_guardar[col] = df_guardar[col].fillna("").astype(str)

    df_guardar.to_parquet(PARQUET_FACTURADO, index=False, engine="pyarrow")
    return PARQUET_FACTURADO


def cargar_facturado() -> pd.DataFrame | None:
    """Carga el facturado guardado. Retorna None si no existe."""
    if PARQUET_FACTURADO.exists():
        return pd.read_parquet(PARQUET_FACTURADO, engine="pyarrow")
    return None


def info_facturado_guardado() -> dict | None:
    """
    Retorna metadata del facturado guardado:
    fecha de modificación, total registros, activos, anulados.
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