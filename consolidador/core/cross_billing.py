"""
Cross
Responsibility: cross processed bases with the billing (invoiced) file.

Default key:
  Base      : documento_paciente + fecha_atencion + cups
  Billed    : IDENTIFICACION + MES (+ CUPS if exists)

Per base_type a custom key may be configured in config.json:
  "llave_cruce": ["documento_paciente", "fecha_atencion"]

Result: adds a new column "estado_cruce" to the bases:
  "Facturado"     → exists in active billed
  "No facturado"  → does not exist
  "Sin cruce"     → no billed file provided
"""

import pandas as pd
from core.exporter import PARQUET_DIR
from pathlib import Path

# Default keys (column names expected in the dataframes)
BASE_KEY_DEFAULT = ["DOCUMENTO", "FECHA DE INICIO DEL SERVICIO", "cups"]
BILLED_KEY_DEFAULT = ["IDENTIFICACION", "MES"]

# Billed CUPS column (may not exist)
BILLED_CUPS_COL = "CUPS"

MONTH_NUM_TO_NAME = {
    "1": "ENERO", "2": "FEBRERO", "3": "MARZO", "4": "ABRIL",
    "5": "MAYO", "6": "JUNIO", "7": "JULIO", "8": "AGOSTO",
    "9": "SEPTIEMBRE", "10": "OCTUBRE", "11": "NOVIEMBRE", "12": "DICIEMBRE",
    "01": "ENERO", "02": "FEBRERO", "03": "MARZO", "04": "ABRIL",
    "05": "MAYO", "06": "JUNIO", "07": "JULIO", "08": "AGOSTO",
    "09": "SEPTIEMBRE",
}


# ════════════════════════════════════════════════════════════
# NORMALIZATION HELPERS
# ════════════════════════════════════════════════════════════

def _normalize_str(series: pd.Series) -> pd.Series:
    """Strip, upper-case and collapse multiple spaces."""
    return series.astype(str).str.strip().str.upper().str.replace(r"\s+", " ", regex=True)


def _normalize_date(series: pd.Series) -> pd.Series:
    """Convert to DD-MM-YYYY string. Invalid values -> ''."""
    dates = pd.to_datetime(series, errors="coerce")
    return dates.dt.strftime("%d-%m-%Y").fillna("")


def _normalize_month(series: pd.Series) -> pd.Series:
    """
    Convert month-like values to uppercase month name: "FEBRERO".
    Accepts:
      - integer or string numbers: 1, "1", "01", "12"
      - month name: "febrero", "FEBRERO"
      - full date: "2025-12-01", "2025-12-01 00:00:00"
    """
    def _conv(val):
        v = str(val).strip()

        # If it looks like a date, extract the month
        if "-" in v or "/" in v:
            try:
                month_num = str(pd.to_datetime(v).month)
                return MONTH_NUM_TO_NAME.get(month_num, month_num)
            except Exception:
                pass

        # If it is a number or name, map to month name
        v_upper = v.upper()
        return MONTH_NUM_TO_NAME.get(v_upper, v_upper)

    return series.apply(_conv)


def _is_month_column(col: str) -> bool:
    """
    Determine if a column should be normalized as a month column for the join key.
    """
    c = str(col).strip().upper()
    return c in {
        "MES",
        "FECHA DE INICIO DEL SERVICIO",
    }


def _construct_key(df: pd.DataFrame, columns: list[str], sep: str = "_") -> pd.Series:
    """
    Build a composite key (string) from the given list of columns.
    Missing columns produce empty parts.
    Month columns are normalized via `_normalize_month`, others via `_normalize_str`.
    """
    parts = []
    for col in columns:
        if col not in df.columns:
            parts.append(pd.Series([""] * len(df), index=df.index))
            continue

        series = df[col].copy()
        is_month = _is_month_column(col)
        if is_month:
            series = _normalize_month(series)
        else:
            series = _normalize_str(series)

        parts.append(series)

    if len(parts) > 1:
        return parts[0].str.cat(parts[1:], sep=sep)
    return parts[0]


# ════════════════════════════════════════════════════════════
# PREPARE BILLED KEY SET
# ════════════════════════════════════════════════════════════

def _prepare_billed_key_set(df_billed: pd.DataFrame) -> set:
    """
    From the billed DataFrame return the set of keys for active billed invoices.
    Uses BILLED_KEY_DEFAULT to construct keys.
    """
    df_active = df_billed[df_billed["_estado_factura"] == "Activo"].copy()
    keys = _construct_key(df_active, BILLED_KEY_DEFAULT)
    return set(keys.tolist())


# ════════════════════════════════════════════════════════════
# MAIN CROSSING FUNCTION
# ════════════════════════════════════════════════════════════

def cross_bases_with_billed(
        df_bases: pd.DataFrame,
        df_billed: pd.DataFrame,
        config: dict,
) -> pd.DataFrame:
    """
    Cross df_bases with df_billed and add a column 'estado_cruce'.

    Parameters:
      df_bases   : consolidated bases DataFrame (standard schema)
      df_billed  : billed (invoiced) DataFrame saved earlier
      config     : config.json content (to read 'llave_cruce' per base_type)

    Returns df_bases with an added 'estado_cruce' column.
    """
    if df_billed is None or df_billed.empty:
        df_bases["estado_cruce"] = "Sin cruce"
        return df_bases

    # prepare billed keys set
    billed_key_set = _prepare_billed_key_set(df_billed)

    results = []

    # process per base type (each type may have a different key)
    for base_type, group in df_bases.groupby("tipo_base"):
        group = group.copy()

        conf_type = config.get(base_type, {})
        key_columns = conf_type.get("llave_cruce", BASE_KEY_DEFAULT)
        group_key = _construct_key(group, key_columns)

        group["llave_cruce"] = group_key
        group["estado_cruce"] = group_key.apply(
            lambda k: "Facturado" if k in billed_key_set else "No facturado"
        )
        results.append(group)

    return pd.concat(results, ignore_index=True)


# ════════════════════════════════════════════════════════════
# KPIs FOR CROSSING
# ════════════════════════════════════════════════════════════

def crossing_kpis(df: pd.DataFrame) -> dict:
    """
    Summary of crossing results.
    Requires df to have column 'estado_cruce'.
    """
    if "estado_cruce" not in df.columns:
        return {}

    total = len(df)
    billed = int((df["estado_cruce"] == "Facturado").sum())
    not_billed = int((df["estado_cruce"] == "No facturado").sum())
    no_cross = int((df["estado_cruce"] == "Sin cruce").sum())
    pct = round(billed / total * 100, 1) if total > 0 else 0.0

    return {
        "total": total,
        "facturados": billed,
        "no_facturado": not_billed,
        "sin_cruce": no_cross,
        "cumplimiento": pct,
    }


def crossing_summary_by_agreement(df: pd.DataFrame) -> pd.DataFrame:
    """Crossing summary grouped by agreement (nombre_convenio)."""
    if "estado_cruce" not in df.columns:
        return pd.DataFrame()

    summary = (
        df.groupby("nombre_convenio")["estado_cruce"]
        .value_counts()
        .unstack(fill_value=0)
        .reset_index()
    )
    for c in ["Facturado", "No facturado", "Sin cruce"]:
        if c not in summary.columns:
            summary[c] = 0

    summary["Total"] = summary[["Facturado", "No facturado", "Sin cruce"]].sum(axis=1)
    summary["Cumplimiento (%)"] = (
            summary["Facturado"] / summary["Total"] * 100
    ).round(1)
    return summary.rename(columns={"nombre_convenio": "Convenio"})


def crossing_summary_by_base_type(df: pd.DataFrame) -> pd.DataFrame:
    """Crossing summary grouped by base type (tipo_base)."""
    if "estado_cruce" not in df.columns:
        return pd.DataFrame()

    summary = (
        df.groupby("tipo_base")["estado_cruce"]
        .value_counts()
        .unstack(fill_value=0)
        .reset_index()
    )
    for c in ["Facturado", "No facturado", "Sin cruce"]:
        if c not in summary.columns:
            summary[c] = 0

    summary["Total"] = summary[["Facturado", "No facturado", "Sin cruce"]].sum(axis=1)
    summary["Cumplimiento (%)"] = (
            summary["Facturado"] / summary["Total"] * 100
    ).round(1)
    return summary.rename(columns={"tipo_base": "Tipo de base"})


# ════════════════════════════════════════════════════════════
# SAVE/LOAD CROSSING RESULT
# ════════════════════════════════════════════════════════════

def save_crossing(df: pd.DataFrame, month_label: str) -> Path:
    """Save crossing result as parquet (replaces if exists)."""
    path = PARQUET_DIR / f"cruce_{_safe_name(month_label)}.parquet"
    df_to_save = df.copy()
    for col in df_to_save.columns:
        if df_to_save[col].dtype == object:
            df_to_save[col] = df_to_save[col].fillna("").astype(str)
    df_to_save.to_parquet(path, index=False, engine="pyarrow")
    return path


def load_crossing(month_label: str) -> pd.DataFrame | None:
    """Load saved crossing for a month. Returns None if not found."""
    path = PARQUET_DIR / f"cruce_{_safe_name(month_label)}.parquet"
    if path.exists():
        return pd.read_parquet(path, engine="pyarrow")
    return None


def available_crossings() -> list[str]:
    """List months that have saved crossing parquet files."""
    files = sorted(PARQUET_DIR.glob("cruce_*.parquet"))
    return [
        f.stem.replace("cruce_", "").replace("_", " ")
        for f in files
    ]


def _safe_name(text: str) -> str:
    return text.replace(" ", "_").replace("/", "-").replace("\\", "-")
