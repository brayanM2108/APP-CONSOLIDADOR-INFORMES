"""
Cross Billing Report
Responsibility: cross consolidated bases with the Billing Report.

Two configurable key types in config.json per base type:
  tipo_llave_informe: "doc_mes_cups"       → document + month + cups
  tipo_llave_informe: "doc_mes_año_codigo" → document + monthYEAR + procedure_code

  where monthYEAR is extracted from the extra column "FECHA DE INICIO DEL SERVICIO"
  example: DICIEMBRE2025

Report column used as counterpart:
  doc_mes_cups       → "concatenado doc_mes_ cups"
  doc_mes_año_codigo → "concatenado doc_mes_servicio"

Result: adds column "estado_cruce_informe" to bases:
  "Facturado"     → exists in active report
  "No facturado"  → does not exist
"""

import pandas as pd
from pathlib import Path
from core.exporter import PARQUET_DIR

# ── Configuration ────────────────────────────────────────────
KEY_TYPE_DEFAULT = "doc_mes_cups"
COL_CONCAT_CUPS = "concatenado doc_mes_ cups"
COL_CONCAT_SERVICE = "concatenado doc_mes_servicio"
COL_REPORT_STATE = "ESTADO DE FACTURA"
COL_SERVICE_START_DATE = "FECHA DE INICIO DEL SERVICIO"

COLS_RESULT = ["VALOR TOTAL", "CUFE", "facturador"]

MONTH_NUM_TO_NAME = {
    "1": "ENERO",  "2": "FEBRERO",   "3": "MARZO",    "4": "ABRIL",
    "5": "MAYO",   "6": "JUNIO",     "7": "JULIO",    "8": "AGOSTO",
    "9": "SEPTIEMBRE", "10": "OCTUBRE", "11": "NOVIEMBRE", "12": "DICIEMBRE",
    "01": "ENERO", "02": "FEBRERO",  "03": "MARZO",   "04": "ABRIL",
    "05": "MAYO",  "06": "JUNIO",    "07": "JULIO",   "08": "AGOSTO",
    "09": "SEPTIEMBRE",
}


# ════════════════════════════════════════════════════════════
# NORMALIZATION HELPERS
# ════════════════════════════════════════════════════════════

def _normalize_str(series: pd.Series) -> pd.Series:
    """Strip, uppercase and collapse multiple spaces."""
    return series.astype(str).str.strip().str.upper().str.replace(r"\s+", " ", regex=True)


def _month_year_from_date(series: pd.Series) -> pd.Series:
    """
    Extract MONTHYEAR from a date column.
    "2025-12-01" → "DICIEMBRE2025"
    "2025-12-01 00:00:00" → "DICIEMBRE2025"
    """
    def _conv(val):
        v = str(val).strip()
        if not v or v.upper() in ("NAN", "NAT", ""):
            return ""
        try:
            dt = pd.to_datetime(v)
            month_name = MONTH_NUM_TO_NAME.get(str(dt.month), str(dt.month))
            return f"{month_name}{dt.year}"
        except Exception:
            return v.upper()
    return series.apply(_conv)


def _month_from_number(series: pd.Series) -> pd.Series:
    """Convert month number or name to uppercase month name."""
    def _conv(val):
        v = str(val).strip()
        if "-" in v or "/" in v:
            try:
                month_num = str(pd.to_datetime(v).month)
                return MONTH_NUM_TO_NAME.get(month_num, v.upper())
            except Exception:
                pass
        return MONTH_NUM_TO_NAME.get(v.upper(), v.upper())
    return series.apply(_conv)


# ════════════════════════════════════════════════════════════
# KEY CONSTRUCTION
# ════════════════════════════════════════════════════════════

def _key_doc_month_cups(group: pd.DataFrame) -> pd.Series:
    """
    Key: documento_paciente + month + cups
    Example: 123456789_FEBRERO_890302
    """
    doc = _normalize_str(group.get("documento_paciente", pd.Series([""] * len(group))))
    month = _month_from_number(group.get("mes", pd.Series([""] * len(group))))
    cups = _normalize_str(group.get("cups", pd.Series([""] * len(group))))
    return doc + "_" + month + "_" + cups


def _key_doc_monthyear_code(group: pd.DataFrame, code_col: str) -> pd.Series:
    """
    Key: documento_paciente + MONTHYEAR + procedure_code
    Example: 123456789_DICIEMBRE2025_890302
    MONTHYEAR is extracted from "FECHA DE INICIO DEL SERVICIO" (extra column).
    """
    doc = _normalize_str(group.get("documento_paciente", pd.Series([""] * len(group))))

    if COL_SERVICE_START_DATE in group.columns:
        month_year = _month_year_from_date(group[COL_SERVICE_START_DATE])
    else:
        # Fallback: separate month + year
        month = _month_from_number(group.get("mes", pd.Series([""] * len(group))))
        year = group.get("año", pd.Series([""] * len(group))).astype(str).str.strip()
        month_year = month + year

    if code_col and code_col in group.columns:
        code = _normalize_str(group[code_col])
    else:
        code = _normalize_str(group.get("cups", pd.Series([""] * len(group))))

    return doc + "_" + month_year + "_" + code


# ════════════════════════════════════════════════════════════
# BUILD REPORT KEY SET
# ════════════════════════════════════════════════════════════

def _build_report_set(df_report: pd.DataFrame, concat_col: str) -> set:
    """
    Build the set of keys from the report (only Active records).
    Normalize the concatenated column: strip + uppercase.
    """
    real_col = concat_col
    # Find actual column name by strip match (handles stray spaces)
    for c in df_report.columns:
        if c.strip() == concat_col.strip():
            real_col = c
            break

    state_col = None
    for c in df_report.columns:
        if c.strip() == COL_REPORT_STATE.strip():
            state_col = c
            break

    if state_col:
        df_active = df_report[
            df_report[state_col].astype(str).str.strip().str.upper() == "ACTIVO"
            ].copy()
    else:
        df_active = df_report.copy()

    if real_col not in df_active.columns:
        return set()

    keys = df_active[real_col].astype(str).str.strip().str.upper()
    return set(keys.tolist())


# ════════════════════════════════════════════════════════════
# MAIN CROSSING FUNCTION
# ════════════════════════════════════════════════════════════

def cross_bases_with_report(
        df_bases: pd.DataFrame,
        df_report: pd.DataFrame,
        config: dict,
) -> pd.DataFrame:
    """
    Cross df_bases with the billing report.
    Adds columns: llave_cruce_informe, estado_cruce_informe.

    Config per base_type in config.json:
      "tipo_llave_informe": "doc_mes_cups" | "doc_mes_año_codigo"
      "col_codigo_procedimiento": "COLUMN_NAME"  (only for doc_mes_año_codigo)
    """
    if df_report is None or df_report.empty:
        df_bases["llave_cruce_informe"] = ""
        df_bases["estado_cruce_informe"] = "Sin cruce"
        return df_bases

    results = []

    for base_type, group in df_bases.groupby("tipo_base"):
        group = group.copy()

        conf_type = config.get(base_type, {})
        key_type = conf_type.get("tipo_llave_informe", KEY_TYPE_DEFAULT)

        if key_type == "doc_mes_año_codigo":
            code_col = conf_type.get("col_codigo_procedimiento", "")
            group_key = _key_doc_monthyear_code(group, code_col)
            report_col = COL_CONCAT_SERVICE
        else:
            group_key = _key_doc_month_cups(group)
            report_col = COL_CONCAT_CUPS

        report_set = _build_report_set(df_report, report_col)

        group["llave_cruce_informe"] = group_key
        group["estado_cruce_informe"] = group_key.apply(
            lambda k: "Facturado" if k in report_set else "No facturado"
        )
        results.append(group)

    return pd.concat(results, ignore_index=True)


# ════════════════════════════════════════════════════════════
# KPIs AND SUMMARIES
# ════════════════════════════════════════════════════════════

def crossing_kpis_report(df: pd.DataFrame) -> dict:
    """Return KPI summary of crossing with the report."""
    if "estado_cruce_informe" not in df.columns:
        return {}
    total = len(df)
    billed = int((df["estado_cruce_informe"] == "Facturado").sum())
    not_billed = int((df["estado_cruce_informe"] == "No facturado").sum())
    pct = round(billed / total * 100, 1) if total > 0 else 0.0
    return {
        "total": total, "facturados": billed,
        "no_facturado": not_billed, "cumplimiento": pct,
    }


def crossing_summary_by_agreement_report(df: pd.DataFrame) -> pd.DataFrame:
    """Crossing summary grouped by agreement (nombre_convenio)."""
    if "estado_cruce_informe" not in df.columns:
        return pd.DataFrame()
    r = (df.groupby("nombre_convenio")["estado_cruce_informe"]
         .value_counts().unstack(fill_value=0).reset_index())
    for c in ["Facturado", "No facturado"]:
        if c not in r.columns:
            r[c] = 0
    r["Total"] = r[["Facturado", "No facturado"]].sum(axis=1)
    r["Cumplimiento (%)"] = (r["Facturado"] / r["Total"] * 100).round(1)
    return r.rename(columns={"nombre_convenio": "Convenio"})


def crossing_summary_by_base_type_report(df: pd.DataFrame) -> pd.DataFrame:
    """Crossing summary grouped by base type (tipo_base)."""
    if "estado_cruce_informe" not in df.columns:
        return pd.DataFrame()
    r = (df.groupby("tipo_base")["estado_cruce_informe"]
         .value_counts().unstack(fill_value=0).reset_index())
    for c in ["Facturado", "No facturado"]:
        if c not in r.columns:
            r[c] = 0
    r["Total"] = r[["Facturado", "No facturado"]].sum(axis=1)
    r["Cumplimiento (%)"] = (r["Facturado"] / r["Total"] * 100).round(1)
    return r.rename(columns={"tipo_base": "Tipo de base"})


# ════════════════════════════════════════════════════════════
# CROSS REPORT PARQUET STORAGE
# ════════════════════════════════════════════════════════════

def _safe_name(text: str) -> str:
    return text.replace(" ", "_").replace("/", "-").replace("\\", "-")


def save_cross_report(df: pd.DataFrame, label: str) -> Path:
    """Save cross-report result as parquet file."""
    path = PARQUET_DIR / f"cruce_informe_{_safe_name(label)}.parquet"
    df_save = df.copy()
    for col in df_save.columns:
        if df_save[col].dtype == object:
            df_save[col] = df_save[col].fillna("").astype(str)
    df_save.to_parquet(path, index=False, engine="pyarrow")
    return path


def load_cross_report(label: str):
    """Load a saved cross-report parquet (or None if missing)."""
    path = PARQUET_DIR / f"cruce_informe_{_safe_name(label)}.parquet"
    return pd.read_parquet(path, engine="pyarrow") if path.exists() else None


def available_cross_reports() -> list:
    """List months that have saved cross-report parquet files."""
    return [
        f.stem.replace("cruce_informe_", "").replace("_", " ")
        for f in sorted(PARQUET_DIR.glob("cruce_informe_*.parquet"))
    ]