"""
Exporter
Responsibility: Generate output files at 3 levels.

Level 1 → General: All agreements and database types
Level 2 → By agreement: One report per agreement
Level 3 → By database type: One report per database type

Formats: CSV (lightweight) + Excel (multiple sheets)
"""

import pandas as pd
import io
import json
from pathlib import Path
from .processor import columns as COLS_STD
from .watcher import PROCESSED_PATH
from .analyzer import (
    summary_by_agreement,
    pending_by_biller,
)

# ════════════════════════════════════════════════════════════
# CONFIGURATION LOAD FOR EXTRA COLUMNS BY DATABASE TYPE
# ════════════════════════════════════════════════════════════

def _load_config() -> dict:
    """Load the config.json to get extra columns by database type."""
    config_path = Path(__file__).parent.parent / "config" / "config.json"
    if config_path.exists():
        return json.loads(config_path.read_text(encoding="utf-8"))
    return {}


def _alias_extra_base_type(base_type: str) -> list[str]:
    """
    Returns the list of aliases (final names) for the extra columns configured for a specific database type.
    It only returns aliases for the specified database type, not others.
    """
    cfg = _load_config()
    conf_type = cfg.get(base_type, {})
    extra_columns = conf_type.get("columnas_extra", [])
    aliases = []
    for item in extra_columns:
        if isinstance(item, dict):
            aliases.append(item.get("alias", item.get("col", "")))
        elif isinstance(item, str):
            aliases.append(item)
    return [a for a in aliases if a]


# ════════════════════════════════════════════════════════════
# HELPERS INTERNOS
# ════════════════════════════════════════════════════════════

def _excel(sheets: dict) -> bytes:
    """Build an Excel spreadsheet with multiple sheets."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            if not df.empty:
                df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
    buf.seek(0)
    return buf.getvalue()


def _csv(df: pd.DataFrame) -> bytes:
    """CSV con encoding correcto para Excel en español."""
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")


def _safe_name(text: str) -> str:
    """Converts text to a secure filename."""
    return text.replace(" ", "_").replace("/", "-").replace("\\", "-")


# ════════════════════════════════════════════════════════════
# LEVEL 1 — GENERAL
# ════════════════════════════════════════════════════════════

def general_csv(df: pd.DataFrame) -> bytes:
    return _csv(df)


def general_excel(df: pd.DataFrame) -> bytes:
    return _excel({
        "Resumen por Convenio":      summary_by_agreement(df),
        "Pendientes por Facturador": pending_by_biller(df),
        "Detalle Pendientes":        df[df["estado"] == "Pendiente"],
        "Consolidado General":       df,
    })


# ════════════════════════════════════════════════════════════
# HELPER — column cleaning#
# ════════════════════════════════════════════════════════════

def _clean_empty_columns(dataframe) -> "pd.DataFrame":
    """Removes completely empty or NaN columns."""
    cols_no_empty = [
        c for c in dataframe.columns
        if not dataframe[c].isna().all() and dataframe[c].astype(str).str.strip().ne("").any()
    ]
    return dataframe[cols_no_empty]


def _proper_columns_base_type(dataframe: "pd.DataFrame", base_type: str) -> list[str]:
    """
    Returns the list of columns belonging to the specified data type:

    the standard columns (COLS_STD) + only the extra columns configured
    for THAT data type in the config.json file.

    This prevents extra columns from other data types from appearing in the report.
    """
    aliases_extra = _alias_extra_base_type(base_type)
    objective_columns = COLS_STD + aliases_extra
    return [c for c in objective_columns if c in dataframe.columns]


# ════════════════════════════════════════════════════════════
# LEVEL 2 — BY AGREEMENT
# ════════════════════════════════════════════════════════════

def agreement_csv(df: pd.DataFrame, convenio: str) -> bytes:
    return _csv(df[df["nombre_convenio"] == convenio])


COLS_PENDING_DETAILS = [
    "tipo_base",
    "documento_paciente",
    "nombre_paciente",
    "descripcion_servicio",
    "fecha_atencion",
    "facturador",
    "observacion",
    "archivo_origen",
]


def agreement_excel(df: pd.DataFrame, convenio: str) -> bytes:
    """
    Excel spreadsheet of the agreement with this structure:
    - Summary: Agreement KPIs
    - Pending by Biller: Grouped summary
    - Pending Details: Row by row, filterable by data type
    - [Data Type 1]: All records with cleared columns
    - [Data Type 2]: Same as above
    """
    df_agreement = df[df["nombre_convenio"] == convenio]

    df_pending = df_agreement[df_agreement["estado"] == "Pendiente"].copy()
    detail_cols = [c for c in COLS_PENDING_DETAILS if c in df_pending.columns]
    df_detail = df_pending[detail_cols].sort_values("tipo_base")

    sheets = {
        "Resumen":                   summary_by_agreement(df_agreement),
        "Pendientes por Facturador": pending_by_biller(df_agreement),
        "Detalle Pendientes":        df_detail,
    }

    for base_type in sorted(df_agreement["tipo_base"].unique()):
        df_type = df_agreement[df_agreement["tipo_base"] == base_type].copy()
        cols = _proper_columns_base_type(df_type, base_type)
        df_type = df_type[cols]
        sheet_name = base_type.split(" - ", 1)[-1] if " - " in base_type else base_type
        sheets[sheet_name] = df_type

    return _excel(sheets)


# ════════════════════════════════════════════════════════════
# NIVEL 3 — BY BASE TYPE
# ════════════════════════════════════════════════════════════

def base_type_csv(df: pd.DataFrame, base_type: str) -> bytes:
    df_type = df[df["tipo_base"] == base_type].copy()
    cols = _proper_columns_base_type(df_type, base_type)
    return _csv(df_type[cols])


def base_type_excel(df: pd.DataFrame, base_type: str) -> bytes:
    from .processor import columns

    df_type = df[df["tipo_base"] == base_type].copy()
    cols = _proper_columns_base_type(df_type, base_type)
    df_type = df_type[cols]

    extra_cols = [c for c in cols if c not in columns]

    cols_detail = [
                      "nombre_convenio", "tipo_base", "documento_paciente",
                      "nombre_paciente", "descripcion_servicio",
                      "facturador", "observacion", "archivo_origen",
                  ] + extra_cols
    cols_detail = [c for c in cols_detail if c in df_type.columns]

    df_pending = df_type[df_type["estado"] == "Pendiente"]

    return _excel({
        "Resumen":             summary_by_agreement(df_type),
        "Detalle Pendientes":  df_pending[cols_detail],
        "Todos los registros": df_type,
    })


# ════════════════════════════════════════════════════════════
# FULL EXPORT — saves all 3 levels to disk#
# ════════════════════════════════════════════════════════════

def export_all_on_disk(
        df: pd.DataFrame,
        reports_folder: str,
        month_label: str,
) -> dict:
    """
  Save all reports in:

    reports_folder/
    month_label/
    general/
    by_agreement/
    by_base_type/

    Returns a dictionary with routes generated by level.
    """
    base_dir  = Path(reports_folder) / _safe_name(month_label)
    routes = {"general": [], "por_convenio": [], "por_tipo_base": []}

    # Level 1
    gen_dir = base_dir / "general"
    gen_dir.mkdir(parents=True, exist_ok=True)
    for ext, data_bytes in [("csv", general_csv(df)), ("xlsx", general_excel(df))]:
        path_file = gen_dir / f"general_{_safe_name(month_label)}.{ext}"
        path_file.write_bytes(data_bytes)
        routes["general"].append(str(path_file))

    # Level 2
    by_agreement_dir = base_dir / "por_convenio"
    by_agreement_dir.mkdir(parents=True, exist_ok=True)
    for agreement in sorted(df["nombre_convenio"].unique()):
        name_safe = _safe_name(agreement)
        for ext, data_bytes in [
            ("csv", agreement_csv(df, agreement)),
            ("xlsx", agreement_excel(df, agreement)),
        ]:
            path_file = by_agreement_dir / f"{name_safe}_{_safe_name(month_label)}.{ext}"
            path_file.write_bytes(data_bytes)
            routes["por_convenio"].append(str(path_file))

    # Level 3
    by_type_dir = base_dir / "por_tipo_base"
    by_type_dir.mkdir(parents=True, exist_ok=True)
    for btype in sorted(df["tipo_base"].unique()):
        name_safe = _safe_name(btype)
        for ext, data_bytes in [
            ("csv", base_type_csv(df, btype)),
            ("xlsx", base_type_excel(df, btype)),
        ]:
            path_file = by_type_dir / f"{name_safe}_{_safe_name(month_label)}.{ext}"
            path_file.write_bytes(data_bytes)
            routes["por_tipo_base"].append(str(path_file))

    return routes


# ════════════════════════════════════════════════════════════
# NOMBRES PARA DESCARGA EN STREAMLIT
# ════════════════════════════════════════════════════════════

def general_name(month_label: str, ext: str) -> str:
    return f"general_{_safe_name(month_label)}.{ext}"

def name_agreement_file(convenio: str, mes_label: str, ext: str) -> str:
    return f"{_safe_name(convenio)}_{_safe_name(mes_label)}.{ext}"

def name_base_type_file(tipo_base: str, mes_label: str, ext: str) -> str:
    return f"{_safe_name(tipo_base)}_{_safe_name(mes_label)}.{ext}"


# ════════════════════════════════════════════════════════════
# PARQUET — efficient internal storage
# ════════════════════════════════════════════════════════════

PARQUET_DIR = Path(__file__).parent.parent / "datos" / "parquet"


def save_parquet(df: pd.DataFrame, month_label: str) -> Path:
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    path = PARQUET_DIR / f"consolidado_{_safe_name(month_label)}.parquet"

    def _clean(df_in):
        df_out = df_in.copy()
        for col in df_out.columns:
            if df_out[col].dtype == object:
                df_out[col] = df_out[col].fillna("").astype(str)
        return df_out

    if path.exists():
        df_existing = pd.read_parquet(path, engine="pyarrow")
        df_final = pd.concat([df_existing, df], ignore_index=True)
    else:
        df_final = df

    _clean(df_final).to_parquet(path, index=False, engine="pyarrow")
    return path

def load_parquet(month_label: str) -> pd.DataFrame | None:
    """
    Loads the consolidated data for one month from Parquet.
    Returns None if it does not exist.
    """
    path = PARQUET_DIR / f"consolidado_{_safe_name(month_label)}.parquet"
    if path.exists():
        return pd.read_parquet(path, engine="pyarrow")
    return None


def load_all_parquet() -> pd.DataFrame | None:
    """
    Load and concatenate all available months in Parquet.
    Useful for building the accumulated master base.
    """
    files = sorted(PARQUET_DIR.glob("consolidado_*.parquet"))
    if not files:
        return None
    dfs = [pd.read_parquet(f, engine="pyarrow") for f in files]
    return pd.concat(dfs, ignore_index=True)


def months_available_parquet() -> list[str]:
    """
    List the months that have already been consolidated and stored in parquet flooring.
    """
    files = sorted(PARQUET_DIR.glob("consolidado_*.parquet"))
    return [
        f.stem.replace("consolidado_", "").replace("_", " ")
        for f in files
    ]

def delete_files_from_parquet(file_name: str) -> dict:
    parquet_files = sorted(PARQUET_DIR.glob("consolidado_*.parquet"))
    total_deleted = 0
    months_affected = []

    for path_file in parquet_files:
        try:
            df_month = pd.read_parquet(path_file, engine="pyarrow")
            mask = df_month["archivo_origen"] == file_name
            n_rows = mask.sum()
            if n_rows == 0:
                continue
            df_clean = df_month[~mask].reset_index(drop=True)
            if df_clean.empty:
                path_file.unlink()
            else:
                for col in df_clean.columns:
                    if df_clean[col].dtype == object:
                        df_clean[col] = df_clean[col].fillna("").astype(str)
                df_clean.to_parquet(path_file, index=False, engine="pyarrow")
            total_deleted += int(n_rows)
            months_affected.append(path_file.stem.replace("consolidado_", "").replace("_", " "))
        except Exception as e:
            return {"eliminados": 0, "meses_afectados": [], "error": str(e)}

    try:
        if PROCESSED_PATH.exists():
            processed = json.loads(PROCESSED_PATH.read_text(encoding="utf-8"))
            cleaned_processed = {
                ruta: datos for ruta, datos in processed.items()
                if Path(ruta).name != file_name
            }
            PROCESSED_PATH.write_text(
                json.dumps(cleaned_processed, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )
    except Exception:
        pass

    return {"eliminados": total_deleted, "meses_afectados": months_affected, "error": None}
