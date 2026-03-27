"""
Processor
Responsabilidad: transformar un DataFrame crudo al esquema estándar.
No sabe nada de interfaz ni de exportación.
"""

import pandas as pd
import unicodedata
import re

columns = [
    "documento_paciente",
    "nombre_paciente",
    "cups",
    "descripcion_servicio",
    "fecha_atencion",
    "facturador",
    "observacion",
    "estado",
    "valor_estado_original",
    "tipo_base",
    "nombre_convenio",
    "archivo_origen",
    "mes",
    "año",
]


def _detect_state(valor, logic: str) -> str:
    """
  Determines if a value indicates that the service was billed.
  The logic can be:
  "has_value" → non-empty cell = billed
  "is_number" → contains a valid number
  "is_date" → contains a valid date
   any text → exact comparison (case-insensitive)
  """
    if pd.isna(valor) or str(valor).strip() == "":
        return "Pendiente"

    val = str(valor).strip()

    if logic == "tiene_valor":
        invoiced = True
    elif logic == "es_numero":
        try:
            float(val)
            invoiced = True
        except ValueError:
            invoiced = False
    elif logic == "es_fecha":
        try:
            pd.to_datetime(val)
            invoiced = True
        except Exception:
            invoiced = False
    else:
        invoiced = val.lower() == logic.strip().lower()

    return "Facturado" if invoiced else "Pendiente"


def _extract_agreement(tipo_base: str) -> str:
    """
    Extracts the name of the agreement from the base type.
    'Agreement A - Laboratory' → 'Agreement A'
     """
    return tipo_base.split(" - ")[0].strip() if " - " in tipo_base else tipo_base


def _map_colum(df_raw: pd.DataFrame, col_real: str | None) -> pd.Series:
    """Returns the column if it exists, or an empty series if it does not."""
    if col_real and col_real in df_raw.columns:
        return df_raw[col_real].reset_index(drop=True)
    return pd.Series([""] * len(df_raw))


def procces_base(
    df_raw: pd.DataFrame,
    config: dict,
    file_name: str,
    base_type: str,
    month: str,
    year: int,
) -> tuple[pd.DataFrame, list[str]]:
    """
    Converts a raw DataFrame to the standard schema.
    Returns:
    - Processed DataFrame
    - List of warnings (columns not found)
    """
    warnings = []

    cols_config = {k: v for k, v in config.items()
                   if k.startswith("col_") and isinstance(v, str)}
    for clave, col_real in cols_config.items():
        if col_real not in df_raw.columns:
            warnings.append(f"Columna '{col_real}' ({clave}) no encontrada en '{file_name}'")

    df = pd.DataFrame()
    df["documento_paciente"] = _map_colum(df_raw, config.get("col_paciente"))
    df["nombre_paciente"]      = _map_colum(df_raw, config.get("col_nombre"))
    df["cups"]                 = _map_colum(df_raw, config.get("col_cups"))
    df["descripcion_servicio"] = _map_colum(df_raw, config.get("col_servicio"))
    df["fecha_atencion"]       = _map_colum(df_raw, config.get("col_fecha"))
    df["facturador"]           = _map_colum(df_raw, config.get("col_facturador"))
    df["observacion"]          = _map_colum(df_raw, config.get("col_observacion"))

    for col_id in ["documento_paciente", "cups"]:
        df[col_id] = _clean_float_to_int(df[col_id])

    col_fact = config.get("col_facturacion")
    logic   = config.get("logica_facturacion", "tiene_valor")

    if col_fact and col_fact in df_raw.columns:
        df["valor_estado_original"] = df_raw[col_fact].astype(str).values
        df["estado"] = df_raw[col_fact].apply(lambda v: _detect_state(v, logic)).values
    else:
        df["valor_estado_original"] = ""
        df["estado"] = "Sin información"
        warnings.append(
            f"Columna de facturación '{col_fact}' no encontrada. Estado marcado como 'Sin información'.")

    df["tipo_base"]      = base_type
    df["nombre_convenio"]= _extract_agreement(base_type)
    df["archivo_origen"] = file_name
    df["mes"]            = month
    df["año"]            = year

    for col in columns:
        if col not in df.columns:
            df[col] = ""

    # ── Extra Columns ───────────────────────────────────────
    # Supports two formats:
    # - Simple string: "Normal Column"
    # - Dict with alias: {"col": "VALUE.1", "alias": "final_value"}
    # Only appear in the report for that type of database.

    extra_columns = config.get("columnas_extra", [])
    extra_finds = []

    for item in extra_columns:
        if isinstance(item, dict):
            col_real = item.get("col", "")
            alias    = item.get("alias", col_real)
        else:
            col_real = item
            alias    = item

        if not col_real:
            continue

        if col_real in df_raw.columns:
            df[alias] = df_raw[col_real].reset_index(drop=True)
            df[alias] = _clean_float_to_int(df[alias])
            extra_finds.append(alias)
        else:
            df[alias] = ""
            warnings.append(
                f"Columna extra '{col_real}' no encontrada en '{file_name}'. "
                f"Se agregó vacía como '{alias}'."
            )

    final_columns = columns + extra_finds
    return df[final_columns], warnings


def real_columns(df_raw: pd.DataFrame) -> list[str]:
    """
    Returns the exact column names as seen by pandas,
    including duplicates (VALUE, VALUE.1, VALUE.2).
    Useful for configuring aliases on duplicate columns.
    """
    return df_raw.columns.tolist()


def read_excel_with_duplicates(archivo) -> pd.DataFrame:
    """
    Reads an Excel file, preserving all columns even if they are duplicated.
    Pandas automatically renames them: VALUE, VALUE.1, VALUE.2...
    """
    return pd.read_excel(archivo, header=0)


def _clean_text(serie: pd.Series) -> pd.Series:
    """
    Removes special Unicode spaces and non-printable characters.
    Normalizes to clean plain text.
    """

    def clean(val):
        if pd.isna(val):
            return ""
        text = str(val)
        text = unicodedata.normalize("NFKC", text)
        text = re.sub(r'[^\x20-\x7E\u00C0-\u024F\u00B0-\u00BF]', '', text)
        return text.strip()

    return serie.apply(clean)

def _clean_float_to_int(serie: pd.Series) -> pd.Series:
    """
    Converts float values representing integers to strings without decimals.
    Example: 1234567.0 → '1234567'
    """
    def clean(val):
        if pd.isna(val) or str(val).strip() == "":
            return ""
        if isinstance(val, float) and val == int(val):
            return str(int(val))
        s = str(val).strip()
        if re.match(r'^\d+\.0$', s):
            return s[:-2]
        return s
    return serie.apply(clean)