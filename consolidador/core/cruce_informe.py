"""
cruce_informe.py
Responsabilidad: cruzar las bases con el Informe de Facturación.

Dos tipos de llave configurables en config.json por tipo de base:
  tipo_llave_informe: "doc_mes_cups"       → documento + mes + cups
  tipo_llave_informe: "doc_mes_año_codigo" → documento + mesAÑO + codigo_procedimiento

  donde mesAÑO se extrae de la columna extra "FECHA DE INICIO DEL SERVICIO"
  ejemplo: DICIEMBRE2025

Columna del informe usada como contraparte:
  doc_mes_cups       → "concatenado doc_mes_ cups"
  doc_mes_año_codigo → "concatenado doc_mes_servicio"

Resultado: columna "estado_cruce_informe" en las bases.
  "Facturado"     → existe en informe activo
  "No facturado"  → no existe
"""

import pandas as pd
from pathlib import Path
from core.exportador import PARQUET_DIR

# ── Configuración ────────────────────────────────────────────
TIPO_LLAVE_DEFAULT  = "doc_mes_cups"
COL_CONCAT_CUPS     = "concatenado doc_mes_ cups"
COL_CONCAT_SERVICIO = "concatenado doc_mes_servicio"
COL_ESTADO_INFORME  = "ESTADO DE FACTURA"
COL_FECHA_INICIO    = "FECHA DE INICIO DEL SERVICIO"

COLS_RESULTADO = ["VALOR TOTAL", "CUFE", "facturador"]

MESES_NUM = {
    "1": "ENERO",  "2": "FEBRERO",   "3": "MARZO",    "4": "ABRIL",
    "5": "MAYO",   "6": "JUNIO",     "7": "JULIO",    "8": "AGOSTO",
    "9": "SEPTIEMBRE", "10": "OCTUBRE", "11": "NOVIEMBRE", "12": "DICIEMBRE",
    "01": "ENERO", "02": "FEBRERO",  "03": "MARZO",   "04": "ABRIL",
    "05": "MAYO",  "06": "JUNIO",    "07": "JULIO",   "08": "AGOSTO",
    "09": "SEPTIEMBRE",
}


# ════════════════════════════════════════════════════════════
# NORMALIZACIÓN
# ════════════════════════════════════════════════════════════

def _norm_str(serie: pd.Series) -> pd.Series:
    return serie.astype(str).str.strip().str.upper().str.replace(r"\s+", " ", regex=True)


def _mes_año_desde_fecha(serie: pd.Series) -> pd.Series:
    """
    Extrae MESAÑO desde una columna de fecha.
    "2025-12-01" → "DICIEMBRE2025"
    "2025-12-01 00:00:00" → "DICIEMBRE2025"
    """
    def _conv(val):
        v = str(val).strip()
        if not v or v.upper() in ("NAN", "NAT", ""):
            return ""
        try:
            dt = pd.to_datetime(v)
            mes_nombre = MESES_NUM.get(str(dt.month), str(dt.month))
            return f"{mes_nombre}{dt.year}"
        except Exception:
            return v.upper()
    return serie.apply(_conv)


def _mes_desde_num(serie: pd.Series) -> pd.Series:
    """Convierte número o nombre de mes a nombre uppercase."""
    def _conv(val):
        v = str(val).strip()
        if "-" in v or "/" in v:
            try:
                mes_num = str(pd.to_datetime(v).month)
                return MESES_NUM.get(mes_num, v.upper())
            except Exception:
                pass
        return MESES_NUM.get(v.upper(), v.upper())
    return serie.apply(_conv)


# ════════════════════════════════════════════════════════════
# CONSTRUCCIÓN DE LLAVES
# ════════════════════════════════════════════════════════════

def _llave_doc_mes_cups(grupo: pd.DataFrame) -> pd.Series:
    """
    Llave: documento_paciente + mes + cups
    Ejemplo: 123456789_FEBRERO_890302
    """
    doc  = _norm_str(grupo.get("documento_paciente", pd.Series([""] * len(grupo))))
    mes  = _mes_desde_num(grupo.get("mes", pd.Series([""] * len(grupo))))
    cups = _norm_str(grupo.get("cups", pd.Series([""] * len(grupo))))
    return doc + "_" + mes + "_" + cups


def _llave_doc_mesaño_codigo(grupo: pd.DataFrame, col_codigo: str) -> pd.Series:
    """
    Llave: documento_paciente + MESAÑO + codigo_procedimiento
    Ejemplo: 123456789_DICIEMBRE2025_890302
    El MESAÑO se extrae de "FECHA DE INICIO DEL SERVICIO" (columna extra).
    """
    doc = _norm_str(grupo.get("documento_paciente", pd.Series([""] * len(grupo))))

    if COL_FECHA_INICIO in grupo.columns:
        mes_año = _mes_año_desde_fecha(grupo[COL_FECHA_INICIO])
    else:
        # Fallback: mes + año separados
        mes  = _mes_desde_num(grupo.get("mes", pd.Series([""] * len(grupo))))
        año  = grupo.get("año", pd.Series([""] * len(grupo))).astype(str).str.strip()
        mes_año = mes + año

    if col_codigo and col_codigo in grupo.columns:
        codigo = _norm_str(grupo[col_codigo])
    else:
        codigo = _norm_str(grupo.get("cups", pd.Series([""] * len(grupo))))

    return doc + "_" + mes_año + "_" + codigo


# ════════════════════════════════════════════════════════════
# PREPARAR SET DEL INFORME
# ════════════════════════════════════════════════════════════

def _set_informe(df_informe: pd.DataFrame, col_concat: str) -> set:
    """
    Construye el set de llaves del informe (solo registros Activos).
    Normaliza la columna concatenada: strip + uppercase.
    """
    col_real = col_concat
    # Buscar con strip por si tiene espacios
    for c in df_informe.columns:
        if c.strip() == col_concat.strip():
            col_real = c
            break

    col_estado = None
    for c in df_informe.columns:
        if c.strip() == COL_ESTADO_INFORME.strip():
            col_estado = c
            break

    if col_estado:
        df_act = df_informe[
            df_informe[col_estado].astype(str).str.strip().str.upper() == "ACTIVO"
        ].copy()
    else:
        df_act = df_informe.copy()

    if col_real not in df_act.columns:
        return set()

    llaves = df_act[col_real].astype(str).str.strip().str.upper()
    return set(llaves.tolist())


# ════════════════════════════════════════════════════════════
# CRUCE PRINCIPAL
# ════════════════════════════════════════════════════════════

def cruzar_bases_con_informe(
    df_bases: pd.DataFrame,
    df_informe: pd.DataFrame,
    config: dict,
) -> pd.DataFrame:
    """
    Cruza df_bases con el informe de facturación.
    Agrega columnas: llave_cruce_informe, estado_cruce_informe.

    Configuración por tipo de base en config.json:
      "tipo_llave_informe": "doc_mes_cups" | "doc_mes_año_codigo"
      "col_codigo_procedimiento": "NOMBRE_COLUMNA"  (solo para doc_mes_año_codigo)
    """
    if df_informe is None or df_informe.empty:
        df_bases["llave_cruce_informe"]  = ""
        df_bases["estado_cruce_informe"] = "Sin cruce"
        return df_bases

    resultados = []

    for tipo_base, grupo in df_bases.groupby("tipo_base"):
        grupo = grupo.copy()

        conf_tipo  = config.get(tipo_base, {})
        tipo_llave = conf_tipo.get("tipo_llave_informe", TIPO_LLAVE_DEFAULT)

        if tipo_llave == "doc_mes_año_codigo":
            col_codigo  = conf_tipo.get("col_codigo_procedimiento", "")
            llave_grupo = _llave_doc_mesaño_codigo(grupo, col_codigo)
            col_informe = COL_CONCAT_SERVICIO
        else:
            llave_grupo = _llave_doc_mes_cups(grupo)
            col_informe = COL_CONCAT_CUPS

        set_inf = _set_informe(df_informe, col_informe)

        grupo["llave_cruce_informe"]  = llave_grupo
        grupo["estado_cruce_informe"] = llave_grupo.apply(
            lambda k: "Facturado" if k in set_inf else "No facturado"
        )
        resultados.append(grupo)

    return pd.concat(resultados, ignore_index=True)


# ════════════════════════════════════════════════════════════
# KPIs Y RESÚMENES
# ════════════════════════════════════════════════════════════

def kpis_cruce_informe(df: pd.DataFrame) -> dict:
    if "estado_cruce_informe" not in df.columns:
        return {}
    total        = len(df)
    facturados   = int((df["estado_cruce_informe"] == "Facturado").sum())
    no_facturado = int((df["estado_cruce_informe"] == "No facturado").sum())
    pct          = round(facturados / total * 100, 1) if total > 0 else 0.0
    return {
        "total": total, "facturados": facturados,
        "no_facturado": no_facturado, "cumplimiento": pct,
    }


def resumen_por_convenio(df: pd.DataFrame) -> pd.DataFrame:
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


def resumen_por_tipo_base(df: pd.DataFrame) -> pd.DataFrame:
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
# PARQUET DE CRUCES
# ════════════════════════════════════════════════════════════

def _nombre_seguro(texto: str) -> str:
    return texto.replace(" ", "_").replace("/", "-").replace("\\", "-")


def guardar_cruce_informe(df: pd.DataFrame, label: str) -> Path:
    ruta = PARQUET_DIR / f"cruce_informe_{_nombre_seguro(label)}.parquet"
    df_g = df.copy()
    for col in df_g.columns:
        if df_g[col].dtype == object:
            df_g[col] = df_g[col].fillna("").astype(str)
    df_g.to_parquet(ruta, index=False, engine="pyarrow")
    return ruta


def cargar_cruce_informe(label: str):
    ruta = PARQUET_DIR / f"cruce_informe_{_nombre_seguro(label)}.parquet"
    return pd.read_parquet(ruta, engine="pyarrow") if ruta.exists() else None


def cruces_informe_disponibles() -> list:
    return [
        f.stem.replace("cruce_informe_", "").replace("_", " ")
        for f in sorted(PARQUET_DIR.glob("cruce_informe_*.parquet"))
    ]