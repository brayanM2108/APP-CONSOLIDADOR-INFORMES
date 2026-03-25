"""
cruce.py
Responsabilidad: cruzar las bases procesadas con el archivo de facturado.

Llave general (default):
  Base       : documento_paciente + fecha_atencion + cups
  Facturado  : IDENTIFICACION     + FECHA FACTURA  + (CUPS si existe)

Por tipo de base se puede configurar una llave distinta en config.json:
  "llave_cruce": ["documento_paciente", "fecha_atencion"]

Resultado: columna nueva "estado_cruce" en las bases.
  "Facturado"     → existe en facturado activo
  "No facturado"  → no existe
  "Sin cruce"     → no había facturado cargado
"""

import pandas as pd
from core.exportador import PARQUET_DIR
from pathlib import Path

# Llave general por defecto
LLAVE_BASE_DEFAULT      = ["DOCUMENTO", "FECHA DE INICIO DEL SERVICIO", "cups"]
LLAVE_FACTURADO_DEFAULT = ["IDENTIFICACION", "MES"]

# Columna CUPS en facturado (puede no existir)
COL_CUPS_FACTURADO = "CUPS"

MESES_NUM_A_NOMBRE = {
    "1": "ENERO", "2": "FEBRERO", "3": "MARZO", "4": "ABRIL",
    "5": "MAYO", "6": "JUNIO", "7": "JULIO", "8": "AGOSTO",
    "9": "SEPTIEMBRE", "10": "OCTUBRE", "11": "NOVIEMBRE", "12": "DICIEMBRE",
    "01": "ENERO", "02": "FEBRERO", "03": "MARZO", "04": "ABRIL",
    "05": "MAYO", "06": "JUNIO", "07": "JULIO", "08": "AGOSTO",
    "09": "SEPTIEMBRE",
}

# ════════════════════════════════════════════════════════════
# NORMALIZACIÓN
# ════════════════════════════════════════════════════════════

def _normalizar_str(serie: pd.Series) -> pd.Series:
    """Strip + uppercase + eliminar espacios dobles."""
    return serie.astype(str).str.strip().str.upper().str.replace(r"\s+", " ", regex=True)


def _normalizar_fecha(serie: pd.Series) -> pd.Series:
    """Convierte a DD-MM-YYYY string. Valores inválidos → ''."""
    fechas = pd.to_datetime(serie, errors="coerce")
    return fechas.dt.strftime("%d-%m-%Y").fillna("")

def _normalizar_mes(serie: pd.Series) -> pd.Series:
    """
    Convierte el mes a string uppercase: "FEBRERO".
    Acepta:
      - Número entero o string: 1, "1", "01", "12"
      - Nombre: "febrero", "FEBRERO"
      - Fecha completa: "2025-12-01", "2025-12-01 00:00:00"
    """
    def _conv(val):
        v = str(val).strip()

        # Si parece fecha (contiene - o /) → extraer mes del datetime
        if "-" in v or "/" in v:
            try:
                mes_num = str(pd.to_datetime(v).month)
                return MESES_NUM_A_NOMBRE.get(mes_num, mes_num)
            except Exception:
                pass

        # Si es número → convertir a nombre
        v_upper = v.upper()
        return MESES_NUM_A_NOMBRE.get(v_upper, v_upper)

    return serie.apply(_conv)


def _es_columna_mes(col: str) -> bool:
    """
    Define si una columna debe normalizarse como mes para la llave de cruce.
    """
    c = str(col).strip().upper()
    return c in {
        "MES",
        "FECHA DE INICIO DEL SERVICIO",
    }

def _construir_llave(df: pd.DataFrame, columnas: list[str], sep: str = "_") -> pd.Series:
    partes = []
    for col in columnas:
        if col not in df.columns:
            partes.append(pd.Series([""] * len(df), index=df.index))
            continue

        serie = df[col].copy()

        es_mes = _es_columna_mes(col)
        if es_mes:
            serie = _normalizar_mes(serie)
        else:
            serie = _normalizar_str(serie)

        partes.append(serie)

    return partes[0].str.cat(partes[1:], sep=sep) if len(partes) > 1 else partes[0]


# ════════════════════════════════════════════════════════════
# PREPARAR FACTURADO
# ════════════════════════════════════════════════════════════

def _preparar_set_facturado(df_fact: pd.DataFrame) -> set:
    df_act = df_fact[df_fact["_estado_factura"] == "Activo"].copy()
    llaves = _construir_llave(df_act, LLAVE_FACTURADO_DEFAULT)
    return set(llaves.tolist())


# ════════════════════════════════════════════════════════════
# CRUCE PRINCIPAL
# ════════════════════════════════════════════════════════════

def cruzar_bases_con_facturado(
    df_bases: pd.DataFrame,
    df_facturado: pd.DataFrame,
    config: dict,
) -> pd.DataFrame:
    """
    Cruza df_bases con df_facturado y agrega columna 'estado_cruce'.

    Parámetros:
      df_bases    : DataFrame consolidado de bases (esquema estándar)
      df_facturado: DataFrame del facturado guardado
      config      : config.json completo (para leer llave_cruce por tipo)

    Retorna el df_bases con columna 'estado_cruce' agregada.
    """
    if df_facturado is None or df_facturado.empty:
        df_bases["estado_cruce"] = "Sin cruce"
        return df_bases

    # Set de llaves del facturado
    set_facturado = _preparar_set_facturado(df_facturado)

    # Procesar por tipo de base (cada uno puede tener llave diferente)
    resultados = []

    for tipo_base, grupo in df_bases.groupby("tipo_base"):
        grupo = grupo.copy()

        # Llave configurada o default
        conf_tipo = config.get(tipo_base, {})
        cols_llave = conf_tipo.get("llave_cruce", LLAVE_BASE_DEFAULT)
        llave_grupo = _construir_llave(grupo, cols_llave)

        grupo["llave_cruce"] = llave_grupo
        grupo["estado_cruce"] = llave_grupo.apply(
            lambda k: "Facturado" if k in set_facturado else "No facturado"
        )
        resultados.append(grupo)

    return pd.concat(resultados, ignore_index=True)


# ════════════════════════════════════════════════════════════
# KPIs DEL CRUCE
# ════════════════════════════════════════════════════════════

def kpis_cruce(df: pd.DataFrame) -> dict:
    """
    Resumen del resultado del cruce.
    Requiere que df tenga columna 'estado_cruce'.
    """
    if "estado_cruce" not in df.columns:
        return {}

    total        = len(df)
    facturados   = (df["estado_cruce"] == "Facturado").sum()
    no_facturado = (df["estado_cruce"] == "No facturado").sum()
    sin_cruce    = (df["estado_cruce"] == "Sin cruce").sum()
    pct          = round(facturados / total * 100, 1) if total > 0 else 0.0

    return {
        "total":         int(total),
        "facturados":    int(facturados),
        "no_facturado":  int(no_facturado),
        "sin_cruce":     int(sin_cruce),
        "cumplimiento":  pct,
    }


def resumen_cruce_por_convenio(df: pd.DataFrame) -> pd.DataFrame:
    """Resumen del cruce agrupado por convenio."""
    if "estado_cruce" not in df.columns:
        return pd.DataFrame()

    resumen = (
        df.groupby("nombre_convenio")["estado_cruce"]
        .value_counts()
        .unstack(fill_value=0)
        .reset_index()
    )
    for c in ["Facturado", "No facturado", "Sin cruce"]:
        if c not in resumen.columns:
            resumen[c] = 0

    resumen["Total"] = resumen[["Facturado", "No facturado", "Sin cruce"]].sum(axis=1)
    resumen["Cumplimiento (%)"] = (
        resumen["Facturado"] / resumen["Total"] * 100
    ).round(1)
    return resumen.rename(columns={"nombre_convenio": "Convenio"})


def resumen_cruce_por_tipo_base(df: pd.DataFrame) -> pd.DataFrame:
    """Resumen del cruce agrupado por tipo de base."""
    if "estado_cruce" not in df.columns:
        return pd.DataFrame()

    resumen = (
        df.groupby("tipo_base")["estado_cruce"]
        .value_counts()
        .unstack(fill_value=0)
        .reset_index()
    )
    for c in ["Facturado", "No facturado", "Sin cruce"]:
        if c not in resumen.columns:
            resumen[c] = 0

    resumen["Total"] = resumen[["Facturado", "No facturado", "Sin cruce"]].sum(axis=1)
    resumen["Cumplimiento (%)"] = (
        resumen["Facturado"] / resumen["Total"] * 100
    ).round(1)
    return resumen.rename(columns={"tipo_base": "Tipo de base"})

def guardar_cruce(df: pd.DataFrame, mes_label: str) -> Path:
    """Guarda el resultado del cruce en parquet separado. Reemplaza si existe."""
    ruta = PARQUET_DIR / f"cruce_{_nombre_seguro(mes_label)}.parquet"
    df_guardar = df.copy()
    for col in df_guardar.columns:
        if df_guardar[col].dtype == object:
            df_guardar[col] = df_guardar[col].fillna("").astype(str)
    df_guardar.to_parquet(ruta, index=False, engine="pyarrow")
    return ruta


def cargar_cruce(mes_label: str) -> pd.DataFrame | None:
    """Carga el cruce guardado de un mes. Retorna None si no existe."""
    ruta = PARQUET_DIR / f"cruce_{_nombre_seguro(mes_label)}.parquet"
    if ruta.exists():
        return pd.read_parquet(ruta, engine="pyarrow")
    return None


def cruces_disponibles() -> list[str]:
    """Lista los meses que tienen cruce guardado."""
    archivos = sorted(PARQUET_DIR.glob("cruce_*.parquet"))
    return [
        f.stem.replace("cruce_", "").replace("_", " ")
        for f in archivos
    ]


def _nombre_seguro(texto: str) -> str:
    return texto.replace(" ", "_").replace("/", "-").replace("\\", "-")