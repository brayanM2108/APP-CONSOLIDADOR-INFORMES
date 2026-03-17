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
LLAVE_BASE_DEFAULT      = ["documento_paciente", "fecha_atencion", "cups"]
LLAVE_FACTURADO_DEFAULT = ["IDENTIFICACION", "FECHA FACTURA"]

# Columna CUPS en facturado (puede no existir)
COL_CUPS_FACTURADO = "CUPS"


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


def _construir_llave(df: pd.DataFrame, columnas: list[str], sep: str = "||") -> pd.Series:
    """
    Construye una llave compuesta concatenando las columnas indicadas.
    Las columnas de fecha se normalizan automáticamente.
    """
    partes = []
    for col in columnas:
        if col not in df.columns:
            partes.append(pd.Series([""] * len(df), index=df.index))
            continue

        serie = df[col].copy()

        # Detectar si es columna de fecha
        es_fecha = (
            "fecha" in col.lower()
            or pd.api.types.is_datetime64_any_dtype(serie)
        )
        if es_fecha:
            serie = _normalizar_fecha(serie)
        else:
            serie = _normalizar_str(serie)

        partes.append(serie)

    return partes[0].str.cat(partes[1:], sep=sep) if len(partes) > 1 else partes[0]


# ════════════════════════════════════════════════════════════
# PREPARAR FACTURADO
# ════════════════════════════════════════════════════════════

def _preparar_set_facturado(df_fact: pd.DataFrame) -> set:
    """
    Construye el set de llaves del facturado (solo Activas).
    Llave: IDENTIFICACION + FECHA FACTURA [+ CUPS si existe]
    """
    df_act = df_fact[df_fact["_estado_factura"] == "Activo"].copy()

    cols_llave = list(LLAVE_FACTURADO_DEFAULT)
    if COL_CUPS_FACTURADO in df_act.columns:
        cols_llave.append(COL_CUPS_FACTURADO)

    llaves = _construir_llave(df_act, cols_llave)
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
        conf_tipo  = config.get(tipo_base, {})
        cols_llave = conf_tipo.get("llave_cruce", LLAVE_BASE_DEFAULT)

        # Construir llave en las bases
        # Ajustar a misma cantidad de campos que la llave del facturado
        cols_fact = list(LLAVE_FACTURADO_DEFAULT)
        if COL_CUPS_FACTURADO in df_facturado.columns:
            cols_fact.append(COL_CUPS_FACTURADO)

        # Si la llave del facturado tiene CUPS pero la base no lo tiene
        # en la llave configurada, igualmente incluir cups si existe
        if len(cols_fact) > len(cols_llave):
            # Agregar cups a la llave de base si está disponible
            if "cups" not in [c.lower() for c in cols_llave] and "cups" in grupo.columns:
                cols_llave = list(cols_llave) + ["cups"]

        llave_grupo = _construir_llave(grupo, cols_llave)

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