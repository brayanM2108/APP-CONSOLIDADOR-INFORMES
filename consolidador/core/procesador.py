"""
procesador.py
Responsabilidad: transformar un DataFrame crudo al esquema estándar.
No sabe nada de interfaz ni de exportación.
"""

import pandas as pd

# Esquema estándar de salida
COLUMNAS = [
    "documento_paciente",
    "nombre_paciente",
    "cups",
    "descripcion_servicio",
    "fecha_atencion",
    "facturador",
    "observacion",
    "estado",            # "Facturado" | "Pendiente" | "Sin información"
    "valor_estado_original",
    "tipo_base",
    "nombre_convenio",
    "archivo_origen",
    "mes",
    "año",
]


def _detectar_estado(valor, logica: str) -> str:
    """
    Determina si un valor indica que el servicio fue facturado.

    logica puede ser:
      "tiene_valor" → celda no vacía = facturado
      "es_numero"   → contiene un número válido
      "es_fecha"    → contiene una fecha válida
      cualquier texto → comparación exacta (sin importar mayúsculas)
    """
    if pd.isna(valor) or str(valor).strip() == "":
        return "Pendiente"

    val = str(valor).strip()

    if logica == "tiene_valor":
        facturado = True
    elif logica == "es_numero":
        try:
            float(val)
            facturado = True
        except ValueError:
            facturado = False
    elif logica == "es_fecha":
        try:
            pd.to_datetime(val)
            facturado = True
        except Exception:
            facturado = False
    else:
        facturado = val.lower() == logica.strip().lower()

    return "Facturado" if facturado else "Pendiente"


def _extraer_convenio(tipo_base: str) -> str:
    """
    Extrae el nombre del convenio del tipo de base.
    'Convenio A - Laboratorio' → 'Convenio A'
    """
    return tipo_base.split(" - ")[0].strip() if " - " in tipo_base else tipo_base


def _mapear_columna(df_raw: pd.DataFrame, col_real: str | None) -> pd.Series:
    """Devuelve la columna si existe, o una serie vacía si no."""
    if col_real and col_real in df_raw.columns:
        return df_raw[col_real].reset_index(drop=True)
    return pd.Series([""] * len(df_raw))


def procesar_base(
    df_raw: pd.DataFrame,
    config: dict,
    nombre_archivo: str,
    tipo_base: str,
    mes: str,
    año: int,
) -> tuple[pd.DataFrame, list[str]]:
    """
    Convierte un DataFrame crudo al esquema estándar.

    Retorna:
      - DataFrame procesado
      - Lista de advertencias (columnas no encontradas)
    """
    advertencias = []

    # Detectar columnas faltantes
    cols_config = {k: v for k, v in config.items() if isinstance(v, str)}
    for clave, col_real in cols_config.items():
        if col_real not in df_raw.columns:
            advertencias.append(f"Columna '{col_real}' ({clave}) no encontrada en '{nombre_archivo}'")

    # Construir DataFrame estándar
    df = pd.DataFrame()
    df["documento_paciente"]   = _mapear_columna(df_raw, config.get("col_paciente"))
    df["nombre_paciente"]      = _mapear_columna(df_raw, config.get("col_nombre"))
    df["cups"]                 = _mapear_columna(df_raw, config.get("col_cups"))
    df["descripcion_servicio"] = _mapear_columna(df_raw, config.get("col_servicio"))
    df["fecha_atencion"]       = _mapear_columna(df_raw, config.get("col_fecha"))
    df["facturador"]           = _mapear_columna(df_raw, config.get("col_facturador"))
    df["observacion"]          = _mapear_columna(df_raw, config.get("col_observacion"))

    # Estado de facturación
    col_fact = config.get("col_facturacion")
    logica   = config.get("logica_facturacion", "tiene_valor")

    if col_fact and col_fact in df_raw.columns:
        df["valor_estado_original"] = df_raw[col_fact].astype(str).values
        df["estado"] = df_raw[col_fact].apply(lambda v: _detectar_estado(v, logica)).values
    else:
        df["valor_estado_original"] = ""
        df["estado"]                = "Sin información"
        advertencias.append(f"Columna de facturación '{col_fact}' no encontrada. Estado marcado como 'Sin información'.")

    # Metadatos
    df["tipo_base"]      = tipo_base
    df["nombre_convenio"]= _extraer_convenio(tipo_base)
    df["archivo_origen"] = nombre_archivo
    df["mes"]            = mes
    df["año"]            = año

    # Garantizar orden de columnas
    for col in COLUMNAS:
        if col not in df.columns:
            df[col] = ""

    return df[COLUMNAS], advertencias