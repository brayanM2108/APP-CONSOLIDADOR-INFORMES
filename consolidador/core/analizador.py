"""
analizador.py
Responsabilidad: calcular resúmenes, KPIs y agrupaciones.
Recibe DataFrames limpios y devuelve DataFrames de resumen.
No sabe nada de interfaz ni de exportación.
"""

import pandas as pd


def kpis_globales(df: pd.DataFrame) -> dict:
    """
    Retorna métricas globales del consolidado.
    """
    total    = len(df)
    fact     = (df["estado"] == "Facturado").sum()
    pend     = (df["estado"] == "Pendiente").sum()
    sin_info = (df["estado"] == "Sin información").sum()
    pct      = round(fact / total * 100, 1) if total > 0 else 0.0

    return {
        "total":        int(total),
        "facturados":   int(fact),
        "pendientes":   int(pend),
        "sin_info":     int(sin_info),
        "cumplimiento": pct,
    }


def resumen_por_convenio(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrupa por convenio: total, facturados, pendientes, cumplimiento.
    """
    if df.empty:
        return pd.DataFrame()

    g = (
        df.groupby("nombre_convenio")["estado"]
        .value_counts()
        .unstack(fill_value=0)
        .reset_index()
    )

    for col in ["Facturado", "Pendiente", "Sin información"]:
        if col not in g.columns:
            g[col] = 0

    g["Total"]            = g[["Facturado", "Pendiente", "Sin información"]].sum(axis=1)
    g["Cumplimiento (%)"] = (g["Facturado"] / g["Total"] * 100).round(1)

    return g.rename(columns={"nombre_convenio": "Convenio"})[[
        "Convenio", "Total", "Facturado", "Pendiente", "Sin información", "Cumplimiento (%)"
    ]]


def pendientes_por_facturador(df: pd.DataFrame) -> pd.DataFrame:
    """
    Lista de pendientes agrupados por convenio y facturador.
    """
    if df.empty:
        return pd.DataFrame()

    df_pend = df[df["estado"] == "Pendiente"]
    if df_pend.empty:
        return pd.DataFrame()

    g = (
        df_pend
        .groupby(["nombre_convenio", "facturador"])
        .agg(
            Pendientes      =("estado", "count"),
            Con_observacion =("observacion", lambda x: (x.str.strip() != "").sum()),
        )
        .reset_index()
        .sort_values("Pendientes", ascending=False)
    )

    return g.rename(columns={
        "nombre_convenio": "Convenio",
        "facturador":      "Facturador",
        "Con_observacion": "Con observación",
    })


def detalle_pendientes(df: pd.DataFrame, convenio: str | None = None) -> pd.DataFrame:
    """
    Detalle fila a fila de los registros pendientes.
    Si se pasa convenio, filtra por ese convenio.
    Incluye columnas extra si existen.
    """
    from .procesador import COLUMNAS

    df_pend = df[df["estado"] == "Pendiente"].copy()

    if convenio and convenio != "Todos":
        df_pend = df_pend[df_pend["nombre_convenio"] == convenio]

    # Columnas base del detalle
    cols_base = [
        "nombre_convenio", "tipo_base", "documento_paciente",
        "nombre_paciente", "descripcion_servicio",
        "facturador", "observacion", "archivo_origen",
    ]

    # Agregar columnas extra (las que no son estándar)
    cols_extra = [c for c in df_pend.columns if c not in COLUMNAS]
    return df_pend[cols_base + cols_extra]


def columnas_extra_de(df: pd.DataFrame, tipo_base: str) -> list[str]:
    """
    Retorna las columnas extra que tiene un tipo de base especifico.
    """
    from .procesador import COLUMNAS
    df_tipo = df[df["tipo_base"] == tipo_base]
    return [c for c in df_tipo.columns if c not in COLUMNAS]


def convenios_disponibles(df: pd.DataFrame) -> list[str]:
    return sorted(df["nombre_convenio"].unique().tolist())