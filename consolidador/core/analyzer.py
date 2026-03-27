"""
Analyzer
Responsibility: Calculate summaries, KPIs, and groupings.
It receives clean DataFrames and returns summary DataFrames. It knows nothing about interface or exporting.
"""
import pandas as pd
from .processor import columns

def global_kpis(df: pd.DataFrame) -> dict:
    """
    Returns global metrics from the consolidated data.
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


def summary_by_agreement(df: pd.DataFrame) -> pd.DataFrame:
    """
    Group by agreement: total, invoiced, pending, compliance.
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
    g["Cumplimiento (%)"] = (g["Facturado"] / g["Total"] * 100).round(0).astype(int)

    return g.rename(columns={"nombre_convenio": "Convenio"})[[
        "Convenio", "Total", "Facturado", "Pendiente", "Sin información", "Cumplimiento (%)"
    ]]


def pending_by_biller(df: pd.DataFrame) -> pd.DataFrame:
    """
    List of pending items grouped by agreement and biller.
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
            Con_observacion=("observacion", lambda x: (x.fillna("").astype(str).str.strip() != "").sum()),
        )
        .reset_index()
        .sort_values("Pendientes", ascending=False)
    )

    return g.rename(columns={
        "nombre_convenio": "Convenio",
        "facturador":      "Facturador",
        "Con_observacion": "Con observación",
    })


def pending_details(df: pd.DataFrame, agreement: str | None = None) -> pd.DataFrame:
    """
    Detailed row-by-row view of pending records.
    If an agreement is specified, filter by that agreement.
    Include extra columns if available.
    """

    df_pend = df[df["estado"] == "Pendiente"].copy()

    if agreement and agreement != "Todos":
        df_pend = df_pend[df_pend["nombre_convenio"] == agreement]

    cols_base = [
        "nombre_convenio", "tipo_base", "documento_paciente",
        "nombre_paciente", "descripcion_servicio",
        "facturador", "observacion", "archivo_origen",
    ]
    cols_extra = [c for c in df_pend.columns if c not in columns]
    return df_pend[cols_base + cols_extra]


def extra_columns(df: pd.DataFrame, tipo_base: str) -> list[str]:
    """
    Returns the extra columns that a specific database type has.
    """
    df_tipo = df[df["tipo_base"] == tipo_base]
    return [c for c in df_tipo.columns if c not in columns]


def available_agreements(df: pd.DataFrame) -> list[str]:
    return sorted(df["nombre_convenio"].unique().tolist())