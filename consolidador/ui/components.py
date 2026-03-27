"Reusable components: KPIs, download block."
import streamlit as st
import pandas as pd

from core.analyzer import global_kpis, available_agreements
from core.exporter import (
    general_csv, general_excel,
    agreement_csv, agreement_excel,
    base_type_csv, base_type_excel,
    general_name,
    name_agreement_file,
    name_base_type_file,
)


def show_kpis(df: pd.DataFrame):
    kpis = global_kpis(df)
    pct = kpis["cumplimiento"]
    color_pct = "verde" if pct >= 80 else "naranja" if pct >= 50 else "rojo"

    k1, k2, k3, k4, k5 = st.columns(5)
    for col, num, label, color in [
        (k1, f"{kpis['total']:,}", "Total registros", "azul"),
        (k2, f"{kpis['facturados']:,}", "Facturados", "verde"),
        (k3, f"{kpis['pendientes']:,}", "Pendientes", "rojo"),
        (k4, f"{kpis['sin_info']:,}", "Sin información", "gris"),
        (k5, f"{pct}%", "Cumplimiento", color_pct),
    ]:
        col.markdown(
            f'<div class="kpi">'
            f'<div class="kpi-num {color}">{num}</div>'
            f'<div class="kpi-label">{label}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
        st.markdown("<br>", unsafe_allow_html=True)


def download_block(df: pd.DataFrame, label: str, key_suffix: str = ""):
    """Renders the 3 download levels."""

    # level 1 — General
    st.markdown(
        '<div class="nivel-header">'
        '<div class="nivel-titulo">📊 Nivel 1 — Reporte general</div>'
        '<div class="nivel-desc">Todos los convenios y tipos de base consolidados</div>'
        '</div>',
        unsafe_allow_html=True,
    )
    c1, c2 = st.columns(2)
    with c1:
        st.download_button(
            "⬇️ General CSV", data=general_csv(df),
            file_name=general_name(label, "csv"),
            mime="text/csv", width = "stretch",
            key=f"dl_gen_csv_{key_suffix}",
        )
    with c2:
        st.download_button(
            "⬇️ General Excel", data=general_excel(df),
            file_name=general_name(label, "xlsx"),
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            width = "stretch",
            key=f"dl_gen_xlsx_{key_suffix}",
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # Level 2 — Per agreement
    st.markdown(
        '<div class="nivel-header">'
        '<div class="nivel-titulo">🏥 Nivel 2 — Por convenio</div>'
        '<div class="nivel-desc">Reporte individual de cada convenio</div>'
        '</div>',
        unsafe_allow_html=True,
    )
    convenios = available_agreements(df)
    conv_sel = st.selectbox(
        "Selecciona el convenio", convenios, key=f"conv_desc_{key_suffix}"
    )
    c3, c4 = st.columns(2)
    with c3:
        st.download_button(
            f"⬇️ {conv_sel} CSV", data=agreement_csv(df, conv_sel),
            file_name=name_agreement_file(conv_sel, label, "csv"),
            mime="text/csv", width = "stretch",
            key=f"dl_conv_csv_{key_suffix}",
        )
    with c4:
        st.download_button(
            f"⬇️ {conv_sel} Excel", data=agreement_excel(df, conv_sel),
            file_name=name_agreement_file(conv_sel, label, "xlsx"),
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            width = "stretch",
            key=f"dl_conv_xlsx_{key_suffix}",
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # Level 3 — By base type
    st.markdown(
        '<div class="nivel-header">'
        '<div class="nivel-titulo">🗂️ Nivel 3 — Por tipo de base</div>'
        '<div class="nivel-desc">Reporte individual de cada tipo de base</div>'
        '</div>',
        unsafe_allow_html=True,
    )
    tipos = sorted(df["tipo_base"].unique().tolist())
    tipo_sel = st.selectbox(
        "Selecciona el tipo de base", tipos, key=f"tipo_desc_{key_suffix}"
    )
    c5, c6 = st.columns(2)
    with c5:
        st.download_button(
            f"⬇️ {tipo_sel} CSV", data=base_type_csv(df, tipo_sel),
            file_name=name_base_type_file(tipo_sel, label, "csv"),
            mime="text/csv", width = "stretch",
            key=f"dl_tipo_csv_{key_suffix}",
        )
    with c6:
        st.download_button(
            f"⬇️ {tipo_sel} Excel", data=base_type_excel(df, tipo_sel),
            file_name=name_base_type_file(tipo_sel, label, "xlsx"),
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            width = "stretch",
            key=f"dl_tipo_xlsx_{key_suffix}",
        )
