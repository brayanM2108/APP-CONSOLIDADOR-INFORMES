"""Tab 'Report' — month mode and agreement mode."""

import streamlit as st


from ui.components import show_kpis, download_block
from core.analyzer import (
    summary_by_agreement,
    pending_by_biller,
    pending_details,
    available_agreements,
)
from core.watcher import MONTHS_NAME


def render_tab_report():
    df_total = st.session_state.df_resultado
    mode = st.session_state.get("modo_reporte", "mes")
    mes_label = st.session_state.mes_label

    if df_total is None or df_total.empty:
        st.info("Carga datos desde el historial o procesa archivos primero.")
        return

    if mode == "mes":
        _report_month(df_total, mes_label)
    else:
        _report_agreement(df_total, mes_label)


def _report_month(df_total, mes_label):
    agreement_options = ["Todos"] + sorted(df_total["nombre_convenio"].unique().tolist())
    agreement_selected = st.selectbox("📌 Convenio", agreement_options, key="filtro_conv_reporte")
    df = (
        df_total
        if agreement_selected == "Todos"
        else df_total[df_total["nombre_convenio"] == agreement_selected]
    )

    if df.empty:
        st.warning("No hay registros para el convenio seleccionado.")
        st.stop()

    show_kpis(df)

    st.subheader("📋 Resumen por convenio")
    summary_rc = summary_by_agreement(df)
    if not summary_rc.empty:
        st.dataframe(
            summary_rc.style.background_gradient(
                subset=["Cumplimiento (%)"], cmap="RdYlGn", vmin=0, vmax=100
            ),
            width = "stretch", hide_index=True,
        )

    st.divider()

    st.subheader("👤 Pendientes por facturador")
    pending_by_biller_df = pending_by_biller(df)
    if pending_by_biller_df.empty:
        st.success("🎉 No hay pendientes.")
    else:
        st.dataframe(pending_by_biller_df, width = "stretch", hide_index=True)

    st.divider()

    st.subheader("⚠️ Detalle de pendientes")
    df_pending = df[df["estado"] == "Pendiente"]
    if df_pending.empty:
        st.success("🎉 No hay registros pendientes.")
    else:
        agreements_for_filter = ["Todos"] + available_agreements(df_pending)
        filter_selected = st.selectbox("Filtrar por convenio", agreements_for_filter, key="det_conv_mes")
        df_details = pending_details(df, filter_selected)
        st.caption(f"{len(df_details):,} registros")
        st.dataframe(df_details, width = "stretch", hide_index=True)

    st.divider()

    st.subheader("⬇️ Descargar reportes")
    download_block(df, mes_label, key_suffix="mes")


def _report_agreement(df_total, mes_label):
    def _order_month(row):
        return str(row["año"]) + "_" + str(row["mes"]).zfill(2)

    def _label_month(key):
        año_m, mes_m = key.split("_")
        nombre = {v: k for k, v in MONTHS_NAME.items()}.get(mes_m, mes_m)
        return f"{nombre.capitalize()} {año_m}"

    df_total["_orden"] = df_total.apply(_order_month, axis=1)
    months_ordered = sorted(df_total["_orden"].unique().tolist())
    months_labels = [_label_month(m) for m in months_ordered]


    agreements_available_list = sorted(df_total["nombre_convenio"].unique().tolist())
    if len(agreements_available_list) > 1:
        agreement_selected = st.selectbox(
            "📌 Convenio", ["Todos"] + agreements_available_list, key="filtro_conv_convenio"
        )
        if agreement_selected != "Todos":
            df_total = df_total[df_total["nombre_convenio"] == agreement_selected].copy()
            months_ordered = sorted(df_total["_orden"].unique().tolist())
            months_labels = [_label_month(m) for m in months_ordered]

    st.caption(f"Convenio: **{mes_label}** · {len(months_labels)} mes(es) disponibles")

    col_d1, col_d2 = st.columns(2)
    with col_d1:
        from_label = st.selectbox("Desde", months_labels, index=0, key="conv_desde")
    with col_d2:
        to_label = st.selectbox(
            "Hasta", months_labels, index=len(months_labels) - 1, key="conv_hasta"
        )

    idx_from = months_labels.index(from_label)
    idx_to = months_labels.index(to_label)

    if idx_from > idx_to:
        st.warning("⚠️ 'Desde' debe ser anterior o igual a 'Hasta'.")
        st.stop()

    selected_keys = months_ordered[idx_from : idx_to + 1]
    df = df_total[df_total["_orden"].isin(selected_keys)].drop(columns=["_orden"])

    if df.empty:
        st.warning("No hay registros para el rango seleccionado.")
        st.stop()

    st.caption(f"**{len(df):,}** registros · {from_label} → {to_label}")
    st.divider()

    show_kpis(df)

    st.subheader("📋 Resumen por mes")
    df["mes_label"] = df.apply(
        lambda r: _label_month(str(r["año"]) + "_" + str(r["mes"]).zfill(2)), axis=1
    )
    month_summary = (
        df.groupby("mes_label")["estado"]
        .value_counts()
        .unstack(fill_value=0)
        .reset_index()
    )
    for c in ["Facturado", "Pendiente", "Sin información"]:
        if c not in month_summary.columns:
            month_summary[c] = 0
    month_summary["Total"] = month_summary[
        ["Facturado", "Pendiente", "Sin información"]
    ].sum(axis=1)
    month_summary["Cumplimiento (%)"] = (
            month_summary["Facturado"] / month_summary["Total"] * 100
    ).round(0).astype(int)
    st.dataframe(
        month_summary.style.background_gradient(
            subset=["Cumplimiento (%)"], cmap="RdYlGn", vmin=0, vmax=100
        ),
        width = "stretch", hide_index=True,
    )

    if len(agreements_available_list) > 1:
        st.divider()
        st.subheader("🏥 Resumen por convenio")
        rc = summary_by_agreement(df)
        if not rc.empty:
            st.dataframe(
                rc.style.background_gradient(
                    subset=["Cumplimiento (%)"], cmap="RdYlGn", vmin=0, vmax=100
                ),
                width = "stretch", hide_index=True,
            )

    st.divider()

    st.subheader("👤 Pendientes por facturador")
    pending_by_biller_df = pending_by_biller(df)
    if pending_by_biller_df.empty:
        st.success("🎉 No hay pendientes.")
    else:
        st.dataframe(pending_by_biller_df, width = "stretch", hide_index=True)

    st.divider()

    st.subheader("⚠️ Detalle de pendientes")
    df_pending = df[df["estado"] == "Pendiente"]
    if df_pending.empty:
        st.success("🎉 No hay registros pendientes.")
    else:
        detail_columns = [
            "mes_label", "tipo_base", "documento_paciente", "nombre_paciente",
            "descripcion_servicio", "facturador", "observacion", "archivo_origen",
        ]
        detail_columns = [c for c in detail_columns if c in df_pending.columns]
        st.caption(f"{len(df_pending):,} registros pendientes")
        st.dataframe(df_pending[detail_columns], width = "stretch", hide_index=True)

    st.divider()

    st.subheader("⬇️ Descargar reportes")
    label_h = f"{mes_label}_{from_label.replace(' ', '_')}_a_{to_label.replace(' ', '_')}"
    download_block(df, label_h, key_suffix="conv")
