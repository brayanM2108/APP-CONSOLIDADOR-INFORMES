"""Tab 'Reporte' — modo mes y modo convenio."""

import streamlit as st

import pandas as pd
from ui.estado import MESES
from ui.componentes import mostrar_kpis, bloque_descargas
from core.analizador import (
    resumen_por_convenio,
    pendientes_por_facturador,
    detalle_pendientes,
    convenios_disponibles,
)
from core.watcher import MESES_NOMBRE


def render_tab_reporte():
    df_total = st.session_state.df_resultado
    modo = st.session_state.get("modo_reporte", "mes")
    mes_label = st.session_state.mes_label

    if df_total is None or df_total.empty:
        st.info("Carga datos desde el historial o procesa archivos primero.")
        return

    if modo == "mes":
        _reporte_mes(df_total, mes_label)
    else:
        _reporte_convenio(df_total, mes_label)


def _reporte_mes(df_total, mes_label):
    opciones_conv = ["Todos"] + sorted(df_total["nombre_convenio"].unique().tolist())
    conv_reporte = st.selectbox("📌 Convenio", opciones_conv, key="filtro_conv_reporte")
    df = (
        df_total
        if conv_reporte == "Todos"
        else df_total[df_total["nombre_convenio"] == conv_reporte]
    )

    if df.empty:
        st.warning("No hay registros para el convenio seleccionado.")
        st.stop()

    mostrar_kpis(df)

    # Resumen por convenio
    st.subheader("📋 Resumen por convenio")
    rc = resumen_por_convenio(df)
    if not rc.empty:
        st.dataframe(
            rc.style.background_gradient(
                subset=["Cumplimiento (%)"], cmap="RdYlGn", vmin=0, vmax=100
            ),
            use_container_width=True, hide_index=True,
        )

    st.divider()

    # Pendientes por facturador
    st.subheader("👤 Pendientes por facturador")
    rf = pendientes_por_facturador(df)
    if rf.empty:
        st.success("🎉 No hay pendientes.")
    else:
        st.dataframe(rf, use_container_width=True, hide_index=True)

    st.divider()

    # Detalle pendientes
    st.subheader("⚠️ Detalle de pendientes")
    df_pend = df[df["estado"] == "Pendiente"]
    if df_pend.empty:
        st.success("🎉 No hay registros pendientes.")
    else:
        convs = ["Todos"] + convenios_disponibles(df_pend)
        filtro = st.selectbox("Filtrar por convenio", convs, key="det_conv_mes")
        df_det = detalle_pendientes(df, filtro)
        st.caption(f"{len(df_det):,} registros")
        st.dataframe(df_det, use_container_width=True, hide_index=True)

    st.divider()

    # Descargas
    st.subheader("⬇️ Descargar reportes")
    bloque_descargas(df, mes_label, key_suffix="mes")


def _reporte_convenio(df_total, mes_label):
    def _orden_mes(row):
        return str(row["año"]) + "_" + str(row["mes"]).zfill(2)

    def _label_mes(clave):
        año_m, mes_m = clave.split("_")
        nombre = {v: k for k, v in MESES_NOMBRE.items()}.get(mes_m, mes_m)
        return f"{nombre.capitalize()} {año_m}"

    df_total["_orden"] = df_total.apply(_orden_mes, axis=1)
    meses_ord = sorted(df_total["_orden"].unique().tolist())
    meses_labs = [_label_mes(m) for m in meses_ord]

    # ── Filtro de convenio (visible cuando se carga "Todos") ──────────
    convenios_disp = sorted(df_total["nombre_convenio"].unique().tolist())
    if len(convenios_disp) > 1:
        conv_sel = st.selectbox(
            "📌 Convenio", ["Todos"] + convenios_disp, key="filtro_conv_convenio"
        )
        if conv_sel != "Todos":
            df_total = df_total[df_total["nombre_convenio"] == conv_sel].copy()
            meses_ord = sorted(df_total["_orden"].unique().tolist())
            meses_labs = [_label_mes(m) for m in meses_ord]

    st.caption(f"Convenio: **{mes_label}** · {len(meses_labs)} mes(es) disponibles")

    col_d1, col_d2 = st.columns(2)
    with col_d1:
        desde = st.selectbox("Desde", meses_labs, index=0, key="conv_desde")
    with col_d2:
        hasta = st.selectbox(
            "Hasta", meses_labs, index=len(meses_labs) - 1, key="conv_hasta"
        )

    idx_d = meses_labs.index(desde)
    idx_h = meses_labs.index(hasta)

    if idx_d > idx_h:
        st.warning("⚠️ 'Desde' debe ser anterior o igual a 'Hasta'.")
        st.stop()

    claves_sel = meses_ord[idx_d : idx_h + 1]
    df = df_total[df_total["_orden"].isin(claves_sel)].drop(columns=["_orden"])

    if df.empty:
        st.warning("No hay registros para el rango seleccionado.")
        st.stop()

    st.caption(f"**{len(df):,}** registros · {desde} → {hasta}")
    st.divider()

    mostrar_kpis(df)

    # Resumen por mes
    st.subheader("📋 Resumen por mes")
    df["mes_label"] = df.apply(
        lambda r: _label_mes(str(r["año"]) + "_" + str(r["mes"]).zfill(2)), axis=1
    )
    resumen_mes = (
        df.groupby("mes_label")["estado"]
        .value_counts()
        .unstack(fill_value=0)
        .reset_index()
    )
    for c in ["Facturado", "Pendiente", "Sin información"]:
        if c not in resumen_mes.columns:
            resumen_mes[c] = 0
    resumen_mes["Total"] = resumen_mes[
        ["Facturado", "Pendiente", "Sin información"]
    ].sum(axis=1)
    resumen_mes["Cumplimiento (%)"] = (
        resumen_mes["Facturado"] / resumen_mes["Total"] * 100
    ).round(0).astype(int)
    st.dataframe(
        resumen_mes.style.background_gradient(
            subset=["Cumplimiento (%)"], cmap="RdYlGn", vmin=0, vmax=100
        ),
        use_container_width=True, hide_index=True,
    )

    # ── Resumen por convenio (solo si se cargaron todos) ─────────────
    if len(convenios_disp) > 1:
        st.divider()
        st.subheader("🏥 Resumen por convenio")
        rc = resumen_por_convenio(df)
        if not rc.empty:
            st.dataframe(
                rc.style.background_gradient(
                    subset=["Cumplimiento (%)"], cmap="RdYlGn", vmin=0, vmax=100
                ),
                use_container_width=True, hide_index=True,
            )

    st.divider()

    # Pendientes por facturador
    st.subheader("👤 Pendientes por facturador")
    rf = pendientes_por_facturador(df)
    if rf.empty:
        st.success("🎉 No hay pendientes.")
    else:
        st.dataframe(rf, use_container_width=True, hide_index=True)

    st.divider()

    # Detalle pendientes
    st.subheader("⚠️ Detalle de pendientes")
    df_pend = df[df["estado"] == "Pendiente"]
    if df_pend.empty:
        st.success("🎉 No hay registros pendientes.")
    else:
        cols_det = [
            "mes_label", "tipo_base", "documento_paciente", "nombre_paciente",
            "descripcion_servicio", "facturador", "observacion", "archivo_origen",
        ]
        cols_det = [c for c in cols_det if c in df_pend.columns]
        st.caption(f"{len(df_pend):,} registros pendientes")
        st.dataframe(df_pend[cols_det], use_container_width=True, hide_index=True)

    st.divider()

    # Descargas
    st.subheader("⬇️ Descargar reportes")
    label_h = f"{mes_label}_{desde.replace(' ', '_')}_a_{hasta.replace(' ', '_')}"
    bloque_descargas(df, label_h, key_suffix="conv")
