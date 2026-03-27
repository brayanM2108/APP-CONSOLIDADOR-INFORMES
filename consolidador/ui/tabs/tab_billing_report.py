"""Tab 'Billing Report' — report loading and cross-referencing with databases."""

import streamlit as st

from core.billing_report import (
    read_report, kpis_report, save_report,
    load_report, info_save_report,
    COLS_CRUCE_RESULTADO,
)
from core.cross_billing_report import (
    cross_bases_with_report,
    crossing_kpis_report,
    crossing_summary_by_agreement_report,
    crossing_summary_by_base_type_report,
    save_cross_report,
)
from core.exporter import (
    _excel, _csv, _safe_name,
    months_available_parquet,
    load_parquet,
)


def render_tab_billing_report():

    st.markdown("### 📑 Informe de Facturación")
    st.caption(
        "Sube el archivo Excel del informe. "
        "Se lee la primera hoja. "
        "Se guarda en Parquet separado del archivo de facturado."
    )

    save_info = info_save_report()
    df_save = load_report() if save_info else None

    if save_info:
        st.success(
            f"✅ Informe guardado · {save_info['fecha_guardado']} · "
            f"{save_info['total']:,} registros "
            f"({save_info['activas']:,} activos / {save_info['anuladas']:,} anulados)"
        )
    else:
        st.info("📭 No hay informe guardado aún.")

    st.divider()

    uploaded_file = st.file_uploader(
        "Selecciona el archivo de informe",
        type=["xlsx", "xls"],
        key="uploader_informe",
    )

    df_new = None
    if uploaded_file:
        with st.spinner("Leyendo informe..."):
            df_new, warnings = read_report(uploaded_file)

        if warnings:
            with st.expander(f"⚠️ {len(warnings)} advertencia(s)", expanded=True):
                for w in warnings:
                    st.markdown(f"- {w}")

        if df_new is None or df_new.empty:
            st.error("❌ No se pudo leer el archivo.")
            return

    df_kpi = df_new if df_new is not None else df_save
    if df_kpi is None or df_kpi.empty:
        return

    kpis = kpis_report(df_kpi)
    st.subheader("📊 Vista previa" if df_new is not None else "📊 Informe guardado")

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total registros", f"{kpis['total']:,}")
    k2.metric("Activos",         f"{kpis['activas']:,}")
    k3.metric("Anulados",        f"{kpis['anuladas']:,}")
    k4.metric("Valor total",     f"${kpis['valor_total']:,.0f}")

    c1, c2 = st.columns(2)
    with c1:
        if kpis["fecha_min"]:
            st.caption(f"📅 Desde: **{kpis['fecha_min'].strftime('%d/%m/%Y')}**")
    with c2:
        if kpis["fecha_max"]:
            st.caption(f"📅 Hasta: **{kpis['fecha_max'].strftime('%d/%m/%Y')}**")

    if kpis["convenios"]:
        st.caption(
            f"🏥 {len(kpis['convenios'])} convenio(s): "
            + ", ".join(str(c) for c in kpis["convenios"][:5])
            + (" ..." if len(kpis["convenios"]) > 5 else "")
        )

    if df_new is not None:
        st.divider()
        if save_info:
            st.warning(
                f"⚠️ Ya existe un informe guardado del {save_info['fecha_guardado']}. "
                "Al confirmar se **reemplazará**."
            )
        if st.button("✅ Confirmar y guardar", type="primary",
                     use_container_width=True, key="btn_guardar_informe"):
            with st.spinner("Guardando..."):
                try:
                    path_saved = save_report(df_new)
                    st.success(f"✅ {len(df_new):,} registros guardados en `{path_saved}`.")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Error: {e}")

    st.divider()
    st.markdown("### 🔀 Cruzar con bases")

    if not save_info:
        st.info("📭 Primero guarda el informe.")
        return

    months = months_available_parquet()
    if not months:
        st.info("📭 No hay bases procesadas.")
        return

    cross_mode = st.session_state.get("modo_cruce_informe", None)

    col_m1, col_m2 = st.columns(2)
    with col_m1:
        if st.button("📅 Cruce por mes", use_container_width=True, key="btn_inf_modo_mes"):
            st.session_state["modo_cruce_informe"] = "mes"
            st.session_state.pop("df_cruce_informe_resultado", None)
            st.rerun()
    with col_m2:
        if st.button("🗂️ Cruce por base", use_container_width=True, key="btn_inf_modo_base"):
            st.session_state["modo_cruce_informe"] = "base"
            st.session_state.pop("df_cruce_informe_resultado", None)
            st.rerun()

    if cross_mode is None:
        st.caption("Selecciona un modo de cruce.")
        return

    st.divider()

    if cross_mode == "mes":
        st.markdown("#### 📅 Cruce por mes")
        st.caption("Cruza todas las bases del mes seleccionado.")

        selected_month_label = st.selectbox("Mes", months, key="inf_mes_sel")
        df_prev_bases = load_parquet(selected_month_label.replace(" ", "_"))
        if df_prev_bases is not None:
            st.caption(f"📋 **{len(df_prev_bases):,}** registros a cruzar")

        if st.button("🔀 Ejecutar cruce por mes", type="primary",
                     use_container_width=True, key="btn_inf_cruce_mes"):
            with st.spinner("Ejecutando cruce..."):
                try:
                    df_bases = load_parquet(selected_month_label.replace(" ", "_"))
                    if df_bases is None or df_bases.empty:
                        st.error("❌ No se encontraron bases.")
                        return
                    df_informe = load_report()
                    df_crossed = cross_bases_with_report(
                        df_bases, df_informe, st.session_state.config
                    )
                    st.session_state["df_cruce_informe_resultado"] = df_crossed
                    st.session_state["label_cruce_informe"] = selected_month_label
                    st.success(f"✅ {len(df_crossed):,} registros procesados.")
                except Exception as e:
                    st.error(f"❌ Error: {e}")

    else:
        st.markdown("#### 🗂️ Cruce por convenio y tipo de base")
        st.caption("Usa todos los meses de la base cargada en sesión.")

        df_session = st.session_state.get("df_resultado")
        if df_session is None or df_session.empty:
            st.warning("⚠️ No hay datos en sesión. Carga desde el historial primero.")
            return

        months_session = sorted(df_session["mes"].unique().tolist()) if "mes" in df_session.columns else []
        st.caption(f"📅 Meses en sesión: **{', '.join(str(m) for m in months_session)}**")

        agreements_available = ["Todos"] + sorted(df_session["nombre_convenio"].unique().tolist())
        agreement_selected = st.selectbox("Convenio", agreements_available, key="inf_conv_sel")

        df_filtered = df_session if agreement_selected == "Todos" else df_session[df_session["nombre_convenio"] == agreement_selected]

        types_available = ["Todos"] + sorted(df_filtered["tipo_base"].unique().tolist())
        type_selected = st.selectbox("Tipo de base", types_available, key="inf_tipo_sel")

        df_filtered2 = df_filtered if type_selected == "Todos" else df_filtered[df_filtered["tipo_base"] == type_selected]
        st.caption(f"📋 **{len(df_filtered2):,}** registros a cruzar")

        if st.button("🔀 Ejecutar cruce por base", type="primary",
                     use_container_width=True, key="btn_inf_cruce_base"):
            with st.spinner("Ejecutando cruce..."):
                try:
                    df_informe = load_report()
                    df_crossed = cross_bases_with_report(
                        df_filtered2.copy(), df_informe, st.session_state.config
                    )
                    label = type_selected if type_selected != "Todos" else agreement_selected
                    st.session_state["df_cruce_informe_resultado"] = df_crossed
                    st.session_state["label_cruce_informe"] = label
                    st.success(f"✅ {len(df_crossed):,} registros procesados.")
                except Exception as e:
                    st.error(f"❌ Error: {e}")

    df_cross = st.session_state.get("df_cruce_informe_resultado")
    label = st.session_state.get("label_cruce_informe", "")

    if df_cross is None or "estado_cruce_informe" not in df_cross.columns:
        return

    st.divider()
    st.subheader(f"📊 Resultados — {label}")

    kci = crossing_kpis_report(df_cross)
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total",           f"{kci['total']:,}")
    k2.metric("✅ Facturados",   f"{kci['facturados']:,}")
    k3.metric("❌ No facturados",f"{kci['no_facturado']:,}")
    k4.metric("Cumplimiento",    f"{kci['cumplimiento']}%")

    st.divider()

    st.subheader("🏥 Por convenio")
    rc = crossing_summary_by_agreement_report(df_cross)
    if not rc.empty:
        st.dataframe(rc.style.background_gradient(
            subset=["Cumplimiento (%)"], cmap="RdYlGn", vmin=0, vmax=100),
            use_container_width=True, hide_index=True)

    st.divider()

    st.subheader("🗂️ Por tipo de base")
    rt = crossing_summary_by_base_type_report(df_cross)
    if not rt.empty:
        st.dataframe(rt.style.background_gradient(
            subset=["Cumplimiento (%)"], cmap="RdYlGn", vmin=0, vmax=100),
            use_container_width=True, hide_index=True)

    st.divider()

    df_not = df_cross[df_cross["estado_cruce_informe"] == "No facturado"]
    df_yes = df_cross[df_cross["estado_cruce_informe"] == "Facturado"]

    st.subheader("⚠️ Detalle — No facturados")
    cols_no = [
        "nombre_convenio", "tipo_base", "documento_paciente", "nombre_paciente",
        "cups", "descripcion_servicio", "fecha_atencion",
        "facturador", "estado", "llave_cruce_informe", "archivo_origen", "mes", "año",
    ]
    cols_no = [c for c in cols_no if c in df_not.columns]
    if df_not.empty:
        st.success("🎉 Todos los registros cruzaron.")
    else:
        st.caption(f"{len(df_not):,} registros")
        st.dataframe(df_not[cols_no], use_container_width=True, hide_index=True)

    st.divider()

    st.subheader("💾 Guardar y descargar")

    col_g, col_d = st.columns(2)

    with col_g:
        if st.button("💾 Guardar cruce en Parquet", type="primary",
                     use_container_width=True, key="btn_guardar_cruce_inf"):
            with st.spinner("Guardando..."):
                try:
                    path_saved = save_cross_report(df_cross, label)
                    st.session_state.pop("df_cruce_informe_resultado", None)
                    st.success(f"✅ Guardado en `{path_saved}`.")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ {e}")

    df_informe_loaded = load_report()
    cols_base_yes = [
        "nombre_convenio", "tipo_base", "documento_paciente", "nombre_paciente",
        "cups", "descripcion_servicio", "fecha_atencion",
        "facturador", "estado", "llave_cruce_informe", "archivo_origen",
    ]
    cols_base_yes = [c for c in cols_base_yes if c in df_yes.columns]

    sheet_yes = df_yes[cols_base_yes].copy()
    if df_informe_loaded is not None and not df_yes.empty:
        concat_col = None
        for c in df_informe_loaded.columns:
            if c.strip() == "concatenado doc_mes_ cups":
                concat_col = c
                break
        if concat_col:
            df_inf_merge = df_informe_loaded.copy()
            df_inf_merge["llave_cruce_informe"] = (
                df_inf_merge[concat_col].astype(str).str.strip().str.upper()
            )
            cols_to_bring = ["llave_cruce_informe"] + [
                c for c in COLS_CRUCE_RESULTADO if c in df_inf_merge.columns
            ]
            df_inf_merge = df_inf_merge[cols_to_bring].drop_duplicates("llave_cruce_informe")
            sheet_yes = sheet_yes.merge(df_inf_merge, on="llave_cruce_informe", how="left")

    sheets = {
        "Resumen Convenio":  crossing_summary_by_agreement_report(df_cross),
        "Resumen Tipo Base": crossing_summary_by_base_type_report(df_cross),
        "Facturados":        sheet_yes,
        "No Facturados":     df_not[cols_no] if not df_not.empty else df_not,
    }

    col_csv, col_xlsx = st.columns(2)
    with col_csv:
        st.download_button(
            "⬇️ No facturados CSV",
            data=_csv(df_not[cols_no] if not df_not.empty else df_not),
            file_name=f"no_facturados_informe_{_safe_name(label)}.csv",
            mime="text/csv",
            use_container_width=True,
            key="dl_inf_no_csv",
        )
    with col_xlsx:
        st.download_button(
            "⬇️ Reporte cruce Excel",
            data=_excel(sheets),
            file_name=f"cruce_informe_{_safe_name(label)}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key="dl_inf_xlsx",
        )