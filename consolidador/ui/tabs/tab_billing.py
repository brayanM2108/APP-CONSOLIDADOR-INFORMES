"""Tab 'Invoiced' — uploading and storing the invoice file."""

import pandas as pd
import streamlit as st
from core.billing import (
    read_billing,
    billing_kpis,
    guardar_facturado,
    info_facturado_guardado,
    cargar_facturado,
)
from core.cross_billing import (
    cross_bases_with_billed,
    crossing_kpis,
    crossing_summary_by_agreement,
    crossing_summary_by_base_type,
)

from core.exporter import (
    _excel, _csv, _safe_name,
    months_available_parquet,
    load_parquet
)

def render_tab_billing():
    st.markdown("### 📋 Archivo de facturado")
    st.caption(
        "Sube el archivo Excel de facturación. "
        "Se leerán las hojas 2 (Activo) y 3 (Anulado). "
        "La hoja 1 (tabla dinámica) se ignora automáticamente."
    )

    info_saved = info_facturado_guardado()
    df_saved = cargar_facturado() if info_saved else None

    if info_saved:
        st.success(
            f"✅ Archivo guardado · {info_saved['fecha_guardado']} · "
            f"{info_saved['total']:,} registros "
            f"({info_saved['activas']:,} activos / {info_saved['anuladas']:,} anulados)"
        )
    else:
        st.info("📭 No hay archivo de facturado guardado aún.")

    st.divider()

    uploaded_file = st.file_uploader(
        "Selecciona el archivo de facturado",
        type=["xlsx", "xls"],
        key="uploader_facturado",
    )

    df_new = None
    warnings = []

    if uploaded_file:
        with st.spinner("Leyendo hojas 2 y 3..."):
            df_new, warnings = read_billing(uploaded_file)

        if warnings:
            with st.expander(f"⚠️ {len(warnings)} advertencia(s)", expanded=True):
                for w in warnings:
                    st.markdown(f"- {w}")

        if df_new is None or df_new.empty:
            st.error("❌ No se pudo leer el archivo. Revisa las advertencias.")
            return

    df_kpi = df_new if df_new is not None else df_saved

    if df_kpi is None or df_kpi.empty:
        st.info("No hay datos para mostrar KPIs todavía.")
        return

    kpis = billing_kpis(df_kpi)

    st.subheader("📊 Vista previa" if df_new is not None else "📊 KPIs desde historial guardado")

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total facturas", f"{kpis['total']:,}")
    k2.metric("Activas", f"{kpis['activas']:,}")
    k3.metric("Anuladas", f"{kpis['anuladas']:,}")
    k4.metric("Valor total", f"${kpis['valor_total']:,.0f}")

    col_left, col_right = st.columns(2)
    with col_left:
        if kpis["fecha_min"]:
            st.caption(f"📅 Desde: **{kpis['fecha_min'].strftime('%d/%m/%Y')}**")
    with col_right:
        if kpis["fecha_max"]:
            st.caption(f"📅 Hasta: **{kpis['fecha_max'].strftime('%d/%m/%Y')}**")

    st.divider()

    if df_new is not None:
        if info_saved:
            st.warning(
                f"⚠️ Ya existe un archivo guardado del {info_saved['fecha_guardado']}. "
                "Al confirmar se **reemplazará** completamente."
            )

        if st.button(
                "✅ Confirmar y guardar",
                type="primary",
                width = "stretch",
                key="btn_guardar_facturado",
        ):
            with st.spinner("Guardando..."):
                try:
                    ruta = guardar_facturado(df_new)
                    st.session_state["df_facturado"] = df_new
                    st.success(f"✅ {len(df_new):,} registros guardados en `{ruta}`.")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Error al guardar: {e}")

    st.divider()
    st.markdown("### 🔀 Cruzar con bases")

    if not info_saved:
        st.info("📭 Primero debes cargar y guardar el archivo de facturado.")
        return

    months = months_available_parquet()
    if not months:
        st.info("📭 No hay bases procesadas. Procesa archivos primero.")
        return


    mode_cross = st.session_state.get("modo_cruce", None)

    col_m1, col_m2 = st.columns(2)
    with col_m1:
        if st.button("📅 Cruce por mes", width = "stretch", key="btn_modo_mes"):
            st.session_state["modo_cruce"] = "mes"
            st.session_state.pop("df_cruce_resultado", None)
            st.rerun()
    with col_m2:
        if st.button("🗂️ Cruce por base", width = "stretch", key="btn_modo_base"):
            st.session_state["modo_cruce"] = "base"
            st.session_state.pop("df_cruce_resultado", None)
            st.rerun()

    if mode_cross is None:
        st.caption("Selecciona un modo de cruce.")
        return

    st.divider()

    if mode_cross == "mes":
        st.markdown("#### 📅 Cruce por mes")
        st.caption("Cruza todas las bases del mes seleccionado.")

        cross_month = st.selectbox("Mes", months, key="mes_cruce_sel")
        df_bases_preview = load_parquet(cross_month.replace(" ", "_"))
        if df_bases_preview is not None:
            st.caption(f"📋 **{len(df_bases_preview):,}** registros a cruzar")

        if st.button("🔀 Ejecutar cruce por mes", type="primary",
                     width = "stretch", key="btn_cruce_mes"):
            with st.spinner("Ejecutando cruce..."):
                try:
                    df_bases = load_parquet(cross_month.replace(" ", "_"))
                    if df_bases is None or df_bases.empty:
                        st.error("❌ No se encontraron bases para el mes seleccionado.")
                        return
                    df_fact = cargar_facturado()
                    if df_fact is None:
                        st.error("❌ No se pudo cargar el facturado guardado.")
                        return
                    df_crossed = cross_bases_with_billed(
                        df_bases, df_fact, st.session_state.config
                    )
                    st.session_state["df_cruce_resultado"] = df_crossed
                    st.session_state["mes_cruce_label"] = cross_month
                    st.success(f"✅ Cruce completado — {len(df_crossed):,} registros.")
                except Exception as e:
                    st.error(f"❌ Error: {e}")

    else:
        st.markdown("#### 🗂️ Cruce por convenio y tipo de base")
        st.caption("Usa todos los meses de la base cargada en sesión.")

        df_bases_session = st.session_state.get("df_resultado")

        if df_bases_session is None or df_bases_session.empty:
            st.warning("⚠️ No hay datos cargados en sesión. Carga un mes o convenio desde el historial primero.")
            return

        months_in_session = sorted(df_bases_session["mes"].unique().tolist()) if "mes" in df_bases_session.columns else []
        st.caption(f"📅 Meses en sesión: **{', '.join(str(m) for m in months_in_session)}**")

        conv_selected = type_selected = None

        convenios_disp = ["Todos"] + sorted(df_bases_session["nombre_convenio"].unique().tolist())
        conv_selected = st.selectbox("Convenio", convenios_disp, key="conv_cruce_sel")

        if conv_selected != "Todos":
            df_filtered = df_bases_session[df_bases_session["nombre_convenio"] == conv_selected]
        else:
            df_filtered = df_bases_session

        tipos_disp = ["Todos"] + sorted(df_filtered["tipo_base"].unique().tolist())
        type_selected = st.selectbox("Tipo de base", tipos_disp, key="tipo_cruce_sel")

        if type_selected != "Todos":
            count_to_cross = len(df_filtered[df_filtered["tipo_base"] == type_selected])
        else:
            count_to_cross = len(df_filtered)
        st.caption(f"📋 **{count_to_cross:,}** registros a cruzar")

        if st.button("🔀 Ejecutar cruce por base", type="primary",
                     width = "stretch", key="btn_cruce_base"):
            with st.spinner("Ejecutando cruce..."):
                try:
                    df_bases_to_cross = df_filtered.copy()
                    if type_selected != "Todos":
                        df_bases_to_cross = df_bases_to_cross[df_bases_to_cross["tipo_base"] == type_selected]

                    df_fact = cargar_facturado()
                    if df_fact is None:
                        st.error("❌ No se pudo cargar el facturado guardado.")
                        return

                    df_crossed = cross_bases_with_billed(
                        df_bases_to_cross, df_fact, st.session_state.config
                    )
                    st.session_state["df_cruce_resultado"] = df_crossed
                    st.session_state["mes_cruce_label"] = type_selected if type_selected != "Todos" else conv_selected
                    st.success(f"✅ Cruce completado — {len(df_crossed):,} registros.")
                except Exception as e:
                    st.error(f"❌ Error: {e}")

    df_cross = st.session_state.get("df_cruce_resultado")
    cross_label = st.session_state.get("mes_cruce_label", "")

    if df_cross is None or "estado_cruce" not in df_cross.columns:
        return

    st.divider()
    st.subheader(f"📊 Resultados del cruce — {cross_label}")

    kc = crossing_kpis(df_cross)
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total registros", f"{kc['total']:,}")
    k2.metric("✅ Facturados", f"{kc['facturados']:,}")
    k3.metric("❌ No facturados", f"{kc['no_facturado']:,}")
    k4.metric("Cumplimiento", f"{kc['cumplimiento']}%")

    st.divider()

    st.subheader("🏥 Por convenio")
    rc = crossing_summary_by_agreement(df_cross)
    if not rc.empty:
        st.dataframe(
            rc.style.background_gradient(
                subset=["Cumplimiento (%)"], cmap="RdYlGn", vmin=0, vmax=100
            ),
            width = "stretch", hide_index=True,
        )

    st.divider()

    st.subheader("🗂️ Por tipo de base")
    rt = crossing_summary_by_base_type(df_cross)
    if not rt.empty:
        st.dataframe(
            rt.style.background_gradient(
                subset=["Cumplimiento (%)"], cmap="RdYlGn", vmin=0, vmax=100
            ),
            width = "stretch", hide_index=True,
        )

    st.divider()

    st.subheader("⚠️ Detalle — No facturados")
    df_no_fact = df_cross[df_cross["estado_cruce"] == "No facturado"]
    if df_no_fact.empty:
        st.success("🎉 Todos los registros cruzaron con el facturado.")
    else:
        cols_no_fact_base = [
            "nombre_convenio", "tipo_base", "documento_paciente",
            "nombre_paciente", "cups", "descripcion_servicio",
            "fecha_atencion", "FECHA DE INICIO DEL SERVICIO",
            "facturador", "estado", "llave_cruce", "archivo_origen",
            "mes", "año",
        ]


        cols_det = [c for c in (cols_no_fact_base ) if c in df_no_fact.columns]
        st.caption(f"{len(df_no_fact):,} registros sin cruce")
        st.dataframe(df_no_fact[cols_no_fact_base], width = "stretch", hide_index=True)

    st.divider()

    st.subheader("💾 Guardar resultado")
    st.caption(
        "Al guardar, el Parquet del mes se actualiza con la columna **estado_cruce**. "
        "Podrás verlo en el Reporte."
    )

    if st.button(
            "💾 Guardar cruce en Parquet",
            type="primary",
            width = "stretch",
            key="btn_guardar_cruce",
    ):
        with st.spinner("Guardando..."):
            try:
                from core.cross_billing import save_crossing
                ruta = save_crossing(df_cross, cross_label)
                st.session_state.pop("df_cruce_resultado", None)
                st.success(f"✅ Cruce guardado en `{ruta}`. Las bases originales no fueron modificadas.")
                st.rerun()
            except Exception as e:
                st.error(f"❌ Error al guardar: {e}")

    st.subheader("⬇️ Descargar cruce")


    col_d1, col_d2 = st.columns(2)

    with col_d1:
        st.download_button(
            "⬇️ No facturados CSV",
            data=_csv(df_no_fact[cols_det] if not df_no_fact.empty else df_no_fact),
            file_name=f"no_facturados_{_safe_name(cross_label)}.csv",
            mime="text/csv",
            width = "stretch",
            key="dl_no_fact_csv",
        )

    with col_d2:
        with col_d2:
            df_fact_loaded = cargar_facturado()
            df_billed = df_cross[df_cross["estado_cruce"] == "Facturado"].copy()

            cols_base_fact = [
                "nombre_convenio", "tipo_base", "documento_paciente",
                "nombre_paciente", "cups", "descripcion_servicio",
                "FECHA DE INICIO DEL SERVICIO", "facturador", "observacion",
                "estado", "llave_cruce", "archivo_origen",
            ]
            cols_base_fact = [c for c in cols_base_fact if c in df_billed.columns]

            COLS_FACT = [
                "FACTURA", "FECHA LEGALIZACION", "FECHA FACTURA", "CUFE",
                "TIPO IDENTIFICACIÓN", "IDENTIFICACION", "PACIENTE",
                "FECHA RADICADO", "RADICADO EXTERNO", "MES", "AÑO"
            ]

            if df_fact_loaded is not None and not df_billed.empty:
                from core.cross_billing import _construct_key, LLAVE_FACTURADO_DEFAULT, COL_CUPS_FACTURADO
                df_fact_active = df_fact_loaded[df_fact_loaded["_estado_factura"] == "Activo"].copy()
                cols_llave_fact = list(LLAVE_FACTURADO_DEFAULT)
                if COL_CUPS_FACTURADO in df_fact_active.columns:
                    cols_llave_fact.append(COL_CUPS_FACTURADO)
                df_fact_active["llave_cruce"] = _construct_key(df_fact_active, cols_llave_fact)

                cols_fact_disp = [c for c in COLS_FACT if c in df_fact_active.columns]
                df_fact_merge = df_fact_active[["llave_cruce"] + cols_fact_disp].drop_duplicates("llave_cruce")

                df_sheet_billed = (
                    df_billed[cols_base_fact]
                    .merge(df_fact_merge, on="llave_cruce", how="left")
                )
            else:
                df_sheet_billed = df_billed[cols_base_fact] if not df_billed.empty else pd.DataFrame()

            df_no_fact_export = df_no_fact.copy()

            df_fact_loaded = cargar_facturado()
            if df_fact_loaded is not None and not df_no_fact_export.empty:
                from core.cross_billing import _construct_key, LLAVE_FACTURADO_DEFAULT, COL_CUPS_FACTURADO

                df_fact_all = df_fact_loaded.copy()
                cols_llave_fact = list(LLAVE_FACTURADO_DEFAULT)
                if COL_CUPS_FACTURADO in df_fact_all.columns:
                    cols_llave_fact.append(COL_CUPS_FACTURADO)

                df_fact_all["llave_cruce"] = _construct_key(df_fact_all, cols_llave_fact)

                cols_fact_no_fact = [c for c in ["FACTURA", "_estado_factura"] if c in df_fact_all.columns]
                df_fact_merge_nf = df_fact_all[["llave_cruce"] + cols_fact_no_fact].drop_duplicates("llave_cruce")

                df_no_fact_export = df_no_fact_export.merge(df_fact_merge_nf, on="llave_cruce", how="left")

                cols_no_fact_final = cols_det + [c for c in ["FACTURA", "_estado_factura"] if
                                                 c in df_no_fact_export.columns]

            hojas = {
                "Resumen Convenio": crossing_summary_by_agreement(df_cross),
                "Resumen Tipo Base": crossing_summary_by_base_type(df_cross),
                "Facturados": df_sheet_billed,
                "No Facturados": df_no_fact_export[cols_no_fact_final] if not df_no_fact_export.empty else df_no_fact_export,
            }
            st.download_button(
                "⬇️ Reporte cruce Excel",
                data=_excel(hojas),
                file_name=f"cruce_{_safe_name(cross_label)}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                width = "stretch",
                key="dl_cruce_xlsx",
            )
