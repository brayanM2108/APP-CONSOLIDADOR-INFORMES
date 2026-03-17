"""Tab 'Facturado' — carga y almacenamiento del archivo de facturado."""

import streamlit as st
from core.facturado import (
    leer_facturado,
    kpis_facturado,
    guardar_facturado,
    info_facturado_guardado,
    cargar_facturado,
)
from core.cruce import (
    cruzar_bases_con_facturado,
    kpis_cruce,
    resumen_cruce_por_convenio,
    resumen_cruce_por_tipo_base,
)

from core.exportador import (
    _excel, _csv, _nombre_seguro,
    meses_disponibles_parquet,
    cargar_parquet
)

def render_tab_facturado():
    st.markdown("### 📋 Archivo de facturado")
    st.caption(
        "Sube el archivo Excel de facturación. "
        "Se leerán las hojas 2 (Activo) y 3 (Anulado). "
        "La hoja 1 (tabla dinámica) se ignora automáticamente."
    )

    # ── Estado del archivo guardado ──────────────────────────
    info = info_facturado_guardado()
    df_guardado = cargar_facturado() if info else None

    if info:
        st.success(
            f"✅ Archivo guardado · {info['fecha_guardado']} · "
            f"{info['total']:,} registros "
            f"({info['activas']:,} activos / {info['anuladas']:,} anulados)"
        )
    else:
        st.info("📭 No hay archivo de facturado guardado aún.")

    st.divider()

    # ── Uploader ─────────────────────────────────────────────
    archivo = st.file_uploader(
        "Selecciona el archivo de facturado",
        type=["xlsx", "xls"],
        key="uploader_facturado",
    )

    df_nuevo = None
    advertencias = []

    # Si hay archivo subido, leer preview desde ese archivo
    if archivo:
        with st.spinner("Leyendo hojas 2 y 3..."):
            df_nuevo, advertencias = leer_facturado(archivo)

        if advertencias:
            with st.expander(f"⚠️ {len(advertencias)} advertencia(s)", expanded=True):
                for w in advertencias:
                    st.markdown(f"- {w}")

        if df_nuevo is None or df_nuevo.empty:
            st.error("❌ No se pudo leer el archivo. Revisa las advertencias.")
            return

    # Fuente de KPIs: archivo nuevo (si existe) o parquet guardado
    df_kpi = df_nuevo if df_nuevo is not None else df_guardado

    if df_kpi is None or df_kpi.empty:
        st.info("No hay datos para mostrar KPIs todavía.")
        return

    # ── KPIs preview / historial ─────────────────────────────
    kpis = kpis_facturado(df_kpi)

    st.subheader("📊 Vista previa" if df_nuevo is not None else "📊 KPIs desde historial guardado")

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total facturas", f"{kpis['total']:,}")
    k2.metric("Activas", f"{kpis['activas']:,}")
    k3.metric("Anuladas", f"{kpis['anuladas']:,}")
    k4.metric("Valor total", f"${kpis['valor_total']:,.0f}")

    col_f1, col_f2 = st.columns(2)
    with col_f1:
        if kpis["fecha_min"]:
            st.caption(f"📅 Desde: **{kpis['fecha_min'].strftime('%d/%m/%Y')}**")
    with col_f2:
        if kpis["fecha_max"]:
            st.caption(f"📅 Hasta: **{kpis['fecha_max'].strftime('%d/%m/%Y')}**")

    st.divider()

    # ── Confirmación y guardado (solo si hay archivo nuevo) ──
    if df_nuevo is not None:
        if info:
            st.warning(
                f"⚠️ Ya existe un archivo guardado del {info['fecha_guardado']}. "
                "Al confirmar se **reemplazará** completamente."
            )

        if st.button(
            "✅ Confirmar y guardar",
            type="primary",
            use_container_width=True,
            key="btn_guardar_facturado",
        ):
            with st.spinner("Guardando..."):
                try:
                    ruta = guardar_facturado(df_nuevo)
                    st.session_state["df_facturado"] = df_nuevo
                    st.success(f"✅ {len(df_nuevo):,} registros guardados en `{ruta}`.")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Error al guardar: {e}")

    # ── Sección 2: Cruce con bases ────────────────────────────
    st.divider()
    st.markdown("### 🔀 Cruzar con bases")

    if not info:
        st.info("📭 Primero debes cargar y guardar el archivo de facturado.")
        return

    meses = meses_disponibles_parquet()
    if not meses:
        st.info("📭 No hay bases procesadas. Procesa archivos primero.")
        return

    mes_cruce = st.selectbox(
        "Selecciona el mes a cruzar", meses, key="mes_cruce_sel"
    )

    st.caption(
        "El cruce agrega la columna **estado_cruce** a las bases del mes seleccionado. "
        "No modifica el estado original."
    )

    if st.button(
            "🔀 Ejecutar cruce",
            type="primary",
            use_container_width=True,
            key="btn_ejecutar_cruce",
    ):
        with st.spinner("Cargando datos y ejecutando cruce..."):
            try:
                df_bases = cargar_parquet(mes_cruce.replace(" ", "_"))
                if df_bases is None or df_bases.empty:
                    st.error("❌ No se encontraron bases para el mes seleccionado.")
                    return

                df_fact = cargar_facturado()
                if df_fact is None:
                    st.error("❌ No se pudo cargar el facturado guardado.")
                    return

                df_cruzado = cruzar_bases_con_facturado(
                    df_bases, df_fact, st.session_state.config
                )
                st.session_state["df_cruce_resultado"] = df_cruzado
                st.session_state["mes_cruce_label"] = mes_cruce
                st.success(f"✅ Cruce completado — {len(df_cruzado):,} registros procesados.")

            except Exception as e:
                st.error(f"❌ Error en el cruce: {e}")

    # ── Resultados del cruce ──────────────────────────────────
    df_cruce = st.session_state.get("df_cruce_resultado")
    mes_cruce_label = st.session_state.get("mes_cruce_label", "")

    if df_cruce is None or "estado_cruce" not in df_cruce.columns:
        return

    st.divider()
    st.subheader(f"📊 Resultados del cruce — {mes_cruce_label}")

    kc = kpis_cruce(df_cruce)
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total registros", f"{kc['total']:,}")
    k2.metric("✅ Facturados", f"{kc['facturados']:,}")
    k3.metric("❌ No facturados", f"{kc['no_facturado']:,}")
    k4.metric("Cumplimiento", f"{kc['cumplimiento']}%")

    st.divider()

    st.subheader("🏥 Por convenio")
    rc = resumen_cruce_por_convenio(df_cruce)
    if not rc.empty:
        st.dataframe(
            rc.style.background_gradient(
                subset=["Cumplimiento (%)"], cmap="RdYlGn", vmin=0, vmax=100
            ),
            use_container_width=True, hide_index=True,
        )

    st.divider()

    st.subheader("🗂️ Por tipo de base")
    rt = resumen_cruce_por_tipo_base(df_cruce)
    if not rt.empty:
        st.dataframe(
            rt.style.background_gradient(
                subset=["Cumplimiento (%)"], cmap="RdYlGn", vmin=0, vmax=100
            ),
            use_container_width=True, hide_index=True,
        )

    st.divider()

    st.subheader("⚠️ Detalle — No facturados")
    df_no_fact = df_cruce[df_cruce["estado_cruce"] == "No facturado"]
    if df_no_fact.empty:
        st.success("🎉 Todos los registros cruzaron con el facturado.")
    else:
        cols_det = [
            "nombre_convenio", "tipo_base", "documento_paciente",
            "nombre_paciente", "cups", "descripcion_servicio",
            "fecha_atencion", "facturador", "estado", "archivo_origen",
        ]
        cols_det = [c for c in cols_det if c in df_no_fact.columns]
        st.caption(f"{len(df_no_fact):,} registros sin cruce")
        st.dataframe(df_no_fact[cols_det], use_container_width=True, hide_index=True)

    st.divider()

    st.subheader("💾 Guardar resultado")
    st.caption(
        "Al guardar, el Parquet del mes se actualiza con la columna **estado_cruce**. "
        "Podrás verlo en el Reporte."
    )

    if st.button(
            "💾 Guardar cruce en Parquet",
            type="primary",
            use_container_width=True,
            key="btn_guardar_cruce",
    ):
        with st.spinner("Guardando..."):
            try:
                from core.cruce import guardar_cruce
                ruta = guardar_cruce(df_cruce, mes_cruce_label)
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
            file_name=f"no_facturados_{_nombre_seguro(mes_cruce_label)}.csv",
            mime="text/csv",
            use_container_width=True,
            key="dl_no_fact_csv",
        )

    with col_d2:
        # Excel con 3 hojas
        hojas = {
            "Resumen Convenio":   resumen_cruce_por_convenio(df_cruce),
            "Resumen Tipo Base":  resumen_cruce_por_tipo_base(df_cruce),
            "No Facturados":      df_no_fact[cols_det] if not df_no_fact.empty else df_no_fact,
        }
        st.download_button(
            "⬇️ Reporte cruce Excel",
            data=_excel(hojas),
            file_name=f"cruce_{_nombre_seguro(mes_cruce_label)}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key="dl_cruce_xlsx",
        )