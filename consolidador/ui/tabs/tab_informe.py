"""Tab 'Informe Facturación' — carga del informe y cruce con bases."""

import streamlit as st
import pandas as pd

from core.informe_facturacion import (
    leer_informe, kpis_informe, guardar_informe,
    cargar_informe, info_informe_guardado,
    COL_CONCAT_CUPS, COL_CONCAT_SERVICIO, COLS_CRUCE_RESULTADO,
)
from core.cruce_informe import (
    cruzar_bases_con_informe,
    kpis_cruce_informe,
    resumen_por_convenio,
    resumen_por_tipo_base,
    guardar_cruce_informe,
)
from core.exportador import (
    _excel, _csv, _nombre_seguro,
    meses_disponibles_parquet,
    cargar_parquet,
)


def render_tab_informe():

    # ── Sección 1: Cargar archivo ─────────────────────────────
    st.markdown("### 📑 Informe de Facturación")
    st.caption(
        "Sube el archivo Excel del informe. "
        "Se lee la primera hoja. "
        "Se guarda en Parquet separado del archivo de facturado."
    )

    info = info_informe_guardado()
    df_guardado = cargar_informe() if info else None

    if info:
        st.success(
            f"✅ Informe guardado · {info['fecha_guardado']} · "
            f"{info['total']:,} registros "
            f"({info['activas']:,} activos / {info['anuladas']:,} anulados)"
        )
    else:
        st.info("📭 No hay informe guardado aún.")

    st.divider()

    archivo = st.file_uploader(
        "Selecciona el archivo de informe",
        type=["xlsx", "xls"],
        key="uploader_informe",
    )

    df_nuevo = None
    if archivo:
        with st.spinner("Leyendo informe..."):
            df_nuevo, advertencias = leer_informe(archivo)

        if advertencias:
            with st.expander(f"⚠️ {len(advertencias)} advertencia(s)", expanded=True):
                for w in advertencias:
                    st.markdown(f"- {w}")

        if df_nuevo is None or df_nuevo.empty:
            st.error("❌ No se pudo leer el archivo.")
            return

    df_kpi = df_nuevo if df_nuevo is not None else df_guardado
    if df_kpi is None or df_kpi.empty:
        return

    # ── KPIs ─────────────────────────────────────────────────
    kpis = kpis_informe(df_kpi)
    st.subheader("📊 Vista previa" if df_nuevo is not None else "📊 Informe guardado")

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

    # ── Confirmar y guardar ───────────────────────────────────
    if df_nuevo is not None:
        st.divider()
        if info:
            st.warning(
                f"⚠️ Ya existe un informe guardado del {info['fecha_guardado']}. "
                "Al confirmar se **reemplazará**."
            )
        if st.button("✅ Confirmar y guardar", type="primary",
                     use_container_width=True, key="btn_guardar_informe"):
            with st.spinner("Guardando..."):
                try:
                    ruta = guardar_informe(df_nuevo)
                    st.success(f"✅ {len(df_nuevo):,} registros guardados en `{ruta}`.")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Error: {e}")

    # ── Sección 2: Cruce ──────────────────────────────────────
    st.divider()
    st.markdown("### 🔀 Cruzar con bases")

    if not info:
        st.info("📭 Primero guarda el informe.")
        return

    meses = meses_disponibles_parquet()
    if not meses:
        st.info("📭 No hay bases procesadas.")
        return

    modo_cruce = st.session_state.get("modo_cruce_informe", None)

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

    if modo_cruce is None:
        st.caption("Selecciona un modo de cruce.")
        return

    st.divider()

    if modo_cruce == "mes":
        st.markdown("#### 📅 Cruce por mes")
        st.caption("Cruza todas las bases del mes seleccionado.")

        mes_sel = st.selectbox("Mes", meses, key="inf_mes_sel")
        df_prev = cargar_parquet(mes_sel.replace(" ", "_"))
        if df_prev is not None:
            st.caption(f"📋 **{len(df_prev):,}** registros a cruzar")

        if st.button("🔀 Ejecutar cruce por mes", type="primary",
                     use_container_width=True, key="btn_inf_cruce_mes"):
            with st.spinner("Ejecutando cruce..."):
                try:
                    df_bases = cargar_parquet(mes_sel.replace(" ", "_"))
                    if df_bases is None or df_bases.empty:
                        st.error("❌ No se encontraron bases.")
                        return
                    df_inf = cargar_informe()
                    df_cruzado = cruzar_bases_con_informe(
                        df_bases, df_inf, st.session_state.config
                    )
                    st.session_state["df_cruce_informe_resultado"] = df_cruzado
                    st.session_state["label_cruce_informe"] = mes_sel
                    st.success(f"✅ {len(df_cruzado):,} registros procesados.")
                except Exception as e:
                    st.error(f"❌ Error: {e}")

    # ── Modo 2: Por base ──────────────────────────────────────
    else:
        st.markdown("#### 🗂️ Cruce por convenio y tipo de base")
        st.caption("Usa todos los meses de la base cargada en sesión.")

        df_sesion = st.session_state.get("df_resultado")
        if df_sesion is None or df_sesion.empty:
            st.warning("⚠️ No hay datos en sesión. Carga desde el historial primero.")
            return

        meses_sesion = sorted(df_sesion["mes"].unique().tolist()) if "mes" in df_sesion.columns else []
        st.caption(f"📅 Meses en sesión: **{', '.join(str(m) for m in meses_sesion)}**")

        convenios_disp = ["Todos"] + sorted(df_sesion["nombre_convenio"].unique().tolist())
        conv_sel = st.selectbox("Convenio", convenios_disp, key="inf_conv_sel")

        df_filt = df_sesion if conv_sel == "Todos" else df_sesion[df_sesion["nombre_convenio"] == conv_sel]

        tipos_disp = ["Todos"] + sorted(df_filt["tipo_base"].unique().tolist())
        tipo_sel = st.selectbox("Tipo de base", tipos_disp, key="inf_tipo_sel")

        df_filt2 = df_filt if tipo_sel == "Todos" else df_filt[df_filt["tipo_base"] == tipo_sel]
        st.caption(f"📋 **{len(df_filt2):,}** registros a cruzar")

        if st.button("🔀 Ejecutar cruce por base", type="primary",
                     use_container_width=True, key="btn_inf_cruce_base"):
            with st.spinner("Ejecutando cruce..."):
                try:
                    df_inf = cargar_informe()
                    df_cruzado = cruzar_bases_con_informe(
                        df_filt2.copy(), df_inf, st.session_state.config
                    )
                    label = tipo_sel if tipo_sel != "Todos" else conv_sel
                    st.session_state["df_cruce_informe_resultado"] = df_cruzado
                    st.session_state["label_cruce_informe"] = label
                    st.success(f"✅ {len(df_cruzado):,} registros procesados.")
                except Exception as e:
                    st.error(f"❌ Error: {e}")

    # ── Resultados ────────────────────────────────────────────
    df_cruce = st.session_state.get("df_cruce_informe_resultado")
    label    = st.session_state.get("label_cruce_informe", "")

    if df_cruce is None or "estado_cruce_informe" not in df_cruce.columns:
        return

    st.divider()
    st.subheader(f"📊 Resultados — {label}")

    kc = kpis_cruce_informe(df_cruce)
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total",           f"{kc['total']:,}")
    k2.metric("✅ Facturados",   f"{kc['facturados']:,}")
    k3.metric("❌ No facturados",f"{kc['no_facturado']:,}")
    k4.metric("Cumplimiento",    f"{kc['cumplimiento']}%")

    st.divider()

    st.subheader("🏥 Por convenio")
    rc = resumen_por_convenio(df_cruce)
    if not rc.empty:
        st.dataframe(rc.style.background_gradient(
            subset=["Cumplimiento (%)"], cmap="RdYlGn", vmin=0, vmax=100),
            use_container_width=True, hide_index=True)

    st.divider()

    st.subheader("🗂️ Por tipo de base")
    rt = resumen_por_tipo_base(df_cruce)
    if not rt.empty:
        st.dataframe(rt.style.background_gradient(
            subset=["Cumplimiento (%)"], cmap="RdYlGn", vmin=0, vmax=100),
            use_container_width=True, hide_index=True)

    st.divider()

    # Detalle no facturados
    df_no = df_cruce[df_cruce["estado_cruce_informe"] == "No facturado"]
    df_si = df_cruce[df_cruce["estado_cruce_informe"] == "Facturado"]

    st.subheader("⚠️ Detalle — No facturados")
    cols_no = [
        "nombre_convenio", "tipo_base", "documento_paciente", "nombre_paciente",
        "cups", "descripcion_servicio", "fecha_atencion",
        "facturador", "estado", "llave_cruce_informe", "archivo_origen", "mes", "año",
    ]
    cols_no = [c for c in cols_no if c in df_no.columns]
    if df_no.empty:
        st.success("🎉 Todos los registros cruzaron.")
    else:
        st.caption(f"{len(df_no):,} registros")
        st.dataframe(df_no[cols_no], use_container_width=True, hide_index=True)

    st.divider()

    # Guardar y descargar
    st.subheader("💾 Guardar y descargar")

    col_g, col_d = st.columns(2)

    with col_g:
        if st.button("💾 Guardar cruce en Parquet", type="primary",
                     use_container_width=True, key="btn_guardar_cruce_inf"):
            with st.spinner("Guardando..."):
                try:
                    ruta = guardar_cruce_informe(df_cruce, label)
                    st.session_state.pop("df_cruce_informe_resultado", None)
                    st.success(f"✅ Guardado en `{ruta}`.")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ {e}")

    # Excel descarga
    df_inf_cargado = cargar_informe()
    cols_base_si = [
        "nombre_convenio", "tipo_base", "documento_paciente", "nombre_paciente",
        "cups", "descripcion_servicio", "fecha_atencion",
        "facturador", "estado", "llave_cruce_informe", "archivo_origen",
    ]
    cols_base_si = [c for c in cols_base_si if c in df_si.columns]

    # Merge con informe para traer VALOR TOTAL, CUFE, facturador
    df_hoja_si = df_si[cols_base_si].copy()
    if df_inf_cargado is not None and not df_si.empty:
        from core.informe_facturacion import _col
        col_concat = None
        for c in df_inf_cargado.columns:
            if c.strip() == "concatenado doc_mes_ cups":
                col_concat = c
                break
        if col_concat:
            df_inf_merge = df_inf_cargado.copy()
            df_inf_merge["llave_cruce_informe"] = (
                df_inf_merge[col_concat].astype(str).str.strip().str.upper()
            )
            cols_traer = ["llave_cruce_informe"] + [
                c for c in COLS_CRUCE_RESULTADO if c in df_inf_merge.columns
            ]
            df_inf_merge = df_inf_merge[cols_traer].drop_duplicates("llave_cruce_informe")
            df_hoja_si = df_hoja_si.merge(df_inf_merge, on="llave_cruce_informe", how="left")

    hojas = {
        "Resumen Convenio":  resumen_por_convenio(df_cruce),
        "Resumen Tipo Base": resumen_por_tipo_base(df_cruce),
        "Facturados":        df_hoja_si,
        "No Facturados":     df_no[cols_no] if not df_no.empty else df_no,
    }

    col_csv, col_xlsx = st.columns(2)
    with col_csv:
        st.download_button(
            "⬇️ No facturados CSV",
            data=_csv(df_no[cols_no] if not df_no.empty else df_no),
            file_name=f"no_facturados_informe_{_nombre_seguro(label)}.csv",
            mime="text/csv",
            use_container_width=True,
            key="dl_inf_no_csv",
        )
    with col_xlsx:
        st.download_button(
            "⬇️ Reporte cruce Excel",
            data=_excel(hojas),
            file_name=f"cruce_informe_{_nombre_seguro(label)}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key="dl_inf_xlsx",
        )