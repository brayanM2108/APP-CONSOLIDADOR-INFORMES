"""Tab 'Facturado' — carga y almacenamiento del archivo de facturado."""
import pandas as pd
import streamlit as st
from core.procesador import COLUMNAS as COLS_STD
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
            width = "stretch",
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


    modo_cruce = st.session_state.get("modo_cruce", None)

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

    if modo_cruce is None:
        st.caption("Selecciona un modo de cruce.")
        return

    st.divider()

    # ── Modo 1: Por mes ───────────────────────────────────────
    if modo_cruce == "mes":
        st.markdown("#### 📅 Cruce por mes")
        st.caption("Cruza todas las bases del mes seleccionado.")

        mes_cruce = st.selectbox("Mes", meses, key="mes_cruce_sel")
        df_bases_prev = cargar_parquet(mes_cruce.replace(" ", "_"))
        if df_bases_prev is not None:
            st.caption(f"📋 **{len(df_bases_prev):,}** registros a cruzar")

        if st.button("🔀 Ejecutar cruce por mes", type="primary",
                     width = "stretch", key="btn_cruce_mes"):
            with st.spinner("Ejecutando cruce..."):
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
                    st.success(f"✅ Cruce completado — {len(df_cruzado):,} registros.")
                except Exception as e:
                    st.error(f"❌ Error: {e}")

        # ── Modo 2: Por base ──────────────────────────────────────
    else:
        st.markdown("#### 🗂️ Cruce por convenio y tipo de base")
        st.caption("Usa todos los meses de la base cargada en sesión.")

        df_bases_prev = st.session_state.get("df_resultado")

        if df_bases_prev is None or df_bases_prev.empty:
            st.warning("⚠️ No hay datos cargados en sesión. Carga un mes o convenio desde el historial primero.")
            return

        meses_en_sesion = sorted(df_bases_prev["mes"].unique().tolist()) if "mes" in df_bases_prev.columns else []
        st.caption(f"📅 Meses en sesión: **{', '.join(str(m) for m in meses_en_sesion)}**")

        conv_cruce = tipo_cruce = None

        convenios_disp = ["Todos"] + sorted(df_bases_prev["nombre_convenio"].unique().tolist())
        conv_cruce = st.selectbox("Convenio", convenios_disp, key="conv_cruce_sel")

        if conv_cruce != "Todos":
            df_filtrado = df_bases_prev[df_bases_prev["nombre_convenio"] == conv_cruce]
        else:
            df_filtrado = df_bases_prev

        tipos_disp = ["Todos"] + sorted(df_filtrado["tipo_base"].unique().tolist())
        tipo_cruce = st.selectbox("Tipo de base", tipos_disp, key="tipo_cruce_sel")

        if tipo_cruce != "Todos":
            n = len(df_filtrado[df_filtrado["tipo_base"] == tipo_cruce])
        else:
            n = len(df_filtrado)
        st.caption(f"📋 **{n:,}** registros a cruzar")

        if st.button("🔀 Ejecutar cruce por base", type="primary",
                     width = "stretch", key="btn_cruce_base"):
            with st.spinner("Ejecutando cruce..."):
                try:
                    df_bases = df_filtrado.copy()
                    if tipo_cruce != "Todos":
                        df_bases = df_bases[df_bases["tipo_base"] == tipo_cruce]

                    df_fact = cargar_facturado()
                    if df_fact is None:
                        st.error("❌ No se pudo cargar el facturado guardado.")
                        return

                    df_cruzado = cruzar_bases_con_facturado(
                        df_bases, df_fact, st.session_state.config
                    )
                    st.session_state["df_cruce_resultado"] = df_cruzado
                    st.session_state["mes_cruce_label"] = tipo_cruce if tipo_cruce != "Todos" else conv_cruce
                    st.success(f"✅ Cruce completado — {len(df_cruzado):,} registros.")
                except Exception as e:
                    st.error(f"❌ Error: {e}")

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
            width = "stretch", hide_index=True,
        )

    st.divider()

    st.subheader("🗂️ Por tipo de base")
    rt = resumen_cruce_por_tipo_base(df_cruce)
    if not rt.empty:
        st.dataframe(
            rt.style.background_gradient(
                subset=["Cumplimiento (%)"], cmap="RdYlGn", vmin=0, vmax=100
            ),
            width = "stretch", hide_index=True,
        )

    st.divider()

    st.subheader("⚠️ Detalle — No facturados")
    df_no_fact = df_cruce[df_cruce["estado_cruce"] == "No facturado"]
    if df_no_fact.empty:
        st.success("🎉 Todos los registros cruzaron con el facturado.")
    else:
        cols_base_no_fact = [
            "nombre_convenio", "tipo_base", "documento_paciente",
            "nombre_paciente", "cups", "descripcion_servicio",
            "fecha_atencion", "FECHA DE INICIO DEL SERVICIO",
            "facturador", "estado", "llave_cruce", "archivo_origen",
            "mes", "año",
        ]


        cols_det = [c for c in (cols_base_no_fact ) if c in df_no_fact.columns]
        st.caption(f"{len(df_no_fact):,} registros sin cruce")
        st.dataframe(df_no_fact[cols_base_no_fact], width = "stretch", hide_index=True)

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
            width = "stretch",
            key="dl_no_fact_csv",
        )

    with col_d2:
        with col_d2:
            # Hoja "Facturados" — cruce exitoso con columnas de ambos lados
            df_fact_cargado = cargar_facturado()
            df_facturados = df_cruce[df_cruce["estado_cruce"] == "Facturado"].copy()

            cols_base_fact = [
                "nombre_convenio", "tipo_base", "documento_paciente",
                "nombre_paciente", "cups", "descripcion_servicio",
                "FECHA DE INICIO DEL SERVICIO", "facturador", "observacion",
                "estado", "llave_cruce", "archivo_origen",
            ]
            cols_base_fact = [c for c in cols_base_fact if c in df_facturados.columns]

            # Traer columnas del facturado haciendo merge por llave
            COLS_FACT = [
                "FACTURA", "FECHA LEGALIZACION", "FECHA FACTURA", "CUFE",
                "TIPO IDENTIFICACIÓN", "IDENTIFICACION", "PACIENTE",
                "FECHA RADICADO", "RADICADO EXTERNO", "MES", "AÑO",
            ]

            if df_fact_cargado is not None and not df_facturados.empty:
                # Reconstruir llave en facturado para el merge
                from core.cruce import _construir_llave, LLAVE_FACTURADO_DEFAULT, COL_CUPS_FACTURADO
                df_fact_activo = df_fact_cargado[df_fact_cargado["_estado_factura"] == "Activo"].copy()
                cols_llave_fact = list(LLAVE_FACTURADO_DEFAULT)
                if COL_CUPS_FACTURADO in df_fact_activo.columns:
                    cols_llave_fact.append(COL_CUPS_FACTURADO)
                df_fact_activo["llave_cruce"] = _construir_llave(df_fact_activo, cols_llave_fact)

                cols_fact_disp = [c for c in COLS_FACT if c in df_fact_activo.columns]
                df_fact_merge = df_fact_activo[["llave_cruce"] + cols_fact_disp].drop_duplicates("llave_cruce")

                df_hoja_fact = (
                    df_facturados[cols_base_fact]
                    .merge(df_fact_merge, on="llave_cruce", how="left")
                )
            else:
                df_hoja_fact = df_facturados[cols_base_fact] if not df_facturados.empty else pd.DataFrame()

            hojas = {
                "Resumen Convenio": resumen_cruce_por_convenio(df_cruce),
                "Resumen Tipo Base": resumen_cruce_por_tipo_base(df_cruce),
                "Facturados": df_hoja_fact,
                "No Facturados": df_no_fact[cols_det] if not df_no_fact.empty else df_no_fact,
            }
            st.download_button(
                "⬇️ Reporte cruce Excel",
                data=_excel(hojas),
                file_name=f"cruce_{_nombre_seguro(mes_cruce_label)}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                width = "stretch",
                key="dl_cruce_xlsx",
            )