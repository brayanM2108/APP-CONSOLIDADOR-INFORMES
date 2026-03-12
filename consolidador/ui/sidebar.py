"""Sidebar: configuración de tipos, mapeo, historial e inspector."""

import streamlit as st
import pandas as pd

from ui.estado import LOGICAS, guardar_config
from core.procesador import leer_excel_con_duplicados, columnas_reales
from core.exportador import (
    cargar_parquet, cargar_todos_parquet, meses_disponibles_parquet,
)


def render_sidebar():
    with st.sidebar:
        st.markdown("## ⚙️ Configuración")
        _indicador_estado()
        st.divider()
        _form_nuevo_tipo()
        if st.session_state.config:
            st.divider()
            from ui.estado import CONFIG_PATH
            st.caption(f"💾 Config guardado en: `{CONFIG_PATH}`")
        st.divider()
        _carpeta_y_mapeo()
        st.divider()
        _historial()
        st.divider()
        _inspector()


def _indicador_estado():
    if st.session_state.config:
        st.success(f"✅ {len(st.session_state.config)} tipo(s) activos")
        st.markdown("**Tipos de base:**")
        for nombre in st.session_state.config:
            st.markdown(f'<span class="badge">{nombre}</span>', unsafe_allow_html=True)
    else:
        st.warning("⚠️ Sin tipos de base configurados")


def _form_nuevo_tipo():
    with st.expander("➕ Nuevo tipo de base", expanded=not bool(st.session_state.config)):
        with st.form("form_tipo", clear_on_submit=True):
            st.markdown("**Identificación**")
            nombre_tipo = st.text_input("Nombre *", placeholder="Convenio A - Laboratorio")
            st.caption("Usa 'Convenio - Tipo' para agrupar automáticamente")

            st.markdown("**Columnas del archivo**")
            col_pac = st.text_input("Documento paciente *")
            col_nom = st.text_input("Nombre paciente")
            col_cups = st.text_input("CUPS")
            col_serv = st.text_input("Descripción servicio")
            col_fech = st.text_input("Fecha atención")
            col_fper = st.text_input("Facturador asignado")
            col_obs = st.text_input("Observaciones")

            st.markdown("**Columna de facturación**")
            col_fact = st.text_input("Nombre de la columna *")
            logica_label = st.selectbox(
                "¿Qué indica que fue facturado?", list(LOGICAS.keys())
            )
            texto_exacto = ""
            if logica_label == "Texto exacto (ej: FAC, SI, RADICADO)":
                texto_exacto = st.text_input("¿Cuál es ese texto?")

            st.markdown("**Columnas adicionales** *(opcional)*")
            st.caption(
                "Una por línea. Para columnas duplicadas usa formato: nombre_en_excel → alias"
            )
            cols_extra_input = st.text_area(
                "Columnas extra",
                placeholder="Codigo IPS\nAutorizacion\nVALOR → valor_inicial\nVALOR.1 → valor_final",
                label_visibility="collapsed",
            )

            if st.form_submit_button("💾 Guardar", type="primary", use_container_width=True):
                if not nombre_tipo or not col_pac or not col_fact:
                    st.error("Nombre, doc. paciente y col. facturación son obligatorios.")
                else:
                    logica_valor = (
                        texto_exacto
                        if logica_label == "Texto exacto (ej: FAC, SI, RADICADO)"
                        else LOGICAS[logica_label]
                    )

                    columnas_extra = []
                    for linea in cols_extra_input.splitlines():
                        linea = linea.strip()
                        if not linea:
                            continue
                        if "→" in linea:
                            partes = linea.split("→")
                            columnas_extra.append(
                                {"col": partes[0].strip(), "alias": partes[1].strip()}
                            )
                        else:
                            columnas_extra.append(linea)

                    st.session_state.config[nombre_tipo] = {
                        "col_paciente": col_pac,
                        "col_nombre": col_nom or None,
                        "col_cups": col_cups or None,
                        "col_servicio": col_serv or None,
                        "col_fecha": col_fech or None,
                        "col_facturador": col_fper or None,
                        "col_observacion": col_obs or None,
                        "col_facturacion": col_fact,
                        "logica_facturacion": logica_valor,
                        "columnas_extra": columnas_extra,
                    }
                    guardar_config(st.session_state.config)
                    st.success(f"✅ '{nombre_tipo}' guardado en config/config.json")
                    st.rerun()


def _carpeta_y_mapeo():
    with st.expander("📁 Carpeta de datos y mapeo"):
        carpeta_datos = st.text_input(
            "Ruta de la carpeta raíz",
            value=st.session_state.get("carpeta_datos", ""),
            placeholder="C:/Users/tu_usuario/datos",
            key="input_carpeta_datos",
        )
        if carpeta_datos:
            st.session_state["carpeta_datos"] = carpeta_datos

        st.markdown("**Mapeo carpeta → tipo de base**")
        st.caption("Escribe: NOMBRE_CARPETA → Tipo en config (una por línea)")

        mapeo_actual = st.session_state.config.get("carpeta_tipo_base", {})
        mapeo_texto = "\n".join(f"{k} → {v}" for k, v in mapeo_actual.items())

        mapeo_input = st.text_area(
            "Mapeo",
            value=mapeo_texto,
            placeholder="CABEZOTE → CAPITALSALUD - Cabezote\nLABORATORIO → CAPITALSALUD - Laboratorio",
            label_visibility="collapsed",
        )

        if st.button("💾 Guardar mapeo", use_container_width=True):
            nuevo_mapeo = {}
            for linea in mapeo_input.splitlines():
                linea = linea.strip()
                if "→" in linea:
                    partes = linea.split("→")
                    nuevo_mapeo[partes[0].strip()] = partes[1].strip()
            st.session_state.config["carpeta_tipo_base"] = nuevo_mapeo
            guardar_config(st.session_state.config)
            st.success("✅ Mapeo guardado")
            st.rerun()


def _historial():
    with st.expander("📅 Cargar desde historial"):
        df_hist_all = cargar_todos_parquet()
        meses_disp = meses_disponibles_parquet()

        if df_hist_all is None or not meses_disp:
            st.caption("Aún no hay datos guardados.")
            return

        modo_carga = st.radio(
            "Cargar por", ["Mes", "Convenio"],
            horizontal=True, key="modo_carga_hist",
        )

        if modo_carga == "Mes":
            mes_sel_h = st.selectbox("Mes", meses_disp, key="hist_mes_sel")
            if st.button("📥 Cargar mes", use_container_width=True, key="btn_cargar_mes"):
                df_cargado = cargar_parquet(mes_sel_h.replace(" ", "_"))
                if df_cargado is not None:
                    st.session_state.df_resultado = df_cargado
                    st.session_state.mes_label = mes_sel_h.replace(" ", "_")
                    st.session_state.modo_reporte = "mes"
                    st.success(f"✅ {mes_sel_h} cargado")
                    st.rerun()
        else:
            convenios_disp = sorted(df_hist_all["nombre_convenio"].unique().tolist())
            conv_sel_h = st.selectbox(
                "Convenio", ["Todos"] + convenios_disp, key="hist_conv_sel"
            )
            if st.button("📥 Cargar convenio", use_container_width=True, key="btn_cargar_conv"):
                if conv_sel_h == "Todos":
                    df_conv_h = df_hist_all.copy()
                    label = "Todos los convenios"
                else:
                    df_conv_h = df_hist_all[
                        df_hist_all["nombre_convenio"] == conv_sel_h
                    ].copy()
                    label = conv_sel_h

                st.session_state.df_resultado = df_conv_h
                st.session_state.mes_label = label
                st.session_state.modo_reporte = "convenio"
                st.success(f"✅ {label} cargado ({len(df_conv_h):,} registros)")
                st.rerun()



def _inspector():
    with st.expander("🔍 Inspeccionar columnas de un archivo"):
        st.caption(
            "Sube un archivo para ver exactamente cómo pandas nombra sus columnas. "
            "Útil para identificar columnas duplicadas (VALOR, VALOR.1, VALOR.2...)"
        )
        f_inspect = st.file_uploader(
            "Archivo a inspeccionar", type=["xlsx", "xls"], key="inspector"
        )
        if f_inspect:
            try:
                df_inspect = leer_excel_con_duplicados(f_inspect)
                cols = columnas_reales(df_inspect)

                st.markdown(f"**{len(cols)} columnas encontradas:**")
                for col in cols:
                    es_dup = any(
                        col.startswith(f"{base}.")
                        for base in [c for c in cols if "." not in c]
                    )
                    if es_dup:
                        st.markdown(
                            f'`{col}` <span style="color:#d97706;font-size:0.78rem">'
                            f"⚠️ duplicada — usa alias</span>",
                            unsafe_allow_html=True,
                        )
                    else:
                        st.markdown(f"`{col}`")
            except Exception as e:
                st.error(f"Error al leer el archivo: {e}")
