"""Sidebar: type settings, mapping, history, and inspector."""

import streamlit as st
from ui.state import LOGICAL, save_config
from core.processor import read_excel_with_duplicates, real_columns
from core.exporter import (
    load_parquet, load_all_parquet, months_available_parquet,
)


def render_sidebar():
    with st.sidebar:
        st.markdown("## ⚙️ Configuración")
        _state_indicator()
        st.divider()
        _form_new_type()
        if st.session_state.config:
            st.divider()
            from ui.state import CONFIG_PATH
            st.caption(f"💾 Config guardado en: `{CONFIG_PATH}`")
        st.divider()
        _file_and_map()
        st.divider()
        _history()
        st.divider()
        _inspector()


def _state_indicator():
    if st.session_state.config:
        st.success(f"✅ {len(st.session_state.config)} tipo(s) activos")
        st.markdown("**Tipos de base:**")
        for type_name in st.session_state.config:
            st.markdown(f'<span class="badge">{type_name}</span>', unsafe_allow_html=True)
    else:
        st.warning("⚠️ Sin tipos de base configurados")


def _form_new_type():
    with st.expander("➕ Nuevo tipo de base", expanded=not bool(st.session_state.config)):
        with st.form("form_tipo", clear_on_submit=True):
            st.markdown("**Identificación**")
            type_name = st.text_input("Nombre *", placeholder="Convenio A - Laboratorio")
            st.caption("Usa 'Convenio - Tipo' para agrupar automáticamente")

            st.markdown("**Columnas del archivo**")
            col_patient = st.text_input("Documento paciente *")
            col_name = st.text_input("Nombre paciente")
            col_cups = st.text_input("CUPS")
            col_service = st.text_input("Descripción servicio")
            col_date = st.text_input("Fecha atención")
            col_biller = st.text_input("Facturador asignado")
            col_observation = st.text_input("Observaciones")

            st.markdown("**Columna de facturación**")
            col_billing = st.text_input("Nombre de la columna *")
            logic_label = st.selectbox(
                "¿Qué indica que fue facturado?", list(LOGICAL.keys())
            )
            exact_text = ""
            if logic_label == "Texto exacto (ej: FAC, SI, RADICADO)":
                exact_text = st.text_input("¿Cuál es ese texto?")

            st.markdown("**Columnas adicionales** *(opcional)*")
            st.caption(
                "Una por línea. Para columnas duplicadas usa formato: nombre_en_excel → alias"
            )
            extra_cols_input = st.text_area(
                "Columnas extra",
                placeholder="Codigo IPS\nAutorizacion\nVALOR → valor_inicial\nVALOR.1 → valor_final",
                label_visibility="collapsed",
            )

            if st.form_submit_button("💾 Guardar", type="primary", width = "stretch"):
                if not type_name or not col_patient or not col_billing:
                    st.error("Nombre, doc. paciente y col. facturación son obligatorios.")
                else:
                    logic_value = (
                        exact_text
                        if logic_label == "Texto exacto (ej: FAC, SI, RADICADO)"
                        else LOGICAL[logic_label]
                    )

                    extra_columns = []
                    for line in extra_cols_input.splitlines():
                        line = line.strip()
                        if not line:
                            continue
                        if "→" in line:
                            parts = line.split("→")
                            extra_columns.append(
                                {"col": parts[0].strip(), "alias": parts[1].strip()}
                            )
                        else:
                            extra_columns.append(line)

                    st.session_state.config[type_name] = {
                        "col_paciente": col_patient,
                        "col_nombre": col_name or None,
                        "col_cups": col_cups or None,
                        "col_servicio": col_service or None,
                        "col_fecha": col_date or None,
                        "col_facturador": col_biller or None,
                        "col_observacion": col_observation or None,
                        "col_facturacion": col_billing,
                        "logica_facturacion": logic_value,
                        "columnas_extra": extra_columns,
                    }
                    save_config(st.session_state.config)
                    st.success(f"✅ '{type_name}' guardado en config/config.json")
                    st.rerun()


def _file_and_map():
    with st.expander("📁 Carpeta de datos y mapeo"):
        data_folder = st.text_input(
            "Ruta de la carpeta raíz",
            value=st.session_state.get("carpeta_datos", ""),
            placeholder="C:/Users/tu_usuario/datos",
            key="input_carpeta_datos",
        )
        if data_folder:
            st.session_state["carpeta_datos"] = data_folder

        st.markdown("**Mapeo carpeta → tipo de base**")
        st.caption("Escribe: NOMBRE_CARPETA → Tipo en config (una por línea)")

        current_map = st.session_state.config.get("carpeta_tipo_base", {})
        map_text = "\n".join(f"{k} → {v}" for k, v in current_map.items())

        map_input = st.text_area(
            "Mapeo",
            value=map_text,
            placeholder="CABEZOTE → CAPITALSALUD - Cabezote\nLABORATORIO → CAPITALSALUD - Laboratorio",
            label_visibility="collapsed",
        )

        if st.button("💾 Guardar mapeo", width = "stretch"):
            new_map = {}
            for line in map_input.splitlines():
                line = line.strip()
                if "→" in line:
                    parts = line.split("→")
                    new_map[parts[0].strip()] = parts[1].strip()
            st.session_state.config["carpeta_tipo_base"] = new_map
            save_config(st.session_state.config)
            st.success("✅ Mapeo guardado")
            st.rerun()


def _history():
    with st.expander("📅 Cargar desde historial"):
        df_hist_all = load_all_parquet()
        months_available = months_available_parquet()

        if df_hist_all is None or not months_available:
            st.caption("Aún no hay datos guardados.")
            return

        load_mode = st.radio(
            "Cargar por", ["Mes", "Convenio"],
            horizontal=True, key="modo_carga_hist",
        )

        if load_mode == "Mes":
            sel_month_hist = st.selectbox("Mes", months_available, key="hist_mes_sel")
            if st.button("📥 Cargar mes", width = "stretch", key="btn_cargar_mes"):
                df_loaded = load_parquet(sel_month_hist.replace(" ", "_"))
                if df_loaded is not None:
                    st.session_state.df_resultado = df_loaded
                    st.session_state.mes_label = sel_month_hist.replace(" ", "_")
                    st.session_state.modo_reporte = "mes"
                    st.success(f"✅ {sel_month_hist} cargado")
                    st.rerun()
        else:
            agreements_available = sorted(df_hist_all["nombre_convenio"].unique().tolist())
            sel_conv_hist = st.selectbox(
                "Convenio", ["Todos"] + agreements_available, key="hist_conv_sel"
            )
            if st.button("📥 Cargar convenio", width = "stretch", key="btn_cargar_conv"):
                if sel_conv_hist == "Todos":
                    df_conv_hist = df_hist_all.copy()
                    label = "Todos los convenios"
                else:
                    df_conv_hist = df_hist_all[
                        df_hist_all["nombre_convenio"] == sel_conv_hist
                        ].copy()
                    label = sel_conv_hist

                st.session_state.df_resultado = df_conv_hist
                st.session_state.mes_label = label
                st.session_state.modo_reporte = "convenio"
                st.success(f"✅ {label} cargado ({len(df_conv_hist):,} registros)")
                st.rerun()



def _inspector():
    with st.expander("🔍 Inspeccionar columnas de un archivo"):
        st.caption(
            "Sube un archivo para ver exactamente cómo pandas nombra sus columnas. "
            "Útil para identificar columnas duplicadas (VALOR, VALOR.1, VALOR.2...)"
        )
        inspect_file = st.file_uploader(
            "Archivo a inspeccionar", type=["xlsx", "xls"], key="inspector"
        )
        if inspect_file:
            try:
                df_inspect = read_excel_with_duplicates(inspect_file)
                cols = real_columns(df_inspect)

                st.markdown(f"**{len(cols)} columnas encontradas:**")
                for col in cols:
                    is_dup = any(
                        col.startswith(f"{base}.")
                        for base in [c for c in cols if "." not in c]
                    )
                    if is_dup:
                        st.markdown(
                            f'`{col}` <span style="color:#d97706;font-size:0.78rem">'
                            f"⚠️ duplicada — usa alias</span>",
                            unsafe_allow_html=True,
                        )
                    else:
                        st.markdown(f"`{col}`")
            except Exception as e:
                st.error(f"Error al leer el archivo: {e}")
