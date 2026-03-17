"""Tab 'Cargar manual' — subida de archivos con asignación de tipo."""

import re
from pathlib import Path
import streamlit as st
import pandas as pd
from datetime import datetime
from ui.estado import MESES
from core.procesador import procesar_base
from core.exportador import guardar_parquet


def render_tab_cargar():
    st.subheader("Período")
    c1, c2 = st.columns(2)
    with c1:
        mes_sel = st.selectbox(
            "Mes", list(MESES.keys()),
            index=datetime.now().month - 1,
            format_func=lambda x: MESES[x],
        )
    with c2:
        año_sel = st.number_input(
            "Año", min_value=2020, max_value=2035,
            value=datetime.now().year, step=1,
        )

    st.divider()
    st.subheader("Archivos del mes")
    st.caption("Sube los archivos y asigna el tipo de base a cada uno.")

    archivos = st.file_uploader(
        "Selecciona archivos Excel",
        type=["xlsx", "xls"],
        accept_multiple_files=True,
    )

    tipos_asignados = _asignar_tipos(archivos)

    st.divider()
    _verificar_y_procesar(archivos, tipos_asignados, mes_sel, int(año_sel))


def _asignar_tipos(archivos):
    tipos_asignados = {}
    if not archivos:
        return tipos_asignados

    tipos_disponibles = ["— Selecciona —"] + list(st.session_state.config.keys())
    for archivo in archivos:
        ca, cb = st.columns([3, 4])
        with ca:
            st.markdown(f"📄 `{archivo.name}`")
        with cb:
            tipo = st.selectbox(
                "tipo", tipos_disponibles,
                key=f"tipo_{archivo.name}",
                label_visibility="collapsed",
            )
            if tipo != "— Selecciona —":
                tipos_asignados[archivo.name] = tipo

    sin_asignar = len(archivos) - len(tipos_asignados)
    if sin_asignar > 0:
        st.warning(f"⚠️ {sin_asignar} archivo(s) sin tipo asignado.")
    else:
        st.success("✅ Todos los archivos tienen tipo asignado.")

    return tipos_asignados

def _periodo_desde_nombre(nombre_archivo: str) -> tuple[str | None, int | None]:
    """
    Extrae mes/año desde nombres tipo: BASE_NOMBREEPS_MES_AÑO.xlsx
    Ej: BASE_CAPITALSALUD_MARZO_2025.xlsx -> ("03", 2025)

    Retorna (None, None) si no detecta.
    """
    stem = Path(nombre_archivo).stem.upper().replace("-", "_")
    tokens = [t.strip() for t in stem.split("_") if t.strip()]

    # {"ENERO": "01", ..., "DICIEMBRE": "12"}
    meses_txt_a_num = {v.upper(): k for k, v in MESES.items()}

    mes = None
    for t in tokens:
        if t in meses_txt_a_num:
            mes = meses_txt_a_num[t]
            break

    ano = None
    for t in reversed(tokens):
        if re.fullmatch(r"20\d{2}", t):
            ano = int(t)
            break

    return mes, ano


def _verificar_y_procesar(archivos, tipos_asignados, mes_sel, año_sel):
    puede_procesar = bool(archivos) and len(tipos_asignados) > 0
    col_v, col_p = st.columns(2)

    with col_v:
        if st.button(
            "🔍 Verificar", disabled=not puede_procesar,
            use_container_width=True, key="verificar_m",
        ):
            dfs_prev, adverts, errores = [], [], []
            archivos_ok = [a for a in archivos if a.name in tipos_asignados]
            progress = st.progress(0)

            for i, archivo in enumerate(archivos_ok):
                tipo_base = tipos_asignados[archivo.name]
                config_base = st.session_state.config[tipo_base]
                try:
                    archivo.seek(0)
                    df_raw = pd.read_excel(archivo)

                    # Detectar periodo por nombre de archivo; fallback al selector
                    mes_arch, año_arch = _periodo_desde_nombre(archivo.name)
                    mes_final = mes_arch or mes_sel
                    año_final = int(año_arch or año_sel)

                    df_proc, warns = procesar_base(
                        df_raw, config_base,
                        archivo.name, tipo_base, mes_final, año_final,
                    )
                    dfs_prev.append(df_proc)

                    if mes_arch is None or año_arch is None:
                        warns.append(
                            f"'{archivo.name}': no se detectó mes/año completo en el nombre; "
                            f"se usó período seleccionado ({MESES[mes_sel]} {año_sel})."
                        )

                    adverts.extend(warns)
                except Exception as e:
                    errores.append(f"**{archivo.name}**: {e}")
                progress.progress((i + 1) / len(archivos_ok))

            progress.empty()
            st.session_state["preview_m"] = dfs_prev
            st.session_state["advertencias_m"] = adverts
            st.session_state["errores_m"] = errores

    with col_p:
        hay_preview = bool(st.session_state.get("preview_m"))
        if st.button(
            "▶️ Procesar y guardar", type="primary",
            disabled=not hay_preview,
            use_container_width=True, key="procesar_m",
        ):
            dfs = st.session_state.pop("preview_m")
            df_nuevos = pd.concat(dfs, ignore_index=True)

            # Actualiza sesión con todo lo procesado
            if st.session_state.df_resultado is not None:
                st.session_state.df_resultado = pd.concat(
                    [st.session_state.df_resultado, df_nuevos], ignore_index=True
                )
            else:
                st.session_state.df_resultado = df_nuevos

            # Guardar por cada (año, mes) detectado
            rutas_guardadas = []
            errores_guardado = []
            for (año_g, mes_g), df_mes in df_nuevos.groupby(["año", "mes"], dropna=False):
                try:
                    mes_norm = str(mes_g).zfill(2)
                    mes_txt = MESES.get(mes_norm, mes_norm)
                    mes_label = f"{mes_txt}_{int(año_g)}"
                    ruta = guardar_parquet(df_mes, mes_label)
                    rutas_guardadas.append(str(ruta))
                except Exception as e:
                    errores_guardado.append(f"{mes_g}-{año_g}: {e}")

            # Modo convenio para análisis multi-mes
            st.session_state.mes_label = "Carga_manual_multi_mes"
            st.session_state.modo_reporte = "convenio"

            total = len(st.session_state.df_resultado)
            if rutas_guardadas:
                st.success(
                    f"✅ {total:,} registros procesados. "
                    f"Guardados {len(rutas_guardadas)} período(s) en Parquet. "
                    "Ve a **📊 Reporte**."
                )
            if errores_guardado:
                st.warning("⚠️ Algunos períodos no se pudieron guardar:")
                for e in errores_guardado:
                    st.markdown(f"- {e}")

    # Resultado de verificación
    if st.session_state.get("advertencias_m"):
        with st.expander(f"⚠️ {len(st.session_state['advertencias_m'])} advertencia(s)"):
            for w in st.session_state["advertencias_m"]:
                st.markdown(f"- {w}")
    elif st.session_state.get("preview_m") is not None:
        st.success("✅ Sin advertencias. Puedes procesar y guardar.")
    if st.session_state.get("errores_m"):
        with st.expander(f"❌ {len(st.session_state['errores_m'])} error(es)"):
            for e in st.session_state["errores_m"]:
                st.markdown(f"- {e}")
