"""Tab 'Manual Upload' — file upload with type assignment."""

import re
from pathlib import Path
import streamlit as st
import pandas as pd
from datetime import datetime
from ui.state import MONTHS
from core.processor import procces_base
from core.exporter import save_parquet
from core.watcher import mark_processed

def render_tab_load():
    st.subheader("Período")
    c1, c2 = st.columns(2)
    with c1:
        selected_month = st.selectbox(
            "Mes", list(MONTHS.keys()),
            index=datetime.now().month - 1,
            format_func=lambda x: MONTHS[x],
        )
    with c2:
        selected_year = st.number_input(
            "Año", min_value=2020, max_value=2035,
            value=datetime.now().year, step=1,
        )

    st.divider()
    st.subheader("Archivos del mes")
    st.caption("Sube los archivos y asigna el tipo de base a cada uno.")

    files = st.file_uploader(
        "Selecciona archivos Excel",
        type=["xlsx", "xls"],
        accept_multiple_files=True,
    )

    assigned_types = _assign_types(files)

    st.divider()
    _verify_and_process(files, assigned_types, selected_month, int(selected_year))


def _assign_types(files):
    assigned_types = {}
    if not files:
        return assigned_types

    available_types = ["— Selecciona —"] + list(st.session_state.config.keys())
    for file in files:
        ca, cb = st.columns([3, 4])
        with ca:
            st.markdown(f"📄 `{file.name}`")
        with cb:
            tipo = st.selectbox(
                "tipo", available_types,
                key=f"tipo_{file.name}",
                label_visibility="collapsed",
            )
            if tipo != "— Selecciona —":
                assigned_types[file.name] = tipo

    unassigned_count = len(files) - len(assigned_types)
    if unassigned_count > 0:
        st.warning(f"⚠️ {unassigned_count} archivo(s) sin tipo asignado.")
    else:
        st.success("✅ Todos los archivos tienen tipo asignado.")

    return assigned_types

def _period_from_name(filename: str) -> tuple[str | None, int | None]:
        """
        Extracts month/year from filenames like: BASE_NOMBREEPS_MES_AÑO.xlsx
        Example: BASE_CAPITALSALUD_MARZO_2025.xlsx -> ("03", 2025)
        Returns (None, None) if not detected.
        """
        stem = Path(filename).stem.upper().replace("-", "_")
        tokens = [t.strip() for t in stem.split("_") if t.strip()]

        month_text_to_num = {v.upper(): k for k, v in MONTHS.items()}

        month = None
        for t in tokens:
            if t in month_text_to_num:
                month = month_text_to_num[t]
                break

        year = None
        for t in reversed(tokens):
            if re.fullmatch(r"20\d{2}", t):
                year = int(t)
                break

        return month, year

def _rout_virtual_manual(filename: str, base_type: str, month: str, year: int) -> str:
    """
    Creates a stable virtual path to record files uploaded by file_uploader.
    It does not depend on the user's local path (Streamlit does not expose it).
    """
    safe_nombre = re.sub(r"[^A-Za-z0-9._-]+", "_", Path(filename).name)
    safe_tipo = re.sub(r"[^A-Za-z0-9._-]+", "_", str(base_type))
    return str(Path("manual_uploads") / str(year) / str(month).zfill(2) / safe_tipo / safe_nombre)

def _solve_actual_route_manual(file_name: str, base_type: str, year: int) -> str | None:
    """
    Attempts to find the actual file path within the configured root folder.
     Returns normalized absolute path if a unique match is found.
    """
    root_folder = st.session_state.get("carpeta_datos", "")
    if not root_folder:
        return None

    root = Path(root_folder)
    if not root.exists():
        return None

    candidates = []
    for p in root.rglob(file_name):
        if p.is_file():
                candidates.append(p)

    if not candidates:
        return None

    candidates_year = [p for p in candidates if str(year) in p.parts]
    if len(candidates_year) == 1:
        return str(candidates_year[0].resolve())

    mapping = st.session_state.config.get("carpeta_tipo_base", {})
    type_folders = [k for k, v in mapping.items() if str(v).strip() == str(base_type).strip()]
    if type_folders:
        candidates_type = [p for p in candidates if p.parent.name in type_folders]
        if len(candidates_type) == 1:
            return str(candidates_type[0].resolve())
    if len(candidates) == 1:
            return str(candidates[0].resolve())

    return None

def _verify_and_process(files, assigned_types, selected_month, selected_year):
        can_process = bool(files) and len(assigned_types) > 0
        col_verify, col_process = st.columns(2)

        with col_verify:
            if st.button(
            "🔍 Verificar", disabled=not can_process,
                width = "stretch", key="verificar_m",
            ):
                dfs_prev, adverts, errors = [], [], []
                selected_items_manual = []
                files_ok = [a for a in files if a.name in assigned_types]
                progress = st.progress(0)

                for i, archivo in enumerate(files_ok):
                    base_type = assigned_types[archivo.name]
                    base_config = st.session_state.config[base_type]
                    try:
                        archivo.seek(0)
                        df_raw = pd.read_excel(archivo)

                        month_from_name, año_arch = _period_from_name(archivo.name)
                        final_month = month_from_name or selected_month
                        final_year = int(año_arch or selected_year)

                        df_proc, warns = procces_base(
                            df_raw, base_config,
                            archivo.name, base_type, final_month, final_year,
                    )
                        dfs_prev.append(df_proc)

                        selected_items_manual.append({
                            "nombre_archivo": archivo.name,
                            "tipo_base": base_type,
                            "mes": final_month,
                            "año": final_year,
                        })

                        if month_from_name is None or año_arch is None:
                            warns.append(
                                f"'{archivo.name}': no se detectó mes/año completo en el nombre; "
                                f"se usó período seleccionado ({MONTHS[selected_month]} {selected_year})."
                            )

                        adverts.extend(warns)
                    except Exception as e:
                        errors.append(f"**{archivo.name}**: {e}")
                    progress.progress((i + 1) / len(files_ok))

                progress.empty()
                st.session_state["preview_m"] = dfs_prev
                st.session_state["advertencias_m"] = adverts
                st.session_state["errores_m"] = errors
                st.session_state["seleccionados_m"] = selected_items_manual  # <-- NUEVO

        with col_process:
            has_preview = bool(st.session_state.get("preview_m"))
            if st.button(
                "▶️ Procesar y guardar", type="primary",
                disabled=not has_preview,
                width = "stretch", key="procesar_m",
            ):
                dfs = st.session_state.pop("preview_m")

                selected_items_manual = st.session_state.pop("seleccionados_m", [])
                for item in selected_items_manual:
                    ruta_real = _solve_actual_route_manual(
                        item["nombre_archivo"],
                        item["tipo_base"],
                        item["año"],
                    )

                    ruta_registro = ruta_real or _rout_virtual_manual(
                        item["nombre_archivo"],
                        item["tipo_base"],
                        item["mes"],
                        item["año"],
                    )

                    mark_processed(ruta_registro, item["tipo_base"])

                df_nuevos = pd.concat(dfs, ignore_index=True)

                if st.session_state.df_resultado is not None:
                    st.session_state.df_resultado = pd.concat(
                        [st.session_state.df_resultado, df_nuevos], ignore_index=True
                    )
                else:
                    st.session_state.df_resultado = df_nuevos

                saved_paths = []
                save_errors = []
                for (año_g, mes_g), df_mes in df_nuevos.groupby(["año", "mes"], dropna=False):
                    try:
                        mes_norm = str(mes_g).zfill(2)
                        mes_txt = MONTHS.get(mes_norm, mes_norm)
                        mes_label = f"{mes_txt}_{int(año_g)}"
                        ruta = save_parquet(df_mes, mes_label)
                        saved_paths.append(str(ruta))
                    except Exception as e:
                        save_errors.append(f"{mes_g}-{año_g}: {e}")

                st.session_state.mes_label = "Carga_manual_multi_mes"
                st.session_state.modo_reporte = "convenio"

                total = len(st.session_state.df_resultado)
                if saved_paths:
                    st.success(
                        f"✅ {total:,} registros procesados. "
                        f"Guardados {len(saved_paths)} período(s) en Parquet. "
                        "Ve a **📊 Reporte**."
                    )
                if save_errors:
                    st.warning("⚠️ Algunos períodos no se pudieron guardar:")
                    for e in save_errors:
                        st.markdown(f"- {e}")

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
