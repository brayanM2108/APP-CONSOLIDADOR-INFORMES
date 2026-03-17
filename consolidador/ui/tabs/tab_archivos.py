"""Tab 'Archivos del mes' — escaneo automático de carpeta."""

import streamlit as st
import pandas as pd
from datetime import datetime

from ui.estado import MESES, guardar_config
from core.procesador import procesar_base
from core.watcher import (
    escanear, archivos_nuevos, archivos_procesados,
    marcar_procesado, desmarcar_procesado,
)
from core.exportador import (
    guardar_parquet, cargar_todos_parquet, eliminar_archivo_de_parquet,
)


def render_tab_archivos():
    st.subheader("Archivos del mes")

    carpeta_raiz = st.session_state.get("carpeta_datos", "")
    if not carpeta_raiz:
        st.info("👈 Define la carpeta raíz de datos en el panel izquierdo.")

    mes_w = _selector_mes()
    archivos_scan = st.session_state.get("archivos_escaneados", [])

    if "archivos_escaneados" not in st.session_state:
        st.info("📂 Presiona **Escanear** para detectar archivos del mes.")
    elif not archivos_scan:
        st.warning(
            f"⚠️ No se encontraron archivos de **{MESES[mes_w]}** en `{carpeta_raiz}`."
        )
    else:
        _mostrar_archivos(archivos_scan, mes_w, carpeta_raiz)

    st.divider()
    _bases_consolidadas()


def _selector_mes():
    w1, w2 = st.columns(2)
    with w1:
        mes_w = st.selectbox(
            "Mes", list(MESES.keys()),
            index=datetime.now().month - 1,
            format_func=lambda x: MESES[x],
            key="mes_watcher",
        )
    with w2:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🔍 Escanear carpeta", use_container_width=True):
            carpeta_raiz = st.session_state.get("carpeta_datos", "")
            st.session_state["archivos_escaneados"] = escanear(
                carpeta_raiz, mes_w,
                st.session_state.config.get("carpeta_tipo_base", {}),
            )
    return mes_w


def _mostrar_archivos(archivos_scan, mes_w, carpeta_raiz):
    nuevos = archivos_nuevos(archivos_scan)
    procesados = archivos_procesados(archivos_scan)

    if not nuevos and not procesados:
        st.warning("⚠️ No se encontraron archivos para este mes.")
        return

    if not nuevos:
        st.success(f"✅ Todos los archivos de **{MESES[mes_w]}** ya fueron procesados.")
    else:
        st.info(
            f"🆕 **{len(nuevos)}** archivo(s) nuevo(s) · "
            f"✅ **{len(procesados)}** ya procesado(s)"
        )
        _archivos_nuevos(nuevos, mes_w, carpeta_raiz)

    if procesados:
        _archivos_procesados(procesados, mes_w, carpeta_raiz)


def _archivos_nuevos(nuevos, mes_w, carpeta_raiz):
    st.markdown("#### 🆕 Archivos nuevos")
    tipos_disponibles_w = ["— Selecciona —"] + [
        k for k in st.session_state.config.keys() if k != "carpeta_tipo_base"
    ]
    seleccionados = []

    for arch in nuevos:
        col_chk, col_info, col_tipo = st.columns([0.5, 3, 3])
        with col_chk:
            sel = st.checkbox("", key=f"sel_{arch.ruta}", value=bool(arch.tipo_base))
        with col_info:
            st.markdown(f"📄 `{arch.nombre}`")
            st.caption(f"{arch.convenio} / {arch.tipo_carpeta}")
        with col_tipo:
            idx = 0
            if arch.tipo_base and arch.tipo_base in tipos_disponibles_w:
                idx = tipos_disponibles_w.index(arch.tipo_base)
            tipo_sel_w = st.selectbox(
                "tipo", tipos_disponibles_w,
                index=idx, key=f"tipow_{arch.ruta}",
                label_visibility="collapsed",
            )
            if sel and tipo_sel_w != "— Selecciona —":
                seleccionados.append((arch, tipo_sel_w))

    st.divider()
    _botones_verificar_procesar(seleccionados, mes_w, carpeta_raiz)


def _botones_verificar_procesar(seleccionados, mes_w, carpeta_raiz):
    puede_procesar_w = len(seleccionados) > 0
    col_v, col_p = st.columns(2)

    with col_v:
        if st.button(
            "🔍 Verificar", disabled=not puede_procesar_w,
            use_container_width=True, key="verificar_w",
        ):
            dfs_prev, advertencias, errores = [], [], []
            progress = st.progress(0)

            for i, (arch, tipo_base_w) in enumerate(seleccionados):
                config_base = st.session_state.config.get(tipo_base_w, {})
                try:
                    df_raw = pd.read_excel(arch.ruta)
                    df_proc, warns = procesar_base(
                        df_raw, config_base,
                        arch.nombre, tipo_base_w, arch.mes, arch.año,
                    )
                    dfs_prev.append(df_proc)
                    advertencias.extend(warns)
                except Exception as e:
                    errores.append(f"**{arch.nombre}**: {e}")
                progress.progress((i + 1) / len(seleccionados))

            progress.empty()
            st.session_state["preview_w"] = dfs_prev
            st.session_state["advertencias_w"] = advertencias
            st.session_state["errores_w"] = errores
            st.session_state["seleccionados_w"] = seleccionados

    with col_p:
        hay_preview = bool(st.session_state.get("preview_w"))
        if st.button(
            "▶️ Procesar y guardar", type="primary",
            disabled=not hay_preview,
            use_container_width=True, key="procesar_w",
        ):
            dfs = st.session_state.pop("preview_w")
            sels = st.session_state.pop("seleccionados_w")

            for arch, tipo_base_w in sels:
                marcar_procesado(arch.ruta, tipo_base_w)

            df_nuevos = pd.concat(dfs, ignore_index=True)
            mes_nuevo = f"{MESES[mes_w]}_{sels[0][0].año}"

            if (
                st.session_state.df_resultado is not None
                and st.session_state.mes_label == mes_nuevo
            ):
                st.session_state.df_resultado = pd.concat(
                    [st.session_state.df_resultado, df_nuevos], ignore_index=True
                )
            else:
                st.session_state.df_resultado = df_nuevos

            st.session_state.mes_label = mes_nuevo
            st.session_state.modo_reporte = "mes"
            total = len(st.session_state.df_resultado)

            try:
                guardar_parquet(st.session_state.df_resultado, mes_nuevo)
                st.success(f"✅ {total:,} registros guardados. Ve a **📊 Reporte**.")
            except Exception:
                st.success(f"✅ {total:,} registros procesados. Ve a **📊 Reporte**.")

            st.session_state["archivos_escaneados"] = escanear(
                carpeta_raiz, mes_w,
                st.session_state.config.get("carpeta_tipo_base", {}),
            )
            st.rerun()

    # Resultado de verificación
    if st.session_state.get("advertencias_w"):
        with st.expander(f"⚠️ {len(st.session_state['advertencias_w'])} advertencia(s)"):
            for w in st.session_state["advertencias_w"]:
                st.markdown(f"- {w}")
    elif st.session_state.get("preview_w") is not None:
        st.success("✅ Sin advertencias. Puedes procesar y guardar.")
    if st.session_state.get("errores_w"):
        with st.expander(f"❌ {len(st.session_state['errores_w'])} error(es)"):
            for e in st.session_state["errores_w"]:
                st.markdown(f"- {e}")


def _archivos_procesados(procesados, mes_w, carpeta_raiz):
    with st.expander(f"✅ {len(procesados)} ya procesado(s)"):
        for arch in procesados:
            col_i, col_r = st.columns([4, 2])
            with col_i:
                st.markdown(f"📄 `{arch.nombre}`")
                st.caption(f"{arch.convenio} · {arch.procesado_el}")
            with col_r:
                if st.button("↩️ Reprocesar", key=f"reproc_{arch.ruta}"):
                    desmarcar_procesado(arch.ruta)
                    st.session_state["archivos_escaneados"] = escanear(
                        carpeta_raiz, mes_w,
                        st.session_state.config.get("carpeta_tipo_base", {}),
                    )
                    st.rerun()


def _bases_consolidadas():
    st.markdown("#### 📊 Bases consolidadas")
    df_total = cargar_todos_parquet()

    if df_total is None:
        st.caption("Aún no hay bases guardadas en el historial.")
        return

    # ── Filtro de mes ──────────────────────────────────────
    meses_disponibles = sorted(df_total["archivo_origen"].apply(
        lambda x: x  # placeholder
    ))
    # Obtener meses únicos desde el DataFrame
    if "mes" in df_total.columns and "año" in df_total.columns:
        df_total["_mes_label"] = df_total["año"].astype(str) + " - " + df_total["mes"].astype(str).str.zfill(2)
        meses_unicos = ["Todos"] + sorted(df_total["_mes_label"].unique().tolist())
    else:
        meses_unicos = ["Todos"]

    col_mes, col_conv = st.columns(2)

    with col_mes:
        mes_filtro = st.selectbox("Mes", meses_unicos, key="bases_mes_filtro")

    # Filtrar por mes antes de poblar el selector de convenio
    if mes_filtro != "Todos":
        df_filtrado_mes = df_total[df_total["_mes_label"] == mes_filtro]
    else:
        df_filtrado_mes = df_total

    convenios_total = ["Todos"] + sorted(df_filtrado_mes["nombre_convenio"].unique().tolist())

    with col_conv:
        conv_filtro = st.selectbox("Convenio", convenios_total, key="bases_conv_filtro")

    # Aplicar filtro de convenio
    if conv_filtro != "Todos":
        df_conv = df_filtrado_mes[df_filtrado_mes["nombre_convenio"] == conv_filtro]
    else:
        df_conv = df_filtrado_mes

    # Limpiar columna auxiliar
    df_total.drop(columns=["_mes_label"], inplace=True, errors="ignore")
    df_conv = df_conv.drop(columns=["_mes_label"], errors="ignore")

    archivos_conv = sorted(df_conv["archivo_origen"].unique().tolist())

    label_filtro = f"**{conv_filtro}**" if conv_filtro != "Todos" else "todos los convenios"
    label_mes = f" en **{mes_filtro}**" if mes_filtro != "Todos" else ""
    st.caption(f"{len(archivos_conv)} archivo(s) consolidado(s) para {label_filtro}{label_mes}:")


    # ── Selección múltiple para borrado rápido ────────────────
    def _key_sel(nombre: str) -> str:
        safe = (
            nombre.replace(" ", "_")
            .replace("/", "_")
            .replace("\\", "_")
            .replace(".", "_")
            .replace(":", "_")
        )
        return f"bulk_del_{safe}"

    # Limpiar selección en un rerun posterior (antes de instanciar widgets)
    if st.session_state.get("bulk_clear_pending", False):
        for k in list(st.session_state.keys()):
            if k.startswith("bulk_del_"):
                st.session_state[k] = False
        st.session_state["bulk_clear_pending"] = False


    seleccionados = []

    for a in archivos_conv:
        col_chk, col_a, col_btn = st.columns([0.6, 4.4, 1])
        with col_chk:
            marcado = st.checkbox("", key=_key_sel(a))
            if marcado:
                seleccionados.append(a)
        with col_a:
            st.markdown(f'<span class="badge">📄 {a}</span>', unsafe_allow_html=True)
        with col_btn:
            if st.button("🗑️", key=f"del_{a}", help=f"Eliminar {a}"):
                st.session_state[f"confirm_del_{a}"] = True

        # Borrado individual (se mantiene)
        if st.session_state.get(f"confirm_del_{a}"):
            st.warning(f"⚠️ ¿Eliminar **{a}** del Parquet permanentemente?")
            col_si, col_no = st.columns(2)
            with col_si:
                if st.button("✅ Sí, eliminar", key=f"si_{a}", type="primary", use_container_width=True):
                    resultado = eliminar_archivo_de_parquet(a)
                    if resultado["error"]:
                        st.error(f"Error: {resultado['error']}")
                    else:
                        st.session_state.pop(f"confirm_del_{a}", None)
                        if st.session_state.df_resultado is not None:
                            df_actual = st.session_state.df_resultado
                            df_filtrado = df_actual[
                                df_actual["archivo_origen"] != a
                            ].reset_index(drop=True)
                            st.session_state.df_resultado = None if df_filtrado.empty else df_filtrado
                        meses_txt = ", ".join(resultado["meses_afectados"]) or "ninguno"
                        st.success(
                            f"✅ {resultado['eliminados']:,} registros eliminados · "
                            f"Meses afectados: {meses_txt}"
                        )
                        st.rerun()
            with col_no:
                if st.button("❌ Cancelar", key=f"no_{a}", use_container_width=True):
                    st.session_state.pop(f"confirm_del_{a}", None)
                    st.rerun()

    # ── Acción masiva ────────────────────────────────────────
    st.divider()
    col_b1, col_b2 = st.columns([2, 1])

    with col_b1:
        st.caption(f"Seleccionados: **{len(seleccionados)}**")

    with col_b2:
        if st.button(
            f"🗑️ Eliminar seleccionados ({len(seleccionados)})",
            type="primary",
            use_container_width=True,
            disabled=len(seleccionados) == 0,
            key="btn_bulk_delete",
        ):
            st.session_state["confirm_bulk_delete"] = True

    if st.session_state.get("confirm_bulk_delete"):
        st.warning(
            f"⚠️ Vas a eliminar **{len(seleccionados)}** archivo(s) del Parquet de forma permanente."
        )
        csi, cno = st.columns(2)
        with csi:
            if st.button("✅ Sí, eliminar seleccionados", type="primary", use_container_width=True, key="yes_bulk_delete"):
                total_eliminados = 0
                meses_afectados = set()
                errores = []

                for nombre_archivo in seleccionados:
                    resultado = eliminar_archivo_de_parquet(nombre_archivo)
                    if resultado["error"]:
                        errores.append(f"{nombre_archivo}: {resultado['error']}")
                    else:
                        total_eliminados += int(resultado["eliminados"])
                        for m in resultado["meses_afectados"]:
                            meses_afectados.add(m)

                # Limpiar en memoria (df_resultado actual)
                if st.session_state.df_resultado is not None and seleccionados:
                    df_actual = st.session_state.df_resultado
                    df_filtrado = df_actual[
                        ~df_actual["archivo_origen"].isin(seleccionados)
                    ].reset_index(drop=True)
                    st.session_state.df_resultado = None if df_filtrado.empty else df_filtrado

                # Pedir limpieza de checkboxes para el siguiente rerun
                st.session_state["bulk_clear_pending"] = True

                st.session_state["confirm_bulk_delete"] = False

                if errores:
                    st.error("❌ Hubo errores en algunas eliminaciones:")
                    for e in errores:
                        st.markdown(f"- {e}")

                meses_txt = ", ".join(sorted(meses_afectados)) if meses_afectados else "ninguno"
                st.success(
                    f"✅ {total_eliminados:,} registros eliminados en borrado masivo · "
                    f"Meses afectados: {meses_txt}"
                )
                st.rerun()

        with cno:
            if st.button("❌ Cancelar", use_container_width=True, key="no_bulk_delete"):
                st.session_state["confirm_bulk_delete"] = False
                st.rerun()
