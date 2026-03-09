"""
app.py
Responsabilidad: interfaz de usuario (Streamlit).
Solo maneja pantallas, inputs y llamadas a core/.
"""

import streamlit as st
import pandas as pd
import json
from datetime import datetime
from pathlib import Path

from core.procesador import procesar_base, COLUMNAS, columnas_reales, leer_excel_con_duplicados
from core.watcher import (
    escanear, archivos_nuevos, archivos_procesados,
    marcar_procesado, desmarcar_procesado,
    MESES_NOMBRE,
)
from core.analizador import (
    kpis_globales,
    resumen_por_convenio,
    pendientes_por_facturador,
    detalle_pendientes,
    convenios_disponibles,
)
from core.exportador import (
    general_csv, general_excel,
    convenio_csv, convenio_excel,
    tipo_base_csv, tipo_base_excel,
    nombre_general,
    nombre_convenio_archivo,
    nombre_tipo_base_archivo,
    guardar_parquet,
    cargar_todos_parquet,
    meses_disponibles_parquet,
)

# ── Constantes ───────────────────────────────────────────────
MESES = {
    "01": "Enero",    "02": "Febrero",  "03": "Marzo",
    "04": "Abril",    "05": "Mayo",     "06": "Junio",
    "07": "Julio",    "08": "Agosto",   "09": "Septiembre",
    "10": "Octubre",  "11": "Noviembre","12": "Diciembre",
}

LOGICAS = {
    "La celda tiene cualquier valor (no está vacía)": "tiene_valor",
    "Texto exacto (ej: FAC, SI, RADICADO)":          "__texto__",
    "Contiene un número (ej: número de radicado)":   "es_numero",
    "Contiene una fecha (ej: fecha de factura)":     "es_fecha",
}

# ── Configuración de página ──────────────────────────────────
st.set_page_config(
    page_title="Control de Facturación",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Estilos ──────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

.titulo { font-size: 1.7rem; font-weight: 700; color: #0f172a; letter-spacing: -0.5px; }
.subtit { color: #64748b; font-size: 0.88rem; margin-top: 2px; }

.kpi {
    background: white; border: 1px solid #e2e8f0;
    border-radius: 10px; padding: 16px; text-align: center;
}
.kpi-num   { font-size: 2rem; font-weight: 700; line-height: 1.1; }
.kpi-label { font-size: 0.72rem; color: #94a3b8; margin-top: 4px;
             text-transform: uppercase; letter-spacing: 0.06em; }

.verde   { color: #16a34a; }
.rojo    { color: #dc2626; }
.azul    { color: #2563eb; }
.naranja { color: #d97706; }
.gris    { color: #6b7280; }

.badge {
    display: inline-block; background: #f1f5f9; color: #334155;
    padding: 2px 9px; border-radius: 20px;
    font-size: 0.76rem; font-weight: 500; margin: 2px;
}

.nivel-header {
    background: #f8fafc; border: 1px solid #e2e8f0;
    border-left: 3px solid #3b82f6;
    border-radius: 8px; padding: 12px 16px;
    margin-bottom: 12px;
}
.nivel-titulo { font-weight: 600; color: #1e40af; font-size: 0.95rem; }
.nivel-desc   { color: #64748b; font-size: 0.82rem; margin-top: 2px; }
</style>
""", unsafe_allow_html=True)

# ── Ruta del config ──────────────────────────────────────────
CONFIG_PATH = Path("config/config.json")


def _guardar_config(config: dict):
    """Persiste el config en disco automáticamente."""
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_PATH.write_text(
        json.dumps(config, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


# ── Estado de sesión ─────────────────────────────────────────
if "df_resultado" not in st.session_state: st.session_state.df_resultado = None
if "mes_label"    not in st.session_state: st.session_state.mes_label    = ""

# Cargar config desde disco al arrancar (solo una vez)
if "config" not in st.session_state:
    if CONFIG_PATH.exists():
        try:
            st.session_state.config = json.loads(
                CONFIG_PATH.read_text(encoding="utf-8")
            )
        except Exception:
            st.session_state.config = {}
    else:
        st.session_state.config = {}


# ════════════════════════════════════════════════════════════
# SIDEBAR — Configuración
# ════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("## ⚙️ Configuración")

    # Indicador de estado del config
    if st.session_state.config:
        st.success(f"✅ {len(st.session_state.config)} tipo(s) activos")
        st.markdown("**Tipos de base:**")
        for nombre in st.session_state.config:
            st.markdown(f'<span class="badge">{nombre}</span>', unsafe_allow_html=True)
    else:
        st.warning("⚠️ Sin tipos de base configurados")

    st.divider()

    with st.expander("➕ Nuevo tipo de base", expanded=not bool(st.session_state.config)):
        with st.form("form_tipo", clear_on_submit=True):
            st.markdown("**Identificación**")
            nombre_tipo = st.text_input("Nombre *", placeholder="Convenio A - Laboratorio")
            st.caption("Usa 'Convenio - Tipo' para agrupar automáticamente")

            st.markdown("**Columnas del archivo**")
            col_pac  = st.text_input("Documento paciente *")
            col_nom  = st.text_input("Nombre paciente")
            col_cups = st.text_input("CUPS")
            col_serv = st.text_input("Descripción servicio")
            col_fech = st.text_input("Fecha atención")
            col_fper = st.text_input("Facturador asignado")
            col_obs  = st.text_input("Observaciones")

            st.markdown("**Columna de facturación**")
            col_fact     = st.text_input("Nombre de la columna *")
            logica_label = st.selectbox("¿Qué indica que fue facturado?", list(LOGICAS.keys()))
            texto_exacto = ""
            if logica_label == "Texto exacto (ej: FAC, SI, RADICADO)":
                texto_exacto = st.text_input("¿Cuál es ese texto?")

            st.markdown("**Columnas adicionales** *(opcional)*")
            st.caption("Una por línea. Para columnas duplicadas usa formato: nombre_en_excel → alias")
            cols_extra_input = st.text_area(
                "Columnas extra",
                placeholder="Codigo IPS\nAutorizacion\nVALOR → valor_inicial\nVALOR.1 → valor_final",
                help="Columnas simples: escribe el nombre exacto.\n"
                     "Columnas duplicadas: usa NOMBRE_EXCEL → alias_unico",
                label_visibility="collapsed"
            )

            if st.form_submit_button("💾 Guardar", type="primary", use_container_width=True):
                if not nombre_tipo or not col_pac or not col_fact:
                    st.error("Nombre, doc. paciente y col. facturación son obligatorios.")
                else:
                    logica_valor = texto_exacto if logica_label == "Texto exacto (ej: FAC, SI, RADICADO)" else LOGICAS[logica_label]

                    # Parsear columnas extra
                    # Formato simple:  "Columna Normal"
                    # Formato alias:   "VALOR.1 → valor_final"
                    columnas_extra = []
                    for linea in cols_extra_input.splitlines():
                        linea = linea.strip()
                        if not linea:
                            continue
                        if "→" in linea:
                            partes = linea.split("→")
                            col_real = partes[0].strip()
                            alias    = partes[1].strip()
                            columnas_extra.append({"col": col_real, "alias": alias})
                        else:
                            columnas_extra.append(linea)

                    st.session_state.config[nombre_tipo] = {
                        "col_paciente":       col_pac,
                        "col_nombre":         col_nom  or None,
                        "col_cups":           col_cups or None,
                        "col_servicio":       col_serv or None,
                        "col_fecha":          col_fech or None,
                        "col_facturador":     col_fper or None,
                        "col_observacion":    col_obs  or None,
                        "col_facturacion":    col_fact,
                        "logica_facturacion": logica_valor,
                        "columnas_extra":     columnas_extra,
                    }
                    _guardar_config(st.session_state.config)
                    st.success(f"✅ '{nombre_tipo}' guardado en config/config.json")
                    st.rerun()

    if st.session_state.config:
        st.divider()
        st.caption(f"💾 Config guardado en: `{CONFIG_PATH}`")

    # ── Carpeta de datos y mapeo ────────────────────────────
    st.divider()
    with st.expander("📁 Carpeta de datos y mapeo"):
        carpeta_datos = st.text_input(
            "Ruta de la carpeta raíz",
            value=st.session_state.get("carpeta_datos", ""),
            placeholder="C:/Users/tu_usuario/datos",
            key="input_carpeta_datos"
        )
        if carpeta_datos:
            st.session_state["carpeta_datos"] = carpeta_datos

        st.markdown("**Mapeo carpeta → tipo de base**")
        st.caption("Escribe: NOMBRE_CARPETA → Tipo en config (una por línea)")

        mapeo_actual = st.session_state.config.get("carpeta_tipo_base", {})
        mapeo_texto  = "\n".join(f"{k} → {v}" for k, v in mapeo_actual.items())

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
            _guardar_config(st.session_state.config)
            st.success("✅ Mapeo guardado")
            st.rerun()

    # ── Historial de meses guardados ────────────────────────
    with st.expander("📅 Historial de meses guardados"):
        meses = meses_disponibles_parquet()
        if not meses:
            st.caption("Aún no hay meses guardados.")
        else:
            st.caption(f"{len(meses)} mes(es) en el historial:")

            # Filtro por convenio
            try:
                from core.exportador import cargar_todos_parquet

                df_todos = cargar_todos_parquet()
                convenios_hist = ["Todos"] + sorted(df_todos["nombre_convenio"].unique()) if df_todos is not None else [
                    "Todos"]
            except Exception:
                convenios_hist = ["Todos"]

            filtro_conv = st.selectbox(
                "Filtrar por convenio",
                convenios_hist,
                key="filtro_hist_convenio"
            )

            for m in meses:
                col_m, col_b = st.columns([3, 2])
                with col_m:
                    st.markdown(f"📦 `{m}`")
                with col_b:
                    if st.button("Cargar", key=f"cargar_{m}"):
                        try:
                            from core.exportador import cargar_parquet

                            df_hist = cargar_parquet(m.replace(" ", "_"))
                            if df_hist is not None:
                                # Aplicar filtro de convenio si está seleccionado
                                if filtro_conv != "Todos":
                                    df_hist = df_hist[df_hist["nombre_convenio"] == filtro_conv]
                                st.session_state.df_resultado = df_hist
                                st.session_state.mes_label = m.replace(" ", "_")
                                st.success(f"✅ {m} cargado" + (f" · {filtro_conv}" if filtro_conv != "Todos" else ""))
                                st.rerun()
                        except Exception as e:
                            st.error(f"Error: {e}")

    # ── Inspector de columnas ────────────────────────────────
    st.divider()
    with st.expander("🔍 Inspeccionar columnas de un archivo"):
        st.caption(
            "Sube un archivo para ver exactamente cómo pandas nombra sus columnas. "
            "Útil para identificar columnas duplicadas (VALOR, VALOR.1, VALOR.2...)"
        )
        f_inspect = st.file_uploader(
            "Archivo a inspeccionar", type=["xlsx", "xls"], key="inspector"
        )
        if f_inspect is not None:
            try:
                df_inspect = leer_excel_con_duplicados(f_inspect)
                cols = columnas_reales(df_inspect)

                # Filtrar valores nulos y convertir todo a string
                cols = [str(c) for c in cols if c is not None and str(c) != "nan"]

                bases_sin_punto = [c for c in cols if "." not in c]

                st.markdown(f"**{len(cols)} columnas encontradas:**")
                for col in cols:
                    es_dup = any(col.startswith(f"{base}.") for base in bases_sin_punto)
                    if es_dup:
                        st.markdown(
                            f'`{col}` <span style="color:#d97706;font-size:0.78rem">'
                            f'⚠️ duplicada — usa alias</span>',
                            unsafe_allow_html=True
                        )
                    else:
                        st.markdown(f"`{col}`")

                st.info("ℹ️ Este archivo no se procesará ni guardará. Solo se muestran sus columnas.")
            except Exception as e:
                st.error(f"Error al leer el archivo: {e}")

# ════════════════════════════════════════════════════════════
# CONTENIDO PRINCIPAL
# ════════════════════════════════════════════════════════════
st.markdown('<p class="titulo">🏥 Control de Facturación</p>', unsafe_allow_html=True)
st.markdown('<p class="subtit">Consolida bases heterogéneas e identifica servicios no facturados.</p>', unsafe_allow_html=True)
st.markdown("<br>", unsafe_allow_html=True)

if not st.session_state.config:
    st.info("👈 Crea los tipos de base en el panel izquierdo para comenzar. El config se guarda automáticamente.")
    st.stop()

tab_archivos, tab_cargar, tab_reporte = st.tabs(["📂 Archivos del mes", "📁 Cargar manual", "📊 Reporte"])


# ════════════════════════════════════════════════════════════
# TAB 0 — ARCHIVOS DEL MES (WATCHER)
# ════════════════════════════════════════════════════════════
with tab_archivos:

    st.subheader("Archivos del mes")

    carpeta_raiz = st.session_state.get("carpeta_datos", "")
    if not carpeta_raiz:
        st.info("👈 Define la carpeta raíz de datos en el panel izquierdo.")
        st.stop()

    # Selector de mes
    w1, w2 = st.columns(2)
    with w1:
        mes_w = st.selectbox("Mes", list(MESES.keys()),
                             index=datetime.now().month - 1,
                             format_func=lambda x: MESES[x],
                             key="mes_watcher")
    with w2:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🔍 Escanear carpeta", use_container_width=True):
            st.session_state["archivos_escaneados"] = escanear(
                carpeta_raiz,
                mes_w,
                st.session_state.config.get("carpeta_tipo_base", {}),
            )

    archivos_scan = st.session_state.get("archivos_escaneados", [])

    if "archivos_escaneados" not in st.session_state:
        st.info("📂 Presiona **Escanear** para detectar archivos del mes.")
    elif not archivos_scan:
        st.warning(f"⚠️ No se encontraron archivos de **{MESES[mes_w]}** en `{carpeta_raiz}`. Verifica que la carpeta exista y que los archivos contengan el nombre del mes.")
    else:
        nuevos     = archivos_nuevos(archivos_scan)
        procesados = archivos_procesados(archivos_scan)

        if not nuevos and not procesados:
            st.warning("⚠️ No se encontraron archivos para este mes.")
        elif not nuevos:
            st.success(f"✅ Todos los archivos de **{MESES[mes_w]}** ya fueron procesados.")
        else:
            st.info(f"🆕 **{len(nuevos)}** archivo(s) nuevo(s) · ✅ **{len(procesados)}** ya procesado(s)")

        # ── Archivos nuevos ──────────────────────────────────
        if nuevos:
            st.markdown("#### 🆕 Archivos nuevos")
            tipos_disponibles_w = ["— Selecciona —"] + [
                k for k in st.session_state.config.keys()
                if k != "carpeta_tipo_base"
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
                    # Preseleccionar si hay mapeo
                    idx = 0
                    if arch.tipo_base and arch.tipo_base in tipos_disponibles_w:
                        idx = tipos_disponibles_w.index(arch.tipo_base)
                    tipo_sel_w = st.selectbox(
                        "tipo", tipos_disponibles_w,
                        index=idx,
                        key=f"tipow_{arch.ruta}",
                        label_visibility="collapsed",
                    )
                    if sel and tipo_sel_w != "— Selecciona —":
                        seleccionados.append((arch, tipo_sel_w))

            st.divider()

            puede_procesar_w = len(seleccionados) > 0
            if st.button("▶️ Procesar seleccionados", type="primary",
                         disabled=not puede_procesar_w, use_container_width=True):

                dfs_w        = []
                advertencias_w = []
                errores_w    = []
                progress_w   = st.progress(0)

                for i, (arch, tipo_base_w) in enumerate(seleccionados):
                    config_base_w = st.session_state.config.get(tipo_base_w, {})
                    try:
                        df_raw_w = pd.read_excel(arch.ruta)
                        df_proc_w, warns_w = procesar_base(
                            df_raw_w, config_base_w,
                            arch.nombre, tipo_base_w,
                            arch.mes, arch.año,
                        )
                        dfs_w.append(df_proc_w)
                        advertencias_w.extend(warns_w)
                        marcar_procesado(arch.ruta, tipo_base_w)
                    except Exception as e:
                        errores_w.append(f"**{arch.nombre}**: {e}")
                    progress_w.progress((i + 1) / len(seleccionados))

                progress_w.empty()

                if advertencias_w:
                    with st.expander(f"⚠️ {len(advertencias_w)} advertencia(s)"):
                        for w in advertencias_w:
                            st.markdown(f"- {w}")
                if errores_w:
                    with st.expander(f"❌ {len(errores_w)} error(es)"):
                        for e in errores_w:
                            st.markdown(f"- {e}")

                if dfs_w:
                    st.session_state.df_resultado = pd.concat(dfs_w, ignore_index=True)
                    st.session_state.mes_label    = f"{MESES[mes_w]}_{seleccionados[0][0].año}"
                    total_w = len(st.session_state.df_resultado)
                    try:
                        ruta_p = guardar_parquet(st.session_state.df_resultado, st.session_state.mes_label)
                        st.success(f"✅ {total_w:,} registros procesados y guardados. Ve a **📊 Reporte**.")
                    except Exception:
                        st.success(f"✅ {total_w:,} registros procesados. Ve a **📊 Reporte**.")
                    # Refrescar escaneo
                    st.session_state["archivos_escaneados"] = escanear(
                        carpeta_raiz, mes_w,
                        st.session_state.config.get("carpeta_tipo_base", {}),
                    )
                    st.rerun()

        # ── Archivos ya procesados ───────────────────────────
        if procesados:
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


# ════════════════════════════════════════════════════════════
# TAB 1 — CARGAR ARCHIVOS
# ════════════════════════════════════════════════════════════
with tab_cargar:

    st.subheader("Período")
    c1, c2 = st.columns(2)
    with c1:
        mes_sel = st.selectbox("Mes", list(MESES.keys()),
                               index=datetime.now().month - 1,
                               format_func=lambda x: MESES[x])
    with c2:
        año_sel = st.number_input("Año", min_value=2020, max_value=2035,
                                  value=datetime.now().year, step=1)

    st.divider()
    st.subheader("Archivos del mes")
    st.caption("Sube los archivos y asigna el tipo de base a cada uno.")

    archivos = st.file_uploader(
        "Selecciona archivos Excel",
        type=["xlsx", "xls"],
        accept_multiple_files=True,
    )

    tipos_asignados = {}

    if archivos:
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

    st.divider()

    puede_procesar = bool(archivos) and len(tipos_asignados) > 0

    if st.button("▶️ Procesar archivos", type="primary",
                 disabled=not puede_procesar, use_container_width=True):

        dfs = []
        advertencias = []
        errores = []
        archivos_ok = [a for a in archivos if a.name in tipos_asignados]
        progress = st.progress(0)

        for i, archivo in enumerate(archivos_ok):
            tipo_base   = tipos_asignados[archivo.name]
            config_base = st.session_state.config[tipo_base]
            try:
                archivo.seek(0)
                df_raw = pd.read_excel(archivo)
                df_proc, warns = procesar_base(
                    df_raw, config_base,
                    archivo.name, tipo_base,
                    mes_sel, int(año_sel),
                )
                dfs.append(df_proc)
                advertencias.extend(warns)
            except Exception as e:
                errores.append(f"**{archivo.name}**: {e}")
            progress.progress((i + 1) / len(archivos_ok))

        progress.empty()

        if advertencias:
            with st.expander(f"⚠️ {len(advertencias)} advertencia(s)"):
                for w in advertencias:
                    st.markdown(f"- {w}")
        if errores:
            with st.expander(f"❌ {len(errores)} error(es)"):
                for e in errores:
                    st.markdown(f"- {e}")

        if dfs:
            df_nuevos  = pd.concat(dfs, ignore_index=True)
            mes_nuevo  = f"{MESES[mes_sel]}_{int(año_sel)}"
            # Acumular con lo que ya había en sesión si es el mismo mes
            if (st.session_state.df_resultado is not None and
                    st.session_state.mes_label == mes_nuevo):
                st.session_state.df_resultado = pd.concat(
                    [st.session_state.df_resultado, df_nuevos], ignore_index=True
                )
            else:
                st.session_state.df_resultado = df_nuevos
            st.session_state.mes_label = mes_nuevo
            total = len(st.session_state.df_resultado)

            # Guardar automáticamente en Parquet
            try:
                ruta = guardar_parquet(
                    st.session_state.df_resultado,
                    st.session_state.mes_label
                )
                st.success(f"✅ {total:,} registros procesados y guardados en `{ruta}`. Ve a **📊 Reporte**.")
            except Exception as e:
                st.success(f"✅ {total:,} registros procesados. Ve a **📊 Reporte**.")
                st.warning(f"⚠️ No se pudo guardar en Parquet: {e}. Instala pyarrow con `pip install pyarrow`.")
        else:
            st.error("No se pudo procesar ningún archivo.")


# ════════════════════════════════════════════════════════════
# TAB 2 — REPORTE
# ════════════════════════════════════════════════════════════
with tab_reporte:

    df = st.session_state.df_resultado

    if df is None or df.empty:
        st.info("Procesa los archivos primero en la pestaña **📁 Cargar archivos**.")
        st.stop()

    mes_label = st.session_state.mes_label

    # ── KPIs ─────────────────────────────────────────────────
    kpis      = kpis_globales(df)
    pct       = kpis["cumplimiento"]
    color_pct = "verde" if pct >= 80 else "naranja" if pct >= 50 else "rojo"

    k1, k2, k3, k4, k5 = st.columns(5)
    for col, num, label, color in [
        (k1, f"{kpis['total']:,}",      "Total registros", "azul"),
        (k2, f"{kpis['facturados']:,}", "Facturados",       "verde"),
        (k3, f"{kpis['pendientes']:,}", "Pendientes",       "rojo"),
        (k4, f"{kpis['sin_info']:,}",   "Sin información",  "gris"),
        (k5, f"{pct}%",                 "Cumplimiento",     color_pct),
    ]:
        col.markdown(f"""<div class="kpi">
            <div class="kpi-num {color}">{num}</div>
            <div class="kpi-label">{label}</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Resumen por convenio ──────────────────────────────────
    st.subheader("📋 Resumen por convenio")
    rc = resumen_por_convenio(df)
    if not rc.empty:
        st.dataframe(
            rc.style.background_gradient(
                subset=["Cumplimiento (%)"], cmap="RdYlGn", vmin=0, vmax=100
            ),
            use_container_width=True, hide_index=True,
        )

    st.divider()

    # ── Pendientes por facturador ─────────────────────────────
    st.subheader("👤 Pendientes por facturador")
    rf = pendientes_por_facturador(df)
    if rf.empty:
        st.success("🎉 No hay pendientes.")
    else:
        st.dataframe(rf, use_container_width=True, hide_index=True)

    st.divider()

    # ── Detalle pendientes ────────────────────────────────────
    st.subheader("⚠️ Detalle de pendientes")
    df_pend = df[df["estado"] == "Pendiente"]

    if df_pend.empty:
        st.success("🎉 No hay registros pendientes.")
    else:
        convs  = ["Todos"] + convenios_disponibles(df_pend)
        filtro = st.selectbox("Filtrar por convenio", convs)
        df_det = detalle_pendientes(df, filtro)
        st.caption(f"{len(df_det):,} registros")
        st.dataframe(df_det, use_container_width=True, hide_index=True)

    st.divider()

    # ════════════════════════════════════════════════════════
    # DESCARGAS — 3 niveles
    # ════════════════════════════════════════════════════════
    st.subheader("⬇️ Descargar reportes")

    # ── Nivel 1: General ─────────────────────────────────────
    st.markdown("""<div class="nivel-header">
        <div class="nivel-titulo">📊 Nivel 1 — Reporte general</div>
        <div class="nivel-desc">Todos los convenios y tipos de base consolidados</div>
    </div>""", unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    with col1:
        st.download_button(
            "⬇️ General CSV",
            data=general_csv(df),
            file_name=nombre_general(mes_label, "csv"),
            mime="text/csv",
            use_container_width=True,
        )
    with col2:
        st.download_button(
            "⬇️ General Excel",
            data=general_excel(df),
            file_name=nombre_general(mes_label, "xlsx"),
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Nivel 2: Por convenio ─────────────────────────────────
    st.markdown("""<div class="nivel-header">
        <div class="nivel-titulo">🏥 Nivel 2 — Por convenio</div>
        <div class="nivel-desc">Reporte individual de cada convenio</div>
    </div>""", unsafe_allow_html=True)

    convenios = convenios_disponibles(df)
    conv_sel  = st.selectbox("Selecciona el convenio", convenios, key="conv_desc")

    col3, col4 = st.columns(2)
    with col3:
        st.download_button(
            f"⬇️ {conv_sel} CSV",
            data=convenio_csv(df, conv_sel),
            file_name=nombre_convenio_archivo(conv_sel, mes_label, "csv"),
            mime="text/csv",
            use_container_width=True,
        )
    with col4:
        st.download_button(
            f"⬇️ {conv_sel} Excel",
            data=convenio_excel(df, conv_sel),
            file_name=nombre_convenio_archivo(conv_sel, mes_label, "xlsx"),
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Nivel 3: Por tipo de base ─────────────────────────────
    st.markdown("""<div class="nivel-header">
        <div class="nivel-titulo">🗂️ Nivel 3 — Por tipo de base</div>
        <div class="nivel-desc">Reporte individual de cada tipo de base</div>
    </div>""", unsafe_allow_html=True)

    tipos_disponibles_rep = sorted(df["tipo_base"].unique().tolist())
    tipo_sel = st.selectbox("Selecciona el tipo de base", tipos_disponibles_rep, key="tipo_desc")

    col5, col6 = st.columns(2)
    with col5:
        st.download_button(
            f"⬇️ {tipo_sel} CSV",
            data=tipo_base_csv(df, tipo_sel),
            file_name=nombre_tipo_base_archivo(tipo_sel, mes_label, "csv"),
            mime="text/csv",
            use_container_width=True,
        )
    with col6:
        st.download_button(
            f"⬇️ {tipo_sel} Excel",
            data=tipo_base_excel(df, tipo_sel),
            file_name=nombre_tipo_base_archivo(tipo_sel, mes_label, "xlsx"),
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )