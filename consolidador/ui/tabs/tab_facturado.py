"""Tab 'Facturado' — carga y almacenamiento del archivo de facturado."""

import streamlit as st
from core.facturado import (
    leer_facturado,
    kpis_facturado,
    guardar_facturado,
    info_facturado_guardado,
    cargar_facturado,
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
