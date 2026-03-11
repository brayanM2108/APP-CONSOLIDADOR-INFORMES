"""
app.py
Punto de entrada de la aplicación Streamlit.
Delega toda la lógica de UI a los módulos en ui/.
"""

import streamlit as st

from ui.estilos import configurar_pagina, aplicar_estilos
from ui.estado import inicializar_estado
from ui.sidebar import render_sidebar
from ui.tabs.tab_archivos import render_tab_archivos
from ui.tabs.tab_cargar import render_tab_cargar
from ui.tabs.tab_reporte import render_tab_reporte

# ── Configuración inicial ────────────────────────────────────
configurar_pagina()
aplicar_estilos()
inicializar_estado()

# ── Sidebar ──────────────────────────────────────────────────
render_sidebar()

# ── Contenido principal ──────────────────────────────────────
st.markdown(
    '<p class="titulo">🏥 Control de Facturación</p>',
    unsafe_allow_html=True,
)
st.markdown(
    '<p class="subtit">Consolida bases heterogéneas e identifica servicios no facturados.</p>',
    unsafe_allow_html=True,
)
st.markdown("<br>", unsafe_allow_html=True)

if not st.session_state.config:
    st.info("👈 Crea los tipos de base en el panel izquierdo para comenzar.")
    st.stop()

tab_archivos, tab_cargar, tab_reporte = st.tabs(
    ["📂 Archivos del mes", "📁 Cargar manual", "📊 Reporte"]
)

with tab_archivos:
    render_tab_archivos()

with tab_cargar:
    render_tab_cargar()

with tab_reporte:
    render_tab_reporte()
