"""
App
Delegate all UI logic to the modules in ui/.
Streamlit application entry point.
"""
import streamlit as st
from ui.styles import page_config, page_styles
from ui.state import inicializate_state
from ui.sidebar import render_sidebar
from ui.tabs.tab_files import render_tab_files
from ui.tabs.tab_manual_load import render_tab_load
from ui.tabs.tab_report import render_tab_report
from ui.tabs.tab_billing import render_tab_billing
from ui.tabs.tab_billing_report import render_tab_billing_report

page_config()
page_styles()
inicializate_state()

render_sidebar()

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

tab_files, tab_load, tab_report, tab_billing, tab_report_billing = st.tabs([
    "📂 Archivos del mes", "📁 Cargar manual", "📊 Reporte", "📋 Facturado", "🧾Informe Facturacion"
])

with tab_files:
    render_tab_files()

with tab_load:
    render_tab_load()

with tab_report:
    render_tab_report()

with tab_billing:
    render_tab_billing()

with tab_report_billing:
    render_tab_billing_report()
