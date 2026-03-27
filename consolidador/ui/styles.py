"""Page settings and CSS styles."""
import streamlit as st


def page_config():
    st.set_page_config(
        page_title="Control de Facturación",
        page_icon="🏥",
        layout="wide",initial_sidebar_state="expanded",
    )


def page_styles():
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
