"""Inicialización de session_state y gestión del config."""

import json
import streamlit as st
from pathlib import Path

CONFIG_PATH = Path("config/config.json")

MESES = {
    "01": "Enero", "02": "Febrero", "03": "Marzo",
    "04": "Abril", "05": "Mayo", "06": "Junio",
    "07": "Julio", "08": "Agosto", "09": "Septiembre",
    "10": "Octubre", "11": "Noviembre", "12": "Diciembre",
}

LOGICAS = {
    "La celda tiene cualquier valor (no está vacía)": "tiene_valor",
    "Texto exacto (ej: FAC, SI, RADICADO)": "__texto__",
    "Contiene un número (ej: número de radicado)": "es_numero",
    "Contiene una fecha (ej: fecha de factura)": "es_fecha",
}


def guardar_config(config: dict):
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_PATH.write_text(
        json.dumps(config, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def inicializar_estado():
    if "df_resultado" not in st.session_state:
        st.session_state.df_resultado = None
    if "mes_label" not in st.session_state:
        st.session_state.mes_label = ""
    if "modo_reporte" not in st.session_state:
        st.session_state.modo_reporte = "mes"

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
