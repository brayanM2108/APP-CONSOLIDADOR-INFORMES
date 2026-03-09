# consolidador/core/__init__.py
from .procesador import procesar_base, COLUMNAS
from .analizador import (
    kpis_globales,
    resumen_por_convenio,
    pendientes_por_facturador,
    detalle_pendientes,
    convenios_disponibles,
)
