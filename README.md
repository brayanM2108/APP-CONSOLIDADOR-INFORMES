# APPCONSOLIDADO

Aplicacion web desarrollada con Streamlit para consolidar informes operativos de multiples bases, identificar que servicios se facturaron y cuales no, y generar KPIs para seguimiento de gestion.

![Python](https://img.shields.io/badge/Python-3.x-blue.svg)
![Streamlit](https://img.shields.io/badge/Streamlit-App-red.svg)
![Pandas](https://img.shields.io/badge/Pandas-Data-green.svg)
![Parquet](https://img.shields.io/badge/Parquet-Historial-orange.svg)

## Objetivo

Centralizar el control de facturacion en una sola aplicacion para:

- consolidar archivos heterogeneos en un esquema comun,
- detectar pendientes de facturacion,
- cruzar contra archivo de facturado,
- y entregar indicadores de gestion listos para analisis y descarga.

## Caracteristicas

### Consolidacion y calidad de datos

- Estandariza columnas clave (documento, servicio, fecha, estado, facturador, observacion, etc.).
- Soporta mapeo por tipo de base y columnas extra por configuracion.
- Maneja columnas duplicadas de Excel (por ejemplo `VALOR`, `VALOR.1`, `VALOR.2`).
- Registra advertencias de columnas faltantes sin detener todo el proceso.

### Carga y persistencia

- Carga automatica por escaneo de carpetas.
- Carga manual de multiples archivos con asignacion de tipo de base.
- Almacenamiento historico en Parquet para reutilizar datos sin reprocesar.
- Historial cargable por mes o por convenio desde la barra lateral.

### Analitica y reportes

- KPIs globales: total, facturados, pendientes, sin informacion y cumplimiento.
- Resumen por convenio y pendientes por facturador.
- Detalle de pendientes con columnas adicionales.
- Exportacion a CSV y Excel (general, por convenio y por tipo de base).

### Cruce con facturado

- Carga de archivo de facturado (hojas activas y anuladas).
- Cruce por llaves configurables por tipo de base.
- Marcacion de estado de cruce: `Facturado`, `No facturado`, `Sin cruce`.
- KPIs del cruce y resúmenes por convenio/tipo de base.

## Modulos principales

### Interfaz (`consolidador/ui`)

- `tab_archivos.py`: escaneo de carpeta, verificacion, procesamiento y bases consolidadas.
- `tab_cargar.py`: carga manual de archivos y guardado en Parquet.
- `tab_reporte.py`: visualizacion de KPIs, resumenes, detalle y descargas.
- `tab_facturado.py`: carga/guardado del archivo de facturado y KPIs asociados.
- `sidebar.py`: configuracion de tipos de base, mapeo de carpetas, historial e inspector.

### Nucleo (`consolidador/core`)

- `procesador.py`: transforma DataFrames crudos al esquema estandar.
- `analizador.py`: calcula KPIs y tablas de resumen.
- `exportador.py`: genera archivos CSV/Excel y administra Parquet historico.
- `watcher.py`: detecta archivos nuevos/procesados por estructura de carpetas.
- `facturado.py`: lee, valida y persiste el archivo de facturado.
- `cruce.py`: cruza consolidado vs facturado con llaves configurables.

### Configuracion (`consolidador/config`)

- `config.json`: tipos de base, mapeo de columnas, columnas extra y llaves de cruce.
- `procesados.json`: registro de archivos ya procesados para evitar reprocesos.

## Funcionalidades clave

- Configuracion dinamica de tipos de base desde la UI.
- Carga por periodo y por historial.
- Guardado incremental por mes en Parquet.
- Eliminacion de registros de consolidado por archivo origen.
- Descarga de reportes operativos en diferentes niveles de detalle.

## Descarga de bases y reportes (3 niveles)

La aplicacion permite exportar reportes en **3 niveles de detalle**, en formatos **CSV** y **Excel**:

### Nivel 1: General
Incluye toda la base consolidada del periodo/rango seleccionado.

- Resumen por convenio
- Pendientes por facturador
- Detalle de pendientes
- Consolidado general

Archivos generados:
- `general_<mes_label>.csv`
- `general_<mes_label>.xlsx`

### Nivel 2: Por convenio
Genera un reporte independiente para cada convenio.

- Resumen del convenio
- Pendientes por facturador
- Detalle de pendientes
- Hojas por tipo de base del convenio (en Excel)

Archivos generados:
- `<convenio>_<mes_label>.csv`
- `<convenio>_<mes_label>.xlsx`

### Nivel 3: Por tipo de base
Genera un reporte independiente por cada tipo de base.

- Resumen del tipo de base
- Detalle de pendientes
- Todos los registros del tipo de base
- Incluye columnas extra configuradas para ese tipo de base

## Estructura del proyecto

```text
APPCONSOLIDADO/
  consolidador/
	app.py
	core/
	ui/
	config/
	datos/parquet/
```

## Requisitos

- Python 3.x
- pip

Dependencias usadas en el proyecto (segun codigo):

- streamlit
- pandas
- pyarrow
- openpyxl

## Instalacion

1. Clonar el repositorio y entrar al proyecto.
2. Crear y activar entorno virtual (recomendado).
3. Instalar dependencias.

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Si no existe `requirements.txt` en tu copia, instala al menos:

```bash
pip install streamlit pandas pyarrow openpyxl
```

## Ejecucion

Desde la raiz del proyecto:

```bash
streamlit run consolidador/app.py
```

## Flujo de trabajo recomendado

1. Crear tipos de base y mapear columnas en la barra lateral.
2. Cargar archivos por escaneo de carpeta o carga manual.
3. Verificar advertencias y procesar.
4. Guardar consolidado en Parquet.
5. Cargar/actualizar archivo de facturado.
6. Ejecutar cruce y revisar KPIs/reportes.
7. Exportar salidas para gestion.

## Flujo de datos (alto nivel)

1. **Entrada**: archivos Excel de bases operativas y archivo de facturado.
2. **Transformacion**: normalizacion a esquema estandar.
3. **Persistencia**: almacenamiento en Parquet por periodos.
4. **Analisis**: KPIs, resumenes y detalle de pendientes.
5. **Cruce**: comparacion contra facturado activo.
6. **Salida**: reportes descargables CSV/Excel.

## Salidas y almacenamiento

- Historial consolidado: `consolidador/datos/parquet/consolidado_*.parquet`
- Facturado guardado: `consolidador/datos/parquet/facturado.parquet`
- Cruces guardados: `consolidador/datos/parquet/cruce_*.parquet`

## Notas

- Proyecto orientado a uso interno operativo.
- Este README no incluye informacion sensible ni configuraciones privadas.


