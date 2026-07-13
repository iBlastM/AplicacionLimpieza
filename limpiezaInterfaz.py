import streamlit as st

from src.acomodador import _KEY_ORDEN as _KEY_ORDEN_COLS
from src.cache_helpers import cargar_archivo, limpiar_nulos
from src.ui_secciones import (
    ejecutar_procesamiento,
    seccion_ayuda,
    seccion_columnas_eliminar,
    seccion_cruce_colonia_secciones,
    seccion_descargas,
    seccion_estado_municipio,
    seccion_georeferenciacion_config,
    seccion_mapeo_columnas,
    seccion_metricas_carga,
    seccion_resultados,
)

st.set_page_config(page_title="Limpiador de Programas Sociales", layout="wide")

st.title("Limpiador de Bases de Datos V6.0")
st.write("Sube tu archivo Excel de Programas Sociales para normalizarlo automáticamente.")

archivo_subido = st.file_uploader(
    "Elige un archivo Excel (.xlsx), CSV (.csv) o Parquet (.parquet)",
    type=["xlsx", "csv", "parquet"],
)

if archivo_subido is not None:
    try:
        nombre_archivo = archivo_subido.name
        df = cargar_archivo(nombre_archivo, archivo_subido.getvalue())
        st.success("Archivo cargado con éxito.")

        # Limpiar estado al cambiar de archivo
        if st.session_state.get("_ultimo_archivo") != nombre_archivo:
            for _k in ["_resultado_procesado", "_mapa_correcciones", _KEY_ORDEN_COLS,
                       "_descarga_datos", "_descarga_formato"]:
                st.session_state.pop(_k, None)
            st.session_state["_ultimo_archivo"] = nombre_archivo

        seccion_metricas_carga(df)
        columnas_a_eliminar                    = seccion_columnas_eliminar(df)
        sobrescribir_em, estado_sel, municipio_sel = seccion_estado_municipio()
        mapeo_personalizado                    = seccion_mapeo_columnas(df)
        aplicar_geo, proveedor_geo, columnas_geo = seccion_georeferenciacion_config()
        aplicar_cruce, anio_secciones = seccion_cruce_colonia_secciones()

        if st.button("Iniciar Limpieza"):
            resultado = ejecutar_procesamiento(
                df, columnas_a_eliminar, mapeo_personalizado,
                aplicar_geo, proveedor_geo, columnas_geo,
                aplicar_cruce, anio_secciones,
                estado_sel, municipio_sel,
                sobrescribir_em,
            )
            st.session_state["_resultado_procesado"] = resultado
            st.session_state.pop("_mapa_correcciones", None)
            st.session_state.pop("_descarga_datos", None)
            st.session_state.pop("_descarga_formato", None)

        if "_resultado_procesado" in st.session_state:
            df_editado = seccion_resultados(st.session_state["_resultado_procesado"])
            df_final   = limpiar_nulos(df_editado)
            seccion_descargas(df_final, df_editado)

    except Exception as e:
        st.error(f"Ocurrió un error al procesar el archivo: {e}")

else:
    st.info("A la espera de un archivo.")

seccion_ayuda()