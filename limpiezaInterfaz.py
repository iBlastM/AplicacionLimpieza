import streamlit as st
import pandas as pd
from io import BytesIO
from limpieza import LimpiadorProgramasSociales
from georeferenciacion import GeoReferenciador, PROVEEDORES

# --- INTERFAZ DE STREAMLIT ---

st.set_page_config(page_title="Limpiador de Programas Sociales", layout="wide")

st.title("Limpiador de Bases de Datos V3.0")
st.write("Sube tu archivo Excel de Programas Sociales para normalizarlo automáticamente.")

# 1. Subida de Archivo
archivo_subido = st.file_uploader("Elige un archivo Excel (.xlsx)", type=["xlsx"])

if archivo_subido is not None:
    try:
        # Cargar datos
        df = pd.read_excel(archivo_subido)
        st.success("Archivo cargado con éxito.")
        
        with st.expander("Ver vista previa de datos originales"):
            st.dataframe(df.head(10))

        # Opción de georeferenciación
        aplicar_geo = st.checkbox(
            "Aplicar Georeferenciación",
            help="Geocodifica las direcciones y asigna sección electoral. "
                 "Este proceso puede tardar varios minutos dependiendo del número de direcciones únicas."
        )

        # Selector de proveedor (solo visible si el checkbox está activo)
        proveedor_geo = "ArcGIS"
        if aplicar_geo:
            opciones = list(PROVEEDORES.keys())
            descripciones = [f"{k} — {v['descripcion']}" for k, v in PROVEEDORES.items()]
            seleccion = st.radio(
                "Proveedor de geocodificación",
                options=opciones,
                captions=[v["descripcion"] for v in PROVEEDORES.values()],
                index=0,
                horizontal=True,
            )
            proveedor_geo = seleccion

        if st.button("Iniciar Limpieza"):
            with st.status("Procesando limpieza de datos...", expanded=True) as status:
                limpiador = LimpiadorProgramasSociales(df)
                df = limpiador.ejecutar_limpieza()
                
            status.update(label="¡Limpieza terminada!", state="complete", expanded=False)

            # --- Georeferenciación opcional ---
            if aplicar_geo:
                st.subheader("Georeferenciación")
                progreso_bar = st.progress(0, text="Iniciando geocodificación...")
                texto_progreso = st.empty()

                def actualizar_progreso(actual, total, direccion):
                    pct = actual / total
                    progreso_bar.progress(pct, text=f"Geocodificando {actual}/{total}")
                    texto_progreso.caption(f"Procesando: {direccion[:80]}...")

                geo = GeoReferenciador(proveedor=proveedor_geo)
                geo.cargar_geojson()

                # Geocodificar
                df = geo.geocodificar_direcciones(df, callback=actualizar_progreso)
                progreso_bar.progress(1.0, text="Geocodificación completada")

                # Asignar sección electoral
                texto_progreso.caption("Calculando secciones electorales...")
                df = geo.asignar_seccion_electoral(df)
                texto_progreso.caption("")

                geocodificados = df['LATITUD'].notna().sum()
                total_registros = len(df)
                con_seccion = df['SECCION_ELECTORAL'].notna().sum()
                st.success(
                    f"Georeferenciación completada: "
                    f"{geocodificados}/{total_registros} registros geocodificados, "
                    f"{con_seccion} con sección electoral asignada."
                )

            # 2. Vista previa del resultado
            st.subheader("Resultado Final")
            st.dataframe(df.head(10))

            # 3. Descarga del archivo
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Limpio')
            
            st.download_button(
                label="Descargar Base Limpia (Excel)",
                data=output.getvalue(),
                file_name="base_limpia.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

    except Exception as e:
        st.error(f"Ocurrió un error al procesar el archivo: {e}")

else:
    st.info("A la espera de un archivo Excel.")