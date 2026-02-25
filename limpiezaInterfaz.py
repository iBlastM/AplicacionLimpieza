import streamlit as st
import pandas as pd
from io import BytesIO
from limpieza import LimpiadorProgramasSociales

# --- INTERFAZ DE STREAMLIT ---

st.set_page_config(page_title="Limpiador de Programas Sociales", layout="wide")

st.title("Limpiador de Bases de Datos V1.0")
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

        if st.button("Iniciar Limpieza"):
            with st.status("Procesando datos...", expanded=True) as status:
                limpiador = LimpiadorProgramasSociales(df)
                df = limpiador.ejecutar_limpieza()
                
            status.update(label="¡Limpieza terminada!", state="complete", expanded=False)

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