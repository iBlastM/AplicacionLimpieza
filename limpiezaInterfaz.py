import streamlit as st
import pandas as pd
import json
from io import BytesIO, StringIO
from limpieza import LimpiadorProgramasSociales
from georeferenciacion import GeoReferenciador, PROVEEDORES


def df_a_geojson(df: pd.DataFrame) -> bytes:
    """Convierte un DataFrame a GeoJSON (FeatureCollection).
    Usa LATITUD/LONGITUD para geometría Point si existen; de lo contrario null.
    """
    tiene_geo = {'LATITUD', 'LONGITUD'}.issubset(df.columns)
    features = []
    for _, fila in df.iterrows():
        if tiene_geo and pd.notna(fila.get('LATITUD')) and pd.notna(fila.get('LONGITUD')):
            geometry = {
                "type": "Point",
                "coordinates": [float(fila['LONGITUD']), float(fila['LATITUD'])]
            }
        else:
            geometry = None
        properties = {col: (None if pd.isna(v) else v) for col, v in fila.items()}
        features.append({"type": "Feature", "geometry": geometry, "properties": properties})
    return json.dumps({"type": "FeatureCollection", "features": features}, ensure_ascii=False, indent=2).encode('utf-8')

# --- INTERFAZ DE STREAMLIT ---

st.set_page_config(page_title="Limpiador de Programas Sociales", layout="wide")

st.title("Limpiador de Bases de Datos V4.0")
st.write("Sube tu archivo Excel de Programas Sociales para normalizarlo automáticamente.")

# 1. Subida de Archivo
archivo_subido = st.file_uploader(
    "Elige un archivo Excel (.xlsx) o CSV (.csv)",
    type=["xlsx", "csv"]
)

if archivo_subido is not None:
    try:
        # Cargar datos
        nombre_archivo = archivo_subido.name
        if nombre_archivo.endswith('.csv'):
            df = pd.read_csv(archivo_subido, encoding='utf-8', on_bad_lines='skip')
        else:
            df = pd.read_excel(archivo_subido)
        st.success("Archivo cargado con éxito.")
        
        with st.expander("Ver vista previa de datos originales"):
            st.dataframe(df.head(10))

        # --- Selección de columnas ---
        with st.expander("Columnas a eliminar (opcional)", expanded=False):
            st.caption(
                "Selecciona las columnas que deseas excluir antes de la normalización. "
                "Si eliminas una columna que otro paso necesita, se mostrará un aviso "
                "pero el proceso continuará sin detenerse."
            )
            columnas_a_eliminar = st.multiselect(
                "Columnas a eliminar",
                options=df.columns.tolist(),
                default=[],
                placeholder="Ninguna — todas las columnas se conservarán",
                label_visibility="collapsed",
            )
            if columnas_a_eliminar:
                st.warning(f"Se eliminarán {len(columnas_a_eliminar)} columna(s): {', '.join(columnas_a_eliminar)}")

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
            # Aplicar eliminación de columnas seleccionadas
            if columnas_a_eliminar:
                df = df.drop(columns=columnas_a_eliminar)

            with st.status("Procesando limpieza de datos...", expanded=True) as status:
                limpiador = LimpiadorProgramasSociales(df)
                df, advertencias = limpiador.ejecutar_limpieza()
                
            status.update(label="¡Limpieza terminada!", state="complete", expanded=False)

            # Mostrar advertencias del pipeline
            if advertencias:
                with st.expander(f"⚠️ {len(advertencias)} aviso(s) durante la normalización", expanded=True):
                    for aviso in advertencias:
                        st.warning(aviso)

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

            # 3. Opciones de descarga
            st.subheader("Descargar resultado")
            col_csv, col_xlsx, col_geo = st.columns(3)

            # --- CSV ---
            csv_bytes = df.to_csv(index=False).encode('utf-8')
            col_csv.download_button(
                label="Descargar CSV",
                data=csv_bytes,
                file_name="base_limpia.csv",
                mime="text/csv",
                use_container_width=True,
            )

            # --- Excel ---
            output_xlsx = BytesIO()
            with pd.ExcelWriter(output_xlsx, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Limpio')
            col_xlsx.download_button(
                label="Descargar Excel (.xlsx)",
                data=output_xlsx.getvalue(),
                file_name="base_limpia.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )

            # --- GeoJSON ---
            tiene_geo = {'LATITUD', 'LONGITUD'}.issubset(df.columns)
            geo_help = (
                "Incluye geometría Point para cada registro georeferenciado."
                if tiene_geo else
                "Sin coordenadas — las geometrías serán null. "
                "Activa la georeferenciación para obtener un GeoJSON con geometría."
            )
            col_geo.download_button(
                label="Descargar GeoJSON",
                data=df_a_geojson(df),
                file_name="base_limpia.geojson",
                mime="application/geo+json",
                use_container_width=True,
                help=geo_help,
            )

    except Exception as e:
        st.error(f"Ocurrió un error al procesar el archivo: {e}")

else:
    st.info("A la espera de un archivo Excel.")