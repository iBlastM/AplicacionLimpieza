import streamlit as st
import pandas as pd
import json
from io import BytesIO, StringIO
from limpieza import LimpiadorProgramasSociales
from georeferenciacion import GeoReferenciador, PROVEEDORES, CAPAS_GEOJSON, COLUMNAS_DISPONIBLES
from mapa_componente import mostrar_mapa_geo
from acomodador import mostrar_acomodador, _KEY_ORDEN as _KEY_ORDEN_COLS


@st.cache_data(show_spinner=False)
def _cargar_archivo(nombre: str, data: bytes) -> pd.DataFrame:
    """Carga CSV o Excel desde bytes. Cacheado por nombre+contenido del archivo."""
    if nombre.endswith('.csv'):
        return pd.read_csv(BytesIO(data), encoding='utf-8', on_bad_lines='skip')
    return pd.read_excel(BytesIO(data))


@st.cache_data(show_spinner=False)
def _contar_duplicados(df: pd.DataFrame) -> int:
    """Cuenta filas duplicadas. Cacheado para evitar recómputo en cada rerun."""
    return int(df.duplicated().sum())


@st.cache_data(show_spinner=False)
def _a_csv(df: pd.DataFrame) -> bytes:
    """Serializa el DataFrame a CSV. Cacheado por contenido del DataFrame."""
    return df.to_csv(index=False).encode('utf-8')


@st.cache_data(show_spinner=False)
def _limpiar_nulos(df: pd.DataFrame) -> pd.DataFrame:
    """Reemplaza None/NA/NaN con cadena vacía. Cacheado para evitar recómputo en cada rerun."""
    return df.replace({None: "", pd.NA: "", float('nan'): ""})


@st.cache_data(show_spinner=False)
def _a_xlsx(df: pd.DataFrame) -> bytes:
    """Serializa el DataFrame a Excel. Cacheado por contenido del DataFrame."""
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Limpio')
    return buf.getvalue()


@st.cache_data(show_spinner=False)
def df_a_geojson(df: pd.DataFrame) -> bytes:
    """Convierte un DataFrame a GeoJSON (FeatureCollection).
    Usa LATITUD/LONGITUD para geometría Point si existen; de lo contrario null.
    Cacheado por contenido del DataFrame.
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

st.title("Limpiador de Bases de Datos V6.0")
st.write("Sube tu archivo Excel de Programas Sociales para normalizarlo automáticamente.")

# 1. Subida de Archivo
archivo_subido = st.file_uploader(
    "Elige un archivo Excel (.xlsx) o CSV (.csv)",
    type=["xlsx", "csv"]
)

if archivo_subido is not None:
    try:
        # Cargar datos (cacheado: sólo se re-lee si cambia el contenido del archivo)
        nombre_archivo = archivo_subido.name
        df = _cargar_archivo(nombre_archivo, archivo_subido.getvalue())
        st.success("Archivo cargado con éxito.")

        # Limpiar resultados anteriores cuando se sube un archivo diferente
        if st.session_state.get("_ultimo_archivo") != nombre_archivo:
            for _k in ["_resultado_procesado", "_mapa_correcciones", _KEY_ORDEN_COLS]:
                st.session_state.pop(_k, None)
            st.session_state["_ultimo_archivo"] = nombre_archivo

        total_cargados = len(df)
        duplicados_cargados = _contar_duplicados(df)
        col_tot, col_dup, col_uniq = st.columns(3)
        col_tot.metric("Total de registros", f"{total_cargados:,}")
        col_dup.metric("Registros duplicados", f"{duplicados_cargados:,}")
        col_uniq.metric("Registros únicos", f"{total_cargados - duplicados_cargados:,}")

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

        # --- Mapeo de columnas ---
        mapeo_personalizado = {}
        with st.expander("Mapeo de columnas (homologación)", expanded=False):
            st.caption(
                "Define cómo se relacionan las columnas de tu archivo con los campos estándar del sistema."
            )
            modo_mapeo = st.radio(
                "Modo de detección de columnas",
                options=["Automático", "Manual"],
                horizontal=True,
                help=(
                    "**Automático:** el script detecta columnas por nombre conocido "
                    "(p.ej. 'NumExt' -> NUM\_EXT). "
                    "**Manual:** tú defines explícitamente qué columna de tu archivo "
                    "corresponde a cada campo estándar."
                ),
            )
            if modo_mapeo == "Manual":
                st.caption(
                    "Selecciona qué columna de tu archivo corresponde a cada campo estándar. "
                    "Las entradas marcadas con '—' se omiten del mapeo personalizado."
                )
                opciones_col = ["—"] + df.columns.tolist()
                CAMPOS_PERSONA = {
                    'NOMBRE(S)_DE_PILA',
                    'AP_PATERNO', 'AP_MATERNO',
                    'CURP',
                    'GENERO',
                    'FECHA_NACIMIENTO', 'EDAD',
                    'TELEFONO', 'CELULAR', 'CORREO'
                }
                CAMPOS_DIRECCION = {'CALLE', 'NUM_EXT', 'NUM_INT', 'COLONIA', 'CODIGO_POSTAL', 'DELEGACION'}
                items_persona   = [(k, v) for k, v in LimpiadorProgramasSociales.COLUMNAS_OBJETIVO.items() if k in CAMPOS_PERSONA]
                items_direccion = [(k, v) for k, v in LimpiadorProgramasSociales.COLUMNAS_OBJETIVO.items() if k in CAMPOS_DIRECCION]

                st.markdown("**Apartado Persona**")
                col_izq, col_der = st.columns(2)
                for i, (col_std, descripcion) in enumerate(items_persona):
                    with (col_izq if i % 2 == 0 else col_der):
                        seleccion = st.selectbox(
                            descripcion,
                            options=opciones_col,
                            key=f"mapeo_{col_std}",
                            help=f"Campo estándar de destino: `{col_std}`",
                        )
                        if seleccion != "—":
                            mapeo_personalizado[seleccion] = col_std

                st.markdown("**Apartado Dirección**")
                usar_dir_completa = st.checkbox(
                    "Usar columna de dirección completa (todos los campos en una sola columna, separados por comas)",
                    key="usar_dir_completa",
                    help=(
                        "Activa esta opción si tu archivo tiene una sola columna con la dirección completa "
                        "en el formato: **CALLE, NUM_EXT, NUM_INT, COLONIA, CODIGO_POSTAL**. "
                        "Si algún campo falta, las columnas individuales quedarán vacías pero la dirección "
                        "homologada se generará como: DIRECCION_COMPLETA, MUNICIPIO, ESTADO."
                    ),
                )

                if usar_dir_completa:
                    seleccion_dir = st.selectbox(
                        "Dirección Completa",
                        options=opciones_col,
                        key="mapeo_DIRECCION_COMPLETA",
                        help=(
                            "Columna de tu archivo que contiene la dirección en formato: "
                            "CALLE, NUM_EXT, NUM_INT, COLONIA, CODIGO_POSTAL"
                        ),
                    )
                    if seleccion_dir != "—":
                        mapeo_personalizado[seleccion_dir] = "DIRECCION_COMPLETA"
                        st.caption(
                            f"Se mapeará **{seleccion_dir}** → `DIRECCION_COMPLETA`. "
                            "Las columnas CALLE, NUM_EXT, NUM_INT, COLONIA y CODIGO_POSTAL "
                            "se generarán automáticamente a partir de esta columna."
                        )
                else:
                    col_izq2, col_der2 = st.columns(2)
                    for i, (col_std, descripcion) in enumerate(items_direccion):
                        with (col_izq2 if i % 2 == 0 else col_der2):
                            seleccion = st.selectbox(
                                descripcion,
                                options=opciones_col,
                                key=f"mapeo_{col_std}",
                                help=f"Campo estándar de destino: `{col_std}`",
                            )
                            if seleccion != "—":
                                mapeo_personalizado[seleccion] = col_std

                if mapeo_personalizado:
                    st.info(
                        f"{len(mapeo_personalizado)} columna(s) mapeada(s) manualmente: "
                        + ", ".join(f"{k} -> {v}" for k, v in mapeo_personalizado.items())
                    )

        aplicar_geo = st.checkbox(
            "Aplicar Georeferenciación",
            help="Geocodifica las direcciones y asigna sección electoral. "
                 "Este proceso puede tardar varios minutos dependiendo del número de direcciones únicas."
        )

        proveedor_geo = "ArcGIS"
        columnas_geo_seleccionadas = list(COLUMNAS_DISPONIBLES.keys())
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

            st.markdown("**Columnas a agregar mediante spatial join**")
            st.caption(
                "Capas disponibles: "
                + " | ".join(
                    f"**{k}**: {', '.join(v['columnas'].keys())}"
                    for k, v in CAPAS_GEOJSON.items()
                )
            )
            columnas_geo_seleccionadas = st.multiselect(
                "Columnas geográficas",
                options=list(COLUMNAS_DISPONIBLES.keys()),
                help=(
                    "Selecciona qué columnas incorporar al resultado vía spatial join. "
                    "Desmarcar columnas de capas que no necesitas acelera el proceso."
                ),
            )

        if st.button("Iniciar Limpieza"):
            if columnas_a_eliminar:
                df = df.drop(columns=columnas_a_eliminar)

            with st.status("Procesando limpieza de datos...", expanded=True) as status:
                limpiador = LimpiadorProgramasSociales(df, mapeo_columnas=mapeo_personalizado)
                df, advertencias = limpiador.ejecutar_limpieza()

                # Eliminar duplicados
                registros_antes_dedup = len(df)
                subset_dedup = ['CURP'] if 'CURP' in df.columns else None
                df = df.drop_duplicates(subset=subset_dedup)
                duplicados_eliminados = registros_antes_dedup - len(df)

            status.update(label="¡Limpieza terminada!", state="complete", expanded=False)

            if aplicar_geo:
                st.subheader("Georeferenciación")
                progreso_bar = st.progress(0, text="Iniciando geocodificación...")
                texto_progreso = st.empty()

                def actualizar_progreso(actual, total, direccion):
                    pct = actual / total
                    progreso_bar.progress(pct, text=f"Geocodificando {actual}/{total}")
                    texto_progreso.caption(f"Procesando: {direccion[:80]}...")

                geo = GeoReferenciador(proveedor=proveedor_geo, columnas_deseadas=columnas_geo_seleccionadas)
                geo.cargar_geojson()

                # Geocodificar
                df = geo.geocodificar_direcciones(df, callback=actualizar_progreso)
                progreso_bar.progress(1.0, text="Geocodificación completada")

                texto_progreso.caption("Calculando secciones electorales...")
                df = geo.asignar_seccion_electoral(df)
                texto_progreso.caption("")

                geocodificados = df['LATITUD'].notna().sum()
                total_registros = len(df)
                cols_asignadas = {
                    col: int(df[col].notna().sum())
                    for col in columnas_geo_seleccionadas
                    if col in df.columns
                }
                detalle = ", ".join(f"{col}: {n}" for col, n in cols_asignadas.items())
                msg_geo = (
                    f"Georeferenciación completada: "
                    f"{geocodificados}/{total_registros} registros geocodificados. "
                    f"Columnas asignadas — {detalle}."
                )
            else:
                msg_geo = ""

            # Guardar en session_state para que persista entre reruns
            # (antes de limpieza_nones para conservar floats en LATITUD/LONGITUD)
            st.session_state["_resultado_procesado"] = {
                "df": df,
                "geo_aplicado": aplicar_geo,
                "registros_antes": registros_antes_dedup,
                "duplicados_eliminados": duplicados_eliminados,
                "advertencias": advertencias,
                "msg_geo": msg_geo,
            }
            # Reiniciar correcciones de mapa al reprocesar
            st.session_state.pop("_mapa_correcciones", None)

        # ----------------------------------------------------------------
        # Seccion de resultados: se renderiza con lo almacenado en
        # session_state para que persista durante la interacción con el mapa.
        # ----------------------------------------------------------------
        if "_resultado_procesado" in st.session_state:
            _res = st.session_state["_resultado_procesado"]
            _df  = _res["df"]

            col_m1, col_m2, col_m3 = st.columns(3)
            col_m1.metric("Registros antes de limpieza", f"{_res['registros_antes']:,}")
            col_m2.metric("Duplicados eliminados",       f"{_res['duplicados_eliminados']:,}")
            col_m3.metric("Registros finales",           f"{len(_df):,}")

            if _res["advertencias"]:
                with st.expander(
                    f"{len(_res['advertencias'])} aviso(s) durante la normalización",
                    expanded=True,
                ):
                    for aviso in _res["advertencias"]:
                        st.warning(aviso)

            if _res["geo_aplicado"]:
                if _res["msg_geo"]:
                    st.success(_res["msg_geo"])
                # Mostrar mapa interactivo con editor de correcciones
                _df = mostrar_mapa_geo(_df)

            # Acomodador de columnas
            with st.expander("Ordenar columnas", expanded=False):
                _df = mostrar_acomodador(_df)

            # Limpieza final de nulos (cacheado: solo se recalcula si el df cambió)
            _df_final = _limpiar_nulos(_df)

            st.subheader("Resultado Final")
            st.dataframe(_df_final.head(10))

            st.subheader("Descargar resultado")
            col_csv, col_xlsx, col_geo = st.columns(3)

            # --- CSV ---
            col_csv.download_button(
                label="Descargar CSV",
                data=_a_csv(_df_final),
                file_name="base_limpia.csv",
                mime="text/csv",
                use_container_width=True,
            )

            # --- Excel ---
            col_xlsx.download_button(
                label="Descargar Excel (.xlsx)",
                data=_a_xlsx(_df_final),
                file_name="base_limpia.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )

            # --- GeoJSON ---
            tiene_geo = {'LATITUD', 'LONGITUD'}.issubset(_df.columns)
            geo_help = (
                "Incluye geometría Point para cada registro georeferenciado."
                if tiene_geo else
                "Sin coordenadas — las geometrías serán null. "
                "Activa la georeferenciación para obtener un GeoJSON con geometría."
            )
            col_geo.download_button(
                label="Descargar GeoJSON",
                data=df_a_geojson(_df),
                file_name="base_limpia.geojson",
                mime="application/geo+json",
                use_container_width=True,
                help=geo_help,
            )

    except Exception as e:
        st.error(f"Ocurrió un error al procesar el archivo: {e}")

else:
    st.info("A la espera de un archivo.")

# --- SECCIÓN DE AYUDA ---
st.divider()
with st.expander("Ayuda: ¿Cómo abrir un CSV en Excel respetando eñes y acentos?", expanded=False):
    st.markdown(
        """
        Si al abrir un archivo CSV en Excel los caracteres especiales (ñ, á, é, etc.)
        aparecen corruptos, sigue estos pasos para una importación limpia:

        1. Abre Excel con un **libro en blanco** (no hagas doble clic directo en el archivo CSV).
        2. Ve a la pestaña **Datos** en la cinta superior.
        3. Selecciona **Obtener datos › Desde un archivo › Desde el texto/CSV**.
        4. Busca tu archivo y haz clic en **Importar**. Se abrirá una ventana de vista previa.
        5. **El paso clave:** en el menú desplegable **Origen del archivo**, selecciona:
           - **65001: Unicode (UTF-8)** ← el más común.
           - Si no funciona, prueba con **1252: Windows (Europa occidental)**.
        6. Verifica en la vista previa que las eñes y acentos se vean correctamente.
        7. Asegúrate de que el **Delimitador** sea el correcto (coma, punto y coma o tabulación)
           para que los datos se separen en columnas.
        8. Haz clic en **Cargar**.
        """
    )