import pandas as pd
import streamlit as st

from src.acomodador import mostrar_acomodador
from src.cache_helpers import a_csv, a_parquet, a_xlsx, cargar_estados_municipios, contar_duplicados, df_a_geojson
from src.georeferenciacion import (
    CAPAS_GEOJSON,
    COLUMNAS_DISPONIBLES,
    GeoReferenciador,
    PROVEEDORES,
    calcular_secciones_por_colonia,
)
from src.limpieza import LimpiadorProgramasSociales
from src.mapa_componente import mostrar_mapa_geo


# ── Métricas de carga ─────────────────────────────────────────────────────

def seccion_metricas_carga(df: pd.DataFrame) -> None:
    total = len(df)
    duplicados = contar_duplicados(df)
    col_tot, col_dup, col_uniq = st.columns(3)
    col_tot.metric("Total de registros",    f"{total:,}")
    col_dup.metric("Registros duplicados",  f"{duplicados:,}")
    col_uniq.metric("Registros únicos",     f"{total - duplicados:,}")
    with st.expander("Ver vista previa de datos originales"):
        st.dataframe(df.head(10))


# ── Columnas a eliminar ───────────────────────────────────────────────────

def seccion_columnas_eliminar(df: pd.DataFrame) -> list[str]:
    with st.expander("Columnas a eliminar (opcional)", expanded=False):
        st.caption(
            "Selecciona las columnas que deseas excluir antes de la normalización. "
            "Si eliminas una columna que otro paso necesita, se mostrará un aviso "
            "pero el proceso continuará sin detenerse."
        )
        columnas = st.multiselect(
            "Columnas a eliminar",
            options=df.columns.tolist(),
            default=[],
            placeholder="Ninguna — todas las columnas se conservarán",
            label_visibility="collapsed",
        )
        if columnas:
            st.warning(f"Se eliminarán {len(columnas)} columna(s): {', '.join(columnas)}")
    return columnas


# ── Estado y Municipio ────────────────────────────────────────────────────

def seccion_estado_municipio() -> tuple[str, str]:
    """Selección en cascada de estado y municipio.

    Devuelve (estado, municipio) en MAYÚSCULAS para asignarlos a toda la base.
    Primero se elige el estado y luego el municipio correspondiente a ese estado.
    """
    _ESTADO_DEFECTO    = "Querétaro"
    _MUNICIPIO_DEFECTO = "Corregidora"

    catalogo = cargar_estados_municipios()

    with st.expander("Estado y Municipio de la base", expanded=True):
        if not catalogo:
            st.warning(
                "No se encontró el catálogo de estados y municipios "
                "(data/estados_municipios.json). Se usarán los valores por defecto: "
                f"{_MUNICIPIO_DEFECTO}, {_ESTADO_DEFECTO}."
            )
            return _ESTADO_DEFECTO.upper(), _MUNICIPIO_DEFECTO.upper()

        st.caption(
            "Selecciona el estado y el municipio que se asignarán a todos los registros "
            "de la base. Primero elige el estado; el listado de municipios se actualizará "
            "en consecuencia."
        )

        estados = list(catalogo.keys())
        idx_estado = estados.index(_ESTADO_DEFECTO) if _ESTADO_DEFECTO in estados else 0

        col_estado, col_municipio = st.columns(2)
        with col_estado:
            estado = st.selectbox(
                "Estado",
                options=estados,
                index=idx_estado,
                key="_sel_estado",
            )

        municipios = catalogo.get(estado, [])
        idx_municipio = (
            municipios.index(_MUNICIPIO_DEFECTO)
            if _MUNICIPIO_DEFECTO in municipios else 0
        )
        with col_municipio:
            municipio = st.selectbox(
                "Municipio",
                options=municipios,
                index=idx_municipio,
                key="_sel_municipio",
                placeholder="Selecciona un municipio",
            ) if municipios else ""

    return str(estado).strip().upper(), str(municipio).strip().upper()


# ── Mapeo de columnas ─────────────────────────────────────────────────────

def seccion_mapeo_columnas(df: pd.DataFrame) -> dict:
    mapeo: dict = {}
    with st.expander("Mapeo de columnas (homologación)", expanded=False):
        st.caption(
            "Define cómo se relacionan las columnas de tu archivo con los campos estándar del sistema."
        )
        modo = st.radio(
            "Modo de detección de columnas",
            options=["Automático", "Manual"],
            horizontal=True,
            help=(
                "**Automático:** el script detecta columnas por nombre conocido "
                "(p.ej. 'NumExt' -> NUM\\_EXT). "
                "**Manual:** tú defines explícitamente qué columna de tu archivo "
                "corresponde a cada campo estándar."
            ),
        )
        if modo == "Manual":
            mapeo = _mapeo_manual(df)
    return mapeo


def _mapeo_manual(df: pd.DataFrame) -> dict:
    mapeo: dict = {}
    opciones = ["—"] + df.columns.tolist()

    _CAMPOS_PERSONA   = {
        'NOMBRE(S)_DE_PILA', 'AP_PATERNO', 'AP_MATERNO', 'CURP',
        'GENERO', 'FECHA_NACIMIENTO', 'EDAD', 'TELEFONO', 'CELULAR', 'CORREO',
    }
    _CAMPOS_DIRECCION = {'CALLE', 'NUM_EXT', 'NUM_INT', 'COLONIA', 'CODIGO_POSTAL', 'DELEGACION'}

    items_persona   = [(k, v) for k, v in LimpiadorProgramasSociales.COLUMNAS_OBJETIVO.items()
                       if k in _CAMPOS_PERSONA]
    items_direccion = [(k, v) for k, v in LimpiadorProgramasSociales.COLUMNAS_OBJETIVO.items()
                       if k in _CAMPOS_DIRECCION]

    st.caption(
        "Selecciona qué columna de tu archivo corresponde a cada campo estándar. "
        "Las entradas marcadas con '—' se omiten del mapeo personalizado."
    )

    st.markdown("**Apartado Persona**")
    col_izq, col_der = st.columns(2)
    for i, (col_std, desc) in enumerate(items_persona):
        with (col_izq if i % 2 == 0 else col_der):
            sel = st.selectbox(desc, options=opciones, key=f"mapeo_{col_std}",
                               help=f"Campo estándar de destino: `{col_std}`")
            if sel != "—":
                mapeo[sel] = col_std

    st.markdown("**Apartado Dirección**")
    usar_dir_completa = st.checkbox(
        "Usar columna de dirección completa (todos los campos en una sola columna, separados por comas)",
        key="usar_dir_completa",
        help=(
            "Activa esta opción si tu archivo tiene una sola columna con la dirección completa "
            "en el formato: **CALLE, NUM_EXT, NUM_INT, COLONIA, CODIGO_POSTAL**."
        ),
    )
    if usar_dir_completa:
        sel = st.selectbox(
            "Dirección Completa", options=opciones, key="mapeo_DIRECCION_COMPLETA",
            help="Columna con dirección en formato: CALLE, NUM_EXT, NUM_INT, COLONIA, CODIGO_POSTAL",
        )
        if sel != "—":
            mapeo[sel] = "DIRECCION_COMPLETA"
            st.caption(
                f"Se mapeará **{sel}** → `DIRECCION_COMPLETA`. "
                "Las columnas CALLE, NUM_EXT, NUM_INT, COLONIA y CODIGO_POSTAL "
                "se generarán automáticamente a partir de esta columna."
            )
    else:
        col_izq2, col_der2 = st.columns(2)
        for i, (col_std, desc) in enumerate(items_direccion):
            with (col_izq2 if i % 2 == 0 else col_der2):
                sel = st.selectbox(desc, options=opciones, key=f"mapeo_{col_std}",
                                   help=f"Campo estándar de destino: `{col_std}`")
                if sel != "—":
                    mapeo[sel] = col_std

    if mapeo:
        st.info(
            f"{len(mapeo)} columna(s) mapeada(s) manualmente: "
            + ", ".join(f"{k} -> {v}" for k, v in mapeo.items())
        )
    return mapeo


# ── Configuración de georeferenciación ────────────────────────────────────

def seccion_georeferenciacion_config() -> tuple[bool, str, list]:
    """Devuelve (aplicar_geo, proveedor, columnas_seleccionadas)."""
    aplicar = st.checkbox(
        "Aplicar Georeferenciación",
        help=(
            "Geocodifica las direcciones y asigna sección electoral. "
            "Este proceso puede tardar varios minutos dependiendo del número de direcciones únicas."
        ),
    )
    proveedor = "ArcGIS"
    columnas  = list(COLUMNAS_DISPONIBLES.keys())

    if aplicar:
        proveedor = st.radio(
            "Proveedor de geocodificación",
            options=list(PROVEEDORES.keys()),
            captions=[v["descripcion"] for v in PROVEEDORES.values()],
            index=0,
            horizontal=True,
        )
        st.markdown("**Columnas a agregar mediante spatial join**")
        st.caption(
            "Capas disponibles: "
            + " | ".join(
                f"**{k}**: {', '.join(v['columnas'].keys())}"
                for k, v in CAPAS_GEOJSON.items()
            )
        )
        columnas = st.multiselect(
            "Columnas geográficas",
            options=list(COLUMNAS_DISPONIBLES.keys()),
            help="Selecciona qué columnas incorporar al resultado vía spatial join. "
                 "Desmarcar columnas de capas que no necesitas acelera el proceso.",
        )
    return aplicar, proveedor, columnas


# ── Cruce Colonia → Secciones ────────────────────────────────────────────

def seccion_cruce_colonia_secciones() -> tuple[bool, str]:
    """Devuelve (aplicar_cruce, anio_secciones)."""
    aplicar = st.checkbox(
        "Verificar secciones por colonia",
        help=(
            "Busca en qué secciones electorales cae el polígono de cada colonia. "
            "Requiere que la base tenga una columna de COLONIA. "
            "Agrega la columna SECCIONES_DE_COLONIA con la lista de secciones donde cae."
        ),
    )
    anio = "2024"
    if aplicar:
        anio = st.radio(
            "Secciones electorales a usar",
            options=["2024", "2025"],
            horizontal=True,
            help="Selecciona el año de las secciones electorales contra las cuales cruzar las colonias.",
        )
    return aplicar, anio


# ── Procesamiento ─────────────────────────────────────────────────────────

def ejecutar_procesamiento(
    df: pd.DataFrame,
    columnas_a_eliminar: list[str],
    mapeo_personalizado: dict,
    aplicar_geo: bool,
    proveedor_geo: str,
    columnas_geo: list[str],
    aplicar_cruce_colonias: bool = False,
    anio_secciones: str = "2024",
    estado: str = "QUERETARO",
    municipio: str = "CORREGIDORA",
) -> dict:
    if columnas_a_eliminar:
        df = df.drop(columns=columnas_a_eliminar)

    with st.status("Procesando limpieza de datos...", expanded=True) as status:
        limpiador = LimpiadorProgramasSociales(
            df,
            mapeo_columnas=mapeo_personalizado,
            estado=estado,
            municipio=municipio,
        )
        df, advertencias = limpiador.ejecutar_limpieza()

        registros_antes = len(df)
        subset_dedup = ['CURP'] if 'CURP' in df.columns else None
        df = df.drop_duplicates(subset=subset_dedup)
        duplicados_eliminados = registros_antes - len(df)

    status.update(label="¡Limpieza terminada!", state="complete", expanded=False)

    msg_geo = ""
    if aplicar_geo:
        df, msg_geo = _ejecutar_georeferenciacion(df, proveedor_geo, columnas_geo)

    msg_cruce = ""
    if aplicar_cruce_colonias:
        df, msg_cruce = _ejecutar_cruce_colonias(df, anio_secciones)

    return {
        "df":                   df,
        "geo_aplicado":         aplicar_geo,
        "registros_antes":      registros_antes,
        "duplicados_eliminados": duplicados_eliminados,
        "advertencias":         advertencias,
        "msg_geo":              msg_geo,
        "msg_cruce":            msg_cruce,
    }


def _ejecutar_georeferenciacion(
    df: pd.DataFrame, proveedor: str, columnas_deseadas: list[str]
) -> tuple[pd.DataFrame, str]:
    st.subheader("Georeferenciación")
    progreso_bar   = st.progress(0, text="Iniciando geocodificación...")
    texto_progreso = st.empty()

    def _progreso(actual, total, direccion):
        progreso_bar.progress(actual / total, text=f"Geocodificando {actual}/{total}")
        texto_progreso.caption(f"Procesando: {direccion[:80]}...")

    geo = GeoReferenciador(proveedor=proveedor, columnas_deseadas=columnas_deseadas)
    geo.cargar_geojson()
    df = geo.geocodificar_direcciones(df, callback=_progreso)
    progreso_bar.progress(1.0, text="Geocodificación completada")

    texto_progreso.caption("Calculando secciones electorales...")
    df = geo.asignar_seccion_electoral(df)
    texto_progreso.caption("")

    geocodificados = int(df['LATITUD'].notna().sum())
    cols_asignadas = {col: int(df[col].notna().sum()) for col in columnas_deseadas if col in df.columns}
    detalle = ", ".join(f"{col}: {n}" for col, n in cols_asignadas.items())
    msg = (
        f"Georeferenciación completada: {geocodificados}/{len(df)} registros geocodificados. "
        f"Columnas asignadas — {detalle}."
    )
    return df, msg


def _ejecutar_cruce_colonias(
    df: pd.DataFrame, anio_secciones: str
) -> tuple[pd.DataFrame, str]:
    col_colonia = "COLONIA"
    if col_colonia not in df.columns:
        return df, "No se encontró columna COLONIA en la base — cruce omitido."

    with st.status("Calculando secciones por colonia...", expanded=True) as status:
        df = calcular_secciones_por_colonia(df, anio_secciones=anio_secciones, col_colonia=col_colonia)
        status.update(label="¡Cruce colonia–secciones terminado!", state="complete", expanded=False)

    con_secciones = int((df["SECCIONES_DE_COLONIA"] != "").sum())
    multi_seccion = int(df["SECCIONES_DE_COLONIA"].str.contains(",").sum())
    msg = (
        f"Cruce colonia→secciones ({anio_secciones}): "
        f"{con_secciones}/{len(df)} registros con secciones asignadas. "
        f"{multi_seccion} registros en colonias que caen en múltiples secciones."
    )
    return df, msg


# ── Resultados ────────────────────────────────────────────────────────────

def seccion_resultados(res: dict) -> pd.DataFrame:
    """Renderiza métricas de resultado y devuelve el df (posiblemente editado)."""
    df = res["df"]

    col_m1, col_m2, col_m3 = st.columns(3)
    col_m1.metric("Registros antes de limpieza", f"{res['registros_antes']:,}")
    col_m2.metric("Duplicados eliminados",        f"{res['duplicados_eliminados']:,}")
    col_m3.metric("Registros finales",            f"{len(df):,}")

    if res["advertencias"]:
        with st.expander(
            f"{len(res['advertencias'])} aviso(s) durante la normalización", expanded=True
        ):
            for aviso in res["advertencias"]:
                st.warning(aviso)

    if res["geo_aplicado"]:
        if res["msg_geo"]:
            st.success(res["msg_geo"])
        df = mostrar_mapa_geo(df)

    if res.get("msg_cruce"):
        st.info(res["msg_cruce"])

    with st.expander("Ordenar columnas", expanded=False):
        df = mostrar_acomodador(df)

    return df


# ── Descargas ─────────────────────────────────────────────────────────────

def seccion_descargas(df_final: pd.DataFrame, df_geo: pd.DataFrame) -> None:
    st.subheader("Resultado Final")
    st.dataframe(df_final.head(10))

    st.subheader("Descargar resultado")

    _FORMATOS = {
        "CSV (.csv)":         "csv",
        "Excel (.xlsx)":      "xlsx",
        "Parquet (.parquet)": "parquet",
        "GeoJSON (.geojson)": "geojson",
    }
    _MIME = {
        "csv":     "text/csv",
        "xlsx":    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "parquet": "application/octet-stream",
        "geojson": "application/geo+json",
    }

    formato_label = st.selectbox(
        "Selecciona el formato de descarga",
        options=list(_FORMATOS.keys()),
        key="_formato_descarga",
    )
    formato = _FORMATOS[formato_label]

    tiene_geo = {"LATITUD", "LONGITUD"}.issubset(df_geo.columns)
    if formato == "geojson" and not tiene_geo:
        st.warning(
            "Sin coordenadas — las geometrías serán null. "
            "Activa la georeferenciación para obtener un GeoJSON con geometría."
        )

    if st.button("Generar archivo", key="_btn_generar_descarga"):
        with st.spinner("Generando archivo…"):
            if formato == "csv":
                datos = a_csv(df_final)
            elif formato == "xlsx":
                datos = a_xlsx(df_final)
            elif formato == "parquet":
                datos = a_parquet(df_final)
            else:  # geojson
                datos = df_a_geojson(df_geo)
        st.session_state["_descarga_datos"]   = datos
        st.session_state["_descarga_formato"] = formato

    if st.session_state.get("_descarga_formato") == formato and "_descarga_datos" in st.session_state:
        st.download_button(
            label=f"Descargar {formato_label}",
            data=st.session_state["_descarga_datos"],
            file_name=f"base_limpia.{formato}",
            mime=_MIME[formato],
            use_container_width=True,
        )


# ── Ayuda ─────────────────────────────────────────────────────────────────

def seccion_ayuda() -> None:
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
