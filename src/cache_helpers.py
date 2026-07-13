import json
from io import BytesIO
from pathlib import Path

import pandas as pd
import streamlit as st


_CATALOGO_ESTADOS_MUNICIPIOS = (
    Path(__file__).resolve().parent.parent / "json" / "estados_municipios.json"
)


@st.cache_data(show_spinner=False)
def cargar_estados_municipios() -> dict[str, list[str]]:
    """Carga el catálogo {estado: [municipios]} de México desde json/estados_municipios.json.

    Devuelve un diccionario ordenado alfabéticamente por estado, con la lista de
    municipios de cada estado también ordenada. Si el archivo no existe o no se puede
    leer, devuelve un diccionario vacío.
    """
    try:
        with open(_CATALOGO_ESTADOS_MUNICIPIOS, encoding="utf-8") as fh:
            datos = json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}

    return {
        estado: sorted(municipios)
        for estado, municipios in sorted(datos.items())
    }


@st.cache_data(show_spinner=False)
def cargar_archivo(nombre: str, data: bytes) -> pd.DataFrame:
    """Carga CSV, Excel o Parquet desde bytes. Cacheado por nombre + contenido."""
    if nombre.endswith('.csv'):
        return pd.read_csv(BytesIO(data), encoding='utf-8', on_bad_lines='skip')
    if nombre.endswith('.parquet'):
        df = pd.read_parquet(BytesIO(data))
        # Parquet puede traer datetime64[us/ms/ns]; normalizar a us para evitar overflow.
        for col in df.select_dtypes(include=['datetime64']).columns:
            df[col] = df[col].astype('datetime64[us]')
        return df
    return pd.read_excel(BytesIO(data))


@st.cache_data(show_spinner=False)
def contar_duplicados(df: pd.DataFrame) -> int:
    return int(df.duplicated().sum())


@st.cache_data(show_spinner=False)
def limpiar_nulos(df: pd.DataFrame) -> pd.DataFrame:
    return df.replace({None: "", pd.NA: "", float('nan'): ""})


@st.cache_data(show_spinner=False)
def a_csv(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode('utf-8')


@st.cache_data(show_spinner=False)
def a_xlsx(df: pd.DataFrame) -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Limpio')
    return buf.getvalue()


@st.cache_data(show_spinner=False)
def a_parquet(df: pd.DataFrame) -> bytes:
    buf = BytesIO()
    df.to_parquet(buf, index=False, engine='pyarrow')
    return buf.getvalue()


@st.cache_data(show_spinner=False)
def df_a_geojson(df: pd.DataFrame) -> bytes:
    """Convierte DataFrame a GeoJSON usando LATITUD/LONGITUD si existen."""
    tiene_geo = {'LATITUD', 'LONGITUD'}.issubset(df.columns)
    features = []
    for _, fila in df.iterrows():
        if tiene_geo and pd.notna(fila.get('LATITUD')) and pd.notna(fila.get('LONGITUD')):
            geometry = {
                "type": "Point",
                "coordinates": [float(fila['LONGITUD']), float(fila['LATITUD'])],
            }
        else:
            geometry = None
        properties = {col: (None if pd.isna(v) else v) for col, v in fila.items()}
        features.append({"type": "Feature", "geometry": geometry, "properties": properties})
    return json.dumps(
        {"type": "FeatureCollection", "features": features},
        ensure_ascii=False,
        indent=2,
    ).encode('utf-8')
