import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from geopy.geocoders import Nominatim, ArcGIS, Photon
from geopy.extra.rate_limiter import RateLimiter
from concurrent.futures import ThreadPoolExecutor, as_completed
import os


# ======================================================================
# Proveedores de geocodificación disponibles
# ======================================================================
PROVEEDORES = {
    "ArcGIS": {
        "descripcion": "Rápido y gratuito (~20 req/s con concurrencia). Sin API key.",
        "workers": 8,
        "delay": 0.05,
    },
    "Photon": {
        "descripcion": "Basado en OpenStreetMap, rápido y sin límites estrictos.",
        "workers": 6,
        "delay": 0.1,
    },
    "Nominatim": {
        "descripcion": "OpenStreetMap oficial. Lento (~1 req/s). Ideal para pocas direcciones.",
        "workers": 1,
        "delay": 1.1,
    },
}


# ======================================================================
# Capas GeoJSON disponibles para spatial join
# Cada capa define: ruta al archivo y mapeo {columna_origen: columna_salida}
# ======================================================================
_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CAPAS_GEOJSON: dict = {
    "SECCION_ELECT_SABANA_2024": {
        "path": os.path.join(_BASE, "GEOJSON/SECCION_ELECT_SABANA_2024.geojson"),
        "columnas": {
            "SECCION_ELECTORAL_2024": "SECCION",
            "DISTRITO_FEDERAL_2024": "DISTRITO_F",
            "DISTRITO_LOCAL_2024": "DISTRITO_L",
        },
    },
    "COL_LOC_EDO_QRO": {
        "path": os.path.join(_BASE, "GEOJSON/COL_LOC_EDO_QRO.geojson"),
        "columnas": {"COLONIA_GEO": "NOM_COL"},
    },
    "CP_EDO_QRO": {
        "path": os.path.join(_BASE, "GEOJSON/CP_EDO_QRO.geojson"),
        "columnas": {"CODIGO_POSTAL_GEO": "C_P"},
    },
    "DELEGACIONES_QRO_CORR": {
        "path": os.path.join(_BASE, "GEOJSON/DELEGACIONES_QRO_CORR.geojson"),
        "columnas": {"DELEGACION": "NOM_DEL"},
    },
    "SE_EDO_QRO_24_25": {
        "path": os.path.join(_BASE, "GEOJSON/SE_EDO_QRO_24_25.geojson"),
        "columnas": {
            "CIRCUNSCRIPCION": "CIRCUNSCRI",
            "DISTRITO_FEDERAL_2025": "25_D_FEDERAL",
            "DISTRITO_LOCAL_2025": "25_D_LOCAL",
            "SECCION_ELECTORAL_2025": "25_SECCION",
        },
    },
}

# Mapa: nombre_columna_salida (key de columnas) -> (capa_key, columna_origen_geojson)
COLUMNAS_DISPONIBLES: dict[str, tuple[str, str]] = {
    out_col: (capa_key, src_col)
    for capa_key, capa in CAPAS_GEOJSON.items()
    for out_col, src_col in capa["columnas"].items()
}


def _crear_geocoder(proveedor: str):
    """Crea la instancia del geocoder según el proveedor seleccionado."""
    if proveedor == "ArcGIS":
        return ArcGIS(timeout=10)
    elif proveedor == "Photon":
        return Photon(user_agent="metrix_programas_sociales", timeout=10)
    elif proveedor == "Nominatim":
        return Nominatim(user_agent="metrix_programas_sociales", timeout=10)
    else:
        raise ValueError(f"Proveedor desconocido: {proveedor}")


# ======================================================================
# Cruce Colonia → Secciones Electorales (sin geocodificación)
# ======================================================================

CAPAS_SECCIONES = {
    "2024": {
        "path": os.path.join(_BASE, "GEOJSON/SECCION_ELECT_SABANA_2024.geojson"),
        "col_seccion": "SECCION",
    },
    "2025": {
        "path": os.path.join(_BASE, "GEOJSON/SE_EDO_QRO_24_25.geojson"),
        "col_seccion": "25_SECCION",
    },
}


def calcular_secciones_por_colonia(
    df: pd.DataFrame,
    anio_secciones: str = "2024",
    col_colonia: str = "COLONIA",
) -> pd.DataFrame:
    """
    Para cada registro, busca la colonia en COL_LOC_EDO_QRO.geojson y determina
    en qué secciones electorales cae el polígono de esa colonia.

    Agrega la columna SECCIONES_DE_COLONIA con una lista de secciones separadas
    por coma.
    """
    if col_colonia not in df.columns:
        return df

    colonias_path = os.path.join(_BASE, "GEOJSON/COL_LOC_EDO_QRO.geojson")
    gdf_colonias = gpd.read_file(colonias_path)
    if gdf_colonias.crs is None or gdf_colonias.crs.to_epsg() != 4326:
        gdf_colonias = gdf_colonias.to_crs(epsg=4326)

    capa_sec = CAPAS_SECCIONES[anio_secciones]
    gdf_secciones = gpd.read_file(capa_sec["path"])
    col_sec = capa_sec["col_seccion"]
    geo_col = gdf_secciones.geometry.name
    gdf_secciones = gdf_secciones[[col_sec, geo_col]]
    if gdf_secciones.crs is None or gdf_secciones.crs.to_epsg() != 4326:
        gdf_secciones = gdf_secciones.to_crs(epsg=4326)

    colonias_unicas = (
        df[col_colonia]
        .dropna()
        .loc[lambda s: s.str.strip() != ""]
        .unique()
        .tolist()
    )

    mapeo_secciones: dict[str, str] = {}

    for nombre_col in colonias_unicas:
        nombre_norm = nombre_col.strip().upper()
        match = gdf_colonias[gdf_colonias["NOM_COL"].str.upper().str.strip() == nombre_norm]
        if match.empty:
            mapeo_secciones[nombre_col] = ""
            continue

        poligono_colonia = match.union_all()
        intersecta = gdf_secciones[gdf_secciones.geometry.intersects(poligono_colonia)]

        if intersecta.empty:
            mapeo_secciones[nombre_col] = ""
        else:
            secciones = sorted(intersecta[col_sec].dropna().unique().tolist(), key=lambda x: str(x))
            mapeo_secciones[nombre_col] = ", ".join(str(int(s)) if isinstance(s, float) else str(s) for s in secciones)

    df = df.copy()
    df["SECCIONES_DE_COLONIA"] = df[col_colonia].map(
        lambda c: mapeo_secciones.get(c, "") if pd.notna(c) else ""
    )

    return df


class GeoReferenciador:
    """Clase que geocodifica direcciones y asigna atributos geográficos mediante spatial join."""

    def __init__(
        self,
        proveedor: str = "ArcGIS",
        columnas_deseadas: list[str] | None = None,
    ):
        if proveedor not in PROVEEDORES:
            raise ValueError(
                f"Proveedor '{proveedor}' no soportado. "
                f"Opciones: {list(PROVEEDORES.keys())}"
            )
        self.proveedor = proveedor
        self.columnas_deseadas: list[str] = columnas_deseadas 
        self._gdfs: dict[str, gpd.GeoDataFrame] = {}

        config = PROVEEDORES[proveedor]
        self._max_workers = config["workers"]

        geocoder = _crear_geocoder(proveedor)
        self._geocode = RateLimiter(
            geocoder.geocode,
            min_delay_seconds=config["delay"],
            max_retries=2,
        )

    # ------------------------------------------------------------------
    # Carga de GeoJSONs
    # ------------------------------------------------------------------
    def cargar_geojson(self):
        """Carga únicamente los GeoJSONs necesarios para las columnas deseadas."""
        capas_necesarias = {
            COLUMNAS_DISPONIBLES[col][0]
            for col in self.columnas_deseadas
            if col in COLUMNAS_DISPONIBLES
        }
        self._gdfs = {}
        for capa_key in capas_necesarias:
            capa = CAPAS_GEOJSON[capa_key]
            src_cols = list(capa["columnas"].values())  # nombres reales en el GeoJSON
            gdf = gpd.read_file(capa["path"])
            geo_col = gdf.geometry.name
            gdf = gdf[src_cols + [geo_col]]
            if gdf.crs is None or gdf.crs.to_epsg() != 4326:
                gdf = gdf.to_crs(epsg=4326)
            self._gdfs[capa_key] = gdf
        return self

    # ------------------------------------------------------------------
    # Geocodificación
    # ------------------------------------------------------------------
    def _geocodificar_una(self, direccion: str) -> tuple[float | None, float | None]:
        """Geocodifica una sola dirección. Devuelve (lat, lon) o (None, None)."""
        if not direccion or direccion.strip() == "":
            return (None, None)
        try:
            location = self._geocode(direccion)
            if location:
                return (location.latitude, location.longitude)
        except Exception:
            pass
        return (None, None)

    def geocodificar_direcciones(
        self,
        df: pd.DataFrame,
        col_direccion: str = "DIRECCION_HOMOLOGADA",
        callback=None,
    ) -> pd.DataFrame:
        """
        Geocodifica las direcciones únicas del DataFrame y añade columnas
        LATITUD y LONGITUD.

        Usa concurrencia (ThreadPoolExecutor) cuando el proveedor lo permite
        para acelerar el proceso significativamente.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame con la columna de dirección.
        col_direccion : str
            Nombre de la columna que contiene las direcciones.
        callback : callable, optional
            ``callback(progreso, total, direccion)`` para reportar avance.

        Returns
        -------
        pd.DataFrame con columnas LATITUD y LONGITUD añadidas.
        """
        df = df.copy()

        # 1. Direcciones únicas no vacías
        direcciones_unicas = (
            df[col_direccion]
            .dropna()
            .loc[lambda s: s.str.strip() != ""]
            .unique()
            .tolist()
        )
        total = len(direcciones_unicas)
        cache: dict[str, tuple] = {}
        progreso_actual = 0

        # 2. Geocodificación concurrente
        if self._max_workers > 1:
            with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
                futuros = {
                    pool.submit(self._geocodificar_una, d): d
                    for d in direcciones_unicas
                }
                for futuro in as_completed(futuros):
                    direccion = futuros[futuro]
                    try:
                        res = futuro.result() 
                        lat = round(res[0], 5) if res[0] is not None else None
                        lon = round(res[1], 5) if res[1] is not None else None
                        cache[direccion] = (lat, lon)
                    except Exception:
                        cache[direccion] = (None, None)
                    progreso_actual += 1
                    if callback:
                        callback(progreso_actual, total, direccion)
        else:
            # Secuencial (Nominatim)
            for i, direccion in enumerate(direcciones_unicas, start=1):
                res = self._geocodificar_una(direccion)
                lat = round(res[0], 5) if res[0] is not None else None
                lon = round(res[1], 5) if res[1] is not None else None
                cache[direccion] = (lat, lon)
                if callback:
                    callback(i, total, direccion)

        # 3. Mapear resultados al DataFrame completo
        df['LATITUD'] = df[col_direccion].map(
            lambda d: cache.get(d, (None, None))[0]
        )
        df['LONGITUD'] = df[col_direccion].map(
            lambda d: cache.get(d, (None, None))[1]
        )

        return df

    # ------------------------------------------------------------------
    # Asignacion de columnas geograficas via spatial join
    # ------------------------------------------------------------------
    def asignar_seccion_electoral(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Asigna columnas geográficas a cada registro mediante spatial join con
        los poligonos de los GeoJSONs configurados en ``CAPAS_GEOJSON``.
        Solo procesa las columnas presentes en ``self.columnas_deseadas``.

        Registros sin coordenadas válidas recibirán None en todas las columnas.
        """
        if not self._gdfs:
            self.cargar_geojson()

        df = df.copy()

        mask_valido = df['LATITUD'].notna() & df['LONGITUD'].notna()
        geometry = [
            Point(lon, lat) if valido else None
            for lat, lon, valido in zip(df['LATITUD'], df['LONGITUD'], mask_valido)
        ]

        gdf_puntos = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
        gdf_con_geom = gdf_puntos[mask_valido].copy()
        gdf_sin_geom = gdf_puntos[~mask_valido].copy()
        gdf_con_geom["__geo_row_id"] = range(len(gdf_con_geom))

        out_cols = [c for c in self.columnas_deseadas if c in COLUMNAS_DISPONIBLES]

        # Inicializar con None las filas sin geometría
        for col in out_cols:
            gdf_sin_geom[col] = ""

        # Spatial join capa por capa
        for capa_key, gdf_ref in self._gdfs.items():
            capa = CAPAS_GEOJSON[capa_key]
            cols_para_capa = {
                src_col: out_col for out_col, src_col in capa["columnas"].items()
                if out_col in self.columnas_deseadas
            }
            if not cols_para_capa:
                continue

            if gdf_con_geom.empty:
                for out_col in cols_para_capa.values():
                    gdf_con_geom[out_col] = pd.Series(dtype='object')
                continue

            puntos = gpd.GeoDataFrame(
                gdf_con_geom[["__geo_row_id", "geometry"]].reset_index(drop=True),
                geometry="geometry",
                crs="EPSG:4326",
            )
            geo_col = gdf_ref.geometry.name
            joined = gpd.sjoin(
                puntos,
                gdf_ref[list(cols_para_capa.keys()) + [geo_col]],
                how="left",
                predicate="within",
            )
            joined = joined.drop_duplicates(subset="__geo_row_id", keep="first")
            joined = joined.set_index("__geo_row_id")

            for src_col, out_col in cols_para_capa.items():
                gdf_con_geom[out_col] = gdf_con_geom["__geo_row_id"].map(joined[src_col])

        df_final = pd.concat([gdf_con_geom, gdf_sin_geom]).sort_index()
        df_final = df_final.drop(columns=['geometry', '__geo_row_id'], errors='ignore')
        df_final = pd.DataFrame(df_final)

        return df_final

    # ------------------------------------------------------------------
    # Pipeline completo
    # ------------------------------------------------------------------
    def ejecutar(self, df: pd.DataFrame, callback=None) -> pd.DataFrame:
        """Ejecuta geocodificación + asignación de sección electoral."""
        self.cargar_geojson()
        df = self.geocodificar_direcciones(df, callback=callback)
        df = self.asignar_seccion_electoral(df)
        return df
    
