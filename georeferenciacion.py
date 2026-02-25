import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import time
import os


class GeoReferenciador:
    """Clase que geocodifica direcciones y asigna sección electoral usando un GeoJSON."""

    DEFAULT_GEOJSON = os.path.join(
        os.path.dirname(__file__), 'SECCION_ELECT_SABANA_2024.geojson'
    )

    def __init__(self, path_geojson: str | None = None):
        self.path_geojson = path_geojson or self.DEFAULT_GEOJSON
        self.gdf: gpd.GeoDataFrame | None = None
        self._geocoder = Nominatim(user_agent="metrix_programas_sociales", timeout=10)
        self._geocode = RateLimiter(
            self._geocoder.geocode, min_delay_seconds=1.1, max_retries=2
        )

    # ------------------------------------------------------------------
    # Carga del GeoJSON de secciones electorales
    # ------------------------------------------------------------------
    def cargar_geojson(self):
        """Carga el GeoJSON y conserva solo SECCION y geometría."""
        self.gdf = gpd.read_file(self.path_geojson)[['SECCION', 'geometry']]
        # Asegurar CRS WGS84
        if self.gdf.crs is None or self.gdf.crs.to_epsg() != 4326:
            self.gdf = self.gdf.to_crs(epsg=4326)
        return self

    # ------------------------------------------------------------------
    # Geocodificación
    # ------------------------------------------------------------------
    def _geocodificar_direccion(self, direccion: str) -> tuple[float | None, float | None]:
        """Geocodifica una sola dirección. Devuelve (latitud, longitud) o (None, None)."""
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
        self, df: pd.DataFrame, col_direccion: str = "DIRECCION_HOMOLOGADA",
        callback=None
    ) -> pd.DataFrame:
        """
        Geocodifica las direcciones únicas del DataFrame y añade columnas
        LATITUD y LONGITUD.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame con la columna de dirección.
        col_direccion : str
            Nombre de la columna que contiene las direcciones.
        callback : callable, optional
            Función callback(progreso, total, direccion) para reportar avance.

        Returns
        -------
        pd.DataFrame con columnas LATITUD y LONGITUD añadidas.
        """
        df = df.copy()

        # 1. Obtener direcciones únicas no vacías
        direcciones_unicas = (
            df[col_direccion]
            .dropna()
            .loc[lambda s: s.str.strip() != ""]
            .unique()
        )
        total = len(direcciones_unicas)

        # 2. Geocodificar cada dirección única
        cache: dict[str, tuple] = {}
        for i, direccion in enumerate(direcciones_unicas, start=1):
            cache[direccion] = self._geocodificar_direccion(direccion)
            if callback:
                callback(i, total, direccion)

        # 3. Mapear resultados al DataFrame completo
        df['LATITUD'] = df[col_direccion].map(lambda d: cache.get(d, (None, None))[0])
        df['LONGITUD'] = df[col_direccion].map(lambda d: cache.get(d, (None, None))[1])

        return df

    # ------------------------------------------------------------------
    # Asignación de sección electoral
    # ------------------------------------------------------------------
    def asignar_seccion_electoral(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Asigna a cada registro su SECCION_ELECTORAL haciendo un spatial join
        entre los puntos (LATITUD, LONGITUD) y los polígonos del GeoJSON.

        Registros sin coordenadas válidas recibirán NaN en SECCION_ELECTORAL.
        """
        if self.gdf is None:
            self.cargar_geojson()

        df = df.copy()

        # Crear geometría solo para filas con coordenadas válidas
        mask_valido = df['LATITUD'].notna() & df['LONGITUD'].notna()
        geometry = [
            Point(lon, lat) if valido else None
            for lat, lon, valido in zip(df['LATITUD'], df['LONGITUD'], mask_valido)
        ]

        gdf_puntos = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

        # Spatial join solo con filas que tienen geometría
        gdf_con_geom = gdf_puntos[mask_valido].copy()
        gdf_sin_geom = gdf_puntos[~mask_valido].copy()

        if not gdf_con_geom.empty:
            resultado = gpd.sjoin(
                gdf_con_geom, self.gdf, how="left", predicate="within"
            )
            # Eliminar duplicados por índice (un punto puede caer en overlap)
            resultado = resultado[~resultado.index.duplicated(keep='first')]
            resultado = resultado.rename(columns={'SECCION': 'SECCION_ELECTORAL'})
            gdf_con_geom['SECCION_ELECTORAL'] = resultado['SECCION_ELECTORAL']
        else:
            gdf_con_geom['SECCION_ELECTORAL'] = pd.Series(dtype='float64')

        gdf_sin_geom['SECCION_ELECTORAL'] = None

        # Recombinar y limpiar
        df_final = pd.concat([gdf_con_geom, gdf_sin_geom]).sort_index()
        df_final = df_final.drop(columns=['geometry'])
        df_final = pd.DataFrame(df_final)  # volver a DataFrame normal

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