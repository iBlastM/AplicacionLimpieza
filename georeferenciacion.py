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


class GeoReferenciador:
    """Clase que geocodifica direcciones y asigna sección electoral usando un GeoJSON."""

    DEFAULT_GEOJSON = os.path.join(
        os.path.dirname(__file__), 'SECCION_ELECT_SABANA_2024.geojson'
    )

    def __init__(self, proveedor: str = "ArcGIS", path_geojson: str | None = None):
        if proveedor not in PROVEEDORES:
            raise ValueError(
                f"Proveedor '{proveedor}' no soportado. "
                f"Opciones: {list(PROVEEDORES.keys())}"
            )
        self.proveedor = proveedor
        self.path_geojson = path_geojson or self.DEFAULT_GEOJSON
        self.gdf: gpd.GeoDataFrame | None = None

        config = PROVEEDORES[proveedor]
        self._max_workers = config["workers"]

        geocoder = _crear_geocoder(proveedor)
        self._geocode = RateLimiter(
            geocoder.geocode,
            min_delay_seconds=config["delay"],
            max_retries=2,
        )

    # ------------------------------------------------------------------
    # Carga del GeoJSON de secciones electorales
    # ------------------------------------------------------------------
    def cargar_geojson(self):
        """Carga el GeoJSON y conserva solo SECCION y geometría."""
        self.gdf = gpd.read_file(self.path_geojson)[['SECCION', 'geometry']]
        if self.gdf.crs is None or self.gdf.crs.to_epsg() != 4326:
            self.gdf = self.gdf.to_crs(epsg=4326)
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
                        cache[direccion] = futuro.result()
                    except Exception:
                        cache[direccion] = (None, None)
                    progreso_actual += 1
                    if callback:
                        callback(progreso_actual, total, direccion)
        else:
            # Secuencial (Nominatim)
            for i, direccion in enumerate(direcciones_unicas, start=1):
                cache[direccion] = self._geocodificar_una(direccion)
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

        mask_valido = df['LATITUD'].notna() & df['LONGITUD'].notna()
        geometry = [
            Point(lon, lat) if valido else None
            for lat, lon, valido in zip(df['LATITUD'], df['LONGITUD'], mask_valido)
        ]

        gdf_puntos = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

        gdf_con_geom = gdf_puntos[mask_valido].copy()
        gdf_sin_geom = gdf_puntos[~mask_valido].copy()

        if not gdf_con_geom.empty:
            resultado = gpd.sjoin(
                gdf_con_geom, self.gdf, how="left", predicate="within"
            )
            resultado = resultado[~resultado.index.duplicated(keep='first')]
            resultado = resultado.rename(columns={'SECCION': 'SECCION_ELECTORAL'})
            gdf_con_geom['SECCION_ELECTORAL'] = resultado['SECCION_ELECTORAL']
        else:
            gdf_con_geom['SECCION_ELECTORAL'] = pd.Series(dtype='float64')

        gdf_sin_geom['SECCION_ELECTORAL'] = None

        df_final = pd.concat([gdf_con_geom, gdf_sin_geom]).sort_index()
        df_final = df_final.drop(columns=['geometry'])
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