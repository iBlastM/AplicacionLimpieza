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
_BASE = os.path.dirname(__file__)

CAPAS_GEOJSON: dict = {
    "SECCION_ELECT_SABANA_2024": {
        "path": os.path.join(_BASE, "GEOJSON/SECCION_ELECT_SABANA_2024.geojson"),
        "columnas": {
            "SECCION": "SECCION_ELECTORAL",
            "DISTRITO_F": "DISTRITO_F",
            "DISTRITO_L": "DISTRITO_L",
        },
    },
    "COL_LOC_EDO_QRO": {
        "path": os.path.join(_BASE, "GEOJSON/COL_LOC_EDO_QRO.geojson"),
        "columnas": {"NOM_COL": "NOM_COL"},
    },
    "CP_EDO_QRO": {
        "path": os.path.join(_BASE, "GEOJSON/CP_EDO_QRO.geojson"),
        "columnas": {"C_P": "C_P"},
    },
    "DELEGACIONES_QRO_CORR": {
        "path": os.path.join(_BASE, "GEOJSON/DELEGACIONES_QRO_CORR.geojson"),
        "columnas": {"NOM_DEL": "NOM_DEL"},
    },
    "SE_EDO_QRO_24_25": {
        "path": os.path.join(_BASE, "GEOJSON/SE_EDO_QRO_24_25.geojson"),
        "columnas": {
            "CIRCUNSCRI": "CIRCUNSCRI",
            "24_D_FEDERAL": "24_D_FEDERAL",
            "24_D_LOCAL": "24_D_LOCAL",
            "24_SECCION": "24_SECCION",
            "25_D_FEDERAL": "25_D_FEDERAL",
            "25_D_LOCAL": "25_D_LOCAL",
            "25_SECCION": "25_SECCION",
        },
    },
}

# Mapa inverso: nombre_columna_salida -> (capa_key, columna_origen)
COLUMNAS_DISPONIBLES: dict[str, tuple[str, str]] = {
    out_col: (capa_key, src_col)
    for capa_key, capa in CAPAS_GEOJSON.items()
    for src_col, out_col in capa["columnas"].items()
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
        self.columnas_deseadas: list[str] = columnas_deseadas or list(COLUMNAS_DISPONIBLES.keys())
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
            src_cols = list(capa["columnas"].keys())
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
            Point(lon, lat) if valido else ""
            for lat, lon, valido in zip(df['LATITUD'], df['LONGITUD'], mask_valido)
        ]

        gdf_puntos = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
        gdf_con_geom = gdf_puntos[mask_valido].copy()
        gdf_sin_geom = gdf_puntos[~mask_valido].copy()

        out_cols = [c for c in self.columnas_deseadas if c in COLUMNAS_DISPONIBLES]

        # Inicializar con None las filas sin geometría
        for col in out_cols:
            gdf_sin_geom[col] = ""

        # Spatial join capa por capa
        for capa_key, gdf_ref in self._gdfs.items():
            capa = CAPAS_GEOJSON[capa_key]
            cols_para_capa = {
                src: out for src, out in capa["columnas"].items()
                if out in self.columnas_deseadas
            }
            if not cols_para_capa:
                continue

            if gdf_con_geom.empty:
                for out_col in cols_para_capa.values():
                    gdf_con_geom[out_col] = pd.Series(dtype='object')
                continue

            puntos = gpd.GeoDataFrame(geometry=gdf_con_geom.geometry, crs="EPSG:4326")
            geo_col = gdf_ref.geometry.name
            joined = gpd.sjoin(
                puntos,
                gdf_ref[list(cols_para_capa.keys()) + [geo_col]],
                how="left",
                predicate="within",
            )
            joined = joined[~joined.index.duplicated(keep='first')]

            for src_col, out_col in cols_para_capa.items():
                gdf_con_geom[out_col] = joined[src_col]

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
    