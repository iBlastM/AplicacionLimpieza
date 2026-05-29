import pandas as pd

from .constantes import COLUMNAS_OBJETIVO
from .limpiadores import (
    BaseLimpiador,
    LimpiadorColumnas,
    LimpiadorContacto,
    LimpiadorDireccion,
    LimpiadorIdentidad,
)


class LimpiadorProgramasSociales(
    LimpiadorColumnas,
    LimpiadorContacto,
    LimpiadorDireccion,
    LimpiadorIdentidad,
    BaseLimpiador,
):
    """Orquestador del pipeline de limpieza de Programas Sociales."""

    COLUMNAS_OBJETIVO = COLUMNAS_OBJETIVO

    def ejecutar_limpieza(self) -> tuple[pd.DataFrame, list[str]]:
        """Ejecuta todo el pipeline de limpieza en orden.

        Returns:
            (df_limpio, advertencias) — el DataFrame normalizado y la lista de avisos
            generados por columnas faltantes o procesamiento parcial.
        """
        (self
            .renombrar_y_eliminar_columnas()
            .homologar_nombres_columnas()
            .crear_columna_nombrePRE()
            .limpiar_cadenas_y_texto()
            .crear_campos_compuestos()
            .limpiar_telefonos()
            .limpiar_correo()
            .procesar_programa()
            .parsear_direccion_completa()
            .limpiar_codigo_postal_y_parentesco()
            .convertir_fechas()
            .extraer_datos_curp()
            .limpiar_direcciones()
            .crear_direccion_homologada()
            .eliminar_espacios_columnas()
        )
        return self.df, self.advertencias
