from typing import Callable

import pandas as pd

from ..constantes import VALORES_VACIOS


class BaseLimpiador:
    """Clase base con estado compartido y helpers reutilizables."""

    VALORES_VACIOS = VALORES_VACIOS

    def __init__(self, df: pd.DataFrame, mapeo_columnas: dict[str, str] | None = None):
        self.df = df.copy()
        self.advertencias: list[str] = []
        self.mapeo_columnas: dict[str, str] = mapeo_columnas or {}

    def _tiene_columnas(self, *columnas: str) -> bool:
        """Devuelve True si todas las columnas existen; si no, registra una advertencia."""
        faltantes = [c for c in columnas if c not in self.df.columns]
        if faltantes:
            self.advertencias.append(f"Columna(s) no encontrada(s): {', '.join(faltantes)}")
            return False
        return True

    def _aplicar_si_existe(self, col: str, fn: Callable) -> "BaseLimpiador":
        """Aplica fn a la columna si existe; si no, registra una advertencia."""
        if col in self.df.columns:
            self.df[col] = self.df[col].apply(fn)
        else:
            self.advertencias.append(f"Columna '{col}' no encontrada; se omitió su limpieza.")
        return self

    def limpieza_nones(self, dfFinal: pd.DataFrame) -> pd.DataFrame:
        """Limpieza final de valores None o NaN en todo el DataFrame."""
        return dfFinal.replace({None: "", pd.NA: "", float('nan'): ""})
