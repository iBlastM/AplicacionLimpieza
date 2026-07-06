import pandas as pd

from ..constantes import BASURA_CALLES, VALORES_VACIOS
from ..utils_texto import limpiar_calle_larga, limpiar_num_ext


class LimpiadorDireccion:
    """Mixin: CP, parentesco, calles, colonias, parseo y dirección homologada."""

    @staticmethod
    def _concatenar_direccion(fila: pd.Series) -> str:
        calle   = str(fila['CALLE']).strip()
        next_   = str(fila['NUM_EXT']).strip()
        nint    = str(fila['NUM_INT']).strip()
        colonia = str(fila['COLONIA']).strip()
        mun     = str(fila['MUNICIPIO']).strip()
        est     = str(fila['ESTADO']).strip()
        cp      = str(fila['CODIGO_POSTAL']).strip()

        partes = []
        if calle not in VALORES_VACIOS:
            partes.append(calle)
            if next_ not in VALORES_VACIOS:
                partes.append(next_)
            if nint not in VALORES_VACIOS:
                partes.append("INT." + nint)

        if colonia not in VALORES_VACIOS:
            partes.append(colonia)
            if next_ not in VALORES_VACIOS and calle in VALORES_VACIOS:
                partes.append(next_)
            if nint not in VALORES_VACIOS and calle in VALORES_VACIOS:
                partes.append("INT." + nint)

        for campo in [cp, mun, est]:
            if campo not in VALORES_VACIOS:
                partes.append(campo)

        return ", ".join(partes)

    def limpiar_codigo_postal_y_parentesco(self):
        if 'CODIGO_POSTAL' in self.df.columns:
            self.df['CODIGO_POSTAL'] = self.df['CODIGO_POSTAL'].astype(str).str.replace(r'\D', '', regex=True)
            mask_cp = (self.df['CODIGO_POSTAL'].str.len() == 5) & (self.df['CODIGO_POSTAL'] != 'nan')
            # Solo se antepone "C.P. " cuando hay un código postal válido de 5 dígitos.
            # Si no lo hay, se deja vacío para que no aparezca un "C.P." suelto en la dirección.
            self.df.loc[mask_cp, 'CODIGO_POSTAL'] = "C.P. " + self.df.loc[mask_cp, 'CODIGO_POSTAL']
            self.df.loc[~mask_cp, 'CODIGO_POSTAL'] = ""
        else:
            self.advertencias.append("Columna 'CODIGO_POSTAL' no encontrada; se omitió su limpieza.")

        if 'ID_PARENTESCO' in self.df.columns:
            self.df['ID_PARENTESCO'] = self.df['ID_PARENTESCO'].astype(str)
            self.df.loc[self.df['ID_PARENTESCO'] != '1', 'ID_PARENTESCO'] = 'DEPENDIENTE'
            self.df.loc[self.df['ID_PARENTESCO'] == '1', 'ID_PARENTESCO'] = 'BENEFICIARIO'
        else:
            self.advertencias.append("Columna 'ID_PARENTESCO' no encontrada; se omitió su limpieza.")
        return self

    def limpiar_direcciones(self):
        for col in ['NUM_EXT', 'NUM_INT']:
            if col in self.df.columns:
                self.df[col] = self.df[col].apply(limpiar_num_ext).astype(str)
                self.df.loc[self.df[col].isin(['0', 'nan', 'None']), col] = ''
            else:
                self.advertencias.append(f"Columna '{col}' no encontrada; se omitió su limpieza.")

        if 'CALLE' in self.df.columns:
            self.df['CALLE'] = self.df['CALLE'].astype(str).str.upper().str.strip()
            self.df['CALLE'] = self.df['CALLE'].str.replace(r'\b(A|AV)\b', 'AVENIDA', regex=True)
            self.df['CALLE'] = self.df['CALLE'].str.replace(r'\bC\b', '', regex=True).str.strip()
            mask_calle = (
                self.df['CALLE'].isin(BASURA_CALLES) |
                self.df['CALLE'].str.contains(r'^\d+$', na=False)
            )
            self.df.loc[mask_calle, 'CALLE'] = ""
            self.df['CALLE'] = self.df['CALLE'].apply(limpiar_calle_larga)
        else:
            self.advertencias.append("Columna 'CALLE' no encontrada; se omitió su limpieza.")

        if 'COLONIA' in self.df.columns:
            self.df['COLONIA'] = self.df['COLONIA'].astype(str).str.upper().str.strip()
            self.df['COLONIA'] = self.df['COLONIA'].str.replace(r'\bFRACC?\b', 'FRACCIONAMIENTO', regex=True)
            mask_colonia = (
                self.df['COLONIA'].isin(['OTRO', 'NAN', 'NONE']) |
                self.df['COLONIA'].str.contains(r'^\d+$', na=False)
            )
            self.df.loc[mask_colonia, 'COLONIA'] = ""
        else:
            self.advertencias.append("Columna 'COLONIA' no encontrada; se omitió su limpieza.")
        return self

    def parsear_direccion_completa(self):
        if 'DIRECCION_COMPLETA' not in self.df.columns:
            return self

        CAMPOS = ['CALLE', 'NUM_EXT', 'NUM_INT', 'COLONIA', 'CODIGO_POSTAL']

        def _parsear(valor):
            partes = [p.strip() for p in str(valor).split(',')]
            return partes[:len(CAMPOS)] if len(partes) >= len(CAMPOS) else [''] * len(CAMPOS)

        parsed = self.df['DIRECCION_COMPLETA'].apply(_parsear)
        for i, campo in enumerate(CAMPOS):
            self.df[campo] = [fila[i] for fila in parsed]
        return self

    def crear_direccion_homologada(self):
        if 'DIRECCION_COMPLETA' in self.df.columns:
            self.df['DIRECCION_HOMOLOGADA'] = self.df.apply(
                lambda fila: ", ".join(
                    p for p in [
                        str(fila.get('DIRECCION_COMPLETA', '')).strip(),
                        str(fila.get('MUNICIPIO', '')).strip(),
                        str(fila.get('ESTADO', '')).strip(),
                    ]
                    if p and p not in VALORES_VACIOS
                ),
                axis=1,
            )
            return self

        requeridas = ['CALLE', 'NUM_EXT', 'NUM_INT', 'COLONIA', 'MUNICIPIO', 'ESTADO', 'CODIGO_POSTAL']
        faltantes = [c for c in requeridas if c not in self.df.columns]
        if faltantes:
            self.advertencias.append(
                f"DIRECCION_HOMOLOGADA se creará de forma parcial; falta(n): {', '.join(faltantes)}."
            )
            df_tmp = self.df.copy()
            for c in faltantes:
                df_tmp[c] = ""
            self.df['DIRECCION_HOMOLOGADA'] = df_tmp.apply(self._concatenar_direccion, axis=1)
        else:
            self.df['DIRECCION_HOMOLOGADA'] = self.df.apply(self._concatenar_direccion, axis=1)
        return self
