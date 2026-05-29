import re


class LimpiadorContacto:
    """Mixin: teléfonos, correo y nombre del programa."""

    def limpiar_telefonos(self):
        for col in ['TELEFONO', 'CELULAR']:
            if col not in self.df.columns:
                self.advertencias.append(f"Columna '{col}' no encontrada; se omitió su limpieza.")
                continue
            self.df[col] = self.df[col].astype(str).str.replace(r'\D', '', regex=True)
            mask = (self.df[col].str.len() != 10) | self.df[col].isin(['NULL', 'nan', 'sin tel'])
            self.df.loc[mask, col] = ""
        return self

    def limpiar_correo(self):
        if 'CORREO' not in self.df.columns:
            self.advertencias.append("Columna 'CORREO' no encontrada; se omitió su limpieza.")
            return self
        self.df['CORREO'] = self.df['CORREO'].astype(str).str.strip().str.lower()
        patron = r'^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$'
        mask_valido = self.df['CORREO'].str.contains(patron, na=False, regex=True)
        self.df.loc[~mask_valido | (self.df['CORREO'] == 'nan'), 'CORREO'] = ""
        return self

    def procesar_programa(self):
        if 'NOMBRE_PROGRAMA' not in self.df.columns:
            self.advertencias.append(
                "No se pudo procesar NOMBRE_PROGRAMA ni crear ANIO_PROGRAMA: "
                "la columna 'NOMBRE_PROGRAMA' (originalmente 'Nombre.1') fue eliminada."
            )
            return self
        self.df['NOMBRE_PROGRAMA'] = self.df['NOMBRE_PROGRAMA'].str.replace(r'^[AZ](?=[A-Z])', '', regex=True)
        self.df['ANIO_PROGRAMA'] = self.df['NOMBRE_PROGRAMA'].str.extract(r'(\d{4})').iloc[:, 0].fillna("")
        self.df['NOMBRE_PROGRAMA'] = (
            self.df['NOMBRE_PROGRAMA']
            .str.replace(r'\d{4}', '', regex=True)
            .str.replace(r'\s+', ' ', regex=True)
            .str.strip()
        )
        self.df.loc[
            self.df['NOMBRE_PROGRAMA'] == 'CALENTADOR SOLAR CORREGIDORA 2',
            'NOMBRE_PROGRAMA'
        ] = 'CALENTADOR SOLAR CORREGIDORA'
        return self
