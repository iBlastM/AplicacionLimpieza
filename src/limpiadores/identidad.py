import pandas as pd


class LimpiadorIdentidad:
    """Mixin: fechas de Excel y derivación de datos desde CURP."""

    def convertir_fechas(self):
        for col in ['FECHA_NACIMIENTO', 'FECHA_REGISTRO']:
            if col not in self.df.columns:
                self.advertencias.append(f"Columna '{col}' no encontrada; se omitió la conversión de fecha.")
                continue

            serie = self.df[col]

            # Si ya es datetime (e.g. Parquet con datetime64[us/ns/ms]), formatear directo
            if pd.api.types.is_datetime64_any_dtype(serie):
                self.df[col] = serie.dt.strftime('%d/%m/%Y')
            else:
                # Intentar conversión desde número serial de Excel (días desde 1899-12-30)
                numerica = pd.to_numeric(serie, errors='coerce')
                if numerica.notna().any():
                    self.df[col] = pd.to_datetime(
                        numerica, unit='D', origin='1899-12-30', errors='coerce'
                    ).dt.strftime('%d/%m/%Y')
                else:
                    # Cadenas de texto con formato de fecha
                    self.df[col] = pd.to_datetime(serie, errors='coerce').dt.strftime('%d/%m/%Y')

            self.df[col] = self.df[col].where(~self.df[col].isin(['NaT', 'nan', 'NaN', 'None']), other="")
        return self

    def extraer_datos_curp(self):
        """Deriva FECHA_NACIMIENTO, EDAD y GENERO desde CURP cuando esas columnas no existan.

        Regla de año: 00-26 → 2000-2026 | 27-99 → 1927-1999
        Género: carácter 11 del CURP (M = MUJER, H = HOMBRE).
        """
        if 'CURP' not in self.df.columns:
            return self

        necesita_fecha  = 'FECHA_NACIMIENTO' not in self.df.columns
        necesita_edad   = 'EDAD'             not in self.df.columns
        necesita_genero = 'GENERO'           not in self.df.columns

        if not (necesita_fecha or necesita_edad or necesita_genero):
            return self

        hoy = pd.Timestamp.today().normalize()

        def _parsear(curp):
            curp = str(curp).strip().upper()
            if len(curp) < 11:
                return '', '', ''
            try:
                anio_2d, mes, dia = int(curp[4:6]), int(curp[6:8]), int(curp[8:10])
                genero_char = curp[10]
            except (ValueError, IndexError):
                return '', '', ''

            anio = (2000 + anio_2d) if anio_2d <= 26 else (1900 + anio_2d)
            try:
                fecha = pd.Timestamp(year=anio, month=mes, day=dia)
            except Exception:
                return '', '', ''

            edad = hoy.year - fecha.year - ((hoy.month, hoy.day) < (fecha.month, fecha.day))
            genero = 'MUJER' if genero_char == 'M' else ('HOMBRE' if genero_char == 'H' else '')
            return fecha.strftime('%d/%m/%Y'), str(edad), genero

        resultados = self.df['CURP'].apply(_parsear)

        if necesita_fecha:
            self.df['FECHA_NACIMIENTO'] = [r[0] for r in resultados]
            self.advertencias.append("FECHA_NACIMIENTO derivada desde CURP.")
        if necesita_edad:
            self.df['EDAD'] = [r[1] for r in resultados]
            self.advertencias.append("EDAD calculada desde CURP.")
        if necesita_genero:
            self.df['GENERO'] = [r[2] for r in resultados]
            self.advertencias.append("GENERO derivado desde CURP.")

        return self
