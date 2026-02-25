import pandas as pd
import unicodedata
import re


class LimpiadorProgramasSociales:
    """Clase que encapsula toda la lógica de limpieza para bases de datos de Programas Sociales."""

    # Valores considerados basura en calles
    BASURA_CALLES = [
        'DOMICILIO CONOCIDO', 'SIN NOMBRE', 'SN', 'S/N',
        'NAN', 'NONE', 'SIN CALLE', 'SINNOMBRE'
    ]

    VALORES_VACIOS = ["", "NAN", "nan"]

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    # --- Funciones de limpieza individuales ---

    @staticmethod
    def limpiar_texto(texto: str) -> str:
        """Limpia texto: elimina acentos, caracteres especiales y espacios extra."""
        if not isinstance(texto, str):
            return texto
        texto = unicodedata.normalize('NFKD', texto)\
                .encode('ascii', 'ignore')\
                .decode('utf-8')
        texto = re.sub(r'[^a-zA-Z0-9\s]', '', texto)
        texto = re.sub(r'\s+', ' ', texto).strip()
        return texto

    @staticmethod
    def limpiar_calle_larga(calle: str) -> str:
        """Recorta calles con más de 50 caracteres dejando solo la parte alfabética inicial."""
        calle = str(calle).strip()
        if len(calle) > 50:
            match = re.search(r'^([^0-9]+)', calle)
            if match:
                return match.group(1).strip()
        return calle

    @staticmethod
    def limpiar_num_ext(valor) -> str:
        """Limpia números exteriores e interiores eliminando basura común."""
        valor = str(valor).strip()
        valor = valor.replace('-', '')
        if re.search(r'\d+\.\d+', valor):
            return ""
        valor = valor.replace('.', '')
        if re.search(r'\d', valor):
            return valor
        return ""

    @classmethod
    def concatenar_direccion_logica(cls, fila: pd.Series) -> str:
        """Genera la dirección homologada a partir de los campos individuales."""
        calle = str(fila['CALLE']).strip()
        next_ = str(fila['NUM_EXT']).strip()
        nint = str(fila['NUM_INT']).strip()
        colonia = str(fila['COLONIA']).strip()
        mun = str(fila['MUNICIPIO']).strip()
        est = str(fila['ESTADO']).strip()
        cp = str(fila['CODIGO_POSTAL']).strip()

        partes = []
        if calle not in cls.VALORES_VACIOS:
            partes.append(calle)
            if next_ not in cls.VALORES_VACIOS:
                partes.append(next_)
            if nint not in cls.VALORES_VACIOS:
                partes.append("INT." + nint)

        if colonia not in cls.VALORES_VACIOS:
            partes.append(colonia)
            if next_ not in cls.VALORES_VACIOS and calle in cls.VALORES_VACIOS:
                partes.append(next_)
            if nint not in cls.VALORES_VACIOS and calle in cls.VALORES_VACIOS:
                partes.append("INT." + nint)

        for campo in [cp, mun, est]:
            if campo not in cls.VALORES_VACIOS:
                partes.append(campo)

        return ", ".join(partes)

    # --- Pipeline de limpieza ---

    def renombrar_y_eliminar_columnas(self):
        """Paso 1: Renombrar y eliminar columnas iniciales."""
        self.df = self.df.rename(columns={'Identificador': 'CURP', 'Nombre.1': 'NOMBRE_PROGRAMA'})
        self.df = self.df.drop(columns=['IdTipoTramite', 'IdEstatus', 'Campo3'])
        return self

    def limpiar_cadenas_y_texto(self):
        """Paso 2: Limpieza general de cadenas y texto."""
        self.df = self.df.apply(lambda x: " ".join(x.split()) if isinstance(x, str) else x)
        columnas_limpiar = ['Nombre', 'ApellidoPaterno', 'ApellidoMaterno', 'Calle', 'Colonia', 'NOMBRE_PROGRAMA']
        for columna in columnas_limpiar:
            self.df[columna] = self.df[columna].apply(self.limpiar_texto)
        return self

    def homologar_nombres_columnas(self):
        """Paso 3: Homologación de nombres de columnas a mayúsculas y formato estándar."""
        self.df.columns = self.df.columns.str.upper()
        self.df = self.df.rename(columns={
            'IDUSUARIO': 'ID_USUARIO', 'IDPERSONA': 'ID_PERSONA',
            'NOMBRE': 'NOMBRE(S)_DE_PILA', 'APELLIDOPATERNO': 'AP_PATERNO',
            'APELLIDOMATERNO': 'AP_MATERNO', 'FECHANACIMIENTO': 'FECHA_NACIMIENTO',
            'NUMEXT': 'NUM_EXT', 'NUMINT': 'NUM_INT',
            'CODIGOPOSTAL': 'CODIGO_POSTAL', 'FECHAREGISTRO': 'FECHA_REGISTRO',
            'IDPARENTESCO': 'ID_PARENTESCO'
        })
        return self

    def crear_campos_compuestos(self):
        """Paso 4: Creación de campos compuestos y ubicación estática."""
        self.df['NOMBRE_COMPLETO'] = (
            self.df['AP_PATERNO'] + " " + self.df['AP_MATERNO'] + " " + self.df['NOMBRE(S)_DE_PILA']
        )
        self.df['ESTADO'] = 'QUERETARO'
        self.df['MUNICIPIO'] = 'CORREGIDORA'
        return self

    def limpiar_telefonos(self):
        """Paso 5: Limpieza de Teléfonos y Celulares."""
        for col in ['TELEFONO', 'CELULAR']:
            self.df[col] = self.df[col].astype(str).str.replace(r'\D', '', regex=True)
            mask = (self.df[col].str.len() != 10) | (self.df[col].isin(['NULL', 'nan', 'sin tel']))
            self.df.loc[mask, col] = ""
        return self

    def limpiar_correo(self):
        """Paso 6: Limpieza de Correo."""
        self.df['CORREO'] = self.df['CORREO'].astype(str).str.strip().str.lower()
        patron_email = r'^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$'
        mask_valido = self.df['CORREO'].str.contains(patron_email, na=False, regex=True)
        self.df.loc[~mask_valido | (self.df['CORREO'] == 'nan'), 'CORREO'] = ""
        return self

    def procesar_programa(self):
        """Paso 7: Procesamiento de Nombre y Año del Programa."""
        self.df['NOMBRE_PROGRAMA'] = self.df['NOMBRE_PROGRAMA'].str.replace(r'^[AZ](?=[A-Z])', '', regex=True)
        self.df['ANO_PROGRAMA'] = self.df['NOMBRE_PROGRAMA'].str.extract(r'(\d{4})')[0]
        self.df['NOMBRE_PROGRAMA'] = self.df['NOMBRE_PROGRAMA'].str.replace(r'\d{4}', '', regex=True)
        self.df['NOMBRE_PROGRAMA'] = self.df['NOMBRE_PROGRAMA'].str.replace(r'\s+', ' ', regex=True).str.strip()
        self.df.loc[
            self.df['NOMBRE_PROGRAMA'] == 'CALENTADOR SOLAR CORREGIDORA 2',
            'NOMBRE_PROGRAMA'
        ] = 'CALENTADOR SOLAR CORREGIDORA'
        return self

    def limpiar_codigo_postal_y_parentesco(self):
        """Paso 8: Limpieza de Código Postal y Parentesco."""
        self.df['CODIGO_POSTAL'] = self.df['CODIGO_POSTAL'].astype(str).str.replace(r'\D', '', regex=True)
        mask_cp = (self.df['CODIGO_POSTAL'].str.len() == 5) & (self.df['CODIGO_POSTAL'] != 'nan')
        self.df.loc[~mask_cp, 'CODIGO_POSTAL'] = ""

        self.df['ID_PARENTESCO'] = self.df['ID_PARENTESCO'].astype(str)
        self.df.loc[self.df['ID_PARENTESCO'] != '1', 'ID_PARENTESCO'] = 'DEPENDIENTE'
        self.df.loc[self.df['ID_PARENTESCO'] == '1', 'ID_PARENTESCO'] = 'BENEFICIARIO'
        return self

    def convertir_fechas(self):
        """Paso 9: Conversión de fechas de Excel (serial) a formato legible."""
        for col in ['FECHA_NACIMIENTO', 'FECHA_REGISTRO']:
            self.df[col] = pd.to_datetime(
                pd.to_numeric(self.df[col], errors='coerce'),
                unit='D', origin='1899-12-30'
            )
            self.df[col] = self.df[col].dt.strftime('%d/%m/%Y').replace(['NaT', 'nan', 'NaN'], "")
        return self

    def limpiar_direcciones(self):
        """Paso 10: Limpieza de Direcciones (Calles, Números, Colonias)."""
        self.df['NUM_EXT'] = self.df['NUM_EXT'].apply(self.limpiar_num_ext)
        self.df['NUM_INT'] = self.df['NUM_INT'].apply(self.limpiar_num_ext)
        self.df.loc[self.df['NUM_EXT'] == '0', 'NUM_EXT'] = ''
        self.df.loc[self.df['NUM_INT'] == '0', 'NUM_INT'] = ''

        self.df['CALLE'] = self.df['CALLE'].astype(str).str.upper().str.strip()
        self.df['CALLE'] = self.df['CALLE'].str.replace(r'\b(A|AV)\b', 'AVENIDA', regex=True)
        self.df['CALLE'] = self.df['CALLE'].str.replace(r'\bC\b', '', regex=True).str.strip()

        mask_calle = (
            self.df['CALLE'].isin(self.BASURA_CALLES) |
            self.df['CALLE'].str.contains(r'^\d+$', na=False)
        )
        self.df.loc[mask_calle, 'CALLE'] = ""
        self.df['CALLE'] = self.df['CALLE'].apply(self.limpiar_calle_larga)

        self.df['COLONIA'] = self.df['COLONIA'].astype(str).str.upper().str.strip()
        self.df['COLONIA'] = self.df['COLONIA'].str.replace(r'\bFRACC?\b', 'FRACCIONAMIENTO', regex=True)
        mask_colonia = (
            self.df['COLONIA'].isin(['OTRO', 'NAN', 'NONE']) |
            self.df['COLONIA'].str.contains(r'^\d+$', na=False)
        )
        self.df.loc[mask_colonia, 'COLONIA'] = ""
        return self

    def crear_direccion_homologada(self):
        """Paso 11: Creación de Dirección Homologada."""
        self.df['DIRECCION_HOMOLOGADA'] = self.df.apply(self.concatenar_direccion_logica, axis=1)
        return self

    def ejecutar_limpieza(self) -> pd.DataFrame:
        """Ejecuta todo el pipeline de limpieza en orden y devuelve el DataFrame limpio."""
        self.renombrar_y_eliminar_columnas()
        self.limpiar_cadenas_y_texto()
        self.homologar_nombres_columnas()
        self.crear_campos_compuestos()
        self.limpiar_telefonos()
        self.limpiar_correo()
        self.procesar_programa()
        self.limpiar_codigo_postal_y_parentesco()
        self.convertir_fechas()
        self.limpiar_direcciones()
        self.crear_direccion_homologada()
        return self.df
