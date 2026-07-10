from ..utils_texto import limpiar_texto


class LimpiadorColumnas:
    """Mixin: renombrado, homologación, campos compuestos y limpieza de texto."""

    def renombrar_y_eliminar_columnas(self):
        rename_map = {
            k: v for k, v in {'Nombre.1': 'NOMBRE_PROGRAMA'}.items()
            if k in self.df.columns
        }
        if 'Identificador' in self.df.columns:
            if 'CURP' in self.df.columns:
                self.advertencias.append(
                    "La columna 'Identificador' no se renombro a 'CURP' porque el archivo ya contiene CURP."
                )
            else:
                rename_map['Identificador'] = 'CURP'
        self.df = self.df.rename(columns=rename_map)
        self.df = self.df.drop(
            columns=[c for c in ['IdTipoTramite', 'IdEstatus', 'Campo3'] if c in self.df.columns]
        )
        return self

    def homologar_nombres_columnas(self):
        self.df.columns = self.df.columns.str.upper()
        self.df.columns = (
            self.df.columns
            .str.replace('Ñ', 'NI')
            .str.normalize('NFKD')
            .str.encode('ascii', errors='ignore')
            .str.decode('utf-8')
        )

        if self.mapeo_columnas:
            mapeo_upper = {
                k.upper(): v for k, v in self.mapeo_columnas.items()
                if k.upper() in self.df.columns
            }
            if mapeo_upper:
                fuentes = set(mapeo_upper.keys())
                for destino in set(mapeo_upper.values()):
                    if destino in self.df.columns and destino not in fuentes:
                        self.df = self.df.drop(columns=[destino])
                        self.advertencias.append(
                            f"La columna '{destino}' fue reemplazada por el mapeo personalizado del usuario."
                        )
                self.df = self.df.rename(columns=mapeo_upper)

        columnas_ya_mapeadas = set(self.mapeo_columnas.values()) if self.mapeo_columnas else set()
        rename_map_hom = {
            'IDUSUARIO': 'ID_USUARIO', 'IDPERSONA': 'ID_PERSONA',
            'NOMBRE': 'NOMBRE(S)_DE_PILA', 'APELLIDOPATERNO': 'AP_PATERNO',
            'APELLIDOMATERNO': 'AP_MATERNO', 'FECHANACIMIENTO': 'FECHA_NACIMIENTO',
            'NUMEXT': 'NUM_EXT', 'NUMINT': 'NUM_INT',
            'CODIGOPOSTAL': 'CODIGO_POSTAL', 'FECHAREGISTRO': 'FECHA_REGISTRO',
            'IDPARENTESCO': 'ID_PARENTESCO',
        }
        rename_map_hom = {
            k: v for k, v in rename_map_hom.items()
            if k in self.df.columns and v not in columnas_ya_mapeadas
        }
        self.df = self.df.rename(columns=rename_map_hom)
        return self

    def crear_columna_nombrePRE(self):
        requeridas = ['AP_PATERNO', 'AP_MATERNO', 'NOMBRE(S)_DE_PILA']
        if not self._tiene_columnas(*requeridas):
            self.advertencias.append(
                "No se pudo crear NOMBRE_PRE: faltan una o más de las columnas "
                "AP_PATERNO, AP_MATERNO o NOMBRE(S)_DE_PILA."
            )
            return self
        self.df['NOMBRE_PRE'] = (
            (self.df['NOMBRE(S)_DE_PILA'] + " " + self.df['AP_PATERNO'] + " " + self.df['AP_MATERNO'])
            .str.title()
        )
        return self

    def limpiar_cadenas_y_texto(self):
        self.df = self.df.apply(lambda x: " ".join(x.split()) if isinstance(x, str) else x)
        for col in ['NOMBRE', 'AP_PATERNO', 'AP_MATERNO', 'CALLE', 'COLONIA', 'NOMBRE_PROGRAMA']:
            if col in self.df.columns:
                self.df[col] = self.df[col].apply(limpiar_texto)
        return self

    def crear_campos_compuestos(self):
        requeridas = ['AP_PATERNO', 'AP_MATERNO', 'NOMBRE(S)_DE_PILA']
        if all(c in self.df.columns for c in requeridas):
            self.df['NOMBRE_COMPLETO'] = (
                self.df['AP_PATERNO'] + " " + self.df['AP_MATERNO'] + " " + self.df['NOMBRE(S)_DE_PILA']
            )
        else:
            faltantes = [c for c in requeridas if c not in self.df.columns]
            self.advertencias.append(f"No se pudo crear NOMBRE_COMPLETO: falta(n) {', '.join(faltantes)}.")
        self.df['ESTADO'] = getattr(self, 'estado', 'QUERETARO') or 'QUERETARO'
        self.df['MUNICIPIO'] = getattr(self, 'municipio', 'CORREGIDORA') or 'CORREGIDORA'
        return self

    def eliminar_espacios_columnas(self):
        self.df.columns = self.df.columns.str.replace(r'\s+', '_', regex=True)
        return self
