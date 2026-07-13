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

        self._asignar_estado_municipio()
        return self

    def _asignar_estado_municipio(self):
        """Asigna ESTADO y MUNICIPIO siguiendo el orden de prioridad:

        1. Si el usuario activó la opción "Estado y Municipio de la base",
           se sobrescriben ambas columnas con los valores seleccionados
           (esto también reemplaza las columnas existentes en la base).
        2. En caso contrario, se conservan las columnas ESTADO/MUNICIPIO ya
           presentes en la base o las creadas por el mapeo manual del usuario.
        """
        sobrescribir = getattr(self, 'sobrescribir_estado_municipio', False)

        if sobrescribir:
            if getattr(self, 'estado', ''):
                self.df['ESTADO'] = self.estado
            if getattr(self, 'municipio', ''):
                self.df['MUNICIPIO'] = self.municipio
            return

        # Sin sobrescritura: se respetan las columnas detectadas en la base.
        faltantes = [c for c in ('ESTADO', 'MUNICIPIO') if c not in self.df.columns]
        if faltantes:
            self.advertencias.append(
                "No se encontraron columnas "
                + " y ".join(f"'{c}'" for c in faltantes)
                + " en la base. Activa la opción 'Estado y Municipio de la base' "
                "para asignarlas manualmente."
            )

    def eliminar_espacios_columnas(self):
        self.df.columns = self.df.columns.str.replace(r'\s+', '_', regex=True)
        return self
