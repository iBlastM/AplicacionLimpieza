import streamlit as st
import pandas as pd
import unicodedata
import re
from io import BytesIO

def limpiar_texto(texto):
    """Limpia texto: elimina acentos, caracteres especiales y espacios extra."""
    if not isinstance(texto, str):
        return texto
    # Normalizar y quitar acentos
    texto = unicodedata.normalize('NFKD', texto)\
            .encode('ascii', 'ignore')\
            .decode('utf-8')
    # Quitar caracteres especiales y mantener letras/números
    texto = re.sub(r'[^a-zA-Z0-9\s]', '', texto)
    # Quitar espacios extra
    texto = re.sub(r'\s+', ' ', texto).strip()
    return texto

def limpiar_calle_larga(calle):
    calle = str(calle).strip()
    
    if len(calle) > 50:
        
        match = re.search(r'^([^0-9]+)', calle)
        if match:
            return match.group(1).strip()
            
    return calle

def limpiar_num_ext(valor):
    """Limpia números exteriores e interiores eliminando basura común."""
    valor = str(valor).strip()
    valor = valor.replace('-', '')
    if re.search(r'\d+\.\d+', valor):  # Ignora si parece decimal
        return ""
    valor = valor.replace('.', '')
    if re.search(r'\d', valor):  # Solo devuelve si contiene números
        return valor
    return ""

def concatenar_direccion_logica(fila):
    """Genera la dirección homologada a partir de los campos individuales."""
    calle = str(fila['CALLE']).strip()
    next_ = str(fila['NUM_EXT']).strip()
    nint = str(fila['NUM_INT']).strip()
    colonia = str(fila['COLONIA']).strip()
    mun = str(fila['MUNICIPIO']).strip()
    est = str(fila['ESTADO']).strip()
    cp = str(fila['CODIGO_POSTAL']).strip()

    partes = []
    if calle not in ["", "NAN", "nan"]:
        partes.append(calle)
        if next_ not in ["", "NAN", "nan"]:
            partes.append(next_)
        if nint not in ["", "NAN", "nan"]:
            partes.append("INT." + nint)

    if colonia not in ["", "NAN", "nan"]:
        partes.append(colonia)
        # Si no había calle, el número se asocia a la colonia para el bloque de dirección
        if next_ not in ["", "NAN", "nan"] and calle in ["", "NAN", "nan"]:
            partes.append(next_)
        if nint not in ["", "NAN", "nan"] and calle in ["", "NAN", "nan"]:
            partes.append("INT." + nint)
    
    for campo in [cp, mun, est]:
        if campo not in ["", "NAN", "nan"]:
            partes.append(campo)

    return ", ".join(partes)

# --- INTERFAZ DE STREAMLIT ---

st.set_page_config(page_title="Limpiador de Programas Sociales", layout="wide")

st.title("Limpiador de Bases de Datos")
st.write("Sube tu archivo Excel de Programas Sociales para normalizarlo automáticamente.")

# 1. Subida de Archivo
archivo_subido = st.file_uploader("Elige un archivo Excel (.xlsx)", type=["xlsx"])

if archivo_subido is not None:
    try:
        # Cargar datos
        df = pd.read_excel(archivo_subido)
        st.success("Archivo cargado con éxito.")
        
        with st.expander("Ver vista previa de datos originales"):
            st.dataframe(df.head(10))

        if st.button("Iniciar Limpieza"):
            with st.status("Procesando datos...", expanded=True) as status:
                
                # 2. Renombrar y eliminar columnas iniciales
                df = df.rename(columns={'Identificador' : 'CURP', 'Nombre.1' : 'NOMBRE_PROGRAMA'})
                df = df.drop(columns=['IdTipoTramite', 'IdEstatus', 'Campo3'])

                # 3. Limpieza general de cadenas y texto
                df = df.apply(lambda x: " ".join(x.split()) if isinstance(x, str) else x)
    
                columnas_limpiar = ['Nombre', 'ApellidoPaterno', 'ApellidoMaterno', 'Calle', 'Colonia', 'NOMBRE_PROGRAMA']
                for columna in columnas_limpiar:
                    df[columna] = df[columna].apply(limpiar_texto)

                # 4. Homologación de nombres de columnas a mayúsculas y formato estándar
                df.columns = df.columns.str.upper()
                df = df.rename(columns={
                    'IDUSUARIO' : 'ID_USUARIO', 'IDPERSONA' : 'ID_PERSONA',
                    'NOMBRE' : 'NOMBRE(S)_DE_PILA', 'APELLIDOPATERNO' : 'AP_PATERNO',
                    'APELLIDOMATERNO' : 'AP_MATERNO', 'FECHANACIMIENTO' : 'FECHA_NACIMIENTO',
                    'NUMEXT' : 'NUM_EXT', 'NUMINT' : 'NUM_INT',
                    'CODIGOPOSTAL' : 'CODIGO_POSTAL', 'FECHAREGISTRO' : 'FECHA_REGISTRO',
                    'IDPARENTESCO' : 'ID_PARENTESCO'
                })

                # 5. Creación de campos compuestos y ubicación estática
                df['NOMBRE_COMPLETO'] = df['AP_PATERNO'] + " " + df['AP_MATERNO'] + " " + df['NOMBRE(S)_DE_PILA']
                df['ESTADO'] = 'QUERETARO'
                df['MUNICIPIO'] = 'CORREGIDORA'

                # 6. Limpieza de Teléfonos y Celulares
                for col in ['TELEFONO', 'CELULAR']:
                    df[col] = df[col].astype(str).str.replace(r'\D', '', regex=True)
                    mask = (df[col].str.len() != 10) | (df[col].isin(['NULL', 'nan', 'sin tel']))
                    df.loc[mask, col] = ""

                # 7. Limpieza de Correo
                df['CORREO'] = df['CORREO'].astype(str).str.strip().str.lower()
                patron_email = r'^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$'
                mask_valido = df['CORREO'].str.contains(patron_email, na=False, regex=True)
                df.loc[~mask_valido | (df['CORREO'] == 'nan'), 'CORREO'] = ""

            # 8. Procesamiento de Nombre y Año del Programa
                df['NOMBRE_PROGRAMA'] = df['NOMBRE_PROGRAMA'].str.replace(r'^[AZ](?=[A-Z])', '', regex=True)
                df['ANO_PROGRAMA'] = df['NOMBRE_PROGRAMA'].str.extract(r'(\d{4})')[0]
                df['NOMBRE_PROGRAMA'] = df['NOMBRE_PROGRAMA'].str.replace(r'\d{4}', '', regex=True)
                df['NOMBRE_PROGRAMA'] = df['NOMBRE_PROGRAMA'].str.replace(r'\s+', ' ', regex=True).str.strip()
                df.loc[df['NOMBRE_PROGRAMA'] == 'CALENTADOR SOLAR CORREGIDORA 2', 'NOMBRE_PROGRAMA'] = 'CALENTADOR SOLAR CORREGIDORA'

            # 9. Limpieza de Código Postal y Parentesco
                df['CODIGO_POSTAL'] = df['CODIGO_POSTAL'].astype(str).str.replace(r'\D', '', regex=True)
                mask_cp = (df['CODIGO_POSTAL'].str.len() == 5) & (df['CODIGO_POSTAL'] != 'nan')
                df.loc[~mask_cp, 'CODIGO_POSTAL'] = ""
    
                df['ID_PARENTESCO'] = df['ID_PARENTESCO'].astype(str)
                df.loc[df['ID_PARENTESCO'] != '1', 'ID_PARENTESCO'] = 'DEPENDIENTE'
                df.loc[df['ID_PARENTESCO'] == '1', 'ID_PARENTESCO'] = 'BENEFICIARIO'

            # 10. Conversión de fechas de Excel (serial) a formato legible
                for col in ['FECHA_NACIMIENTO', 'FECHA_REGISTRO']:
                    df[col] = pd.to_datetime(pd.to_numeric(df[col], errors='coerce'), 
                                 unit='D', origin='1899-12-30')
                    df[col] = df[col].dt.strftime('%d/%m/%Y').replace(['NaT', 'nan', 'NaN'], "")

            # 11. Limpieza de Direcciones (Calles, Números, Colonias)
                df['NUM_EXT'] = df['NUM_EXT'].apply(limpiar_num_ext)
                df['NUM_INT'] = df['NUM_INT'].apply(limpiar_num_ext)
                df.loc[df['NUM_EXT'] == '0', 'NUM_EXT'] = ''
                df.loc[df['NUM_INT'] == '0', 'NUM_INT'] = ''

                df['CALLE'] = df['CALLE'].astype(str).str.upper().str.strip()
                df['CALLE'] = df['CALLE'].str.replace(r'\b(A|AV)\b', 'AVENIDA', regex=True)
                df['CALLE'] = df['CALLE'].str.replace(r'\bC\b', '', regex=True).str.strip()
    
            # Eliminar valores basura en calles
                basura = ['DOMICILIO CONOCIDO', 'SIN NOMBRE', 'SN', 'S/N', 'NAN', 'NONE', "SIN CALLE", "SINNOMBRE"]
                mask_calle = df['CALLE'].isin(basura) | df['CALLE'].str.contains(r'^\d+$', na=False)
                df.loc[mask_calle, 'CALLE'] = ""
                df['CALLE'] = df['CALLE'].apply(limpiar_calle_larga)

                df['COLONIA'] = df['COLONIA'].astype(str).str.upper().str.strip()
                df['COLONIA'] = df['COLONIA'].str.replace(r'\bFRACC?\b', 'FRACCIONAMIENTO', regex=True)
                mask_colonia = df['COLONIA'].isin(['OTRO', 'NAN', 'NONE']) | df['COLONIA'].str.contains(r'^\d+$', na=False)
                df.loc[mask_colonia, 'COLONIA'] = ""

            # 12. Creación de Dirección Homologada y Exportación
                df['DIRECCION_HOMOLOGADA'] = df.apply(concatenar_direccion_logica, axis=1)
                
            status.update(label="¡Limpieza terminada!", state="complete", expanded=False)

            # 2. Vista previa del resultado
            st.subheader("Resultado Final")
            st.dataframe(df.head(10))

            # 3. Descarga del archivo
            # Convertimos el DF a un buffer de Excel para que Streamlit pueda descargarlo
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Limpio')
            
            st.download_button(
                label="Descargar Base Limpia (Excel)",
                data=output.getvalue(),
                file_name="base_limpia.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

    except Exception as e:
        st.error(f"Ocurrió un error al procesar el archivo: {e}")

else:
    st.info("A la espera de un archivo Excel.")