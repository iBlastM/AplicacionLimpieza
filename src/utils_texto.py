import re
import unicodedata


def limpiar_texto(texto: str) -> str:
    """Limpia texto: elimina acentos, caracteres especiales y espacios extra."""
    if not isinstance(texto, str):
        return texto
    texto = unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('utf-8')
    texto = re.sub(r'[^a-zA-Z0-9\s]', '', texto)
    return re.sub(r'\s+', ' ', texto).strip()


def limpiar_calle_larga(calle: str) -> str:
    """Recorta calles con más de 50 caracteres dejando solo la parte alfabética inicial."""
    calle = str(calle).strip()
    if len(calle) > 50:
        match = re.search(r'^([^0-9]+)', calle)
        if match:
            return match.group(1).strip()
    return calle


def limpiar_num_ext(valor) -> str:
    """Limpia números exteriores e interiores eliminando basura común."""
    valor = str(valor).strip().replace('-', '')
    if re.search(r'\d+\.\d+', valor):
        return ""
    valor = valor.replace('.', '')
    return valor if re.search(r'\d', valor) else ""
