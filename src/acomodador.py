"""
acomodador.py
Componente Streamlit para reordenar las columnas del DataFrame resultado.

Uso:
    from acomodador import mostrar_acomodador

    df = mostrar_acomodador(df)   # devuelve el df con las columnas en el orden elegido
"""

import pandas as pd
import streamlit as st

_KEY_ORDEN = "_acomodador_orden"


def mostrar_acomodador(df: pd.DataFrame) -> pd.DataFrame:
    """Muestra un panel interactivo para reordenar las columnas del DataFrame.

    - Persiste el orden en st.session_state para sobrevivir reruns.
    - Sincroniza automáticamente si el conjunto de columnas cambia (p.ej. tras
      una nueva georeferenciación).
    - Retorna el DataFrame con las columnas en el orden seleccionado por el usuario.
    """
    columnas_base = df.columns.tolist()

    # Inicializar / sincronizar el orden guardado
    orden_guardado = st.session_state.get(_KEY_ORDEN, [])
    columnas_guardadas_set = set(orden_guardado)
    columnas_actuales_set = set(columnas_base)

    if columnas_guardadas_set != columnas_actuales_set:
        # Conservar el orden relativo de las columnas que ya estaban y
        # agregar al final las nuevas que aún no existen.
        conocidas = [c for c in orden_guardado if c in columnas_actuales_set]
        nuevas = [c for c in columnas_base if c not in columnas_guardadas_set]
        st.session_state[_KEY_ORDEN] = conocidas + nuevas

    orden = st.session_state[_KEY_ORDEN]

    st.subheader("Ordenar columnas del resultado")
    st.caption(
        "Selecciona una columna de la lista y usa los botones para cambiar su posición. "
        "El orden que definas aquí se reflejará en la vista previa y en los archivos descargados."
    )

    col_sel, col_botones = st.columns([4, 1], vertical_alignment="bottom")

    with col_sel:
        columna_sel = st.selectbox(
            "Columna",
            options=orden,
            key="_acomodador_sel",
            label_visibility="collapsed",
        )

    idx = orden.index(columna_sel)

    with col_botones:
        b1, b2, b3 = st.columns(3)
        with b1:
            subir = st.button(
                "▲",
                disabled=(idx == 0),
                help="Mover arriba",
                key="_acomodador_up",
                use_container_width=True,
            )
        with b2:
            bajar = st.button(
                "▼",
                disabled=(idx == len(orden) - 1),
                help="Mover abajo",
                key="_acomodador_down",
                use_container_width=True,
            )
        with b3:
            reset = st.button(
                "↺",
                help="Restablecer orden original",
                key="_acomodador_reset",
                use_container_width=True,
            )

    if subir:
        orden[idx], orden[idx - 1] = orden[idx - 1], orden[idx]
        st.session_state[_KEY_ORDEN] = orden
        st.rerun()

    if bajar:
        orden[idx], orden[idx + 1] = orden[idx + 1], orden[idx]
        st.session_state[_KEY_ORDEN] = orden
        st.rerun()

    if reset:
        st.session_state[_KEY_ORDEN] = columnas_base.copy()
        st.rerun()

    # Vista de la lista ordenada
    with st.expander("Ver orden actual", expanded=True):
        filas = []
        for i, col in enumerate(orden):
            marcador = "▶" if col == columna_sel else ""
            filas.append({"#": i + 1, "Columna": col, "": marcador})
        st.dataframe(
            pd.DataFrame(filas),
            hide_index=True,
            use_container_width=True,
            height=min(35 * len(orden) + 38, 400),
        )

    return df[orden]
