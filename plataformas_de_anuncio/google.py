import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import altair as alt

# Verificar se uma loja está selecionada na sessão
if "selected_store" in st.session_state and st.session_state["selected_store"] is not None:
    loja = st.session_state["selected_store"]
    
    # Exibir informação da loja selecionada
    st.header(f"Google: {loja['name']}")
    
    # Separador
    st.divider()
    
    # Informações da integração
    st.subheader("Integração")
    st.info(f"A integração com o Google para {loja['name']} ainda não foi configurada.")
    
else:
    # Caso não tenha loja selecionada (não deve acontecer devido à verificação no iniciar.py)
    st.warning("Por favor, selecione uma loja para visualizar as métricas do Google.")