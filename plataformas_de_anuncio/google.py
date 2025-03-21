import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import altair as alt

# Título da página
st.title("Google")

# Verificar se uma loja está selecionada na sessão
if "selected_store" in st.session_state and st.session_state["selected_store"] is not None:
    loja = st.session_state["selected_store"]
    
    # Exibir informação da loja selecionada
    st.header(f"Loja: {loja['name']}")
    
    # Container para futuras métricas
    with st.container():
        st.subheader("Métricas do Google")
        st.info("As métricas detalhadas do Google serão implementadas em uma futura atualização.")
        
        # Placeholder para métricas
        col1, col2 = st.columns(2)
        with col1:
            st.metric(label="Impressões", value="--")
            st.metric(label="Cliques", value="--")
        with col2:
            st.metric(label="CPC", value="--")
            st.metric(label="Conversões", value="--")
    
    # Separador
    st.divider()
    
    # Informações da integração
    st.subheader("Integração")
    st.info(f"A integração com o Google para {loja['name']} ainda não foi configurada.")
    
else:
    # Caso não tenha loja selecionada (não deve acontecer devido à verificação no iniciar.py)
    st.warning("Por favor, selecione uma loja para visualizar as métricas do Google.")