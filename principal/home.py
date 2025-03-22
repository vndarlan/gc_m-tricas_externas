import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import sys
import os
import logging

# Adicionar a raiz do projeto ao path para importar módulos
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importar utilitários de banco de dados
try:
    from db_utils import get_db_connection, is_railway_environment
except ImportError as e:
    st.error(f"Erro ao importar módulos: {str(e)}")

# Configuração do logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("home_dashboard")

# Verificar se uma loja foi selecionada globalmente
if "selected_store" not in st.session_state or st.session_state["selected_store"] is None:
    # Na página Home, permitimos visualização sem loja selecionada
    st.title("Métricas Externas - Dashboard")
    st.write("Bem-vindo ao sistema de métricas externas. Selecione uma loja na barra lateral para visualizar dados específicos.")
else:
    # Loja selecionada - mostrar dashboard resumido
    selected_store = st.session_state["selected_store"]
    
    # Título com o nome da loja
    st.title(f"Visão Geral - {selected_store['name']}")