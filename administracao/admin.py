import streamlit as st
import pandas as pd
import sys
import os

# Adicionar a raiz do projeto ao path para importar módulos
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importar utilitários de banco de dados
from db_utils import load_stores, delete_store_by_id

# Verificar se o usuário tem permissão de administrador
if st.session_state.get("cargo") != "Administrador":
    st.error("Você não tem permissão para acessar esta página.")
    st.stop()

# Título da página
st.title("Administração do Sistema")

# Seção de Gestão de Lojas
st.header("Gestão de Lojas")

# Botão para forçar atualização da lista de lojas
if st.button("Atualizar Lista de Lojas"):
    st.rerun()

# Carregar todas as lojas
stores = load_stores()

if not stores:
    st.info("Não há lojas cadastradas no sistema.")
else:
    # Exibir informações sobre exclusão
    st.write("Selecione uma loja para excluir. Esta ação é irreversível e removerá todos os dados relacionados à loja.")
    
    # Exibir dropdown para selecionar a loja
    store_options = [f"{store_name} (ID: {store_id})" for store_id, store_name in stores]
    selected_store = st.selectbox("Selecione uma loja:", store_options)
    
    if selected_store:
        # Extrair ID da loja do texto selecionado
        selected_id = selected_store.split("(ID: ")[1].split(")")[0]
        selected_name = selected_store.split(" (ID:")[0]
        
        # Mostrar ID para verificação
        st.info(f"ID da loja selecionada: {selected_id}")
        
        # Botão de exclusão com confirmação
        if st.button(f"Excluir a loja '{selected_name}'"):
            # Adicionar uma segunda camada de confirmação
            st.warning(f"Tem certeza que deseja excluir a loja '{selected_name}'? Esta ação é irreversível.")
            
            confirm_col1, confirm_col2 = st.columns(2)
            with confirm_col1:
                if st.button("Sim, tenho certeza"):
                    # Usar a função do db_utils para excluir a loja
                    success, message = delete_store_by_id(selected_id)
                    
                    if success:
                        st.success(message)
                        # Recarregar a página para atualizar a lista
                        st.rerun()
                    else:
                        st.error(message)
            
            with confirm_col2:
                if st.button("Não, cancelar"):
                    st.rerun()  # Recarregar a página