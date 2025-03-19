import streamlit as st
import pandas as pd
import sys
import os

# Adicionar a raiz do projeto ao path para importar módulos
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importar utilitários de banco de dados
from db_utils import (
    execute_query, get_db_connection, load_stores, is_railway_environment
)

# Verificar se o usuário tem permissão de administrador
if st.session_state.get("cargo") != "Administrador":
    st.error("Você não tem permissão para acessar esta página.")
    st.stop()

# Título da página
st.title("Administração do Sistema")

# Função para excluir uma loja e todos os seus dados
def delete_store(store_id):
    try:
        # Registrar ID para verificação
        st.session_state['last_deleted_id'] = store_id
        
        # Excluir dados relacionados primeiro
        tables = ["product_metrics", "dropi_metrics", "product_effectiveness"]
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Iniciar transação
            if is_railway_environment():
                cursor.execute("BEGIN")
            else:
                cursor.execute("BEGIN TRANSACTION")
            
            # Excluir dados relacionados
            for table in tables:
                if is_railway_environment():
                    # PostgreSQL usa %s
                    cursor.execute(f"DELETE FROM {table} WHERE store_id = %s", (store_id,))
                else:
                    # SQLite usa ?
                    cursor.execute(f"DELETE FROM {table} WHERE store_id = ?", (store_id,))
            
            # Excluir a loja
            if is_railway_environment():
                cursor.execute("DELETE FROM stores WHERE id = %s", (store_id,))
            else:
                cursor.execute("DELETE FROM stores WHERE id = ?", (store_id,))
            
            # Verificar quantas linhas foram afetadas
            if is_railway_environment():
                # No PostgreSQL, podemos usar a propriedade rowcount
                deleted_rows = cursor.rowcount
            else:
                # No SQLite, precisamos verificar manualmente
                cursor.execute("SELECT changes()")
                deleted_rows = cursor.fetchone()[0]
            
            # Confirmar transação
            conn.commit()
            
            # Log para debug
            st.session_state['last_deleted_count'] = deleted_rows
            
            conn.close()
            
            if deleted_rows > 0:
                return True, f"Loja excluída com sucesso! ({deleted_rows} registros removidos)"
            else:
                return False, "Nenhum registro foi removido. Verifique o ID da loja."
                
        except Exception as e:
            # Reverter em caso de erro
            conn.rollback()
            conn.close()
            raise e
            
    except Exception as e:
        return False, f"Erro ao excluir loja: {str(e)}"

# Seção de Gestão de Lojas
st.header("Gestão de Lojas")

# Botão para forçar atualização da lista de lojas
if st.button("Atualizar Lista de Lojas"):
    # Limpar cache e recarregar a página
    st.cache_data.clear()
    st.rerun()

# Carregar todas as lojas
stores = load_stores()

if not stores:
    st.info("Não há lojas cadastradas no sistema.")
else:
    # Exibir debug info
    if 'last_deleted_id' in st.session_state:
        with st.expander("Informações de Depuração (última exclusão)"):
            st.write(f"Último ID excluído: {st.session_state['last_deleted_id']}")
            if 'last_deleted_count' in st.session_state:
                st.write(f"Registros afetados: {st.session_state['last_deleted_count']}")
    
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
                    success, message = delete_store(selected_id)
                    if success:
                        st.success(message)
                        # Recarregar a lista de lojas
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error(message)
            
            with confirm_col2:
                if st.button("Não, cancelar"):
                    st.rerun()  # Recarregar a página