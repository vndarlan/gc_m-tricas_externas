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
    
    # Mostrar instruções de uso
    st.subheader("Como usar este dashboard")
    st.markdown("""
    1. **Selecione uma loja**: Use o seletor na parte superior da barra lateral para escolher uma loja
    2. **Navegue entre as páginas**: Use o menu na barra lateral para acessar:
       - **Dropi + Shopify**: Dados de vendas e desempenho de produtos
       - **Facebook**: Métricas das campanhas de Facebook Ads
       - **TikTok**: Métricas das campanhas de TikTok Ads
       - **Google**: Métricas das campanhas de Google Ads
    3. **Filtre os dados**: Cada página possui filtros específicos
    """)
    
    # Mostrar informações do sistema
    with st.expander("Informações do Sistema", expanded=False):
        st.info("Este dashboard está conectado ao banco de dados PostgreSQL no Railway.")
        st.markdown("""
        **Recursos disponíveis:**
        - Integração com Shopify via API
        - Extração automática de dados da DroPi
        - Visualização de métricas de plataformas de anúncios
        - Análise de efetividade de produtos
        """)
else:
    # Loja selecionada - mostrar dashboard resumido
    selected_store = st.session_state["selected_store"]
    
    # Título com o nome da loja
    st.title(f"Dashboard Resumido - {selected_store['name']}")
    
    # Data de referência (hoje)
    today = datetime.now()
    last_week = today - timedelta(days=7)
    
    # Função para carregar dados resumidos
    def load_summary_data(store_id):
        conn = get_db_connection()
        
        # Carregar dados de produto mais vendido (últimos 7 dias)
        product_query = f"""
            SELECT product, SUM(total_orders) as orders, SUM(total_value) as value
            FROM product_metrics 
            WHERE store_id = '{store_id}' 
              AND date >= '{last_week.strftime('%Y-%m-%d')}'
            GROUP BY product
            ORDER BY orders DESC
            LIMIT 1
        """
        
        try:
            top_product_df = pd.read_sql_query(product_query, conn)
            if not top_product_df.empty:
                top_product = {
                    'name': top_product_df.iloc[0]['product'],
                    'orders': int(top_product_df.iloc[0]['orders']),
                    'value': float(top_product_df.iloc[0]['value'])
                }
            else:
                top_product = {'name': 'N/A', 'orders': 0, 'value': 0}
        except Exception as e:
            logger.error(f"Erro ao buscar produto mais vendido: {str(e)}")
            top_product = {'name': 'Erro', 'orders': 0, 'value': 0}
        
        # Carregar dados de efetividade
        effectiveness_query = f"""
            SELECT AVG(general_effectiveness) as avg_effectiveness
            FROM product_effectiveness 
            WHERE store_id = '{store_id}'
        """
        
        try:
            effectiveness_df = pd.read_sql_query(effectiveness_query, conn)
            if not effectiveness_df.empty and effectiveness_df.iloc[0]['avg_effectiveness'] is not None:
                avg_effectiveness = float(effectiveness_df.iloc[0]['avg_effectiveness'])
            else:
                avg_effectiveness = 0
        except Exception as e:
            logger.error(f"Erro ao buscar efetividade média: {str(e)}")
            avg_effectiveness = 0
        
        # Carregar dados da DroPi (lucro total últimos 7 dias)
        dropi_query = f"""
            SELECT SUM(profits) as total_profits
            FROM dropi_metrics 
            WHERE store_id = '{store_id}' 
              AND date >= '{last_week.strftime('%Y-%m-%d')}'
        """
        
        try:
            profits_df = pd.read_sql_query(dropi_query, conn)
            if not profits_df.empty and profits_df.iloc[0]['total_profits'] is not None:
                total_profits = float(profits_df.iloc[0]['total_profits'])
            else:
                total_profits = 0
        except Exception as e:
            logger.error(f"Erro ao buscar lucro total: {str(e)}")
            total_profits = 0
        
        conn.close()
        
        return {
            'top_product': top_product,
            'avg_effectiveness': avg_effectiveness,
            'total_profits': total_profits
        }
    
    # Carregar dados resumidos
    summary_data = load_summary_data(selected_store['id'])
    
    # Exibir cards de informações
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.info("Produto Mais Vendido (7 dias)")
        st.markdown(f"**{summary_data['top_product']['name']}**")
        st.write(f"Pedidos: {summary_data['top_product']['orders']}")
        st.write(f"Valor: ${summary_data['top_product']['value']:.2f}")
    
    with col2:
        st.info("Efetividade Média")
        st.markdown(f"**{summary_data['avg_effectiveness']:.1f}%**")
        
        # Adicionar indicador visual
        if summary_data['avg_effectiveness'] >= 70:
            st.success("Efetividade Ótima")
        elif summary_data['avg_effectiveness'] >= 50:
            st.warning("Efetividade Boa")
        else:
            st.error("Efetividade Baixa")
    
    with col3:
        st.info("Lucro Total (7 dias)")
        st.markdown(f"**${summary_data['total_profits']:.2f}**")
    
    # Adicionar links rápidos para as principais páginas
    st.subheader("Links Rápidos")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("📦 Dropi + Shopify", use_container_width=True):
            # Redirecionar para a página
            st.session_state["_current_page"] = "vendas/dropi_+_shopify.py"
            st.rerun()
    
    with col2:
        if st.button("📢 Facebook Ads", use_container_width=True):
            st.session_state["_current_page"] = "plataformas_de_anuncio/facebook.py"
            st.rerun()
    
    with col3:
        if st.button("📊 Google Ads", use_container_width=True):
            st.session_state["_current_page"] = "plataformas_de_anuncio/google.py"
            st.rerun()
    
    # Mostrar instruções e dicas
    st.subheader("Dicas de Uso")
    st.markdown("""
    - Para visualizar relatórios detalhados, acesse as páginas específicas no menu lateral
    - Atualize os dados regularmente usando os botões de atualização em cada página
    - Você pode exportar dados de relatórios expandindo as tabelas de dados detalhados
    """)