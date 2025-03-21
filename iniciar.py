import streamlit as st
from streamlit.runtime.scriptrunner import RerunException, RerunData
import os
from datetime import datetime, timedelta

# Configuração global da página
st.set_page_config(
    layout="wide", 
    page_title="GC Métricas Externas", 
    page_icon=":chart_with_upwards_trend:"
)

# Importar utilitários de banco de dados
from db_utils import load_stores, get_store_details, save_store

# CSS atualizado com bordas arredondadas e fundo verde para tabelas
st.markdown("""
<style>
    /* Reset completo do estilo da barra lateral */
    section[data-testid="stSidebar"] {
        background-color: #EBF3EF !important; /* Cor de fundo Tracky */
        padding: 1rem !important;
        border-right: 1px solid rgba(0, 0, 0, 0.1) !important; /* Linha divisória lateral */
    }
    
    /* Esconder elementos padrão do Streamlit na barra lateral */
    section[data-testid="stSidebarNav"] {
        display: none !important;
    }
    
    /* Estilos para o layout limpo */
    .brand {
        font-size: 28px;
        font-weight: bold;
        margin-bottom: 20px;
        padding-bottom: 15px;
        color: #00774D; /* Verde Tracky */
        text-align: center;
        border-bottom: 1px solid rgba(0, 0, 0, 0.1); /* Linha divisória abaixo do logo */
    }
    
    /* Título da seção com linha divisória */
    .panel-title {
        margin-top: 20px;
        margin-bottom: 15px;
        color: #666;
        font-weight: 500;
        border-top: 1px solid rgba(0, 0, 0, 0.1); /* Linha divisória acima */
        padding-top: 15px;
    }
    
    /* Estilo para inputs e selects com bordas arredondadas */
    input, textarea, select, div[data-baseweb="select"] {
        border: 1px solid #ccc !important;
        border-radius: 6px !important; /* Borda mais arredondada */
        background-color: white !important;
    }
    
    /* Correção para o campo de senha - borda única */
    div[data-baseweb="input"] {
        border: 1px solid #ccc !important;
        border-radius: 6px !important; /* Borda arredondada consistente */
        background-color: white !important;
        overflow: hidden !important;
    }
    
    /* Remover bordas internas do campo de senha */
    div[data-baseweb="input"] input {
        border: none !important;
        box-shadow: none !important;
    }
    
    /* Remover divisor entre input e ícone */
    div[data-baseweb="input"] input + div {
        border-left: none !important;
    }
    
    /* Corrigir ícone de olho */
    div[data-baseweb="input"] [data-baseweb="button"] {
        border: none !important;
        background: transparent !important;
    }
    
    /* Estilizar campos quando estão em foco */
    div[data-baseweb="input"]:focus-within, 
    input:focus, 
    textarea:focus, 
    select:focus {
        border-color: #00774D !important;
        box-shadow: 0 0 0 1px #00774D !important;
    }
    
    /* Estilo para o seletor da loja */
    .selectbox-wrapper div[data-baseweb="select"] {
        border: 1px solid #ccc !important;
        border-radius: 6px !important; /* Borda arredondada */
        background-color: white !important;
    }
    
    /* Estilo para labels de formulário */
    label, .stTextInput label, .stSelectbox label {
        font-size: 14px !important;
        font-weight: 500 !important;
        color: #333 !important;
        margin-bottom: 5px !important;
    }
    
    /* ESTILO PARA TABELAS E EXPANSORES COM FUNDO VERDE */
    .st-expander, .stExpander {
        background-color: #EBF3EF !important; /* Fundo verde claro */
        border: 1px solid #e0e0e0 !important;
        border-radius: 6px !important; /* Borda arredondada */
        overflow: hidden !important;
        margin-bottom: 15px !important;
    }
    
    /* Cabeçalho dos expansores */
    .st-expander > div:first-child, .stExpander > div:first-child {
        background-color: #EBF3EF !important; /* Fundo verde claro */
        padding: 10px 15px !important;
        font-weight: 500 !important;
        color: #333 !important;
        border-bottom: 1px solid #e0e0e0 !important;
    }
    
    /* Corpo dos expansores */
    .st-expander > div:last-child, .stExpander > div:last-child {
        background-color: #f5f5f5 !important; /* Fundo cinza muito claro */
        padding: 15px !important;
    }
    
    /* Ajustes para dataframes e tabelas */
    .dataframe, div[data-testid="stTable"], table {
        border: 1px solid #e0e0e0 !important;
        border-radius: 6px !important; /* Borda arredondada */
        overflow: hidden !important;
    }
    
    /* Cabeçalho das tabelas */
    .dataframe th, div[data-testid="stTable"] th, table th {
        background-color: #EBF3EF !important; /* Fundo verde claro */
        color: #333 !important;
        font-weight: 500 !important;
        border-bottom: 1px solid #e0e0e0 !important;
        padding: 10px 15px !important;
        text-align: left !important;
    }
    
    /* Células das tabelas */
    .dataframe td, div[data-testid="stTable"] td, table td {
        padding: 8px 15px !important;
        border-bottom: 1px solid #f0f0f0 !important;
        color: #333 !important;
        background-color: white !important;
    }
    
    /* Células de tabela - só quando dentro de um expansor */
    .st-expander .dataframe td, .stExpander .dataframe td, 
    .st-expander div[data-testid="stTable"] td, .stExpander div[data-testid="stTable"] td,
    .st-expander table td, .stExpander table td {
        background-color: #f5f5f5 !important; /* Fundo cinza claro para tabelas dentro de expansores */
    }
    
    /* Estilo para botões com bordas arredondadas */
    button, [data-testid="baseButton-secondary"], [data-testid="baseButton-primary"] {
        border-radius: 6px !important; /* Borda arredondada */
    }
    
    /* Botões de ação com bordas arredondadas */
    .action-button button {
        border: 1px solid #ccc !important;
        border-radius: 6px !important; /* Borda arredondada */
        background-color: white !important;
        color: #333 !important;
        padding: 0.5rem 1rem !important;
    }
    
    /* Estilo específico para widgets de data */
    div[data-baseweb="datepicker"] {
        background-color: white !important;
        border: 1px solid #ccc !important;
        border-radius: 6px !important; /* Borda arredondada */
    }
    
    /* Estilo para cabeçalhos de seção */
    h1, h2, h3, h4, h5 {
        color: #333 !important;
    }
    
    /* Ajustes para elementos específicos - Filtros */
    .filter-section {
        margin-bottom: 20px;
    }
    
    .filter-title {
        font-size: 16px;
        font-weight: 500;
        color: #333;
        margin-bottom: 10px;
    }
    
    .filter-select {
        background-color: white;
        border: 1px solid #ccc;
        border-radius: 6px; /* Borda arredondada */
        padding: 8px 12px;
        width: 100%;
        margin-bottom: 10px;
    }
    
    .filter-date-container {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 10px;
        margin-bottom: 10px;
    }
    
    .filter-button {
        background-color: white;
        border: 1px solid #ccc;
        border-radius: 6px; /* Borda arredondada */
        padding: 8px 12px;
        width: 100%;
        text-align: center;
        cursor: pointer;
        margin-bottom: 15px;
    }
    
    .filter-button:hover {
        border-color: #00774D;
        color: #00774D;
    }
    
    /* Esconder a sidebar na página de login */
    .login-page section[data-testid="stSidebar"] {
        display: none !important;
    }
</style>
""", unsafe_allow_html=True)

# Função interna para forçar rerun
def force_rerun():
    raise RerunException(RerunData(None))

# Dicionário de usuários
USERS = {
    "adminmetricasexternas@grupochegou.com": {"password": "admgcexterna2025", "cargo": "Administrador"},
    "metricasexternas@grupochegou.com":  {"password": "gcexterna2025",  "cargo": "Usuário"},
}

def login_page():
    """Página de Login."""
    # Adiciona classe para esconder a sidebar na página de login
    st.markdown('<div class="login-page"></div>', unsafe_allow_html=True)
    
    st.title("GC Métricas Externas")
    st.subheader("Faça seu login")

    email = st.text_input("Email")
    password = st.text_input("Senha", type="password")

    if st.button("Entrar", type="primary"):
        if email in USERS and USERS[email]["password"] == password:
            st.session_state["logged_in"] = True
            st.session_state["cargo"] = USERS[email]["cargo"]
            st.session_state["current_page"] = "home"  # Página inicial após login
            force_rerun()
        else:
            st.error("Credenciais inválidas. Tente novamente.")

def custom_sidebar():
    """Cria uma barra lateral totalmente customizada."""
    with st.sidebar:
        # 1. MARCA NO TOPO
        st.markdown('<div class="brand">GC MÉTRICAS</div>', unsafe_allow_html=True)
        
        # 2. SELETOR DE LOJA
        stores = load_stores()
        store_options = ["Selecione uma loja..."] + [store[1] for store in stores] + ["➕ Cadastrar Nova Loja"]
        
        # Verificar se já existe uma loja selecionada
        default_index = 0
        if "selected_store" in st.session_state and st.session_state["selected_store"] is not None:
            for i, store_name in enumerate(store_options):
                if store_name == st.session_state["selected_store"]["name"]:
                    default_index = i
                    break
        
        # Adicionar div para o seletor com classe para estilização
        st.markdown('<div class="selectbox-wrapper">', unsafe_allow_html=True)
        
        # Dropdown de seleção com estilo customizado
        selected_option = st.selectbox(
            label="Escolha uma loja",
            options=store_options,
            index=default_index,
            label_visibility="collapsed"  # Esconde o label
        )
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Processar a seleção da loja
        handle_store_selection(selected_option, stores)
        
        # 3. MENU DE NAVEGAÇÃO
        st.markdown('<h3 class="panel-title">Seu painel</h3>', unsafe_allow_html=True)
        
        # Verificar página atual
        if "current_page" not in st.session_state:
            st.session_state["current_page"] = "home"
        
        # Renderizar itens do menu
        render_menu_items()
        
        # 4. BOTÃO DE LOGOUT
        logout_clicked = st.button(
            "Logout", 
            key="logout_button", 
            use_container_width=True
        )
        if logout_clicked:
            st.session_state["logged_in"] = False
            st.session_state["cargo"] = None
            st.session_state["selected_store"] = None
            st.session_state["current_page"] = None
            force_rerun()
        
        # Adicionar CSS para esconder o segundo seletor de loja
        st.markdown("""
        <style>
        /* Esconder o texto "Escolha uma loja:" e o seletor na parte inferior */
        div[data-testid="stSidebar"] > div:nth-child(1) > div:nth-child(1) > div:last-child > div:nth-child(1) {
            display: none !important;
        }
        </style>
        """, unsafe_allow_html=True)

def render_menu_items():
    """Renderiza os itens do menu com ícones similares aos da imagem."""
    # Menu items com seus ícones - mantendo os originais
    menu_items = [
        {"id": "home", "icon": "📊", "text": "Visão Geral", "page": "principal/home.py"},
        {"id": "dropi_shopify", "icon": "📋", "text": "Métricas de Produtos", "page": "vendas/dropi_+_shopify.py"},
        {"id": "facebook", "icon": "💰", "text": "Facebook", "page": "plataformas_de_anuncio/facebook.py"},
        {"id": "tiktok", "icon": "📊", "text": "TikTok", "page": "plataformas_de_anuncio/tiktok.py"},
        {"id": "google", "icon": "📊", "text": "Google", "page": "plataformas_de_anuncio/google.py"},
    ]
    
    # Adicionar item Admin para administradores
    if st.session_state.get("cargo") == "Administrador":
        menu_items.append({"id": "admin", "icon": "⚙️", "text": "Configurações", "page": "administracao/admin.py"})
    
    # Render each menu item
    for item in menu_items:
        active_class = "active" if st.session_state.get("current_page") == item["id"] else ""
        menu_clicked = st.button(
            f"{item['icon']} {item['text']}", 
            key=f"menu_{item['id']}",
            use_container_width=True,
            help=f"Ir para {item['text']}"
        )
        
        if menu_clicked:
            st.session_state["current_page"] = item["id"]
            force_rerun()

def handle_store_selection(selected_option, stores):
    """Manipula a seleção da loja."""
    # Lógica para "Cadastrar Nova Loja"
    if selected_option == "➕ Cadastrar Nova Loja":
        st.subheader("Cadastrar Nova Loja")
        
        # Campos para cadastro
        store_name = st.text_input("Nome da Loja:")
        shop_name = st.text_input("Nome da Loja Shopify (prefixo):")
        access_token = st.text_input("Token de Acesso:", type="password")
        
        # DroPi fields
        st.markdown("#### Dados DroPi")
        dropi_url = st.text_input("URL DroPi:", value="https://app.dropi.mx/")
        dropi_username = st.text_input("Email/Usuário DroPi:")
        dropi_password = st.text_input("Senha DroPi:", type="password")
        
        # Currency fields
        st.markdown("#### Configurações de Moeda")
        currencies = {
            "MXN": "Peso Mexicano (MXN)",
            "BRL": "Real Brasileiro (BRL)",
            "USD": "Dólar Americano (USD)",
            "EUR": "Euro (EUR)",
            "GBP": "Libra Esterlina (GBP)",
            "ARS": "Peso Argentino (ARS)",
            "CLP": "Peso Chileno (CLP)",
            "COP": "Peso Colombiano (COP)",
            "PEN": "Sol Peruano (PEN)",
        }
        
        currency_from = st.selectbox(
            "Moeda da Loja:",
            options=list(currencies.keys()),
            format_func=lambda x: currencies[x],
            index=0  # MXN como padrão
        )
        
        currency_to = st.selectbox(
            "Converter para Moeda:",
            options=list(currencies.keys()),
            format_func=lambda x: currencies[x],
            index=1  # BRL como padrão
        )
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Salvar Loja", use_container_width=True, type="primary"):
                if store_name and shop_name and access_token and dropi_username and dropi_password:
                    saved_id = save_store(
                        store_name, 
                        shop_name, 
                        access_token, 
                        dropi_url, 
                        dropi_username, 
                        dropi_password,
                        currency_from,
                        currency_to
                    )
                    
                    if saved_id:
                        st.success(f"Loja '{store_name}' cadastrada com sucesso!")
                        force_rerun()
                    else:
                        st.error("Erro ao cadastrar loja.")
                else:
                    st.error("Preencha todos os campos!")
        
        with col2:
            if st.button("Cancelar", use_container_width=True):
                st.session_state["selected_store"] = None
                force_rerun()
    
    # Atualizar a loja selecionada na session_state
    elif selected_option != "Selecione uma loja...":
        # Encontrar o ID da loja selecionada
        for store_id, store_name in stores:
            if store_name == selected_option:
                selected_store = get_store_details(store_id)
                selected_store["id"] = store_id
                st.session_state["selected_store"] = selected_store
                break
    else:
        st.session_state["selected_store"] = None

def load_page_content():
    """Carrega o conteúdo da página atual."""
    current_page = st.session_state.get("current_page", "home")
    
    # Verificar se a loja foi selecionada para páginas que exigem seleção
    if current_page != "home" and (
        "selected_store" not in st.session_state or 
        st.session_state["selected_store"] is None
    ):
        st.warning("Por favor, selecione uma loja para visualizar esta página.")
        return
    
    # Mapear IDs de página para caminhos de arquivo
    page_paths = {
        "home": "principal/home.py",
        "dropi_shopify": "vendas/dropi_+_shopify.py",
        "facebook": "plataformas_de_anuncio/facebook.py",
        "tiktok": "plataformas_de_anuncio/tiktok.py",
        "google": "plataformas_de_anuncio/google.py",
        "admin": "administracao/admin.py"
    }
    
    # Carregar o conteúdo da página correspondente
    if current_page in page_paths:
        page_path = page_paths[current_page]
        if os.path.exists(page_path):
            import importlib.util
            spec = importlib.util.spec_from_file_location(current_page, page_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        else:
            st.title(f"Página {current_page}")
            st.warning(f"Módulo não encontrado: {page_path}")
    else:
        st.title("Página não encontrada")
        st.warning("A página solicitada não existe ou você não tem permissão para acessá-la.")

def main():
    # Inicializa variáveis de sessão
    if "logged_in" not in st.session_state:
        st.session_state["logged_in"] = False
    if "cargo" not in st.session_state:
        st.session_state["cargo"] = None
    if "selected_store" not in st.session_state:
        st.session_state["selected_store"] = None
    if "current_page" not in st.session_state:
        st.session_state["current_page"] = "home"

    # Se NÃO estiver logado, exibe apenas a página de login
    if not st.session_state["logged_in"]:
        # Não mostra mais nada na sidebar durante o login
        login_page()
    else:
        # Interface customizada
        custom_sidebar()
        
        # Carregar conteúdo da página atual
        load_page_content()

if __name__ == "__main__":
    main()