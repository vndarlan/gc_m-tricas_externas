import streamlit as st
from streamlit.runtime.scriptrunner import RerunException, RerunData
import os
from datetime import datetime, timedelta

# Configuração global da página
st.set_page_config(
    layout="wide", 
    page_title="Chegou Insights", 
    page_icon=":chart_with_upwards_trend:"
)

# Importar utilitários de banco de dados
from db_utils import load_stores, get_store_details, save_store

# CSS atualizado com bordas arredondadas e fundo verde para tabelas
st.markdown("""
<style>
/* Correção para remover a barra branca no topo */
.main .block-container {
    padding-top: 0 !important;
    max-width: 1000px;
}

/* Ajuste para o container principal - remove espaços em branco */
.css-1d391kg, .css-1wrcr25, .css-ocqkz7, .css-uf99v8 {
    padding-top: 0 !important;
    margin-top: 0 !important;
}

/* Ajuste para o elemento root - remove espaços em branco no topo */
.st-emotion-cache-ffhzg2 {
    padding-top: 0 !important;
}

/* Estilos gerais para melhorar a aparência */
.stApp {
    background-color: #f0f5f3;
}

/* Cabeçalho do dashboard */
h1 {
    padding: 15px;
    border-radius: 10px;
    background: linear-gradient(135deg, #0E9E6D 0%, #008555 100%);
    color: white !important;
    text-align: center;
    margin-bottom: 25px;
    box-shadow: 0 4px 12px rgba(14, 158, 109, 0.3);
}

/* Containers para as seções principais */
.main-section {
    background-color: white;
    border-radius: 12px;
    padding: 20px;
    margin-bottom: 30px;
    box-shadow: 0 6px 16px rgba(0, 0, 0, 0.1);
    border-top: 5px solid #0E9E6D;
}

/* Estilo para a barra lateral - cópia exata */
section[data-testid="stSidebar"] {
    background: linear-gradient(160deg, #ffffff, #f0f5f3);
    border-right: 1px solid rgba(14, 158, 109, 0.2);
    box-shadow: 2px 0 10px rgba(0, 0, 0, 0.05);
}

/* Título principal da barra lateral - original mantido */
.brand-title {
    color: #0E9E6D;
    font-size: 2rem;
    font-weight: bold;
    margin-bottom: 1rem;
    text-align: left;
}

/* Linha divisória simples - original mantido */
.sidebar-divider {
    border-top: 1px solid #ddd;
    margin: 1rem 0;
}

/* Título "Seu painel" - original mantido */
.panel-title {
    text-align: left;
    font-size: 1rem;
    margin-bottom: 1rem;
}

/* Linha única de controles para o dashboard */
.single-line-container {
    display: flex;
    flex-direction: row;
    align-items: center;
    gap: 20px;
    padding: 15px;
    background: linear-gradient(to right, #ffffff, #f5f9f7);
    border-radius: 10px;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08);
    margin-bottom: 20px;
    border-left: 4px solid #0E9E6D;
}

/* Containers de métricas - estilo geral */
div[data-testid="metric-container"] {
    background: linear-gradient(145deg, #ffffff, #f8fcfa);
    border-radius: 10px;
    padding: 20px;
    box-shadow: 0 8px 20px rgba(0, 0, 0, 0.1);
    margin-bottom: 20px;
    border: 1px solid rgba(14, 158, 109, 0.2);
    transition: transform 0.3s, box-shadow 0.3s;
    position: relative;
    overflow: hidden;
}

div[data-testid="metric-container"]:hover {
    transform: translateY(-5px);
    box-shadow: 0 12px 25px rgba(0, 0, 0, 0.15);
}

/* Efeito de destaque lateral */
div[data-testid="metric-container"]::before {
    content: "";
    position: absolute;
    top: 0;
    left: 0;
    width: 6px;
    height: 100%;
    background: linear-gradient(to bottom, #0E9E6D, #08724e);
}

/* Título da métrica (Pedidos, Em Trânsito, Entregues) */
div[data-testid="metric-container"] > div:first-child {
    font-weight: bold;
    color: #0E9E6D;
    font-size: 1.2rem;
    text-transform: uppercase;
    letter-spacing: 1px;
    margin-bottom: 10px;
}

/* Valor da métrica (números) */
div[data-testid="metric-container"] > div:nth-child(2) {
    font-size: 2.5rem;
    font-weight: 800;
    color: #333;
    text-shadow: 1px 1px 2px rgba(0,0,0,0.1);
    letter-spacing: -1px;
}

/* Valor monetário da métrica */
div[data-testid="metric-container"] > div:nth-child(3) {
    color: #0E9E6D;
    font-weight: bold;
    font-size: 1.15rem;
    background: linear-gradient(45deg, #0E9E6D, #077a52);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    padding: 5px 10px;
    border-radius: 20px;
    display: inline-block;
    margin-top: 5px;
    box-shadow: 0 2px 10px rgba(14, 158, 109, 0.2);
}

/* Estilo específico para métricas Dropi */
.dropi-metrics div[data-testid="metric-container"]::before {
    background: linear-gradient(to bottom, #FF8C00, #FF6347);
}

.dropi-metrics div[data-testid="metric-container"] > div:first-child {
    color: #FF8C00;
}

.dropi-metrics div[data-testid="metric-container"] > div:nth-child(3) {
    background: linear-gradient(45deg, #FF8C00, #FF6347);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    box-shadow: 0 2px 10px rgba(255, 140, 0, 0.2);
}

/* Caixas de informação */
.info-box {
    background: linear-gradient(to right, #f0f7f4, #ffffff);
    border-radius: 8px;
    padding: 12px;
    margin-top: 15px;
    margin-bottom: 20px;
    text-align: center;
    border-left: 4px solid #0E9E6D;
    box-shadow: 0 3px 10px rgba(0, 0, 0, 0.05);
    font-weight: 500;
}

/* Estilizando tabelas e gráficos */
[data-testid="stDataFrame"] {
    border-radius: 10px;
    overflow: hidden;
    box-shadow: 0 4px 15px rgba(0, 0, 0, 0.08);
}

/* Botões mais atraentes - preservando alinhamento à esquerda original */
button {
    background: linear-gradient(135deg, #0E9E6D 0%, #008555 100%) !important;
    color: white !important;
    border-radius: 8px !important;
    font-weight: bold !important;
    box-shadow: 0 4px 10px rgba(14, 158, 109, 0.3) !important;
    transition: all 0.2s !important;
}

button:hover {
    transform: translateY(-2px) !important;
    box-shadow: 0 6px 15px rgba(14, 158, 109, 0.4) !important;
}

/* Forçar alinhamento à esquerda nos botões da barra lateral - mantido original */
button[data-testid="baseButton-secondary"] {
    text-align: left !important;
    justify-content: flex-start !important;
}

/* Estilo dos ícones Material - mantido original */
.material-symbols-rounded {
    font-variation-settings: 'FILL' 0, 'wght' 400, 'GRAD' 0, 'opsz' 24;
    margin-right: 8px;
    vertical-align: text-bottom;
}

/* Estilo para divisores */
hr {
    height: 3px !important;
    background: linear-gradient(to right, transparent, #0E9E6D, transparent) !important;
    border: none !important;
    margin: 30px 0 !important;
}

/* Estilo para subcabeçalhos */
h3, h4, h5 {
    color: #0E9E6D !important;
    border-bottom: 2px solid #e0e0e0;
    padding-bottom: 8px;
    margin-bottom: 15px !important;
}

/* Melhoria na exibição de selectbox e date input */
.stSelectbox, .stDateInput {
    background-color: white;
    border-radius: 8px;
    padding: 5px;
    box-shadow: 0 2px 5px rgba(0, 0, 0, 0.05);
}

/* Estilos originais da página de login mantidos exatamente como estavam */
.login-page {
    background-color: #0E9E6D;
    position: fixed;
    top: 0;
    left: 0;
    width: 100vw;
    height: 100vh;
    z-index: -1;
}

/* Estilização de botões primários - original mantido */
button[data-baseweb="button"].st-eb {
    background-color: #0E9E6D;
}

/* Aplica padronização para cards - original mantido */
.st-emotion-cache-1kyxreq, .st-emotion-cache-16txtl3 {
    padding: 1.5rem;
    border-radius: 10px;
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
}

/* Estilização para as fontes de login - original mantido */
.login-title {
    margin-bottom: 0.5rem;
    color: #0E9E6D;
    font-size: 1.8rem;
    font-weight: bold;
}

.login-subtitle {
    color: #666;
    margin-bottom: 1.5rem;
}
/* CSS Simplificado para a aplicação */

/* Fundo do login */
.main .block-container {
    padding-top: 2rem;
    max-width: 1000px; /* Permite que o conteúdo ocupe mais espaço */
}

/* Centraliza o conteúdo do login e adiciona fundo verde */
.login-page {
    background-color: #0E9E6D;
    position: fixed;
    top: 0;
    left: 0;
    width: 100vw;
    height: 100vh;
    z-index: -1;
}

/* Aplica cores do tema a elementos comuns */
h1, h2, h3 {
    color: #0E9E6D;
}

/* Estilização de botões primários */
button[data-baseweb="button"].st-eb {
    background-color: #0E9E6D;
}

/* Aplica padronização para cards */
.st-emotion-cache-1kyxreq, .st-emotion-cache-16txtl3 {
    padding: 1.5rem;
    border-radius: 10px;
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
}

/* Estilização para as fontes */
.login-title {
    margin-bottom: 0.5rem;
    color: #0E9E6D;
    font-size: 1.8rem;
    font-weight: bold;
}

.login-subtitle {
    color: #666;
    margin-bottom: 1.5rem;
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
    """
    Página de login simplificada usando componentes nativos do Streamlit.
    """
    # Define o fundo verde e esconde a barra lateral
    st.markdown("""
        <style>
        /* Cor de fundo para toda a página */
        section.main {
            background-color: #0E9E6D;
        }
        
        /* Esconde a barra lateral */
        section[data-testid="stSidebar"] {
            display: none !important;
        }
        
        /* Estilo do botão - texto branco e centralizado */
        div.stButton > button {
            background-color: #0E9E6D !important;
            color: white !important;
            width: 100%;
            text-align: center !important;
            justify-content: center !important;
            font-weight: bold;
        }
        
        /* Estilo do card */
        section.main div.block-container > div:nth-child(1) > div:nth-child(1) > div {
            background-color: white;
            padding: 2rem;
            border-radius: 10px;
            box-shadow: 0 4px 10px rgba(0,0,0,0.1);
            margin-top: 3rem;
        }
        </style>
    """, unsafe_allow_html=True)
    
    # Layout de colunas para criar um card centralizado
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        # Card de login com estilo simples
        with st.container():
            # Conteúdo do card
            st.markdown(f'<h1 style="color:#0E9E6D; font-size:2rem;">Chegou Insights</h1>', unsafe_allow_html=True)
            st.markdown(f'<h2>Bem vindo de volta!</h2>', unsafe_allow_html=True)
            
            # Formulário
            email = st.text_input("Email")
            password = st.text_input("Senha", type="password")
            
            # Botão de login com texto branco centralizado
            connect_button = st.button("Conectar")
            
            if connect_button:
                if email in USERS and USERS[email]["password"] == password:
                    st.session_state["logged_in"] = True
                    st.session_state["cargo"] = USERS[email]["cargo"]
                    st.session_state["current_page"] = "home"
                    force_rerun()
                else:
                    st.error("Credenciais inválidas. Tente novamente.")

def custom_sidebar():
    """Cria uma barra lateral totalmente customizada com ícones Material Symbols."""
    # Adicionar CSS para a sidebar e para os ícones Material Symbols
    st.markdown("""
    <link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Material+Symbols+Rounded:opsz,wght,FILL,GRAD@20..48,100..700,0..1,-50..200" />
    
    <style>
    /* Estilo para o título principal */
    .brand-title {
        color: #0E9E6D;
        font-size: 2rem;
        font-weight: bold;
        margin-bottom: 1rem;
        text-align: left;
    }
    
    /* Linha divisória simples */
    .sidebar-divider {
        border-top: 1px solid #ddd;
        margin: 1rem 0;
    }
    
    /* Título "Seu painel" */
    .panel-title {
        text-align: left;
        font-size: 1rem;
        margin-bottom: 1rem;
    }
    
    /* Forçar alinhamento à esquerda nos botões */
    button[data-testid="baseButton-secondary"] {
        text-align: left !important;
        justify-content: flex-start !important;
    }
    
    /* Estilo dos ícones Material */
    .material-symbols-rounded {
        font-variation-settings: 'FILL' 0, 'wght' 400, 'GRAD' 0, 'opsz' 24;
        margin-right: 8px;
        vertical-align: text-bottom;
    }           
    </style>
    """, unsafe_allow_html=True)
    
    with st.sidebar:
        # 1. TÍTULO TRACKY
        st.markdown('<div class="brand-title">Chegou Insights</div>', unsafe_allow_html=True)
        
        # 2. SELETOR DE LOJA
        stores = load_stores()
        store_options = ["Selecione uma loja..."] + [store[1] for store in stores] + ["📥 Nova Loja"]
        
        default_index = 0
        if "selected_store" in st.session_state and st.session_state["selected_store"] is not None:
            for i, store_name in enumerate(store_options):
                if store_name == st.session_state["selected_store"]["name"]:
                    default_index = i
                    break
        
        selected_option = st.selectbox(
            label="Escolha uma loja",
            options=store_options,
            index=default_index,
            label_visibility="collapsed"
        )
        
        # Processar a seleção da loja
        handle_store_selection(selected_option, stores)
        
        # 3. LINHA DIVISÓRIA SIMPLES
        st.markdown('<hr class="sidebar-divider">', unsafe_allow_html=True)
        
        # 4. TÍTULO DO PAINEL
        st.markdown('<div class="panel-title">Seu painel</div>', unsafe_allow_html=True)
        
        # 5. ITENS DE MENU
        if "current_page" not in st.session_state:
            st.session_state["current_page"] = "home"
        
        # Renderizar os itens do menu
        render_menu_items()
        
def render_menu_items():
    """Renderiza os itens do menu com ícones Material Symbols."""
    # Menu items com ícones Material Symbols no formato correto
    menu_items = [
        {"id": "home", "icon": ":material/home:", "text": "Visão Geral", "page": "principal/home.py"},
        {"id": "dropi_shopify", "icon": ":material/analytics:", "text": "Métricas de Produtos", "page": "vendas/dropi_+_shopify.py"},
        {"id": "facebook", "icon": ":material/ads_click:", "text": "Facebook", "page": "plataformas_de_anuncio/facebook.py"},
        {"id": "tiktok", "icon": ":material/ads_click:", "text": "TikTok", "page": "plataformas_de_anuncio/tiktok.py"},
        {"id": "google", "icon": ":material/ads_click:", "text": "Google", "page": "plataformas_de_anuncio/google.py"},
    ]
    
    # Adicionar item Admin para administradores
    if st.session_state.get("cargo") == "Administrador":
        menu_items.append({"id": "admin", "icon": ":material/key:", "text": "ADM", "page": "administracao/admin.py"})
    
    # Render each menu item
    for item in menu_items:
        # O nome do ícone está no formato ":material/icon_name:" que o Streamlit reconhece
        menu_clicked = st.button(
            f"{item['icon']} {item['text']}", 
            key=f"menu_{item['id']}",
            use_container_width=True
        )
        
        if menu_clicked:
            st.session_state["current_page"] = item["id"]
            force_rerun()
    
    # Adicionar o botão de logout separadamente
    logout_clicked = st.button(
        ":material/logout: Logout", 
        key="logout_button", 
        use_container_width=True
    )
    if logout_clicked:
        st.session_state["logged_in"] = False
        st.session_state["cargo"] = None
        st.session_state["selected_store"] = None
        st.session_state["current_page"] = None
        force_rerun()

def handle_store_selection(selected_option, stores):
    """Manipula a seleção da loja."""
    # Lógica para "Cadastrar Nova Loja"
    if selected_option == "📥 Nova Loja":
        st.subheader("Cadastrar Nova Loja")
        
        # Campos para cadastro
        st.markdown("#### Dados Shopi")
        store_name = st.text_input("Nome da Loja:")
        shop_name = st.text_input("Prefixo da Loja Shopify:")
        access_token = st.text_input("Token de Acesso:", type="password")
        
        # DroPi fields
        st.markdown("#### Dados Dropi")
        dropi_url = st.text_input("URL:", value="")
        dropi_username = st.text_input("Email/Usuário:")
        dropi_password = st.text_input("Senha:", type="password")
        
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
    # Adicionar CSS personalizado para a barra lateral
    st.markdown("""
    <style>
    /* Estilo para o título principal (tracky) */
    .brand-title {
        color: #0E9E6D;
        font-size: 2rem;
        font-weight: bold;
        margin-bottom: 1.5rem;
        text-align: left;
        padding-left: 0.5rem;
    }

    /* Estilo para o seletor de loja */
    .selectbox-wrapper {
        background-color: #e6f7f0;
        border-radius: 10px;
        padding: 0.5rem;
        margin-bottom: 1rem;
    }

    /* Alinhamento do texto à esquerda nos botões do menu */
    div[data-testid="stVerticalBlock"] div[data-testid="stButton"] > button {
        text-align: left !important;
        justify-content: flex-start !important;
    }

    /* Ícones pretos nos botões */
    div[data-testid="stVerticalBlock"] div[data-testid="stButton"] > button {
        color: black !important;
    }

    /* Estilo do título "Seu painel" */
    .panel-title {
        font-size: 1rem;
        color: #666;
        margin-top: 0.5rem;
        margin-bottom: 1rem;
        text-align: left;
        padding-left: 0.5rem;
    }

    /* Linha divisória */
    .sidebar-divider {
        margin-top: 1rem;
        margin-bottom: 1rem;
        border: 0;
        border-top: 1px solid #ddd;
    }

    /* Botão de logout na parte inferior */
    button[key="logout_button"] {
        margin-top: 1rem;
        background-color: #f8f9fa !important;
        color: black !important;
        border: 1px solid #ddd !important;
    }
    </style>
    """, unsafe_allow_html=True)
    
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