import streamlit as st
from streamlit.runtime.scriptrunner import RerunException, RerunData

# Configuração global da página - adicionada como primeira chamada Streamlit
st.set_page_config(
    layout="wide", 
    page_title="GC Métricas Externas", 
    page_icon=":chart_with_upwards_trend:"
)

# Função interna para forçar rerun (substitui st.experimental_rerun())
def force_rerun():
    raise RerunException(RerunData(None))

# Dicionário de usuários (NÃO use em produção sem hashing de senhas)
USERS = {
    "adminmetricasexternas@grupochegou.com": {"password": "admgcexterna2025", "cargo": "Administrador"},
    "metricasexternas@grupochegou.com":  {"password": "gcexterna2025",  "cargo": "Usuário"},
}

def login_page():
    """Página de Login."""
    st.title("GC Métricas Externas")
    st.subheader("Faça seu login")

    email = st.text_input("Email")
    password = st.text_input("Senha", type="password")

    if st.button("Entrar"):
        if email in USERS and USERS[email]["password"] == password:
            st.session_state["logged_in"] = True
            st.session_state["cargo"] = USERS[email]["cargo"]
            # Em vez de st.experimental_rerun(), usamos force_rerun():
            force_rerun()
        else:
            st.error("Credenciais inválidas. Tente novamente.")

def show_logout_button():
    """Exibe um botão de logout na sidebar."""
    if st.sidebar.button("Sair"):
        st.session_state["logged_in"] = False
        st.session_state["cargo"] = None
        force_rerun()

def main():
    # Inicializa variáveis de sessão
    if "logged_in" not in st.session_state:
        st.session_state["logged_in"] = False
    if "cargo" not in st.session_state:
        st.session_state["cargo"] = None

    # Se NÃO estiver logado, exibe apenas a página de login
    if not st.session_state["logged_in"]:
        pages = [st.Page(login_page, title="Login", icon="🔒")]
        pg = st.navigation(pages, position="sidebar", expanded=False)
        pg.run()
    else:
        # Define páginas de acordo com o cargo
        if st.session_state["cargo"] == "Administrador":
            pages = {
                "Principal": [
                    st.Page("principal/home.py", title="Home", icon="🏠"),
                ],
                "Vendas": [
                    st.Page("vendas/dropi_+_shopify.py",   title="Dropi + Shopify",   icon="🇲🇽"),
                ],
                "Plataformas de Anúncios": [
                    st.Page("plataformas_de_anuncio/facebook.py",   title="Facebook",   icon="🇲🇽"),
                    st.Page("plataformas_de_anuncio/tiktok.py",   title="Tiktok",   icon="🇲🇽"),
                    st.Page("plataformas_de_anuncio/google.py",   title="Google",   icon="🇲🇽"),
                ],
            }

        else:
            # Usuário comum
            pages = {
                "Principal": [
                    st.Page("principal/home.py", title="Home", icon="🏠"),
                ],
                "Vendas": [
                    st.Page("vendas/dropi_+_shopify.py",   title="Dropi + Shopify",   icon="🇲🇽"),
                ],
                "Plataformas de Anúncios": [
                    st.Page("plataformas_de_anuncio/facebook.py",   title="Facebook",   icon="🇲🇽"),
                    st.Page("plataformas_de_anuncio/tiktok.py",   title="Tiktok",   icon="🇲🇽"),
                    st.Page("plataformas_de_anuncio/google.py",   title="Google",   icon="🇲🇽"),
                ],
            }

        # Cria a barra de navegação
        pg = st.navigation(pages, position="sidebar", expanded=False)
        # Exibe botão de logout
        show_logout_button()
        # Executa a página selecionada
        pg.run()

if __name__ == "__main__":
    main()
