import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import requests
import json
import os
import time
import logging
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import matplotlib.pyplot as plt
import numpy as np
import altair as alt
import sys
import sqlite3

# Adicionar a raiz do projeto ao path para importar módulos
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importar utilitários de banco de dados
try:
    from db_utils import (
        init_db, get_db_connection, execute_query, execute_upsert, 
        load_stores, get_store_details, save_store, get_store_currency,
        save_effectiveness, is_railway_environment
    )
except ImportError as e:
    st.error(f"Erro ao importar módulos: {str(e)}")
    # Fallback para funções locais se necessário

# Configuração do logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dashboard_automation")

# Tema modificado para integração
st.markdown("""
<style>
    /* Remover background escuro e cores azuis */
    .stApp {
        background-color: transparent !important;
        color: inherit !important;
    }
    
    /* Manter apenas formatações de layout */
    .header {
        font-size: 36px;
        font-weight: bold;
        text-align: center;
        margin-bottom: 20px;
    }
    .subheader {
        font-size: 24px;
        font-weight: bold;
        text-align: center;
        margin-bottom: 30px;
    }
    
    /* Remover cores especiais de fundo */
    div[data-testid="stMetric"] {
        background-color: transparent !important;
    }
    
    div[data-testid="stMetric"] > div:first-child {
        color: inherit !important;
    }
</style>
""", unsafe_allow_html=True)

# Garantir que o diretório de configurações exista
if not os.path.exists("store_config"):
    os.makedirs("store_config")

# Inicializar banco de dados
def init_db():
    """Inicializa o banco de dados SQLite para todas as lojas."""
    conn = get_db_connection()
    c = conn.cursor()
    
    # Tabela para métricas de produtos
    c.execute("""
        CREATE TABLE IF NOT EXISTS product_metrics (
            store_id TEXT,
            date TEXT,
            product TEXT,
            total_orders INTEGER,
            processed_orders INTEGER,
            delivered_orders INTEGER,
            total_value REAL DEFAULT 0,
            PRIMARY KEY (store_id, date, product)
        )
    """)
    
    # Verificar se a coluna product_url existe e adicioná-la se não existir
    try:
        c.execute("SELECT product_url FROM product_metrics LIMIT 1")
    except sqlite3.OperationalError:
        # A coluna não existe, vamos adicioná-la
        st.info("Atualizando estrutura do banco de dados...")
        c.execute("ALTER TABLE product_metrics ADD COLUMN product_url TEXT")
        st.success("Banco de dados atualizado com sucesso!")
    
    # Tabela para armazenar as lojas
    c.execute("""
        CREATE TABLE IF NOT EXISTS stores (
            id TEXT PRIMARY KEY,
            name TEXT,
            shop_name TEXT,
            access_token TEXT
        )
    """)
    
    # Verificar e adicionar colunas para DroPi
    try:
        c.execute("SELECT dropi_url FROM stores LIMIT 1")
    except sqlite3.OperationalError:
        st.info("Atualizando estrutura do banco de dados para DroPi...")
        c.execute("ALTER TABLE stores ADD COLUMN dropi_url TEXT")
        c.execute("ALTER TABLE stores ADD COLUMN dropi_username TEXT")
        c.execute("ALTER TABLE stores ADD COLUMN dropi_password TEXT")
        st.success("Banco de dados atualizado com sucesso para DroPi!")
    
    # Verificar e adicionar colunas para moedas
    try:
        c.execute("SELECT currency_from FROM stores LIMIT 1")
    except sqlite3.OperationalError:
        st.info("Atualizando estrutura do banco de dados para suporte a moedas...")
        c.execute("ALTER TABLE stores ADD COLUMN currency_from TEXT DEFAULT 'MXN'")
        c.execute("ALTER TABLE stores ADD COLUMN currency_to TEXT DEFAULT 'BRL'")
        st.success("Banco de dados atualizado com sucesso para suporte a moedas!")
    
    # Criar tabela para métricas da DroPi
    c.execute("""
        CREATE TABLE IF NOT EXISTS dropi_metrics (
            store_id TEXT,
            date TEXT,
            product TEXT,
            provider TEXT,
            stock INTEGER,
            orders_count INTEGER,
            orders_value REAL,
            transit_count INTEGER, 
            transit_value REAL,
            delivered_count INTEGER,
            delivered_value REAL,
            profits REAL,
            PRIMARY KEY (store_id, date, product)
        )
    """)
    
    # Criar tabela para métricas de efetividade
    c.execute("""
        CREATE TABLE IF NOT EXISTS product_effectiveness (
            store_id TEXT,
            product TEXT,
            general_effectiveness REAL,
            last_updated TEXT,
            PRIMARY KEY (store_id, product)
        )
    """)
    
    # Confirmar todas as alterações e fechar a conexão
    conn.commit()
    conn.close()

# Carregar lista de lojas
def load_stores():
    """Carrega a lista de lojas cadastradas."""
    try:
        # Tenta importar nossa função de conexão com o banco
        from db_utils import get_db_connection, init_db
        
        # Inicializa o banco de dados
        init_db()
        
        # Obtém conexão
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Executa consulta
        cursor.execute("SELECT id, name FROM stores")
        
        # O resultado difere entre PostgreSQL e SQLite
        if os.getenv("DATABASE_URL"):
            # PostgreSQL retorna resultados como lista de tuplas
            stores = cursor.fetchall()
        else:
            # SQLite também retorna lista de tuplas, então mantemos
            stores = cursor.fetchall()
        
        conn.close()
        return stores
        
    except ImportError:
        # Fallback para SQLite
        conn = get_db_connection()
        c = conn.cursor()
        
        try:
            c.execute("SELECT id, name FROM stores")
            stores = c.fetchall()
            conn.close()
            return stores
        except sqlite3.OperationalError:
            # Se a tabela não existe, inicializa o banco
            conn.close()
            init_db()  # Isso pode falhar se init_db também não foi definido
            # Tenta novamente
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("SELECT id, name FROM stores")
            stores = c.fetchall()
            conn.close()
            return stores

# Função para obter taxa de conversão de moeda
def get_exchange_rate(from_currency, to_currency):
    """Obtém a taxa de câmbio atual entre duas moedas."""
    try:
        # Usando a API pública do ExchangeRate-API
        url = f"https://open.er-api.com/v6/latest/{from_currency}"
        response = requests.get(url)
        data = response.json()
        
        if data["result"] == "success":
            rates = data["rates"]
            if to_currency in rates:
                return rates[to_currency]
            else:
                logger.warning(f"Moeda de destino {to_currency} não encontrada. Usando taxa 1.0")
                return 1.0
        else:
            logger.warning("Não foi possível obter taxas de câmbio atualizadas. Usando taxa 1.0")
            return 1.0
    except Exception as e:
        logger.warning(f"Erro ao obter taxa de câmbio: {str(e)}. Usando taxa 1.0")
        return 1.0

# Funções para Shopify
def get_shopify_products(url, headers):
    """Consulta produtos da Shopify para obter os URLs."""
    query = """
    query getProducts($cursor: String) {
      products(first: 50, after: $cursor) {
        edges {
          node {
            id
            title
            handle
            onlineStoreUrl
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
    """
    
    all_products = []
    cursor = None
    
    while True:
        variables = {"cursor": cursor}
        payload = {"query": query, "variables": variables}
        
        try:
            response = requests.post(url, headers=headers, json=payload)
            
            if response.status_code != 200:
                st.error(f"Erro na conexão com a Shopify. Código: {response.status_code}")
                break
            
            data = response.json()
            
            # Verificar erros na resposta
            if "errors" in data:
                st.error(f"Erro na API Shopify: {data['errors']}")
                break
            
            products_data = data.get("data", {}).get("products", {})
            edges = products_data.get("edges", [])
            all_products.extend(edges)
            
            page_info = products_data.get("pageInfo", {})
            if page_info.get("hasNextPage"):
                cursor = page_info.get("endCursor")
            else:
                break
        except Exception as e:
            st.error(f"Erro ao acessar a API Shopify: {str(e)}")
            break
    
    # Criar um dicionário de produtos por handle
    product_urls = {}
    for product_edge in all_products:
        product = product_edge.get("node", {})
        title = product.get("title", "")
        handle = product.get("handle", "")
        url = product.get("onlineStoreUrl", "")
        
        if not url and handle:
            # Se a URL não estiver disponível, construa-a a partir do handle
            url = f"/products/{handle}"
            
        product_urls[title] = url
    
    return product_urls

def get_shopify_orders(url, headers, start_date, end_date):
    """Consulta pedidos da Shopify no intervalo de datas especificado."""
    date_filter = f"created_at:>={start_date} AND created_at:<={end_date}"
    
    query = f"""
    query getOrders($cursor: String) {{
      orders(first: 50, after: $cursor, query: "{date_filter}") {{
        edges {{
          node {{
            id
            name
            createdAt
            totalPriceSet {{
              shopMoney {{
                amount
              }}
            }}
            lineItems(first: 50) {{
              edges {{
                node {{
                  title
                  quantity
                  originalTotalSet {{
                    shopMoney {{
                      amount
                    }}
                  }}
                }}
              }}
            }}
          }}
        }}
        pageInfo {{
          hasNextPage
          endCursor
        }}
      }}
    }}
    """
    
    all_orders = []
    cursor = None
    
    while True:
        variables = {"cursor": cursor}
        payload = {"query": query, "variables": variables}
        
        try:
            response = requests.post(url, headers=headers, json=payload)
            
            if response.status_code != 200:
                st.error(f"Erro na conexão com a Shopify. Código: {response.status_code}")
                break
            
            data = response.json()
            
            # Verificar erros na resposta
            if "errors" in data:
                st.error(f"Erro na API Shopify: {data['errors']}")
                break
            
            orders_data = data.get("data", {}).get("orders", {})
            edges = orders_data.get("edges", [])
            all_orders.extend(edges)
            
            page_info = orders_data.get("pageInfo", {})
            if page_info.get("hasNextPage"):
                cursor = page_info.get("endCursor")
            else:
                break
        except Exception as e:
            st.error(f"Erro ao acessar a API Shopify: {str(e)}")
            break
    
    return all_orders

def process_shopify_products(orders, product_urls):
    """Processa pedidos e retorna dicionários com contagens e valores por produto."""
    product_total = {}
    product_processed = {}
    product_delivered = {}
    product_url_map = {}
    product_value = {}  # Novo dicionário para armazenar valores totais
    
    for order_edge in orders:
        order_node = order_edge.get("node", {})
        # Consideramos todos os pedidos como processados e entregues neste exemplo
        # No mundo real, você usaria campos específicos da Shopify para determinar isso
        is_processed = True
        is_delivered = True  # Simplificado para o exemplo
        
        line_items = order_node.get("lineItems", {}).get("edges", [])
        for line_item_edge in line_items:
            line_item = line_item_edge.get("node", {})
            product_title = line_item.get("title", "Unknown")
            quantity = line_item.get("quantity", 0)
            
            # Obter o valor do item (preço total)
            item_value = 0
            try:
                original_total = line_item.get("originalTotalSet", {}).get("shopMoney", {}).get("amount", "0")
                # Converter para float - isto garante que o valor está em formato decimal correto
                item_value = float(original_total)
            except (ValueError, TypeError):
                # Se não conseguir converter para float, usa 0
                item_value = 0
            
            # Armazenar URL do produto
            if product_title in product_urls:
                product_url_map[product_title] = product_urls[product_title]
            else:
                product_url_map[product_title] = ""
            
            # Adicionar ao total de pedidos
            if product_title in product_total:
                product_total[product_title] += quantity
            else:
                product_total[product_title] = quantity
            
            # Adicionar ao valor total do produto
            if product_title in product_value:
                product_value[product_title] += item_value
            else:
                product_value[product_title] = item_value
            
            # Adicionar aos processados se aplicável
            if is_processed:
                if product_title in product_processed:
                    product_processed[product_title] += quantity
                else:
                    product_processed[product_title] = quantity
            
            # Adicionar aos entregues se aplicável
            if is_delivered:
                if product_title in product_delivered:
                    product_delivered[product_title] += quantity
                else:
                    product_delivered[product_title] = quantity
    
    return product_total, product_processed, product_delivered, product_url_map, product_value

def save_metrics_to_db(store_id, date, product_total, product_processed, product_delivered, product_url_map, product_value):
    """Salva as métricas no banco de dados."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Limpar dados existentes para este período
        delete_query = "DELETE FROM product_metrics WHERE store_id = ? AND date = ?"
        if is_railway_environment():
            delete_query = delete_query.replace("?", "%s")
        
        cursor.execute(delete_query, (store_id, date))
        conn.commit()
        
        # Inserir novos dados
        for product in product_total:
            data = {
                "store_id": store_id,
                "date": date, 
                "product": product,
                "product_url": product_url_map.get(product, ""),
                "total_orders": product_total.get(product, 0),
                "processed_orders": product_processed.get(product, 0),
                "delivered_orders": product_delivered.get(product, 0),
                "total_value": product_value.get(product, 0)
            }
            
            execute_upsert("product_metrics", data, ["store_id", "date", "product"])
    except Exception as e:
        logger.error(f"Erro ao salvar métricas: {str(e)}")
        conn.rollback()
        st.error(f"Erro ao salvar dados: {str(e)}")
    finally:
        conn.close()

def get_url_categories(store_id, start_date_str, end_date_str):
    """Obtém as categorias de URLs (Google, TikTok, Facebook) com base nos padrões nas URLs."""
    conn = get_db_connection()
    query = f"""
        SELECT DISTINCT product_url FROM product_metrics 
        WHERE store_id = '{store_id}' AND date BETWEEN '{start_date_str}' AND '{end_date_str}'
    """
    
    # Adaptação para PostgreSQL/SQLite
    if is_railway_environment():
        query = query.replace("?", "%s")
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    # Dicionário para armazenar as URLs por categoria
    categories = {
        "Google": [],
        "TikTok": [],
        "Facebook": []
    }
    
    # Categorizar as URLs
    for url in df["product_url"]:
        if url and '/' in url:
            suffix = url.split('/')[-1].lower()  # Obter o texto após a última barra
            
            # Categorizar com base nos padrões
            if 'gg' in suffix or 'goog' in suffix:
                categories["Google"].append(url)
            elif 'ttk' in suffix or 'tktk' in suffix:
                categories["TikTok"].append(url)
            else:
                categories["Facebook"].append(url)
    
    # Remover categorias vazias
    categories = {k: v for k, v in categories.items() if v}
    
    # Retornar as categorias que possuem URLs
    return list(categories.keys())

# === FUNÇÕES PARA DroPi ===

def setup_selenium(headless=True):
    """Configure and initialize Selenium WebDriver for cloud environment."""
    # Configure Chrome options for cloud environment
    chrome_options = Options()
    
    # Sempre use headless mode no ambiente cloud
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    
    # Configurações adicionais para melhorar a estabilidade
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--dns-prefetch-disable")
    
    try:
        if is_railway_environment():
            # No Railway, o ChromeDriver deve estar disponível no PATH
            driver = webdriver.Chrome(options=chrome_options)
            logger.info("Selenium WebDriver initialized in production mode")
        else:
            # Em desenvolvimento, usar ChromeDriverManager 
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            logger.info("Selenium WebDriver initialized in development mode")
        
        return driver
        
    except Exception as e:
        logger.error(f"Failed to initialize WebDriver: {str(e)}")
        st.error(f"Erro ao inicializar o navegador: {str(e)}")
        return None

def login(driver, email, password, logger):
    """Função de login super robusta."""
    try:
        # Abre o site em uma nova janela maximizada
        driver.maximize_window()
        
        # Navega para a página de login
        logger.info("Navegando para a página de login...")
        driver.get("https://app.dropi.mx/")
        time.sleep(5)  # Espera fixa de 5 segundos
        
        # Tira screenshot para análise
        driver.save_screenshot("login_page.png")
        logger.info("Screenshot da página salvo como login_page.png")
        
        # Inspeciona a página e loga a estrutura HTML para análise
        logger.info("Analisando estrutura da página de login...")
        html = driver.page_source
        logger.info(f"Título da página: {driver.title}")
        logger.info(f"URL atual: {driver.current_url}")
        
        # Tenta encontrar os campos usando diferentes métodos
        
        # MÉTODO 1: Tenta encontrar os campos por XPath direto
        try:
            logger.info("Tentando encontrar campos por XPath...")
            
            # Lista todos os inputs para depuração
            inputs = driver.find_elements(By.TAG_NAME, 'input')
            logger.info(f"Total de campos input encontrados: {len(inputs)}")
            for i, inp in enumerate(inputs):
                input_type = inp.get_attribute('type')
                input_id = inp.get_attribute('id')
                input_name = inp.get_attribute('name')
                logger.info(f"Input #{i}: tipo={input_type}, id={input_id}, name={input_name}")
            
            # Tenta localizar o campo de email/usuário - tentando diferentes atributos
            email_field = None
            
            # Tenta por tipo "email"
            try:
                email_field = driver.find_element(By.XPATH, "//input[@type='email']")
                logger.info("Campo de email encontrado por type='email'")
            except:
                pass
                
            # Tenta por tipo "text"
            if not email_field:
                try:
                    email_field = driver.find_element(By.XPATH, "//input[@type='text']")
                    logger.info("Campo de email encontrado por type='text'")
                except:
                    pass
            
            # Tenta pelo primeiro input
            if not email_field and len(inputs) > 0:
                email_field = inputs[0]
                logger.info("Usando primeiro campo input encontrado para email")
            
            # Se encontrou o campo de email, preenche
            if email_field:
                email_field.clear()
                email_field.send_keys(email)
                logger.info(f"Email preenchido: {email}")
            else:
                raise Exception("Não foi possível encontrar o campo de email")
            
            # Procura o campo de senha
            password_field = None
            
            # Tenta por tipo "password"
            try:
                password_field = driver.find_element(By.XPATH, "//input[@type='password']")
                logger.info("Campo de senha encontrado por type='password'")
            except:
                pass
            
            # Tenta usando o segundo input
            if not password_field and len(inputs) > 1:
                password_field = inputs[1]
                logger.info("Usando segundo campo input encontrado para senha")
            
            # Se encontrou o campo de senha, preenche
            if password_field:
                password_field.clear()
                password_field.send_keys(password)
                logger.info("Senha preenchida")
            else:
                raise Exception("Não foi possível encontrar o campo de senha")
            
            # Lista todos os botões para depuração
            buttons = driver.find_elements(By.TAG_NAME, 'button')
            logger.info(f"Total de botões encontrados: {len(buttons)}")
            for i, btn in enumerate(buttons):
                btn_text = btn.text
                btn_type = btn.get_attribute('type')
                logger.info(f"Botão #{i}: texto='{btn_text}', tipo={btn_type}")
            
            # Procura o botão de login
            login_button = None
            
            # Tenta por tipo "submit"
            try:
                login_button = driver.find_element(By.XPATH, "//button[@type='submit']")
                logger.info("Botão de login encontrado por type='submit'")
            except:
                pass
            
            # Tenta por texto
            if not login_button:
                for btn in buttons:
                    if "iniciar" in btn.text.lower() or "login" in btn.text.lower() or "entrar" in btn.text.lower():
                        login_button = btn
                        logger.info(f"Botão de login encontrado pelo texto: '{btn.text}'")
                        break
            
            # Se não encontrou por texto específico, usa o primeiro botão
            if not login_button and len(buttons) > 0:
                login_button = buttons[0]
                logger.info("Usando primeiro botão encontrado para login")
            
            # Se encontrou o botão, clica
            if login_button:
                login_button.click()
                logger.info("Clicado no botão de login")
            else:
                raise Exception("Não foi possível encontrar o botão de login")
            
            # Aguarda a navegação
            time.sleep(8)
            
            # Tira screenshot após o login
            driver.save_screenshot("after_login.png")
            logger.info("Screenshot após login salvo como after_login.png")
            
            # Verifica se o login foi bem-sucedido
            current_url = driver.current_url
            logger.info(f"URL após tentativa de login: {current_url}")
            
            # Tenta encontrar elementos que aparecem após login bem-sucedido
            menu_items = driver.find_elements(By.TAG_NAME, 'a')
            for item in menu_items:
                logger.info(f"Item de menu encontrado: '{item.text}'")
                if "dashboard" in item.text.lower() or "orders" in item.text.lower():
                    logger.info(f"Item de menu confirmando login: '{item.text}'")
                    return True
            
            # Se não encontrou elementos claros de login, verifica se estamos na URL de dashboard
            if "dashboard" in current_url or "orders" in current_url:
                logger.info("Login confirmado pela URL")
                return True
            
            # Se chegou aqui, o login pode ter falhado
            logger.warning("Não foi possível confirmar se o login foi bem-sucedido. Tentando continuar mesmo assim.")
            return True
            
        except Exception as e:
            logger.error(f"Erro no método 1: {str(e)}")
            # Continua para o próximo método
            return False
    
    except Exception as e:
        logger.error(f"Erro geral no login: {str(e)}")
        return False

def navigate_to_product_sold(driver, logger):
    """Navigate to the Product Sold report in DroPi."""
    try:
        # Esperar que a página carregue completamente após o login
        time.sleep(5)
        
        # Capturar screenshot para diagnóstico
        driver.save_screenshot("post_login.png")
        logger.info(f"URL atual: {driver.current_url}")
        
        # Primeiro, tentar encontrar o menu Reports/Reportes
        reports_xpath_options = [
            "//a[contains(text(), 'Reports')]",
            "//a[contains(text(), 'Reportes')]",
            "//span[contains(text(), 'Reports')]/parent::a",
            "//span[contains(text(), 'Reportes')]/parent::a",
            "//div[contains(@class, 'sidebar')]//a[contains(., 'Report')]"
        ]
        
        for xpath in reports_xpath_options:
            try:
                logger.info(f"Tentando xpath: {xpath}")
                WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, xpath)))
                reports_link = driver.find_element(By.XPATH, xpath)
                logger.info(f"Menu Reports encontrado: {reports_link.text}")
                reports_link.click()
                logger.info("Clicou no menu Reports")
                time.sleep(3)
                break
            except Exception as e:
                logger.warning(f"Xpath {xpath} falhou: {str(e)}")
        
        # Agora tenta encontrar e clicar em Product Sold
        product_sold_xpath_options = [
            "//a[contains(text(), 'Product Sold')]",
            "//a[contains(text(), 'Productos Vendidos')]",
            "//span[contains(text(), 'Product Sold')]/parent::a",
            "//span[contains(text(), 'Productos Vendidos')]/parent::a"
        ]
        
        for xpath in product_sold_xpath_options:
            try:
                logger.info(f"Tentando xpath para Product Sold: {xpath}")
                WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, xpath)))
                product_sold_link = driver.find_element(By.XPATH, xpath)
                logger.info(f"Link Product Sold encontrado: {product_sold_link.text}")
                product_sold_link.click()
                logger.info("Clicou em Product Sold")
                time.sleep(3)
                break
            except Exception as e:
                logger.warning(f"Xpath {xpath} falhou: {str(e)}")
        
        # Confirmar que estamos na página correta
        driver.save_screenshot("product_sold_page.png")
        logger.info(f"URL após navegar: {driver.current_url}")
        
        # Verificar se há elementos que indicam sucesso
        page_loaded = False
        for check_elem in ["Rango de fecha", "Date Range", "producto", "Vendidos"]:
            try:
                driver.find_element(By.XPATH, f"//*[contains(text(), '{check_elem}')]")
                page_loaded = True
                logger.info(f"Página confirmada pelo elemento: {check_elem}")
                break
            except:
                pass
        
        if page_loaded:
            return True
        else:
            logger.error("Não foi possível confirmar se estamos na página correta")
            return False
            
    except Exception as e:
        logger.error(f"Erro ao navegar para Product Sold: {str(e)}")
        return False

def select_date_range(driver, start_date, end_date, logger):
    """Select a specific date range in the Product Sold report."""
    try:
        # Formatação das datas para exibição no formato esperado pelo DroPi (DD/MM/YYYY)
        start_date_formatted = start_date.strftime("%d/%m/%Y")
        end_date_formatted = end_date.strftime("%d/%m/%Y")
        
        logger.info(f"Tentando selecionar intervalo de datas: {start_date_formatted} a {end_date_formatted}")
        
        # Capturar screenshot para diagnóstico
        driver.save_screenshot("before_date_select.png")
        
        # Seletores específicos para o campo de data, baseado na captura de tela e no log
        date_selectors = [
            "//div[contains(@class, 'date-field') or contains(@class, 'date-picker')]",
            "//div[@class='datepicker-toggle']",
            "//div[contains(@class, 'datepicker')]//input",
            "//div[contains(@class, 'daterangepicker')]",
            "//input[contains(@class, 'form-control') and contains(@class, 'daterange')]",
            "//button[contains(@class, 'date') or contains(@class, 'calendar')]",
            "//div[contains(text(), '/') and (contains(@class, 'date') or contains(@class, 'calendar'))]",
            "//*[contains(text(), 'Date Range') or contains(text(), 'Rango de fecha')]"
        ]
        
        # Tentar clicar no seletor de data
        clicked = False
        for selector in date_selectors:
            try:
                logger.info(f"Tentando seletor de data: {selector}")
                try:
                    element = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, selector)))
                    driver.execute_script("arguments[0].style.border='3px solid red'", element)
                    driver.save_screenshot("highlighted_date_field.png")
                    logger.info(f"Elemento de data encontrado, HTML: {element.get_attribute('outerHTML')}")
                    element.click()
                    logger.info(f"Clicou no seletor de data: {selector}")
                    clicked = True
                    break
                except Exception as e:
                    logger.warning(f"Clique direto no seletor {selector} falhou: {str(e)}")
                    
                # Tentar JavaScript click como alternativa
                element = driver.find_element(By.XPATH, selector)
                driver.execute_script("arguments[0].click();", element)
                logger.info(f"Clicou via JavaScript no seletor de data: {selector}")
                clicked = True
                break
            except Exception as e:
                logger.warning(f"Seletor {selector} falhou completamente: {str(e)}")
        
        # Se as opções acima falharem, tentar encontrar qualquer elemento que pareça com um seletor de data
        if not clicked:
            try:
                logger.info("Tentando abordagem alternativa: procurando elementos de data na página")
                driver.save_screenshot("date_field_search.png")
                potential_date_elements = driver.find_elements(By.XPATH, 
                                                            "//*[contains(@class, 'date') or contains(@class, 'calendar') or contains(@class, 'picker')]")
                
                logger.info(f"Encontrou {len(potential_date_elements)} potenciais elementos de data")
                for i, elem in enumerate(potential_date_elements):
                    try:
                        logger.info(f"Elemento {i+1}: {elem.get_attribute('outerHTML')}")
                        driver.execute_script("arguments[0].style.border='3px solid blue'", elem)
                        driver.save_screenshot(f"date_candidate_{i+1}.png")
                        elem.click()
                        logger.info(f"Clicou com sucesso no potencial elemento de data {i+1}")
                        clicked = True
                        break
                    except Exception as e:
                        logger.warning(f"Não conseguiu clicar no elemento {i+1}: {str(e)}")
                        try:
                            driver.execute_script("arguments[0].click();", elem)
                            logger.info(f"Clicou via JavaScript no potencial elemento de data {i+1}")
                            clicked = True
                            break
                        except:
                            pass
            except Exception as e:
                logger.error(f"Falha na abordagem alternativa para encontrar o seletor de data: {str(e)}")
        
        if not clicked:
            logger.error("Não foi possível clicar no seletor de data")
            return False
        
        # Esperar o popup do calendário aparecer
        time.sleep(3)
        driver.save_screenshot("date_popup.png")
        
        # Verificar e navegar para o mês/ano correto
        expected_month = start_date.strftime("%B")  # Nome do mês em inglês
        expected_year = start_date.strftime("%Y")
        logger.info(f"Mês/ano desejado: {expected_month} {expected_year}")
        
        # Funções de utilidade para verificar o mês atual e navegar
        def get_current_month_year():
            try:
                month_year_elements = driver.find_elements(By.XPATH, "//div[contains(@class, 'p-datepicker-title') or contains(@class, 'datepicker-title')]")
                if month_year_elements:
                    return month_year_elements[0].text
                return None
            except:
                return None

        def is_desired_month(current_text):
            if not current_text:
                return False
                
            # Mapeamento para nomes de meses em inglês
            month_map = {
                'January': 1, 'February': 2, 'March': 3, 'April': 4,
                'May': 5, 'June': 6, 'July': 7, 'August': 8,
                'September': 9, 'October': 10, 'November': 11, 'December': 12
            }
            
            # Mapear nomes em espanhol
            spanish_month_map = {
                'Enero': 1, 'Febrero': 2, 'Marzo': 3, 'Abril': 4,
                'Mayo': 5, 'Junio': 6, 'Julio': 7, 'Agosto': 8,
                'Septiembre': 9, 'Octubre': 10, 'Noviembre': 11, 'Diciembre': 12
            }
            
            import re
            match = re.search(r'(\w+)\s+(\d{4})', current_text)
            if not match:
                return False
                
            current_month_str = match.group(1)
            current_year_str = match.group(2)
            
            # Tentar obter o número do mês
            current_month = None
            if current_month_str in month_map:
                current_month = month_map[current_month_str]
            elif current_month_str in spanish_month_map:
                current_month = spanish_month_map[current_month_str]
            else:
                return False
                
            # Comparar com o mês e ano desejados
            desired_month = int(start_date.strftime("%m"))
            desired_year = int(start_date.strftime("%Y"))
            
            return (current_month == desired_month and int(current_year_str) == desired_year)
            
        # Tentar navegar até encontrar o mês desejado (máximo de 12 tentativas)
        max_attempts = 12
        attempt = 0
        correct_month_found = False
        
        while attempt < max_attempts and not correct_month_found:
            current_month_year = get_current_month_year()
            logger.info(f"Mês/ano atual do calendário: {current_month_year}")
            
            if is_desired_month(current_month_year):
                logger.info(f"Mês desejado encontrado: {current_month_year}")
                correct_month_found = True
                break
                
            # Se não estiver no mês desejado, clicar no botão de mês anterior
            logger.info(f"Mês atual não é o desejado. Tentando navegar para o mês anterior.")
            
            # Botões de navegação - foco especial no botão para mês anterior (observado no log)
            prev_button_selectors = [
                "//button[contains(@class, 'prev')]",
                "//a[contains(@class, 'prev')]",
                "//div[contains(@class, 'p-datepicker-prev')]",
                "//span[contains(@class, 'p-datepicker-prev-icon')]/..",
                "//div[contains(@class, 'datepicker-prev')]",
                "//i[contains(@class, 'prev-icon')]/.."
            ]
            
            prev_clicked = False
            for selector in prev_button_selectors:
                try:
                    prev_elements = driver.find_elements(By.XPATH, selector)
                    if prev_elements:
                        prev_button = prev_elements[0]
                        driver.execute_script("arguments[0].style.border='3px solid purple'", prev_button)
                        driver.save_screenshot(f"prev_button_{attempt+1}.png")
                        
                        # Tentar clique direto
                        try:
                            prev_button.click()
                            logger.info(f"Clicou no botão de mês anterior, tentativa {attempt+1}")
                            prev_clicked = True
                            break
                        except:
                            # Tentar via JavaScript
                            driver.execute_script("arguments[0].click();", prev_button)
                            logger.info(f"Clicou via JavaScript no botão de mês anterior, tentativa {attempt+1}")
                            prev_clicked = True
                            break
                except Exception as e:
                    logger.warning(f"Erro ao tentar clicar no botão prev {selector}: {str(e)}")
            
            if not prev_clicked:
                logger.warning(f"Não conseguiu clicar no botão de mês anterior na tentativa {attempt+1}")
                break
                
            # Esperar a atualização do calendário
            time.sleep(2)
            attempt += 1
        
        if not correct_month_found:
            logger.warning("Não foi possível navegar até o mês desejado após múltiplas tentativas")
            # Vamos tentar selecionar os dias mesmo assim, no mês atual
            
        # Capturar screenshot após navegação entre meses
        driver.save_screenshot("after_month_navigation.png")
        
        # Função para tentar selecionar um dia específico
        def try_select_day(day_number, day_description):
            day_str = str(day_number)
            selected = False
            
            # Capturar screenshot para diagnóstico
            driver.save_screenshot(f"calendar_before_{day_description}.png")
            
            # Registrar elementos de dia visíveis
            try:
                all_day_elements = driver.find_elements(By.XPATH, "//*[contains(@class, 'day') or contains(@class, 'date')]")
                logger.info(f"Total de elementos de dia encontrados: {len(all_day_elements)}")
                for i, day_elem in enumerate(all_day_elements[:10]):
                    try:
                        logger.info(f"Dia {i+1}: texto='{day_elem.text}', classe='{day_elem.get_attribute('class')}'")
                    except:
                        pass
            except:
                logger.warning("Não foi possível listar todos os elementos de dia")
            
            # Lista de seletores para o dia específico - adaptados com base no log
            day_selectors_templates = [
                "//table[contains(@class, 'calendar') or contains(@class, 'datepicker')]//td[text()='{day}']",
                "//table[contains(@class, 'calendar') or contains(@class, 'datepicker')]//td[.='{day}']",
                "//table[contains(@class, 'p-datepicker-calendar')]//td/span[text()='{day}']",
                "//table[contains(@class, 'p-datepicker-calendar')]//span[text()='{day}']",
                "//div[contains(@class, 'day') and text()='{day}']",
                "//*[contains(@class, 'day') and text()='{day}']",
                "//*[text()='{day}' and not(contains(@class, 'disabled'))]"
            ]
            
            # Tentar cada seletor
            for template in day_selectors_templates:
                day_xpath = template.format(day=day_str)
                try:
                    logger.info(f"Tentando seletor de dia: {day_xpath}")
                    day_elements = driver.find_elements(By.XPATH, day_xpath)
                    
                    if day_elements:
                        logger.info(f"Encontrou {len(day_elements)} elementos para o dia {day_str}")
                        
                        for i, day_elem in enumerate(day_elements):
                            try:
                                class_attr = day_elem.get_attribute("class") or ""
                                if "disabled" not in class_attr and "hidden" not in class_attr:
                                    driver.execute_script("arguments[0].style.border='3px solid green'", day_elem)
                                    driver.save_screenshot(f"{day_description}_found_{i+1}.png")
                                    
                                    day_elem.click()
                                    logger.info(f"Selecionou o dia {day_str} (elemento {i+1})")
                                    selected = True
                                    return selected
                            except Exception as e:
                                logger.warning(f"Erro ao tentar clicar no elemento {i+1} para o dia {day_str}: {str(e)}")
                                try:
                                    driver.execute_script("arguments[0].click();", day_elem)
                                    logger.info(f"Selecionou o dia {day_str} via JavaScript (elemento {i+1})")
                                    selected = True
                                    return selected
                                except:
                                    pass
                except Exception as e:
                    logger.warning(f"Seletor {day_xpath} falhou: {str(e)}")
            
            # Se não conseguiu encontrar o dia, tentar qualquer elemento com esse número
            try:
                logger.info("Tentativa genérica para encontrar o dia")
                all_elements = driver.find_elements(By.XPATH, f"//*[text()='{day_str}' or contains(text(), '{day_str}')]")
                
                for elem in all_elements:
                    try:
                        # Verificar se parece ser um elemento de dia de calendário
                        parent_html = elem.find_element(By.XPATH, "..").get_attribute("outerHTML")
                        if ("day" in parent_html.lower() or "calendar" in parent_html.lower() or 
                            "date" in parent_html.lower() or "picker" in parent_html.lower()):
                            
                            logger.info(f"Encontrou potencial elemento de dia: {elem.get_attribute('outerHTML')}")
                            elem.click()
                            logger.info(f"Clicou no dia {day_str} (tentativa genérica)")
                            selected = True
                            return selected
                    except:
                        pass
            except Exception as e:
                logger.warning(f"Tentativa genérica para encontrar o dia falhou: {str(e)}")
            
            return selected
        
        # Tentar selecionar a data inicial
        start_day = int(start_date.strftime("%d"))
        start_day_selected = try_select_day(start_day, "start_day")
        
        # Aguardar processamento da seleção da data inicial
        time.sleep(2)
        
        # Tentar selecionar a data final
        end_day = int(end_date.strftime("%d"))
        end_day_selected = try_select_day(end_day, "end_day")
        
        # Tentar confirmar a seleção se houver botão
        try:
            confirm_buttons_xpaths = [
                "//button[contains(text(), 'Apply') or contains(text(), 'Aplicar')]",
                "//button[contains(text(), 'OK') or contains(text(), 'Ok')]",
                "//button[contains(text(), 'Done') or contains(text(), 'Concluir')]",
                "//button[contains(@class, 'confirm') or contains(@class, 'apply')]",
                "//button[contains(@class, 'btn-primary') or contains(@class, 'btn-success')]"
            ]
            
            for xpath in confirm_buttons_xpaths:
                try:
                    confirm_button = driver.find_element(By.XPATH, xpath)
                    logger.info(f"Botão de confirmação encontrado: {confirm_button.text}")
                    confirm_button.click()
                    logger.info("Clicou no botão de confirmação")
                    break
                except:
                    pass
        except:
            logger.info("Não encontrou botão de confirmação, continuando...")
        
        # Esperar carregamento dos dados
        time.sleep(5)
        driver.save_screenshot("after_date_select.png")
        
        # Verificar resultado da seleção
        success = start_day_selected or end_day_selected
        
        if success:
            logger.info(f"Pelo menos uma data foi selecionada com sucesso")
            if start_day_selected and end_day_selected:
                logger.info("Ambas as datas foram selecionadas com sucesso")
            elif start_day_selected:
                logger.warning("Apenas a data inicial foi selecionada")
            else:
                logger.warning("Apenas a data final foi selecionada")
        else:
            logger.error("Não foi possível selecionar nenhuma das datas")
        
        return success
    except Exception as e:
        logger.error(f"Erro ao selecionar intervalo de datas: {str(e)}")
        return False

def extract_product_data(driver, logger):
    """Extract product data from the Product Sold report with improved accuracy."""
    try:
        logger.info("Iniciando extração de dados dos produtos com método melhorado")
        driver.save_screenshot("product_cards.png")
        
        # Esperar que a página carregue completamente
        time.sleep(5)
        
        products_data = []
        
        # Lista de textos a ignorar (cabeçalhos, títulos, etc)
        ignore_texts = [
            "Informe de productos", 
            "Reporte de productos",
            "Productos vendidos",
            "Productos en transito",
            "Resumen",
            "Total",
            "Filtrar"
        ]
        
        # Localizar todos os cards de produtos usando um seletor mais específico
        product_cards = driver.find_elements(By.XPATH, "//div[contains(@class, 'card') or contains(@class, 'product-card') or contains(@class, 'item')]")
        
        logger.info(f"Encontrados {len(product_cards)} cards de produtos")
        
        if not product_cards or len(product_cards) < 1:
            # Método alternativo: tentar localizar pelo container de imagem que geralmente precede os dados
            product_cards = driver.find_elements(By.XPATH, "//div[.//img]/following-sibling::div[1]")
            logger.info(f"Método alternativo: Encontrados {len(product_cards)} cards via containers de imagem")
        
        # Processar cada card de produto individualmente
        for i, card in enumerate(product_cards):
            try:
                # Verificar se este é realmente um card de produto válido
                card_text = card.text
                
                # Ignorar cards vazios ou inválidos
                if not card_text or len(card_text) < 20:
                    continue
                
                # Para debug
                logger.info(f"Processando card #{i+1}:\n{card_text[:100]}...")
                
                # Extrair nome do produto (geralmente o primeiro texto substancial no card)
                lines = card_text.split('\n')
                if not lines:
                    continue
                    
                product_name = lines[0]
                
                # Verificar se o nome do produto não é um dos textos a ignorar
                if any(ignore_text.lower() in product_name.lower() for ignore_text in ignore_texts):
                    logger.info(f"Ignorando texto '{product_name}' por ser um cabeçalho ou título")
                    continue
                
                # Verificações adicionais para validar que é um produto real
                # 1. Deve ter pelo menos alguns caracteres no nome
                if len(product_name) < 5:
                    logger.info(f"Ignorando '{product_name}': nome muito curto")
                    continue
                    
                # 2. Deve ter palavras "Stock" ou "Proveedor" no card
                if "Stock:" not in card_text and "Proveedor:" not in card_text:
                    logger.info(f"Ignorando '{product_name}': não contém informações de produto")
                    continue
                
                # Inicializar dados do produto
                product_data = {
                    "product": product_name,
                    "provider": "",
                    "stock": 0,
                    "orders_count": 0,
                    "orders_value": 0.0,
                    "transit_count": 0,
                    "transit_value": 0.0,
                    "delivered_count": 0,
                    "delivered_value": 0.0,
                    "profits": 0.0
                }
                
                # Extrair informações específicas deste card apenas
                # Tentar extrair o fornecedor diretamente deste card
                try:
                    provider_element = card.find_element(By.XPATH, ".//div[contains(text(), 'Proveedor:')]")
                    provider_text = provider_element.text
                    if "Proveedor:" in provider_text:
                        product_data["provider"] = provider_text.split("Proveedor:")[1].strip()
                except:
                    # Tentar método alternativo com regex no texto completo
                    provider_match = re.search(r'Proveedor:\s*([^\n]+)', card_text)
                    if provider_match:
                        product_data["provider"] = provider_match.group(1).strip()
                
                # Extrair estoque diretamente deste card
                try:
                    stock_element = card.find_element(By.XPATH, ".//div[contains(text(), 'Stock:')]")
                    stock_text = stock_element.text
                    stock_match = re.search(r'Stock:\s*(\d+)', stock_text)
                    if stock_match:
                        product_data["stock"] = int(stock_match.group(1))
                except:
                    # Método alternativo com regex
                    stock_match = re.search(r'Stock:\s*(\d+)', card_text)
                    if stock_match:
                        product_data["stock"] = int(stock_match.group(1))
                
                # Extrair vendidos (ordens)
                orders_match = re.search(r'(\d+)\s+ordenes', card_text)
                if orders_match:
                    product_data["orders_count"] = int(orders_match.group(1))
                    
                    # Buscar o valor das ordens (geralmente na linha seguinte)
                    orders_value_match = re.search(r'ordenes\s*\n\s*\$\s*([\d.,]+)', card_text)
                    if orders_value_match:
                        value_str = orders_value_match.group(1).replace('.', '').replace(',', '.')
                        try:
                            product_data["orders_value"] = float(value_str)
                        except:
                            pass
                
                # Extrair em trânsito
                transit_match = re.search(r'(\d+)\s+productos\s*\n\s*En\s+transito', card_text, re.IGNORECASE) or \
                               re.search(r'En\s+transito\s*\n\s*(\d+)\s+productos', card_text, re.IGNORECASE)
                
                if transit_match:
                    product_data["transit_count"] = int(transit_match.group(1))
                    
                    # Buscar o valor em trânsito
                    transit_value_match = re.search(r'En\s+transito\s*\n.*\n\s*\$\s*([\d.,]+)', card_text, re.IGNORECASE)
                    if transit_value_match:
                        value_str = transit_value_match.group(1).replace('.', '').replace(',', '.')
                        try:
                            product_data["transit_value"] = float(value_str)
                        except:
                            pass
                
                # Extrair entregados
                delivered_match = re.search(r'(\d+)\s+productos\s*\n\s*Entregados', card_text, re.IGNORECASE) or \
                                 re.search(r'Entregados\s*\n\s*(\d+)\s+productos', card_text, re.IGNORECASE)
                
                if delivered_match:
                    product_data["delivered_count"] = int(delivered_match.group(1))
                    
                    # Buscar o valor entregados
                    delivered_value_match = re.search(r'Entregados\s*\n.*\n\s*\$\s*([\d.,]+)', card_text, re.IGNORECASE)
                    if delivered_value_match:
                        value_str = delivered_value_match.group(1).replace('.', '').replace(',', '.')
                        try:
                            product_data["delivered_value"] = float(value_str)
                        except:
                            pass
                
                # Extrair ganâncias
                profits_match = re.search(r'Ganancias\s*\n\s*\$\s*([\d.,]+)', card_text, re.IGNORECASE)
                if profits_match:
                    value_str = profits_match.group(1).replace('.', '').replace(',', '.')
                    try:
                        product_data["profits"] = float(value_str)
                    except:
                        pass
                
                # Verificação final mais rigorosa - produto real deve ter pelo menos duas métricas válidas
                valid_metrics_count = sum([
                    product_data["stock"] > 0,
                    product_data["orders_count"] > 0,
                    product_data["transit_count"] > 0,
                    product_data["delivered_count"] > 0,
                    product_data["profits"] > 0
                ])
                
                if valid_metrics_count >= 1:
                    products_data.append(product_data)
                    logger.info(f"Produto '{product_name}' processado com sucesso")
                else:
                    logger.warning(f"Produto '{product_name}' ignorado por não ter dados significativos")
                
            except Exception as e:
                logger.error(f"Erro ao processar card #{i+1}: {str(e)}")
        
        logger.info(f"Total de {len(products_data)} produtos extraídos com sucesso")
        return products_data
        
    except Exception as e:
        logger.error(f"Erro geral ao extrair dados dos produtos: {str(e)}")
        return []

def save_dropi_metrics_to_db(store_id, date, products_data):
    """Save DroPi product metrics to the database."""
    try:
        # Remover dados existentes para este período
        delete_query = "DELETE FROM dropi_metrics WHERE store_id = ? AND date = ?"
        from db_utils import execute_query
        execute_query(delete_query, (store_id, date))
        
        # Verificar se há produtos duplicados e agrupá-los
        product_names = {}
        for product in products_data:
            product_name = product["product"]
            if product_name in product_names:
                logger.warning(f"Produto duplicado encontrado: {product_name}")
                # Somar os valores para dados duplicados
                for key in ["orders_count", "orders_value", "transit_count", "transit_value", 
                           "delivered_count", "delivered_value", "profits"]:
                    product_names[product_name][key] += product[key]
            else:
                product_names[product_name] = product
        
        # Inserir novos dados
        from db_utils import execute_upsert
        for product_name, product in product_names.items():
            data = {
                "store_id": store_id,
                "date": date, 
                "product": product_name,
                "provider": product["provider"],
                "stock": product["stock"],
                "orders_count": product["orders_count"],
                "orders_value": product["orders_value"],
                "transit_count": product["transit_count"],
                "transit_value": product["transit_value"],
                "delivered_count": product["delivered_count"],
                "delivered_value": product["delivered_value"],
                "profits": product["profits"]
            }
            
            execute_upsert("dropi_metrics", data, ["store_id", "date", "product"])
            logger.info(f"Inserido/atualizado produto: {product_name}")
        
        logger.info(f"Transação concluída com sucesso: {len(product_names)} produtos salvos")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao salvar dados no banco: {str(e)}")
        return False

# === FUNÇÕES PARA O LAYOUT MELHORADO ===

def display_sidebar_filters(store):
    """Exibe os filtros na barra lateral para a loja selecionada."""
    st.sidebar.markdown(f"## Filtros para {store['name']}")
    
    # Seção Shopify
    with st.sidebar.expander("Filtros Shopify", expanded=True):
        # Filtro de Data
        st.sidebar.markdown("#### Período")
        col1, col2 = st.sidebar.columns(2)
        with col1:
            start_date = st.date_input(
                "De:",
                datetime.today() - timedelta(days=7),
                format="DD/MM/YYYY",
            )
        with col2:
            end_date = st.date_input(
                "Até:",
                datetime.today(),
                format="DD/MM/YYYY",
            )
        
        # Filtro de Categoria de URL
        st.sidebar.markdown("#### Filtro de Origem")
        url_categories = get_url_categories(store["id"], start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
        if url_categories:
            category_options = ["Todos"] + url_categories
            selected_category = st.sidebar.selectbox("", category_options)
        else:
            selected_category = "Todos"
        
        # Botão para atualizar dados
        update_shopify = st.sidebar.button("Atualizar Dados Shopify", use_container_width=True)
    
    # Seção DroPi
    with st.sidebar.expander("Filtros DroPi", expanded=True):
        # Substituir as opções de período por seleção de data
        dropi_col1, dropi_col2 = st.sidebar.columns(2)
        
        with dropi_col1:
            dropi_start_date = st.date_input(
                "Data inicial:",
                datetime.today() - timedelta(days=7),
                format="DD/MM/YYYY",
                key="dropi_start_date"
            )
        
        with dropi_col2:
            dropi_end_date = st.date_input(
                "Data final:",
                datetime.today(),
                format="DD/MM/YYYY",
                key="dropi_end_date"
            )
        
        # Botão para atualizar dados
        update_dropi = st.sidebar.button("Atualizar Dados DroPi", use_container_width=True)
    
    return {
        'shopify': {
            'start_date': start_date,
            'end_date': end_date,
            'selected_category': selected_category,
            'update_clicked': update_shopify
        },
        'dropi': {
            'start_date': dropi_start_date,
            'end_date': dropi_end_date,
            'update_clicked': update_dropi
        }
    }
    
    # Seção DroPi
    with st.sidebar.expander("Filtros DroPi", expanded=True):
        # Opções de período
        period_options = {
            "today": "Hoje",
            "last_7_days": "Últimos 7 dias",
            "last_30_days": "Últimos 30 dias",
            "last_90_days": "Últimos 90 dias"
        }
        
        selected_period = st.sidebar.selectbox(
            "Período",
            list(period_options.keys()),
            format_func=lambda x: period_options[x],
            index=1  # Default to "Últimos 7 dias"
        )
        
        # Botão para atualizar dados
        update_dropi = st.sidebar.button("Atualizar Dados DroPi", use_container_width=True)
    
    return {
        'shopify': {
            'start_date': start_date,
            'end_date': end_date,
            'selected_category': selected_category,
            'update_clicked': update_shopify
        },
        'dropi': {
            'selected_period': selected_period,
            'update_clicked': update_dropi
        }
    }

def display_shopify_data(data, selected_category):
    """Exibe os dados da Shopify em tabelas colapsáveis com quatro colunas: produto, total de pedidos, valor total e URL."""
    
    if not data.empty:
        # Filtrar dados por categoria de URL, se selecionado
        filtered_data = data
        if selected_category != "Todos" and 'product_url' in data.columns:
            # Determinar quais URLs pertencem à categoria selecionada
            mask = filtered_data['product_url'].apply(
                lambda url: (
                    ('gg' in url.split('/')[-1].lower() or 'goog' in url.split('/')[-1].lower()) if selected_category == "Google" else
                    ('ttk' in url.split('/')[-1].lower() or 'tktk' in url.split('/')[-1].lower()) if selected_category == "TikTok" else
                    (not any(pattern in url.split('/')[-1].lower() for pattern in ['gg', 'goog', 'ttk', 'tktk']))
                ) if url and '/' in url else False
            )
            filtered_data = filtered_data[mask]
        
        # Agrupar dados por produto
        if not filtered_data.empty:
            # Preparar os dados para agrupamento
            # Como URLs podem ser diferentes para mesmo produto, pegamos a primeira URL para cada produto
            url_mapping = {}
            for _, row in filtered_data.iterrows():
                product = row['product']
                url = row.get('product_url', '')
                if product not in url_mapping and url:
                    url_mapping[product] = url
            
            # Verificar se a coluna total_value existe
            if 'total_value' in filtered_data.columns:
                product_data = filtered_data.groupby(['product']).agg({
                    'total_orders': 'sum',
                    'total_value': 'sum'
                }).reset_index()
                
                # Adicionar coluna formatada para exibição
                product_data['valor_formatado'] = product_data['total_value'].apply(lambda x: "${:,.2f}".format(x))
            else:
                product_data = filtered_data.groupby(['product']).agg({
                    'total_orders': 'sum'
                }).reset_index()
                product_data['valor_formatado'] = "$0.00"  # Adicionar coluna vazia se não existir
            
            # Adicionar coluna de URL
            product_data['url'] = product_data['product'].map(url_mapping)
            
            # Exibir tabela com dados agrupados
            with st.expander("Tabela de Produtos Shopify", expanded=True):
                # Selecionar apenas as colunas que queremos exibir (removendo total_value que é redundante)
                display_df = product_data[['product', 'total_orders', 'valor_formatado', 'url']]
                
                st.dataframe(
                    display_df,
                    column_config={
                        "product": "Produto",
                        "total_orders": "Total de Pedidos",
                        "valor_formatado": "Valor Total",
                        "url": "URL do Produto"
                    },
                    hide_index=True,
                    use_container_width=True
                )
        else:
            st.warning("Nenhum produto encontrado com o filtro selecionado")
    else:
        st.info("Não há dados disponíveis para o intervalo selecionado")

def display_shopify_chart(data, selected_category):
    """Exibe gráfico de barras (colunas) para produtos vs número de pedidos para os dados Shopify."""
    if not data.empty:
        # Filtrar dados por categoria, se selecionado
        filtered_data = data
        if selected_category != "Todos" and 'product_url' in data.columns:
            # Determinar quais URLs pertencem à categoria selecionada
            mask = filtered_data['product_url'].apply(
                lambda url: (
                    ('gg' in url.split('/')[-1].lower() or 'goog' in url.split('/')[-1].lower()) if selected_category == "Google" else
                    ('ttk' in url.split('/')[-1].lower() or 'tktk' in url.split('/')[-1].lower()) if selected_category == "TikTok" else
                    (not any(pattern in url.split('/')[-1].lower() for pattern in ['gg', 'goog', 'ttk', 'tktk']))
                ) if url and '/' in url else False
            )
            filtered_data = filtered_data[mask]
        
        if not filtered_data.empty:
            # Agrupar por produto e calcular total de pedidos
            product_data = filtered_data.groupby(['product']).agg({
                'total_orders': 'sum'
            }).reset_index()
            
            # Ordenar por número de pedidos para melhor visualização
            product_data = product_data.sort_values('total_orders', ascending=False)
            
            # Exibir gráfico
            with st.expander("Gráfico de Vendas Shopify", expanded=True):
                # Criar gráfico de barras usando Altair (mais interativo)
                import altair as alt
                
                # Limitar a 15 produtos para melhor visualização, se houver muitos
                if len(product_data) > 15:
                    chart_data = product_data.head(15)
                else:
                    chart_data = product_data
                
                chart = alt.Chart(chart_data).mark_bar().encode(
                    x=alt.X('product:N', title='Produto', sort='-y'),
                    y=alt.Y('total_orders:Q', title='Total de Pedidos'),
                    color=alt.Color('total_orders:Q', scale=alt.Scale(scheme='blues'), legend=None),
                    tooltip=['product', 'total_orders']
                ).properties(
                    width='container',
                    height=400,
                    title=f'Total de Pedidos por Produto - {selected_category}'
                ).interactive()
                
                st.altair_chart(chart, use_container_width=True)

def display_dropi_data(store_id, date_str):
    """Exibe os dados da DroPi em tabelas colapsáveis e gráficos para uma data específica."""
    conn = get_db_connection()
    
    # Consulta com filtro exato por data
    query = """
        SELECT * FROM dropi_metrics 
        WHERE store_id = ? AND date = ?
    """
    
    if is_railway_environment():
        query = query.replace("?", "%s")
    
    cursor = conn.cursor()
    cursor.execute(query, (store_id, date_str))
    
    # Converter resultados para DataFrame
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    
    # Obter informações da moeda da loja
    currency_info = get_store_currency(store_id)
    currency_from = currency_info["from"]
    currency_to = currency_info["to"]
    
    # Converter para DataFrame
    data_df = pd.DataFrame(data, columns=columns)
    
    conn.close()
    
    # Obter taxa de conversão
    exchange_rate = get_exchange_rate(currency_from, currency_to)
    logger.info(f"Taxa de conversão de {currency_from} para {currency_to}: {exchange_rate}")
    
    # Converter valores monetários
    if not data_df.empty:
        for col in ['orders_value', 'transit_value', 'delivered_value', 'profits']:
            if col in data_df.columns:
                data_df[col] = data_df[col] * exchange_rate
    
    logger.info(f"Encontrados {len(data_df)} registros DroPi para a data selecionada")
    
    if not data_df.empty:
        # Summary statistics
        total_orders = data_df["orders_count"].sum()
        total_orders_value = data_df["orders_value"].sum()
        total_transit = data_df["transit_count"].sum()
        total_transit_value = data_df["transit_value"].sum()
        total_delivered = data_df["delivered_count"].sum()
        total_delivered_value = data_df["delivered_value"].sum()
        total_profits = data_df["profits"].sum()
        
        # Display metrics in three columns
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Pedidos", f"{total_orders}", f"{currency_to} {total_orders_value:,.2f}")
            
        with col2:
            st.metric("Em Trânsito", f"{total_transit}", f"{currency_to} {total_transit_value:,.2f}")
            
        with col3:
            st.metric("Entregues", f"{total_delivered}", f"{currency_to} {total_delivered_value:,.2f}")
        
        # Exibir tabela com todos os dados e uma mensagem de confirmação do período
        period_text = f"Mostrando {len(data_df)} produtos para a data: {date_str} (Valores em {currency_to})"
        
        with st.expander("Produtos DroPi", expanded=True):
            st.info(period_text)
            st.dataframe(
                data_df,
                column_config={
                    "date": "Data",
                    "product": "Produto",
                    "provider": "Fornecedor",
                    "stock": "Estoque",
                    "orders_count": "Pedidos",
                    "orders_value": st.column_config.NumberColumn(f"Valor Pedidos ({currency_to})", format="%.2f"),
                    "transit_count": "Em Trânsito",
                    "transit_value": st.column_config.NumberColumn(f"Valor Trânsito ({currency_to})", format="%.2f"),
                    "delivered_count": "Entregues",
                    "delivered_value": st.column_config.NumberColumn(f"Valor Entregues ({currency_to})", format="%.2f"),
                    "profits": st.column_config.NumberColumn(f"Lucros ({currency_to})", format="%.2f")
                },
                use_container_width=True
            )
        
        return data_df
    else:
        st.info(f"Não há dados disponíveis da DroPi para a data {date_str}.")
        return pd.DataFrame()

def adapt_upsert_query(base_query, update_columns, key_columns):
    """
    Adapta a sintaxe de UPSERT/ON CONFLICT para funcionar em ambos PostgreSQL e SQLite.
    
    Args:
        base_query: Query INSERT base
        update_columns: Lista de colunas a serem atualizadas em caso de conflito
        key_columns: Lista de colunas que compõem a chave primária
    """
    if os.getenv("DATABASE_URL"):
        # PostgreSQL
        conflict_columns = ", ".join(key_columns)
        update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])
        
        return f"{base_query} ON CONFLICT ({conflict_columns}) DO UPDATE SET {update_clause}"
    else:
        # SQLite
        return f"{base_query} ON CONFLICT({', '.join(key_columns)}) DO UPDATE SET " + \
               ", ".join([f"{col} = excluded.{col}" for col in update_columns])

def display_dropi_chart(data):
    """Exibe gráfico interativo para os dados DroPi com barras empilhadas por produto."""
    if not data.empty:
        # Preparar dados para gráfico
        chart_data = data[['product', 'orders_count', 'transit_count', 'delivered_count']].copy()
        
        # Ordenar por número de pedidos para melhor visualização
        chart_data = chart_data.sort_values('orders_count', ascending=False)
        
        with st.expander("Gráfico de Produtos DroPi", expanded=True):
            # Criar gráfico interativo usando Altair
            import altair as alt
            
            # Transformar os dados para formato long, adequado para gráficos de barras empilhadas
            chart_data_long = pd.melt(
                chart_data,
                id_vars=['product'],
                value_vars=['orders_count', 'transit_count', 'delivered_count'],
                var_name='status',
                value_name='quantidade'
            )
            
            # Mapear os nomes das colunas para rótulos mais amigáveis
            status_mapping = {
                'orders_count': 'Pedidos',
                'transit_count': 'Em Trânsito',
                'delivered_count': 'Entregues'
            }
            
            chart_data_long['status'] = chart_data_long['status'].map(status_mapping)
            
            # Definir ordem específica para a legenda
            status_order = ['Pedidos', 'Em Trânsito', 'Entregues']
            
            # Criar gráfico de barras empilhadas
            chart = alt.Chart(chart_data_long).mark_bar().encode(
                x=alt.X('product:N', title='Produto', sort='-y'),
                y=alt.Y('quantidade:Q', title='Quantidade', stack='zero'),
                color=alt.Color('status:N', 
                               scale=alt.Scale(domain=status_order, 
                                               range=['#4B9CD3', '#FFD700', '#50C878']),
                               title='Status'),
                order=alt.Order('status:N', sort='ascending'),
                tooltip=['product', 'status', 'quantidade']
            ).properties(
                width='container',
                height=500,
                title='Status por Produto'
            ).interactive()
            
            st.altair_chart(chart, use_container_width=True)

# === FUNÇÕES PARA TABELA DE EFETIVIDADE ===

def get_saved_effectiveness(store_id):
    """Retrieve previously saved general effectiveness values."""
    conn = get_db_connection()
    query = f"""
        SELECT product, general_effectiveness FROM product_effectiveness 
        WHERE store_id = '{store_id}'
    """
    effectiveness_data = pd.read_sql_query(query, conn)
    conn.close()
    
    # Convert to dictionary for easier lookup
    effectiveness_dict = {}
    if not effectiveness_data.empty:
        for _, row in effectiveness_data.iterrows():
            effectiveness_dict[row['product']] = row['general_effectiveness']
    
    return effectiveness_dict

def save_general_effectiveness(store_id, product, value):
    """Save a manually entered general effectiveness value."""
    from db_utils import get_db_connection, adapt_query
    conn = get_db_connection()
    cursor = conn.cursor()
    
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Adaptar a query para o banco correto
    query = adapt_query("""
        INSERT INTO product_effectiveness 
        (store_id, product, general_effectiveness, last_updated)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(store_id, product) DO UPDATE SET
        general_effectiveness = EXCLUDED.general_effectiveness,
        last_updated = EXCLUDED.last_updated
    """)
    
    cursor.execute(query, (store_id, product, value, current_time))
    
    conn.commit()
    conn.close()

def display_effectiveness_table(store_id, date_str):
    """Display a table with effectiveness metrics based only on DroPi data for a specific date."""
    st.markdown("## Tabela de Efetividade", unsafe_allow_html=True)
    
    # Debug info
    logger.info(f"Buscando dados de efetividade para store_id={store_id}, data: {date_str}")
    
    conn = get_db_connection()
    
    # Get DroPi data only for the specific date
    dropi_query = """
        SELECT product, SUM(orders_count) as orders_count, SUM(delivered_count) as delivered_count
        FROM dropi_metrics 
        WHERE store_id = ? AND date = ?
        GROUP BY product
    """
    
    if is_railway_environment():
        dropi_query = dropi_query.replace("?", "%s")
    
    cursor = conn.cursor()
    cursor.execute(dropi_query, (store_id, date_str))
    
    # Converter resultados para DataFrame
    columns = ["product", "orders_count", "delivered_count"]
    dropi_data = pd.DataFrame(cursor.fetchall(), columns=columns)
    
    # Get saved general effectiveness values for ALL products
    effectiveness_query = """
        SELECT product, general_effectiveness, last_updated
        FROM product_effectiveness 
        WHERE store_id = ?
    """
    
    if is_railway_environment():
        effectiveness_query = effectiveness_query.replace("?", "%s")
    
    cursor.execute(effectiveness_query, (store_id,))
    columns = ["product", "general_effectiveness", "last_updated"]
    effectiveness_data = pd.DataFrame(cursor.fetchall(), columns=columns)
    
    conn.close()
    
    # Process the data if we have any
    if not dropi_data.empty:
        # Calculate effectiveness
        dropi_data['effectiveness'] = 0.0
        for idx, row in dropi_data.iterrows():
            if row['orders_count'] > 0:
                dropi_data.at[idx, 'effectiveness'] = (row['delivered_count'] / row['orders_count']) * 100
        
        # Add general effectiveness column from saved values using left join
        dropi_data = pd.merge(
            dropi_data, 
            effectiveness_data[['product', 'general_effectiveness']], 
            on='product', 
            how='left'
        )
        
        # Adicionar coluna de alerta para baixa efetividade
        dropi_data['alerta'] = dropi_data['effectiveness'].apply(
            lambda x: "⚠️ BAIXA EFETIVIDADE" if x <= 40.0 else ""
        )
        
        # Informar data dos dados
        st.info(f"Mostrando {len(dropi_data)} produtos para a data: {date_str}")
        
        # Create the editable dataframe with styling
        edited_df = st.data_editor(
            dropi_data,
            column_config={
                "product": "Produto",
                "orders_count": st.column_config.NumberColumn("Pedidos"),
                "delivered_count": st.column_config.NumberColumn("Entregues"),
                "effectiveness": st.column_config.NumberColumn(
                    "Efetividade (%)",
                    format="%.1f%%",
                    help="Calculado como (Entregues / Pedidos) * 100"
                ),
                "alerta": st.column_config.TextColumn(
                    "Alerta",
                    help="Produtos com efetividade ≤ 40% recebem alerta"
                ),
                "general_effectiveness": st.column_config.NumberColumn(
                    "Efetividade Geral (%)",
                    format="%.1f%%",
                    help="Valor definido manualmente",
                    disabled=False
                )
            },
            disabled=["product", "orders_count", "delivered_count", "effectiveness", "alerta"],
            use_container_width=True,
            hide_index=True
        )
        
        # Check if any general_effectiveness values have been edited
        if not edited_df.equals(dropi_data):
            # Find rows where general_effectiveness has changed
            for idx, row in edited_df.iterrows():
                product = row['product']
                new_value = row['general_effectiveness']
                
                # Get the old value (if exists)
                try:
                    old_value = dropi_data.loc[dropi_data['product'] == product, 'general_effectiveness'].iloc[0]
                    # Save if changed
                    if pd.notna(new_value) and (pd.isna(old_value) or new_value != old_value):
                        save_effectiveness(store_id, product, new_value)
                        logger.info(f"Salvou efetividade geral para '{product}': {new_value}")
                except:
                    # If there's no old value, save the new one if it's not None/NaN
                    if pd.notna(new_value):
                        save_effectiveness(store_id, product, new_value)
                        logger.info(f"Salvou nova efetividade geral para '{product}': {new_value}")
        
        # Add a button to save all changes at once
        if st.button("Salvar Todas as Alterações", key="save_effectiveness"):
            # Save all values
            for idx, row in edited_df.iterrows():
                if pd.notna(row['general_effectiveness']):
                    save_effectiveness(store_id, row['product'], row['general_effectiveness'])
            st.success("Valores de efetividade geral salvos com sucesso!")
            
    else:
        st.warning(f"Não há dados disponíveis da DroPi para a data {date_str}.")
        
def update_dropi_data_silent(store, start_date, end_date):
    """Atualiza os dados da DroPi sem exibir feedback de progresso."""
    # Configurar o driver Selenium
    driver = setup_selenium(headless=True)
    
    if not driver:
        return False
    
    try:
        # Usar a data final como referência para armazenar os dados
        date_str = end_date.strftime("%Y-%m-%d")
        logger.info(f"Buscando dados DroPi para data: {date_str}")
        
        # Fazer login no DroPi
        success = login(driver, store["dropi_username"], store["dropi_password"], logger)
        
        if not success:
            driver.quit()
            return False
        
        # Navegar para o relatório de produtos vendidos
        if not navigate_to_product_sold(driver, logger):
            driver.quit()
            return False
        
        # Selecionar intervalo de datas específicas
        if not select_date_range(driver, start_date, end_date, logger):
            driver.quit()
            return False
        
        # Extrair dados dos produtos
        product_data = extract_product_data(driver, logger)
        
        if not product_data:
            driver.quit()
            return False
        
        # Limpar dados antigos e salvar os novos
        save_dropi_metrics_to_db(store["id"], date_str, product_data)
        
        # Verificar após salvar (depuração)
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM dropi_metrics WHERE store_id = ? AND date = ?", (store["id"], date_str))
        count = c.fetchone()[0]
        logger.info(f"Verificação: {count} produtos salvos no banco para a data {date_str}")
        conn.close()
        
        driver.quit()
        return True
            
    except Exception as e:
        logger.error(f"Erro ao atualizar dados da DroPi: {str(e)}")
        try:
            driver.quit()
        except:
            pass
        return False
    
def store_dashboard(store):
    """Exibe o dashboard para a loja selecionada com o novo layout."""
    # Configurações da Shopify
    SHOP_NAME = store["shop_name"]
    ACCESS_TOKEN = store["access_token"]
    API_VERSION = "2025-01"
    URL = f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/graphql.json"
    
    HEADERS = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": ACCESS_TOKEN,
    }
    
    # Título principal
    st.markdown(f'<h1 style="text-align: center; color: white;">Dashboard {store["name"]}</h1>', unsafe_allow_html=True)
    
    # Exibir filtros na barra lateral e obter valores selecionados
    filters = display_sidebar_filters(store)
    
    # Processar valores dos filtros
    shopify_filters = filters['shopify']
    dropi_filters = filters['dropi']
    
    start_date = shopify_filters['start_date']
    end_date = shopify_filters['end_date']
    selected_category = shopify_filters['selected_category']
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    
    # Atualizar dados se o botão foi clicado
    if shopify_filters['update_clicked']:
        # Atualizar dados da Shopify
        with st.spinner("Atualizando dados da Shopify..."):
            product_urls = get_shopify_products(URL, HEADERS)
            orders = get_shopify_orders(URL, HEADERS, start_date_str, end_date_str)
            
            if orders:
                product_total, product_processed, product_delivered, product_url_map, product_value = process_shopify_products(orders, product_urls)
                
                # Limpar dados antigos para esse período
                try:
                    query = "DELETE FROM product_metrics WHERE store_id = ? AND date BETWEEN ? AND ?"
                    execute_query(query, (store["id"], start_date_str, end_date_str))
                except Exception as e:
                    st.error(f"Erro ao limpar dados antigos: {str(e)}")
                
                # Salvar os novos dados
                save_metrics_to_db(store["id"], start_date_str, product_total, product_processed, product_delivered, product_url_map, product_value)
                
                st.success("Dados da Shopify atualizados com sucesso!")

    if dropi_filters['update_clicked']:
        # Atualizar dados da DroPi sem mostrar o progresso
        with st.spinner("Atualizando dados da DroPi..."):
            success = update_dropi_data_silent(
                store,
                dropi_filters['start_date'],
                dropi_filters['end_date']
            )
    
        if success:
            st.success("Dados da DroPi atualizados com sucesso!")
        else:
            st.error("Erro ao atualizar dados da DroPi.")
    
    # Criar duas colunas principais
    col_shopify, col_dropi = st.columns(2)
    
    # ========== COLUNA SHOPIFY ==========
    with col_shopify:
        st.markdown('<h2 style="text-align: center; font-weight: bold;">SHOPIFY</h2>', unsafe_allow_html=True)
        
        # Recuperar dados atualizados para o intervalo de datas
        conn = get_db_connection()
        query = f"""
            SELECT * FROM product_metrics 
            WHERE store_id = '{store["id"]}' AND date BETWEEN '{start_date_str}' AND '{end_date_str}'
        """
        shopify_data = pd.read_sql_query(query, conn)
        conn.close()
        
        # Aplicar filtro de categoria antes de calcular métricas
        filtered_data = shopify_data
        if selected_category != "Todos" and 'product_url' in shopify_data.columns:
            # Determinar quais URLs pertencem à categoria selecionada
            mask = filtered_data['product_url'].apply(
                lambda url: (
                    ('gg' in url.split('/')[-1].lower() or 'goog' in url.split('/')[-1].lower()) if selected_category == "Google" else
                    ('ttk' in url.split('/')[-1].lower() or 'tktk' in url.split('/')[-1].lower()) if selected_category == "TikTok" else
                    (not any(pattern in url.split('/')[-1].lower() for pattern in ['gg', 'goog', 'ttk', 'tktk']))
                ) if url and '/' in url else False
            )
            filtered_data = filtered_data[mask]
        
        # Exibir métricas resumidas (similar à imagem da DroPi)
        if not filtered_data.empty:
            # Calcular métricas
            total_orders = filtered_data["total_orders"].sum()
            
            # Verificar se a coluna total_value existe
            total_value = 0
            if 'total_value' in filtered_data.columns:
                total_value = filtered_data["total_value"].sum()
            
            # Display metrics in two columns
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("Pedidos", f"{total_orders}")
                
            with col2:
                # Formatação de moeda adequada com separadores de milhar
                formatted_value = "${:,.2f}".format(total_value)
                st.metric("Valor Total", formatted_value)
        else:
            # Exibir zeros se não houver dados para o filtro selecionado
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Pedidos", "0")
            with col2:
                st.metric("Valor Total", "$0.00")
        
        # Exibir dados em tabelas colapsáveis
        display_shopify_data(shopify_data, selected_category)
        
        # Exibir gráficos
        display_shopify_chart(shopify_data, selected_category)

    # ========== COLUNA DROPI ==========
    with col_dropi:
        st.markdown('<h2 style="text-align: center; font-weight: bold;">DROPI</h2>', unsafe_allow_html=True)
    
        # Usar apenas a data final selecionada ao invés de um intervalo
        dropi_date = dropi_filters['end_date']
        dropi_date_str = dropi_date.strftime("%Y-%m-%d")
    
        # Exibir dados em tabelas colapsáveis e obter os dados para os gráficos
        # Usar apenas a data específica, não um intervalo
        dropi_data = display_dropi_data(store["id"], dropi_date_str)
    
        # Exibir gráficos
        if not dropi_data.empty:
            display_dropi_chart(dropi_data)

    # ========== SEÇÃO DE EFETIVIDADE ==========
    st.markdown('<hr style="height:2px;border-width:0;color:gray;background-color:gray;margin-top:20px;margin-bottom:20px;">', unsafe_allow_html=True)
    st.markdown('<h3 style="text-align: center; font-weight: normal;">Análise de Efetividade</h3>', unsafe_allow_html=True)

    # Exibir tabela de efetividade apenas com dados da mesma data específica
    display_effectiveness_table(store["id"], dropi_date_str)

# Main code execution starts here

# Inicializar banco de dados
init_db()

# Sidebar para seleção de loja
st.sidebar.title("Seleção de Loja")

# Carregar lojas existentes (usando a função do db_utils)
stores = load_stores()
store_options = ["Selecione uma loja..."] + [store[1] for store in stores] + ["➕ Cadastrar Nova Loja"]
selected_option = st.sidebar.selectbox("Escolha uma loja:", store_options)

# Variável para armazenar a loja selecionada
selected_store = None

# Lógica para quando "Cadastrar Nova Loja" é selecionado
if selected_option == "➕ Cadastrar Nova Loja":
    st.sidebar.subheader("Cadastrar Nova Loja")
    
    # Shopify fields
    st.sidebar.markdown("#### Dados Shopify")
    store_name = st.sidebar.text_input("Nome da Loja:")
    shop_name = st.sidebar.text_input("Nome da Loja Shopify (prefixo):")
    access_token = st.sidebar.text_input("Token de Acesso:", type="password")
    
    # DroPi fields
    st.sidebar.markdown("#### Dados DroPi")
    dropi_url = st.sidebar.text_input("URL DroPi:", value="https://app.dropi.mx/")
    dropi_username = st.sidebar.text_input("Email/Usuário DroPi:")
    dropi_password = st.sidebar.text_input("Senha DroPi:", type="password")
    
    # Currency fields
    st.sidebar.markdown("#### Configurações de Moeda")
    
    # Lista de moedas comuns
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
    
    currency_from = st.sidebar.selectbox(
        "Moeda da Loja:",
        options=list(currencies.keys()),
        format_func=lambda x: currencies[x],
        index=0  # MXN como padrão
    )
    
    currency_to = st.sidebar.selectbox(
        "Converter para Moeda:",
        options=list(currencies.keys()),
        format_func=lambda x: currencies[x],
        index=1  # BRL como padrão
    )
    
    if st.sidebar.button("Salvar Loja"):
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
                st.sidebar.success(f"Loja '{store_name}' cadastrada com sucesso!")
                # Recarregar a página para atualizar a lista
                st.rerun()
            else:
                st.sidebar.error("Erro ao cadastrar loja.")
        else:
            st.sidebar.error("Preencha todos os campos!")

# Se uma loja foi selecionada (exceto a opção padrão e cadastrar nova)
elif selected_option != "Selecione uma loja...":
    # Encontrar o ID da loja selecionada
    for store_id, store_name in stores:
        if store_name == selected_option:
            selected_store = get_store_details(store_id)
            selected_store["id"] = store_id
            break

# Dashboard principal quando uma loja é selecionada
if selected_store:
    # Mostrar o dashboard para a loja selecionada
    store_dashboard(selected_store)
else:
    # Tela inicial quando nenhuma loja está selecionada
    st.title("Bem-vindo ao Dashboard Shopify & DroPi")
    st.write("Selecione uma loja no menu lateral ou cadastre uma nova para começar.")