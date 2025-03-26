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
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
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
    
    # Verificar e adicionar colunas para Dropi
    try:
        c.execute("SELECT dropi_url FROM stores LIMIT 1")
    except sqlite3.OperationalError:
        st.info("Atualizando estrutura do banco de dados para Dropi...")
        c.execute("ALTER TABLE stores ADD COLUMN dropi_url TEXT")
        c.execute("ALTER TABLE stores ADD COLUMN dropi_username TEXT")
        c.execute("ALTER TABLE stores ADD COLUMN dropi_password TEXT")
        st.success("Banco de dados atualizado com sucesso para Dropi!")
    
    # Verificar e adicionar colunas para moedas
    try:
        c.execute("SELECT currency_from FROM stores LIMIT 1")
    except sqlite3.OperationalError:
        st.info("Atualizando estrutura do banco de dados para suporte a moedas...")
        c.execute("ALTER TABLE stores ADD COLUMN currency_from TEXT DEFAULT 'MXN'")
        c.execute("ALTER TABLE stores ADD COLUMN currency_to TEXT DEFAULT 'BRL'")
        st.success("Banco de dados atualizado com sucesso para suporte a moedas!")
    
    # Criar tabela para métricas da Dropi
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
    """Consulta produtos da Shopify para obter os URLs e imagens."""
    query = """
    query getProducts($cursor: String) {
      products(first: 50, after: $cursor) {
        edges {
          node {
            id
            title
            handle
            onlineStoreUrl
            images(first: 1) {
              edges {
                node {
                  originalSrc
                }
              }
            }
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
    
    # Criar dicionário de produtos com URLs e imagens
    product_urls = {}
    product_images = {}
    
    for product_edge in all_products:
        product = product_edge.get("node", {})
        title = product.get("title", "")
        handle = product.get("handle", "")
        url = product.get("onlineStoreUrl", "")
        
        if not url and handle:
            # Se a URL não estiver disponível, construa-a a partir do handle
            url = f"/products/{handle}"
        
        # Extrair URL da imagem
        image_url = ""
        images = product.get("images", {}).get("edges", [])
        if images and len(images) > 0:
            image_url = images[0].get("node", {}).get("originalSrc", "")
        
        product_urls[title] = url
        product_images[title] = image_url
    
    return product_urls, product_images

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

def process_shopify_products(orders, product_urls, product_images):
    """Processa pedidos e retorna dicionários com contagens e valores por produto."""
    product_total = {}
    product_processed = {}
    product_delivered = {}
    product_url_map = {}
    product_image_map = {}  # Novo dicionário para imagens
    product_value = {}      # Para armazenar valores totais
    
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
            
            # Armazenar imagem do produto
            if product_title in product_images:
                product_image_map[product_title] = product_images[product_title]
            else:
                product_image_map[product_title] = ""
            
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
    
    return product_total, product_processed, product_delivered, product_url_map, product_value, product_image_map

def save_metrics_to_db(store_id, date, product_total, product_processed, product_delivered, product_url_map, product_value, product_image_map):
    """Salva as métricas no banco de dados."""
    conn = None
    try:
        # Obter nova conexão
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Verificar se a coluna product_image_url existe em uma transação separada
        try:
            # Tente fazer um select para ver se a coluna existe
            cursor.execute("SELECT product_image_url FROM product_metrics LIMIT 1")
        except Exception as schema_error:
            # Se der erro, a coluna não existe e precisamos criá-la
            logger.info("Adicionando coluna product_image_url à tabela product_metrics")
            
            # Certifique-se de que qualquer transação abortada seja finalizada
            try:
                conn.rollback()
            except:
                pass
                
            # Feche a conexão e abra uma nova
            cursor.close()
            conn.close()
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Adicionar coluna em uma nova transação
            try:
                if is_railway_environment():
                    # PostgreSQL
                    cursor.execute("ALTER TABLE product_metrics ADD COLUMN IF NOT EXISTS product_image_url TEXT")
                else:
                    # SQLite
                    cursor.execute("ALTER TABLE product_metrics ADD COLUMN product_image_url TEXT")
                conn.commit()
            except Exception as alter_error:
                logger.error(f"Erro ao adicionar coluna: {str(alter_error)}")
                conn.rollback()
                # Continuar mesmo assim, talvez a coluna já exista em outro formato
        
        # Feche e reabra a conexão para garantir que começamos com uma transação limpa
        cursor.close()
        conn.close()
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Limpar dados existentes para este período
        delete_query = "DELETE FROM product_metrics WHERE store_id = ? AND date = ?"
        if is_railway_environment():
            delete_query = delete_query.replace("?", "%s")
        
        cursor.execute(delete_query, (store_id, date))
        conn.commit()
        
        # Inserir novos dados - em uma nova transação
        for product in product_total:
            data = {
                "store_id": store_id,
                "date": date, 
                "product": product,
                "product_url": product_url_map.get(product, ""),
                "product_image_url": product_image_map.get(product, ""),  # Nova coluna para imagem
                "total_orders": product_total.get(product, 0),
                "processed_orders": product_processed.get(product, 0),
                "delivered_orders": product_delivered.get(product, 0),
                "total_value": product_value.get(product, 0)
            }
            
            execute_upsert("product_metrics", data, ["store_id", "date", "product"])
        
        logger.info(f"Métricas salvas com sucesso para {len(product_total)} produtos")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao salvar métricas: {str(e)}")
        if conn:
            try:
                conn.rollback()
            except:
                pass
        st.error(f"Erro ao salvar dados: {str(e)}")
        return False
        
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

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

# === FUNÇÕES PARA Dropi ===

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

def login(driver, email, password, logger, url="https://app.dropi.mx/"):
    """Função de login super robusta."""
    try:
        # Abre o site em uma nova janela maximizada
        driver.maximize_window()
        
        # Navega para a página de login usando a URL fornecida
        logger.info(f"Navegando para a página de login: {url}")
        driver.get(url)
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
    """Navigate to the Product Sold report in Dropi."""
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

def try_select_day(driver, day_number, day_description, logger):
    """Tenta selecionar um dia específico no calendário com identificação melhorada de elementos."""
    day_str = str(day_number)
    selected = False
    
    # Capturar screenshot para diagnóstico
    driver.save_screenshot(f"calendar_before_{day_description}.png")
    
    # Registrar elementos de dia visíveis para diagnóstico
    try:
        all_day_elements = driver.find_elements(By.XPATH, "//span[contains(@class, 'p-element') and not(contains(@class, 'disabled'))]")
        logger.info(f"Total de elementos de dia encontrados: {len(all_day_elements)}")
        for i, day_elem in enumerate(all_day_elements[:15]):
            try:
                logger.info(f"Dia potencial {i+1}: texto='{day_elem.text}', classe='{day_elem.get_attribute('class')}'")
            except:
                pass
    except Exception as e:
        logger.warning(f"Não foi possível listar todos os elementos de dia: {str(e)}")
    
    # Lista de seletores para o dia específico - incluindo padrões observados nos exemplos
    day_selectors_templates = [
        # Seletores específicos baseados nos exemplos fornecidos
        "//span[contains(@class, 'p-ripple') and contains(@class, 'p-element') and text()='{day}']",
        "//span[contains(@class, 'ng-star-inserted') and text()='{day}']",
        "//span[contains(@class, 'p-element') and text()='{day}']",
        # Seletores genéricos para dias
        "//table[contains(@class, 'calendar') or contains(@class, 'datepicker')]//td[text()='{day}']",
        "//table[contains(@class, 'calendar') or contains(@class, 'datepicker')]//td[.='{day}']",
        "//table[contains(@class, 'p-datepicker-calendar')]//td/span[text()='{day}']",
        "//table[contains(@class, 'p-datepicker-calendar')]//span[text()='{day}']",
        "//div[contains(@class, 'day') and text()='{day}']",
        "//*[contains(@class, 'day') and text()='{day}']",
        "//*[text()='{day}' and not(contains(@class, 'disabled'))]"
    ]
    
    # Tentativa 1: Usar seletores específicos
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
                            # Destaque o elemento para diagnóstico
                            driver.execute_script("arguments[0].style.border='3px solid green'", day_elem)
                            driver.save_screenshot(f"{day_description}_found_{i+1}.png")
                            
                            # Tente clicar usando diferentes métodos
                            try:
                                # Método 1: Clique direto
                                day_elem.click()
                                logger.info(f"Selecionou o dia {day_str} com clique direto (elemento {i+1})")
                                selected = True
                                return selected
                            except Exception as click_error:
                                logger.warning(f"Clique direto falhou: {str(click_error)}")
                                
                                try:
                                    # Método 2: Clique via JavaScript
                                    driver.execute_script("arguments[0].click();", day_elem)
                                    logger.info(f"Selecionou o dia {day_str} via JavaScript (elemento {i+1})")
                                    selected = True
                                    return selected
                                except Exception as js_error:
                                    logger.warning(f"Clique JavaScript falhou: {str(js_error)}")
                                    
                                    try:
                                        # Método 3: Ações avançadas
                                        actions = ActionChains(driver)
                                        actions.move_to_element(day_elem).click().perform()
                                        logger.info(f"Selecionou o dia {day_str} via ActionChains (elemento {i+1})")
                                        selected = True
                                        return selected
                                    except Exception as action_error:
                                        logger.warning(f"ActionChains falhou: {str(action_error)}")
                    except Exception as e:
                        logger.warning(f"Erro ao processar elemento {i+1} para o dia {day_str}: {str(e)}")
        except Exception as e:
            logger.warning(f"Seletor {day_xpath} falhou: {str(e)}")
    
    # Tentativa 2: Abordagem mais genérica - encontrar qualquer elemento com o número do dia
    try:
        logger.info("Tentativa de encontrar o dia usando abordagem genérica")
        # Pegar todos os elementos span na página
        all_spans = driver.find_elements(By.TAG_NAME, "span")
        logger.info(f"Total de elementos span: {len(all_spans)}")
        
        # Filtrar para spans que contêm o texto exato do dia
        day_spans = [span for span in all_spans if span.text == day_str]
        logger.info(f"Encontrados {len(day_spans)} spans com texto '{day_str}'")
        
        for i, span in enumerate(day_spans):
            try:
                # Verificar se está dentro de algum elemento de calendário/datepicker
                parent_html = driver.execute_script("""
                    var element = arguments[0];
                    var current = element;
                    var html = "";
                    for (var i = 0; i < 5; i++) {
                        current = current.parentElement;
                        if (!current) break;
                        html += current.outerHTML;
                    }
                    return html;
                """, span)
                
                if any(term in parent_html.lower() for term in ['calendar', 'date', 'picker', 'day']):
                    logger.info(f"Span {i+1} está dentro de um contexto de calendário")
                    driver.execute_script("arguments[0].style.border='3px solid blue'", span)
                    driver.save_screenshot(f"generic_day_found_{i+1}.png")
                    
                    try:
                        # Tente clicar de várias maneiras
                        try:
                            span.click()
                            logger.info(f"Clicou no dia {day_str} (span genérico, clique direto)")
                            selected = True
                            return selected
                        except:
                            driver.execute_script("arguments[0].click();", span)
                            logger.info(f"Clicou no dia {day_str} (span genérico, JavaScript)")
                            selected = True
                            return selected
                    except Exception as click_e:
                        logger.warning(f"Não conseguiu clicar no span {i+1}: {str(click_e)}")
            except Exception as span_e:
                logger.warning(f"Erro ao processar span {i+1}: {str(span_e)}")
    except Exception as e:
        logger.warning(f"Tentativa genérica para encontrar o dia falhou: {str(e)}")
    
    # Tentativa 3: Implementação robusta adicional para casos difíceis
    try:
        logger.info("Tentando técnica alternativa para selecionar o dia")
        
        # Técnica: Clicar diretamente no número via JavaScript executando código mais complexo
        js_result = driver.execute_script(f"""
        // Tenta encontrar elementos de dia com o texto especificado
        var allElements = document.querySelectorAll('*');
        var candidates = [];
        
        for (var i = 0; i < allElements.length; i++) {{
            var el = allElements[i];
            // Verifica se o texto exato do elemento é o dia procurado
            if (el.textContent.trim() === '{day_str}') {{
                // Verifica se está em algum contexto de calendário
                var isInCalendar = false;
                var current = el;
                for (var j = 0; j < 5; j++) {{
                    if (!current) break;
                    var classList = current.className || '';
                    if (classList.indexOf('calendar') >= 0 || 
                        classList.indexOf('date') >= 0 || 
                        classList.indexOf('picker') >= 0 ||
                        classList.indexOf('day') >= 0) {{
                        isInCalendar = true;
                        break;
                    }}
                    current = current.parentElement;
                }}
                
                if (isInCalendar) {{
                    candidates.push(el);
                }}
            }}
        }}
        
        // Tenta clicar em cada candidato até ter sucesso
        for (var k = 0; k < candidates.length; k++) {{
            try {{
                candidates[k].style.border = '2px solid red';
                candidates[k].scrollIntoView({{behavior: 'auto', block: 'center'}});
                candidates[k].click();
                return true;
            }} catch(e) {{
                console.log('Erro ao clicar:', e);
            }}
        }}
        
        return false;
        """)
        
        if js_result:
            logger.info(f"Selecionou o dia {day_str} usando script JavaScript avançado")
            driver.save_screenshot(f"js_advanced_click_{day_description}.png")
            selected = True
            return selected
            
    except Exception as js_e:
        logger.warning(f"Técnica JavaScript avançada falhou: {str(js_e)}")
    
    # Se chegou até aqui, não conseguiu selecionar o dia
    logger.error(f"FALHA: Não foi possível selecionar o dia {day_str}")
    driver.save_screenshot(f"day_selection_failed_{day_description}.png")
    return selected

def select_date_range(driver, start_date, end_date, logger):
    """Select a specific date range in the Product Sold report with improved robustness."""
    try:
        # Formatação das datas para exibição no formato esperado pelo Dropi (DD/MM/YYYY)
        start_date_formatted = start_date.strftime("%d/%m/%Y")
        end_date_formatted = end_date.strftime("%d/%m/%Y")
        
        logger.info(f"Tentando selecionar intervalo de datas: {start_date_formatted} a {end_date_formatted}")
        
        # Capturar screenshot para diagnóstico
        driver.save_screenshot("before_date_select.png")
        
        # 1. ABORDAGEM MELHORADA: Primeiro, tentar preencher diretamente campos de input se existirem
        try:
            logger.info("Tentando abordagem direta - procurando campos de input de data")
            date_inputs = driver.find_elements(By.XPATH, "//input[contains(@class, 'date') or @type='date' or contains(@placeholder, '/')]")
            
            if len(date_inputs) >= 2:
                logger.info(f"Encontrados {len(date_inputs)} campos de input de data")
                
                # Preencha o primeiro campo (data inicial)
                start_input = date_inputs[0]
                driver.execute_script("arguments[0].style.border='3px solid red'", start_input)
                driver.save_screenshot("start_date_input.png")
                
                # Limpar o campo e preencher
                start_input.clear()
                start_input.send_keys(start_date_formatted)
                logger.info(f"Preencheu data inicial: {start_date_formatted}")
                
                # Preencha o segundo campo (data final)
                end_input = date_inputs[1]
                driver.execute_script("arguments[0].style.border='3px solid blue'", end_input)
                driver.save_screenshot("end_date_input.png")
                
                # Limpar o campo e preencher
                end_input.clear()
                end_input.send_keys(end_date_formatted)
                logger.info(f"Preencheu data final: {end_date_formatted}")
                
                # Pressionar Enter no último campo para confirmar
                end_input.send_keys(Keys.ENTER)
                
                # Esperar processamento
                time.sleep(3)
                driver.save_screenshot("after_direct_date_input.png")
                
                # Verificar se há um botão de confirmação próximo
                try:
                    apply_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Apply') or contains(text(), 'Aplicar') or contains(text(), 'Filtrar')]")
                    apply_button.click()
                    logger.info("Clicou no botão de aplicar/filtrar após preencher datas")
                except:
                    logger.info("Nenhum botão de aplicar/filtrar encontrado após preencher datas")
                
                # Período foi configurado com sucesso
                return True
            else:
                logger.info(f"Não encontrou dois campos de input de data ({len(date_inputs)} encontrados)")
        except Exception as input_error:
            logger.warning(f"Abordagem direta de preenchimento falhou: {str(input_error)}")
        
        # 2. ABORDAGEM ALTERNATIVA: Procurar por elemento de seletor de intervalo de datas (comum em muitas interfaces)
        try:
            logger.info("Tentando abordagem de seletor de intervalo")
            date_range_selectors = [
                "//div[contains(@class, 'daterangepicker') or contains(@class, 'date-range')]",
                "//div[contains(text(), ' - ') and (contains(@class, 'date') or contains(@class, 'calendar'))]",
                "//input[contains(@class, 'daterange') or contains(@placeholder, '-')]",
                "//div[contains(@class, 'date-field') and contains(text(), '/')]"
            ]
            
            for selector in date_range_selectors:
                try:
                    range_element = driver.find_element(By.XPATH, selector)
                    driver.execute_script("arguments[0].style.border='3px solid purple'", range_element)
                    driver.save_screenshot("date_range_element.png")
                    
                    # Tenta clicar no elemento
                    range_element.click()
                    logger.info(f"Clicou em elemento de intervalo de datas usando seletor: {selector}")
                    
                    # Esperar popup do calendário aparecer
                    time.sleep(2)
                    
                    # Tente preencher as datas no popup
                    popup_inputs = driver.find_elements(By.XPATH, "//div[contains(@class, 'popup') or contains(@class, 'dropdown')]//input")
                    
                    if len(popup_inputs) >= 2:
                        # Preencher início e fim
                        popup_inputs[0].clear()
                        popup_inputs[0].send_keys(start_date_formatted)
                        popup_inputs[1].clear()
                        popup_inputs[1].send_keys(end_date_formatted)
                        popup_inputs[1].send_keys(Keys.ENTER)
                        logger.info("Preencheu datas no popup de intervalo")
                        
                        # Esperar processamento
                        time.sleep(2)
                        
                        # Procurar botão aplicar
                        apply_buttons = driver.find_elements(By.XPATH, "//button[contains(text(), 'Apply') or contains(text(), 'Aplicar')]")
                        if apply_buttons:
                            apply_buttons[0].click()
                            logger.info("Clicou em botão aplicar no popup")
                        
                        return True
                    else:
                        logger.info("Não encontrou campos de input no popup de intervalo")
                except Exception as e:
                    logger.warning(f"Seletor de intervalo {selector} falhou: {str(e)}")
        except Exception as range_error:
            logger.warning(f"Abordagem de seletor de intervalo falhou: {str(range_error)}")
        
        # 3. ABORDAGEM ORIGINAL MELHORADA: Clicar no seletor de data e usar o calendário
        # Seletores específicos para o campo de data, baseado na captura de tela e no log
        date_selectors = [
            "//div[contains(@class, 'date-field') or contains(@class, 'date-picker')]",
            "//div[@class='datepicker-toggle']",
            "//div[contains(@class, 'datepicker')]//input",
            "//div[contains(@class, 'daterangepicker')]",
            "//input[contains(@class, 'form-control') and contains(@class, 'daterange')]",
            "//button[contains(@class, 'date') or contains(@class, 'calendar')]",
            "//div[contains(text(), '/') and (contains(@class, 'date') or contains(@class, 'calendar'))]",
            "//*[contains(text(), 'Date Range') or contains(text(), 'Rango de fecha')]",
            "//span[contains(@class, 'p-input-icon-right')]/input", # Novo seletor baseado no PrimeNG
            "//p-calendar//input" # Seletor específico para PrimeNG Calendar
        ]
        
        # Tentar clicar no seletor de data
        clicked = False
        for selector in date_selectors:
            try:
                logger.info(f"Tentando seletor de data: {selector}")
                try:
                    element = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, selector))
                    )
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
        
        # Se todas as tentativas acima falharem, fazemos uma tentativa mais abrangente
        if not clicked:
            try:
                logger.info("Tentando abordagem avançada para encontrar o seletor de data")
                
                # Script JavaScript para encontrar e clicar em qualquer elemento que pareça um seletor de data
                js_clicked = driver.execute_script("""
                    // Função para verificar se um elemento parece ser um seletor de data
                    function looksLikeDatePicker(element) {
                        // Verificar classes e atributos
                        var classes = element.className || '';
                        var id = element.id || '';
                        var placeholder = element.placeholder || '';
                        var type = element.type || '';
                        
                        // Verificar se o elemento ou algum filho contém uma data no formato DD/MM/YYYY
                        var hasDateText = false;
                        var text = element.textContent || '';
                        if (text.match(/\\d{1,2}\\/\\d{1,2}\\/\\d{4}/)) {
                            hasDateText = true;
                        }
                        
                        // Verificar se parece ser um seletor de data
                        return (
                            classes.indexOf('date') >= 0 || 
                            classes.indexOf('calendar') >= 0 ||
                            classes.indexOf('picker') >= 0 ||
                            id.indexOf('date') >= 0 ||
                            placeholder.indexOf('/') >= 0 ||
                            type === 'date' ||
                            hasDateText
                        );
                    }
                    
                    // Encontrar todos os elementos que parecem ser seletores de data
                    var allElements = document.querySelectorAll('*');
                    var datePickers = [];
                    
                    for (var i = 0; i < allElements.length; i++) {
                        if (looksLikeDatePicker(allElements[i])) {
                            datePickers.push(allElements[i]);
                        }
                    }
                    
                    // Se encontrou algum seletor de data, tentar clicar no primeiro
                    if (datePickers.length > 0) {
                        try {
                            // Destaque visual e scroll
                            datePickers[0].style.border = '3px solid orange';
                            datePickers[0].scrollIntoView({behavior: 'auto', block: 'center'});
                            
                            // Tenta clicar
                            datePickers[0].click();
                            return true;
                        } catch(e) {
                            console.log('Erro ao clicar:', e);
                            return false;
                        }
                    }
                    
                    return false;
                """)
                
                if js_clicked:
                    logger.info("Conseguiu clicar em um seletor de data via JavaScript avançado")
                    clicked = True
                else:
                    logger.warning("JavaScript avançado não conseguiu encontrar/clicar em um seletor de data")
            except Exception as js_error:
                logger.warning(f"Abordagem JavaScript avançada falhou: {str(js_error)}")
        
        if not clicked:
            logger.error("Não foi possível clicar no seletor de data após múltiplas tentativas")
            return False
        
        # Esperar o popup do calendário aparecer
        time.sleep(3)
        driver.save_screenshot("date_popup.png")
        
        # Verificar e navegar para o mês/ano correto de forma melhorada
        expected_month = start_date.month
        expected_year = start_date.year
        logger.info(f"Mês/ano desejado: {expected_month}/{expected_year}")
        
        # Função melhorada para verificar e navegar até o mês/ano correto
        def navigate_to_month_year():
            try:
                # Encontrar onde está exibido o mês/ano atual
                month_year_elements = driver.find_elements(
                    By.XPATH, 
                    "//div[contains(@class, 'month') or contains(@class, 'year') or contains(@class, 'title')]"
                )
                
                if not month_year_elements:
                    logger.warning("Não encontrou elementos de mês/ano para navegação")
                    return False
                
                # Mapear nomes de meses em inglês e espanhol
                month_map = {
                    'January': 1, 'February': 2, 'March': 3, 'April': 4,
                    'May': 5, 'June': 6, 'July': 7, 'August': 8,
                    'September': 9, 'October': 10, 'November': 11, 'December': 12,
                    'Enero': 1, 'Febrero': 2, 'Marzo': 3, 'Abril': 4,
                    'Mayo': 5, 'Junio': 6, 'Julio': 7, 'Agosto': 8,
                    'Septiembre': 9, 'Octubre': 10, 'Noviembre': 11, 'Diciembre': 12
                }
                
                for element in month_year_elements:
                    try:
                        # Obter texto do elemento
                        text = element.text
                        logger.info(f"Texto do elemento mês/ano: '{text}'")
                        
                        # Tentar extrair mês e ano usando regex
                        import re
                        match = re.search(r'(\w+)\s+(\d{4})', text)
                        if match:
                            current_month_str = match.group(1)
                            current_year_str = match.group(2)
                            
                            # Converter para números
                            current_month = None
                            if current_month_str in month_map:
                                current_month = month_map[current_month_str]
                            
                            current_year = int(current_year_str)
                            
                            # Se encontramos o mês/ano procurado, retorna True
                            if current_month == expected_month and current_year == expected_year:
                                logger.info(f"Já estamos no mês/ano desejado: {current_month}/{current_year}")
                                return True
                            
                            # Se não, precisamos navegar
                            logger.info(f"Mês/ano atual ({current_month}/{current_year}) difere do desejado ({expected_month}/{expected_year})")
                            
                            # Calcular diferença em meses
                            months_diff = (expected_year - current_year) * 12 + (expected_month - current_month)
                            logger.info(f"Diferença de {months_diff} meses")
                            
                            # Clicar no botão de navegar para frente ou para trás conforme necessário
                            navigate_button = None
                            if months_diff < 0:
                                # Navegar para trás
                                navigate_button = driver.find_element(
                                    By.XPATH, 
                                    "//button[contains(@class, 'prev') or contains(@class, 'previous') or contains(@class, 'p-datepicker-prev')]"
                                )
                                logger.info("Navegando para trás")
                            else:
                                # Navegar para frente
                                navigate_button = driver.find_element(
                                    By.XPATH, 
                                    "//button[contains(@class, 'next') or contains(@class, 'p-datepicker-next')]"
                                )
                                logger.info("Navegando para frente")
                            
                            # Clicar no botão de navegação quantas vezes for necessário (limitado a 12)
                            clicks = min(abs(months_diff), 12)
                            for i in range(clicks):
                                navigate_button.click()
                                logger.info(f"Clique de navegação {i+1}/{clicks}")
                                time.sleep(0.5)
                            
                            # Após a navegação, verificar novamente
                            time.sleep(1)
                            return navigate_to_month_year()
                    except Exception as e:
                        logger.warning(f"Erro ao navegar no calendário: {str(e)}")
                
                # Se chegou aqui, não conseguiu navegar
                return False
            except Exception as e:
                logger.error(f"Erro na função de navegação: {str(e)}")
                return False
        
        # Tentar navegar para o mês/ano correto
        if not navigate_to_month_year():
            logger.warning("Não conseguiu navegar para o mês/ano correto, tentando selecionar dias mesmo assim")
        
        # Capturar screenshot após navegação entre meses
        driver.save_screenshot("after_month_navigation.png")
        
        # Selecionar os dias usando a função melhorada
        start_day = int(start_date.strftime("%d"))
        start_day_selected = try_select_day(driver, start_day, "start_day", logger)
        
        # Aguardar processamento da seleção da data inicial
        time.sleep(2)
        
        # Tentar selecionar a data final
        end_day = int(end_date.strftime("%d"))
        end_day_selected = try_select_day(driver, end_day, "end_day", logger)
        
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
    """Extract product data from the Product Sold report with enhanced robustness for modern interfaces."""
    try:
        logger.info("Iniciando extração de dados dos produtos com método aprimorado")
        driver.save_screenshot("product_cards.png")
        
        # Esperar que a página carregue completamente com timeout mais longo
        time.sleep(8)
        
        # Verificar se há um indicador de carregamento e esperar até que desapareça
        try:
            loading_elements = driver.find_elements(By.XPATH, "//*[contains(@class, 'loading') or contains(@class, 'spinner')]")
            if loading_elements:
                logger.info("Detectado indicador de carregamento. Aguardando...")
                WebDriverWait(driver, 30).until(
                    EC.invisibility_of_element_located((By.XPATH, "//*[contains(@class, 'loading') or contains(@class, 'spinner')]"))
                )
                logger.info("Carregamento concluído.")
        except Exception as loading_error:
            logger.warning(f"Erro ao verificar/aguardar carregamento: {str(loading_error)}")
        
        products_data = []
        
        # Lista de textos a ignorar (cabeçalhos, títulos, etc)
        ignore_texts = [
            "Informe de productos", 
            "Reporte de productos",
            "Productos vendidos",
            "Productos en transito",
            "Resumen",
            "Total",
            "Filtrar",
            "Pedidos",
            "Ver detalles"
        ]
        
        # Abordagem 1: Detectar automaticamente o tipo de interface
        # Verificar se é uma interface moderna ou tradicional
        modern_interface = False
        try:
            modern_elements = driver.find_elements(By.XPATH, "//div[contains(@class, 'mat-') or contains(@class, 'p-') or contains(@class, 'ng-')]")
            if len(modern_elements) > 10:  # Muitos elementos com classes angular/primeng/material
                modern_interface = True
                logger.info("Detectada interface moderna (Angular/PrimeNG/Material)")
        except:
            pass
        
        # ABORDAGEM PRINCIPAL: Localizar todos os cards de produtos com seletores específicos
        product_cards = []
        
        # Lista de seletores para diferentes tipos de interfaces
        card_selectors = [
            # Seletores para interfaces tradicionais
            "//div[contains(@class, 'card') or contains(@class, 'product-card') or contains(@class, 'item')]",
            "//div[.//img]/following-sibling::div[1]",
            # Seletores para interfaces modernas
            "//div[contains(@class, 'p-card') or contains(@class, 'p-panel') or contains(@class, 'p-fieldset')]",
            "//mat-card",
            "//div[contains(@class, 'product-item')]",
            # Seletores genéricos para contêineres que podem conter produtos
            "//div[contains(@class, 'col') and .//div[contains(@class, 'row')]]",
            "//div[@class='row']/div[@class='col']"
        ]
        
        # Tentar cada seletor na lista
        for selector in card_selectors:
            try:
                found_cards = driver.find_elements(By.XPATH, selector)
                if found_cards and len(found_cards) > 0:
                    logger.info(f"Encontrados {len(found_cards)} cards usando seletor: {selector}")
                    product_cards.extend(found_cards)
            except Exception as selector_error:
                logger.warning(f"Erro com seletor {selector}: {str(selector_error)}")
        
        # Remover duplicatas (elementos que podem ter sido selecionados mais de uma vez)
        product_cards = list(set(product_cards))
        logger.info(f"Total de {len(product_cards)} cards únicos encontrados")
        
        # Se não encontrou nenhum card, tentar abordagem alternativa
        if not product_cards or len(product_cards) < 1:
            logger.warning("Nenhum card encontrado. Tentando abordagem alternativa...")
            
            # 1. Verificar se há uma tabela de produtos
            try:
                product_tables = driver.find_elements(By.XPATH, "//table[contains(@class, 'product') or contains(@class, 'items')]")
                if product_tables and len(product_tables) > 0:
                    logger.info(f"Encontrada tabela de produtos. Tentando extrair dados...")
                    
                    # Extrair linhas da tabela
                    rows = product_tables[0].find_elements(By.XPATH, ".//tr[not(contains(@class, 'header') or contains(@class, 'head'))]")
                    logger.info(f"Encontradas {len(rows)} linhas na tabela")
                    
                    for row in rows:
                        try:
                            cells = row.find_elements(By.TAG_NAME, "td")
                            if len(cells) >= 3:  # Precisa ter pelo menos 3 células para ser uma linha de produto válida
                                # Tentar extrair dados da linha
                                product_name = cells[0].text.strip()
                                if not product_name or any(ignore_text.lower() in product_name.lower() for ignore_text in ignore_texts):
                                    continue
                                
                                # Inicializar dados básicos
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
                                    "profits": 0.0,
                                    "image_url": ""
                                }
                                
                                # Extrair métricas das células (assumindo uma ordem comum)
                                if len(cells) > 1:
                                    stock_text = cells[1].text.strip()
                                    stock_match = re.search(r'\d+', stock_text)
                                    if stock_match:
                                        product_data["stock"] = int(stock_match.group(0))
                                
                                if len(cells) > 2:
                                    orders_text = cells[2].text.strip()
                                    orders_match = re.search(r'\d+', orders_text)
                                    if orders_match:
                                        product_data["orders_count"] = int(orders_match.group(0))
                                
                                # Adicionar mais extração de células conforme necessário...
                                
                                # Adicionar aos dados coletados
                                products_data.append(product_data)
                                logger.info(f"Extraído produto da tabela: {product_name}")
                        except Exception as row_error:
                            logger.warning(f"Erro ao processar linha da tabela: {str(row_error)}")
                    
                    # Se extraiu produtos da tabela, retorna os dados
                    if products_data:
                        logger.info(f"Extraídos {len(products_data)} produtos da tabela")
                        return products_data
                        
            except Exception as table_error:
                logger.warning(f"Erro ao tentar extrair dados da tabela: {str(table_error)}")
            
            # 2. Tentar extrair quaisquer elementos que contenham dados de produtos
            try:
                logger.info("Tentando abordagem genérica para encontrar elementos de produto...")
                # Buscar quaisquer elementos que contenham textos relacionados a produtos
                product_elements = driver.find_elements(By.XPATH, 
                    "//*[contains(text(), 'Stock:') or contains(text(), 'Ordenes:') or contains(text(), 'Proveedor:')]")
                
                logger.info(f"Encontrados {len(product_elements)} elementos com textos relacionados a produtos")
                
                # Para cada elemento encontrado, buscar o container pai que pode conter todos os dados do produto
                for element in product_elements:
                    try:
                        # Navegar até 4 níveis acima para encontrar o container completo
                        container = element
                        for _ in range(4):
                            if container.tag_name == "body":
                                break
                            container = container.find_element(By.XPATH, "..")
                            
                            # Verificar se este container tem textos suficientes para ser um card de produto
                            container_text = container.text
                            if len(container_text.split('\n')) >= 3 and "Stock:" in container_text:
                                product_cards.append(container)
                                logger.info(f"Encontrado possível container de produto: {container_text[:100]}...")
                                break
                    except Exception as container_error:
                        logger.warning(f"Erro ao buscar container: {str(container_error)}")
                
                # Remover duplicatas novamente
                product_cards = list(set(product_cards))
                logger.info(f"Encontrados {len(product_cards)} possíveis containers de produto")
                
            except Exception as generic_error:
                logger.warning(f"Erro na abordagem genérica: {str(generic_error)}")
        
        # Processar cada card de produto individualmente
        for i, card in enumerate(product_cards):
            try:
                # Verificar se este é realmente um card de produto válido
                card_text = card.text
                
                # Ignorar cards vazios ou inválidos
                if not card_text or len(card_text) < 20:
                    continue
                
                # Para debug
                logger.info(f"Processando card #{i+1}:\n{card_text[:200]}...")
                
                # Tentar extrair a imagem do produto - procurando um elemento img no card ou no elemento anterior
                image_url = ""
                try:
                    # Primeiro tenta encontrar imagem dentro do card atual
                    img_elements = card.find_elements(By.XPATH, ".//img")
                    if img_elements:
                        image_url = img_elements[0].get_attribute("src")
                        logger.info(f"Imagem encontrada no card: {image_url}")
                    else:
                        # Se não encontrar, tenta em elementos próximos
                        try:
                            # Tenta elemento pai que pode conter a imagem
                            # Tenta elemento pai que pode conter a imagem
                            parent = card.find_element(By.XPATH, "..")
                            img_elements = parent.find_elements(By.XPATH, ".//img")
                            if img_elements:
                                image_url = img_elements[0].get_attribute("src")
                                logger.info(f"Imagem encontrada no pai: {image_url}")
                        except:
                            pass
                            
                        # Tenta elemento anterior que pode conter a imagem
                        try:
                            # Usando JavaScript para acessar o elemento irmão anterior
                            sibling_img = driver.execute_script("""
                                var el = arguments[0];
                                var sibling = el.previousElementSibling;
                                return sibling ? sibling.querySelector('img') : null;
                            """, card)
                            
                            if sibling_img:
                                image_url = sibling_img.get_attribute("src")
                                logger.info(f"Imagem encontrada no irmão anterior via JS: {image_url}")
                        except Exception as e:
                            logger.warning(f"Erro ao tentar encontrar imagem no elemento adjacente: {str(e)}")
                except Exception as e:
                    logger.warning(f"Erro ao buscar imagem para o produto: {str(e)}")
                
                # Extrair nome do produto usando diferentes métodos
                product_name = ""
                
                # Método 1: Primeiro texto não vazio no card (comum em muitas interfaces)
                lines = card_text.split('\n')
                if lines:
                    product_name = lines[0].strip()
                
                # Método 2: Se o primeiro método não forneceu um nome adequado, procurar por um cabeçalho
                if not product_name or len(product_name) < 3:
                    try:
                        headers = card.find_elements(By.XPATH, ".//h1 | .//h2 | .//h3 | .//h4 | .//strong")
                        if headers:
                            product_name = headers[0].text.strip()
                    except:
                        pass
                
                # Método 3: Como último recurso, procurar qualquer elemento com classe que sugira um título
                if not product_name or len(product_name) < 3:
                    try:
                        title_elements = card.find_elements(By.XPATH, ".//*[contains(@class, 'title') or contains(@class, 'name') or contains(@class, 'header')]")
                        if title_elements:
                            product_name = title_elements[0].text.strip()
                    except:
                        pass
                
                # Verificar se o nome do produto não é um dos textos a ignorar
                if not product_name or any(ignore_text.lower() in product_name.lower() for ignore_text in ignore_texts):
                    logger.info(f"Ignorando texto '{product_name}' por ser um cabeçalho ou título")
                    continue
                
                # Verificações adicionais para validar que é um produto real
                # 1. Deve ter pelo menos alguns caracteres no nome
                if len(product_name) < 3:
                    logger.info(f"Ignorando '{product_name}': nome muito curto")
                    continue
                    
                # 2. Deve ter pelo menos um dos termos comuns em cards de produto
                product_indicators = ["Stock", "Proveedor", "Ordenes", "Orders", "Pedidos", "Inventory"]
                if not any(indicator in card_text for indicator in product_indicators):
                    logger.info(f"Ignorando '{product_name}': não contém indicadores de produto")
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
                    "profits": 0.0,
                    "image_url": image_url
                }
                
                # Extrair informações específicas utilizando expressões regulares
                # Estas regex são mais robustas para lidar com diferentes formatos
                
                # Extrair o fornecedor (Provider/Proveedor)
                provider_patterns = [
                    r'Proveedor:\s*([^\n]+)',
                    r'Provider:\s*([^\n]+)',
                    r'Fornecedor:\s*([^\n]+)'
                ]
                
                for pattern in provider_patterns:
                    provider_match = re.search(pattern, card_text)
                    if provider_match:
                        product_data["provider"] = provider_match.group(1).strip()
                        break
                
                # Extrair estoque (Stock)
                stock_patterns = [
                    r'Stock:\s*(\d+)',
                    r'Inventory:\s*(\d+)',
                    r'Estoque:\s*(\d+)'
                ]
                
                for pattern in stock_patterns:
                    stock_match = re.search(pattern, card_text)
                    if stock_match:
                        product_data["stock"] = int(stock_match.group(1))
                        break
                
                # Extrair vendidos (Orders/Ordenes/Pedidos)
                orders_patterns = [
                    r'(\d+)\s*[Oo]rdenes',
                    r'(\d+)\s*[Oo]rders',
                    r'(\d+)\s*[Pp]edidos',
                    r'[Oo]rdenes:\s*(\d+)',
                    r'[Oo]rders:\s*(\d+)',
                    r'[Pp]edidos:\s*(\d+)'
                ]
                
                for pattern in orders_patterns:
                    orders_match = re.search(pattern, card_text)
                    if orders_match:
                        product_data["orders_count"] = int(orders_match.group(1))
                        
                        # Buscar o valor das ordens (geralmente próximo do número de ordens)
                        value_patterns = [
                            r'[Oo]rdenes.*?\$\s*([\d.,]+)',
                            r'[Oo]rders.*?\$\s*([\d.,]+)',
                            r'[Pp]edidos.*?\$\s*([\d.,]+)',
                            r'\$\s*([\d.,]+).*?[Oo]rdenes',
                            r'\$\s*([\d.,]+).*?[Oo]rders',
                            r'\$\s*([\d.,]+).*?[Pp]edidos'
                        ]
                        
                        for value_pattern in value_patterns:
                            value_match = re.search(value_pattern, card_text)
                            if value_match:
                                value_str = value_match.group(1).replace('.', '').replace(',', '.')
                                try:
                                    product_data["orders_value"] = float(value_str)
                                except ValueError:
                                    # Tenta novamente considerando separadores diferentes
                                    try:
                                        value_str = value_match.group(1).replace(',', '')
                                        product_data["orders_value"] = float(value_str)
                                    except:
                                        pass
                                break
                        break
                
                # Extrair em trânsito (Transit/Transito)
                transit_patterns = [
                    r'(\d+)\s*[Pp]roductos.*?[Tt]ransit',
                    r'(\d+)\s*[Pp]roducts.*?[Tt]ransit',
                    r'[Tt]ransit.*?(\d+)\s*[Pp]roduct',
                    r'[Ee]n\s*[Tt]ransito\s*(\d+)',
                    r'[Ii]n\s*[Tt]ransit\s*(\d+)'
                ]
                
                for pattern in transit_patterns:
                    transit_match = re.search(pattern, card_text, re.IGNORECASE)
                    if transit_match:
                        product_data["transit_count"] = int(transit_match.group(1))
                        
                        # Buscar o valor em trânsito
                        transit_value_patterns = [
                            r'[Tt]ransit.*?\$\s*([\d.,]+)',
                            r'\$\s*([\d.,]+).*?[Tt]ransit'
                        ]
                        
                        for value_pattern in transit_value_patterns:
                            value_match = re.search(value_pattern, card_text, re.IGNORECASE)
                            if value_match:
                                value_str = value_match.group(1).replace('.', '').replace(',', '.')
                                try:
                                    product_data["transit_value"] = float(value_str)
                                except ValueError:
                                    try:
                                        value_str = value_match.group(1).replace(',', '')
                                        product_data["transit_value"] = float(value_str)
                                    except:
                                        pass
                                break
                        break
                
                # Extrair entregados (Delivered/Entregados)
                delivered_patterns = [
                    r'(\d+)\s*[Pp]roductos.*?[Ee]ntregados',
                    r'(\d+)\s*[Pp]roducts.*?[Dd]elivered',
                    r'[Ee]ntregados.*?(\d+)\s*[Pp]roduct',
                    r'[Dd]elivered.*?(\d+)\s*[Pp]roduct'
                ]
                
                for pattern in delivered_patterns:
                    delivered_match = re.search(pattern, card_text, re.IGNORECASE)
                    if delivered_match:
                        product_data["delivered_count"] = int(delivered_match.group(1))
                        
                        # Buscar o valor entregados
                        delivered_value_patterns = [
                            r'[Ee]ntregados.*?\$\s*([\d.,]+)',
                            r'[Dd]elivered.*?\$\s*([\d.,]+)',
                            r'\$\s*([\d.,]+).*?[Ee]ntregados',
                            r'\$\s*([\d.,]+).*?[Dd]elivered'
                        ]
                        
                        for value_pattern in delivered_value_patterns:
                            value_match = re.search(value_pattern, card_text, re.IGNORECASE)
                            if value_match:
                                value_str = value_match.group(1).replace('.', '').replace(',', '.')
                                try:
                                    product_data["delivered_value"] = float(value_str)
                                except ValueError:
                                    try:
                                        value_str = value_match.group(1).replace(',', '')
                                        product_data["delivered_value"] = float(value_str)
                                    except:
                                        pass
                                break
                        break
                
                # Extrair ganâncias/lucros (Profits/Ganancias)
                profits_patterns = [
                    r'[Gg]anancias.*?\$\s*([\d.,]+)',
                    r'[Pp]rofits.*?\$\s*([\d.,]+)',
                    r'[Ll]ucros.*?\$\s*([\d.,]+)',
                    r'\$\s*([\d.,]+).*?[Gg]anancias',
                    r'\$\s*([\d.,]+).*?[Pp]rofits',
                    r'\$\s*([\d.,]+).*?[Ll]ucros'
                ]
                
                for pattern in profits_patterns:
                    profits_match = re.search(pattern, card_text, re.IGNORECASE)
                    if profits_match:
                        value_str = profits_match.group(1).replace('.', '').replace(',', '.')
                        try:
                            product_data["profits"] = float(value_str)
                        except ValueError:
                            try:
                                value_str = profits_match.group(1).replace(',', '')
                                product_data["profits"] = float(value_str)
                            except:
                                pass
                        break
                
                # Verificação final para garantir que temos dados significativos
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
        
        # FALLBACK: Se não conseguiu extrair produtos com o método usual, tentar abordagem JavaScript
        if not products_data:
            logger.warning("Nenhum produto extraído com métodos convencionais. Tentando abordagem JavaScript...")
            
            try:
                # Script JavaScript para extrair dados de produtos de forma genérica
                js_result = driver.execute_script("""
                    var results = [];
                    
                    // Função para extrair número de um texto
                    function extractNumber(text) {
                        var match = text.match(/\\d+/);
                        return match ? parseInt(match[0]) : 0;
                    }
                    
                    // Função para extrair valor monetário
                    function extractMoneyValue(text) {
                        var match = text.match(/\\$\\s*([\\d.,]+)/);
                        if (!match) return 0;
                        
                        var valueStr = match[1].replace(/\\./g, '').replace(',', '.');
                        return parseFloat(valueStr) || 0;
                    }
                    
                    // Encontrar todos os possíveis containers de produtos
                    var containers = document.querySelectorAll('div.card, div.product-card, div.item, div.p-card, mat-card, div.product-item');
                    
                    // Se não encontrou containers específicos, procurar divs que parecem containers
                    if (containers.length === 0) {
                        var allDivs = document.querySelectorAll('div');
                        containers = Array.from(allDivs).filter(function(div) {
                            var text = div.textContent || '';
                            return (
                                (text.indexOf('Stock:') >= 0 || text.indexOf('Inventory:') >= 0) && 
                                (text.indexOf('Ordenes') >= 0 || text.indexOf('Orders') >= 0 || text.indexOf('Pedidos') >= 0) &&
                                div.children.length >= 2
                            );
                        });
                    }
                    
                    console.log('Encontrados ' + containers.length + ' possíveis containers de produto');
                    
                    // Processar cada container
                    containers.forEach(function(container) {
                        try {
                            var text = container.textContent || '';
                            var lines = text.split('\\n').filter(function(line) { return line.trim().length > 0; });
                            
                            // Verificar se parece ser um card de produto válido
                            if (lines.length < 3) return;
                            if (text.indexOf('Stock:') < 0 && text.indexOf('Inventory:') < 0) return;
                            
                            // Tentar extrair nome do produto (normalmente o primeiro texto significativo)
                            var productName = lines[0].trim();
                            if (productName.length < 3) return;
                            
                            // Ignorar se for um cabeçalho/título conhecido
                            var ignoreTexts = ['Informe de productos', 'Reporte de productos', 'Productos vendidos', 
                                              'Productos en transito', 'Resumen', 'Total', 'Filtrar', 'Pedidos'];
                            
                            if (ignoreTexts.some(function(ignore) { return productName.toLowerCase().indexOf(ignore.toLowerCase()) >= 0; })) {
                                return;
                            }
                            
                            // Encontrar imagem se existir
                            var imageUrl = '';
                            var img = container.querySelector('img');
                            if (img) {
                                imageUrl = img.src;
                            } else {
                                // Tentar encontrar em elementos próximos
                                if (container.previousElementSibling) {
                                    img = container.previousElementSibling.querySelector('img');
                                    if (img) imageUrl = img.src;
                                }
                            }
                            
                            // Criar objeto para armazenar dados do produto
                            var productData = {
                                product: productName,
                                provider: '',
                                stock: 0,
                                orders_count: 0,
                                orders_value: 0,
                                transit_count: 0,
                                transit_value: 0,
                                delivered_count: 0,
                                delivered_value: 0,
                                profits: 0,
                                image_url: imageUrl
                            };
                            
                            // Extrair provider/fornecedor
                            if (text.indexOf('Proveedor:') >= 0) {
                                var match = text.match(/Proveedor:\\s*([^\\n]+)/);
                                if (match) productData.provider = match[1].trim();
                            } else if (text.indexOf('Provider:') >= 0) {
                                var match = text.match(/Provider:\\s*([^\\n]+)/);
                                if (match) productData.provider = match[1].trim();
                            }
                            
                            // Extrair stock
                            if (text.indexOf('Stock:') >= 0) {
                                var match = text.match(/Stock:\\s*(\\d+)/);
                                if (match) productData.stock = parseInt(match[1]);
                            } else if (text.indexOf('Inventory:') >= 0) {
                                var match = text.match(/Inventory:\\s*(\\d+)/);
                                if (match) productData.stock = parseInt(match[1]);
                            }
                            
                            // Extrair orders/pedidos
                            var ordersFound = false;
                            var ordersPatterns = [/(\\d+)\\s*[Oo]rdenes/, /[Oo]rdenes:\\s*(\\d+)/, 
                                                 /(\\d+)\\s*[Oo]rders/, /[Oo]rders:\\s*(\\d+)/,
                                                 /(\\d+)\\s*[Pp]edidos/, /[Pp]edidos:\\s*(\\d+)/];
                            
                            for (var i = 0; i < ordersPatterns.length; i++) {
                                var match = text.match(ordersPatterns[i]);
                                if (match) {
                                    productData.orders_count = parseInt(match[1]);
                                    ordersFound = true;
                                    break;
                                }
                            }
                            
                            // Extrair valor dos pedidos
                            if (ordersFound) {
                                // Procurar por valores monetários próximos às menções de pedidos
                                var valueLines = lines.filter(function(line) {
                                    return line.indexOf('$') >= 0 && 
                                          (line.indexOf('Ordenes') >= 0 || line.indexOf('Orders') >= 0 || 
                                           line.indexOf('Pedidos') >= 0);
                                });
                                
                                if (valueLines.length > 0) {
                                    productData.orders_value = extractMoneyValue(valueLines[0]);
                                }
                            }
                            
                            // Extrair transit/transito
                            var transitFound = false;
                            var transitPatterns = [/(\\d+)\\s*[Pp]roductos.*?[Tt]ransit/, /[Tt]ransit.*?(\\d+)\\s*[Pp]roduct/,
                                                  /[Ee]n\\s*[Tt]ransito\\s*(\\d+)/, /[Ii]n\\s*[Tt]ransit\\s*(\\d+)/];
                            
                            for (var i = 0; i < transitPatterns.length; i++) {
                                var match = text.match(transitPatterns[i]);
                                if (match) {
                                    productData.transit_count = parseInt(match[1]);
                                    transitFound = true;
                                    break;
                                }
                            }
                            
                            // Extrair valor em trânsito
                            if (transitFound) {
                                var valueLines = lines.filter(function(line) {
                                    return line.indexOf('$') >= 0 && 
                                          (line.indexOf('Transit') >= 0 || line.indexOf('Transito') >= 0);
                                });
                                
                                if (valueLines.length > 0) {
                                    productData.transit_value = extractMoneyValue(valueLines[0]);
                                }
                            }
                            
                            // Extrair delivered/entregados
                            var deliveredFound = false;
                            var deliveredPatterns = [/(\\d+)\\s*[Pp]roductos.*?[Ee]ntregados/, /[Ee]ntregados.*?(\\d+)\\s*[Pp]roduct/,
                                                    /(\\d+)\\s*[Pp]roducts.*?[Dd]elivered/, /[Dd]elivered.*?(\\d+)\\s*[Pp]roduct/];
                            
                            for (var i = 0; i < deliveredPatterns.length; i++) {
                                var match = text.match(deliveredPatterns[i]);
                                if (match) {
                                    productData.delivered_count = parseInt(match[1]);
                                    deliveredFound = true;
                                    break;
                                }
                            }
                            
                            // Extrair valor entregue
                            if (deliveredFound) {
                                var valueLines = lines.filter(function(line) {
                                    return line.indexOf('$') >= 0 && 
                                          (line.indexOf('Entregados') >= 0 || line.indexOf('Delivered') >= 0);
                                });
                                
                                if (valueLines.length > 0) {
                                    productData.delivered_value = extractMoneyValue(valueLines[0]);
                                }
                            }
                            
                            // Extrair profits/ganancias/lucros
                            var profitsPatterns = [/[Gg]anancias.*?\\$\\s*([\\d.,]+)/, /[Pp]rofits.*?\\$\\s*([\\d.,]+)/,
                                                  /[Ll]ucros.*?\\$\\s*([\\d.,]+)/, /\\$\\s*([\\d.,]+).*?[Gg]anancias/,
                                                  /\\$\\s*([\\d.,]+).*?[Pp]rofits/, /\\$\\s*([\\d.,]+).*?[Ll]ucros/];
                            
                            for (var i = 0; i < profitsPatterns.length; i++) {
                                var match = text.match(profitsPatterns[i]);
                                if (match) {
                                    var valueStr = match[1].replace(/\\./g, '').replace(',', '.');
                                    productData.profits = parseFloat(valueStr) || 0;
                                    break;
                                }
                            }
                            
                            // Verificar se tem dados significativos
                            var hasData = productData.stock > 0 || 
                                          productData.orders_count > 0 || 
                                          productData.transit_count > 0 || 
                                          productData.delivered_count > 0 || 
                                          productData.profits > 0;
                            
                            if (hasData) {
                                results.push(productData);
                                console.log('Produto extraído via JS: ' + productData.product);
                            }
                            
                        } catch (e) {
                            console.error('Erro ao processar container:', e);
                        }
                    });
                    
                    return results;
                """)
                
                if js_result and len(js_result) > 0:
                    logger.info(f"Extraídos {len(js_result)} produtos via JavaScript")
                    products_data = js_result
            except Exception as js_error:
                logger.error(f"Erro na extração via JavaScript: {str(js_error)}")
        
        logger.info(f"Total de {len(products_data)} produtos extraídos com sucesso")
        return products_data
        
    except Exception as e:
        logger.error(f"Erro geral ao extrair dados dos produtos: {str(e)}")
        return []

def save_dropi_metrics_to_db(store_id, date_str, products_data, start_date_str=None, end_date_str=None):
    """Save Dropi product metrics to the database with date interval support."""
    # Se as datas de início e fim não foram fornecidas, use a data de referência para ambas
    if not start_date_str:
        start_date_str = date_str
    if not end_date_str:
        end_date_str = date_str
    
    # PRIMEIRA ETAPA: Verificar e adicionar a coluna image_url se necessário
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Verificar se a coluna image_url existe
        column_exists = False
        try:
            # Tente fazer um select para ver se a coluna existe
            cursor.execute("SELECT image_url FROM dropi_metrics LIMIT 1")
            column_exists = True
        except:
            logger.info("Coluna image_url não encontrada. Tentando adicionar...")
            
        if not column_exists:
            try:
                # Adicionar a coluna
                if is_railway_environment():
                    cursor.execute("ALTER TABLE dropi_metrics ADD COLUMN image_url TEXT")
                else:
                    cursor.execute("ALTER TABLE dropi_metrics ADD COLUMN image_url TEXT")
                conn.commit()
                logger.info("Coluna image_url adicionada com sucesso")
            except Exception as e:
                logger.error(f"Erro ao adicionar coluna: {str(e)}")
                conn.rollback()
        
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Erro ao verificar/adicionar coluna: {str(e)}")
        if conn:
            try:
                conn.rollback()
                conn.close()
            except:
                pass
    
    # SEGUNDA ETAPA: Remover dados existentes para o período
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Delete existing records for the date range
        if is_railway_environment():
            cursor.execute("""
                DELETE FROM dropi_metrics 
                WHERE store_id = %s AND date = %s
            """, (store_id, date_str))
            
            if start_date_str and end_date_str:
                cursor.execute("""
                    DELETE FROM dropi_metrics 
                    WHERE store_id = %s AND date_start = %s AND date_end = %s
                """, (store_id, start_date_str, end_date_str))
        else:
            cursor.execute("""
                DELETE FROM dropi_metrics 
                WHERE store_id = ? AND date = ?
            """, (store_id, date_str))
            
            if start_date_str and end_date_str:
                cursor.execute("""
                    DELETE FROM dropi_metrics 
                    WHERE store_id = ? AND date_start = ? AND date_end = ?
                """, (store_id, start_date_str, end_date_str))
        
        conn.commit()
        logger.info(f"Dados antigos removidos para o período {start_date_str} a {end_date_str}")
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Erro ao remover dados existentes: {str(e)}")
        if conn:
            try:
                conn.rollback()
                conn.close()
            except:
                pass
    
    # TERCEIRA ETAPA: Agrupar produtos duplicados
    product_names = {}
    for product in products_data:
        product_name = product.get("product", "")
        if not product_name:
            continue
            
        if product_name in product_names:
            logger.warning(f"Produto duplicado encontrado: {product_name}")
            # Somar os valores para dados duplicados
            for key in ["orders_count", "orders_value", "transit_count", "transit_value", 
                       "delivered_count", "delivered_value", "profits"]:
                product_names[product_name][key] += product.get(key, 0)
            # Para campos que não são numéricos, manter o valor não vazio
            if not product_names[product_name].get("image_url") and product.get("image_url"):
                product_names[product_name]["image_url"] = product.get("image_url")
            if not product_names[product_name].get("provider") and product.get("provider"):
                product_names[product_name]["provider"] = product.get("provider")
        else:
            product_names[product_name] = product
    
    # QUARTA ETAPA: Inserir os dados produto por produto
    saved_count = 0
    for product_name, product in product_names.items():
        try:
            # Use raw SQL para maior controle e debug
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Insert with all fields explicitly defined
            if is_railway_environment():
                cursor.execute("""
                    INSERT INTO dropi_metrics (
                        store_id, date, date_start, date_end, product, provider, stock,
                        orders_count, orders_value, transit_count, transit_value,
                        delivered_count, delivered_value, profits, image_url
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """, (
                    store_id, date_str, start_date_str, end_date_str, 
                    product_name, product.get("provider", ""), product.get("stock", 0),
                    product.get("orders_count", 0), product.get("orders_value", 0),
                    product.get("transit_count", 0), product.get("transit_value", 0),
                    product.get("delivered_count", 0), product.get("delivered_value", 0),
                    product.get("profits", 0), product.get("image_url", "")
                ))
            else:
                cursor.execute("""
                    INSERT INTO dropi_metrics (
                        store_id, date, date_start, date_end, product, provider, stock,
                        orders_count, orders_value, transit_count, transit_value,
                        delivered_count, delivered_value, profits, image_url
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    store_id, date_str, start_date_str, end_date_str, 
                    product_name, product.get("provider", ""), product.get("stock", 0),
                    product.get("orders_count", 0), product.get("orders_value", 0),
                    product.get("transit_count", 0), product.get("transit_value", 0),
                    product.get("delivered_count", 0), product.get("delivered_value", 0),
                    product.get("profits", 0), product.get("image_url", "")
                ))
            
            # Log the values for debugging
            logger.info(f"Salvando produto: {product_name}")
            logger.info(f"  Image URL: {product.get('image_url', '')}")
            
            conn.commit()
            saved_count += 1
            cursor.close()
            conn.close()
        except Exception as e:
            logger.error(f"Erro ao salvar produto {product_name}: {str(e)}")
            if conn:
                try:
                    conn.rollback()
                    conn.close()
                except:
                    pass
    
    logger.info(f"Total de {saved_count} produtos salvos com sucesso de {len(product_names)}")
    return saved_count > 0

def update_dropi_data_silent(store, start_date, end_date):
    """Atualiza os dados da Dropi sem exibir feedback de progresso, com manejo de erros melhorado."""
    # Configurar o driver Selenium com timeout ampliado
    driver = setup_selenium(headless=True)
    
    if not driver:
        logger.error("Falha ao inicializar o driver Selenium")
        return False
    
    try:
        # Definir timeout global mais longo para operações que envolvem espera explícita
        driver.implicitly_wait(15)  # Aumentado de 10 para 15 segundos
        
        # Converter datas para strings
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        
        # Data de referência é a mesma da final (mantido para compatibilidade)
        date_str = end_date_str
        
        logger.info(f"Buscando dados Dropi para o período: {start_date_str} a {end_date_str}")
        
        # Verificar e ajustar URLs - garantir que tenha o protocolo e a barra final
        dropi_url = store["dropi_url"]
        if not dropi_url:
            dropi_url = "https://app.dropi.mx/"
        if not dropi_url.startswith(("http://", "https://")):
            dropi_url = "https://" + dropi_url
        if not dropi_url.endswith("/"):
            dropi_url += "/"
            
        logger.info(f"URL da Dropi ajustada: {dropi_url}")
        
        # Verificar e garantir que temos credenciais
        if not store["dropi_username"] or not store["dropi_password"]:
            logger.error("Credenciais da Dropi ausentes ou inválidas")
            driver.quit()
            return False
        
        # 1. FAZER LOGIN NO DROPI
        login_attempts = 0
        login_success = False
        max_login_attempts = 3
        
        while login_attempts < max_login_attempts and not login_success:
            try:
                login_success = login(driver, store["dropi_username"], store["dropi_password"], logger, dropi_url)
                if login_success:
                    logger.info("Login no Dropi realizado com sucesso")
                    break
                else:
                    login_attempts += 1
                    logger.warning(f"Tentativa de login {login_attempts} falhou. Tentando novamente...")
                    time.sleep(3)  # Pausa entre tentativas
            except Exception as login_error:
                login_attempts += 1
                logger.error(f"Erro na tentativa de login {login_attempts}: {str(login_error)}")
                time.sleep(3)  # Pausa entre tentativas
        
        if not login_success:
            logger.error("Todas as tentativas de login falharam")
            driver.quit()
            return False
        
        # 2. NAVEGAR PARA O RELATÓRIO DE PRODUTOS - com retentativas
        navigation_attempts = 0
        navigation_success = False
        max_navigation_attempts = 3
        
        while navigation_attempts < max_navigation_attempts and not navigation_success:
            try:
                navigation_success = navigate_to_product_sold(driver, logger)
                if navigation_success:
                    logger.info("Navegação para relatório de produtos concluída com sucesso")
                    break
                else:
                    navigation_attempts += 1
                    logger.warning(f"Tentativa de navegação {navigation_attempts} falhou. Tentando novamente...")
                    time.sleep(3)  # Pausa entre tentativas
                    
                    # Tentar voltar para a página inicial e navegar novamente
                    try:
                        driver.get(dropi_url)
                        time.sleep(5)
                    except:
                        pass
            except Exception as navigation_error:
                navigation_attempts += 1
                logger.error(f"Erro na tentativa de navegação {navigation_attempts}: {str(navigation_error)}")
                time.sleep(3)
        
        if not navigation_success:
            logger.error("Todas as tentativas de navegação falharam")
            driver.quit()
            return False
        
        # 3. SELECIONAR INTERVALO DE DATAS - com retentativas
        date_selection_attempts = 0
        date_selection_success = False
        max_date_selection_attempts = 3
        
        while date_selection_attempts < max_date_selection_attempts and not date_selection_success:
            try:
                date_selection_success = select_date_range(driver, start_date, end_date, logger)
                if date_selection_success:
                    logger.info("Seleção de intervalo de datas concluída com sucesso")
                    break
                else:
                    date_selection_attempts += 1
                    logger.warning(f"Tentativa de seleção de datas {date_selection_attempts} falhou. Tentando novamente...")
                    time.sleep(3)
            except Exception as date_error:
                date_selection_attempts += 1
                logger.error(f"Erro na tentativa de seleção de datas {date_selection_attempts}: {str(date_error)}")
                time.sleep(3)
        
        if not date_selection_success:
            logger.error("Todas as tentativas de seleção de datas falharam")
            driver.quit()
            return False
        
        # 4. EXTRAIR DADOS DOS PRODUTOS - com verificação de dados vazios
        try:
            # Aguardar carregamento completo da página após seleção de datas
            time.sleep(10)
            
            # Verificar se há elementos de carregamento e aguardar até que desapareçam
            try:
                loading_elements = driver.find_elements(By.XPATH, "//*[contains(@class, 'loading') or contains(@class, 'spinner')]")
                if loading_elements:
                    logger.info("Detectado indicador de carregamento. Aguardando...")
                    WebDriverWait(driver, 30).until(
                        EC.invisibility_of_element_located((By.XPATH, "//*[contains(@class, 'loading') or contains(@class, 'spinner')]"))
                    )
                    logger.info("Carregamento concluído.")
            except Exception as loading_error:
                logger.warning(f"Erro ao verificar/aguardar carregamento: {str(loading_error)}")
            
            # Capturar screenshot antes da extração
            driver.save_screenshot(f"before_extraction_{start_date_str}_{end_date_str}.png")
            
            # Extrair dados
            product_data = extract_product_data(driver, logger)
            
            # Verificar se obteve dados
            if not product_data or len(product_data) == 0:
                logger.warning("Nenhum produto extraído! Tentando abordagem alternativa...")
                
                # Tentar novamente com mais tempo de espera
                time.sleep(10)
                driver.save_screenshot(f"retry_extraction_{start_date_str}_{end_date_str}.png")
                
                # Segunda tentativa de extração
                product_data = extract_product_data(driver, logger)
                
                if not product_data or len(product_data) == 0:
                    logger.error("Todas as tentativas de extração falharam - nenhum produto encontrado")
                    driver.quit()
                    return False
            
            # 5. SALVAR OS DADOS NO BANCO
            logger.info(f"Extraídos {len(product_data)} produtos. Salvando no banco de dados...")
            
            save_success = save_dropi_metrics_to_db(store["id"], date_str, product_data, start_date_str, end_date_str)
            
            if save_success:
                logger.info("Dados salvos com sucesso no banco de dados")
            else:
                logger.error("Falha ao salvar dados no banco de dados")
                driver.quit()
                return False
            
            # 6. VERIFICAÇÃO APÓS SALVAR
            try:
                from db_utils import execute_query
                result = execute_query(
                    """
                    SELECT COUNT(*) FROM dropi_metrics 
                    WHERE store_id = ? 
                      AND date_start = ? 
                      AND date_end = ?
                    """, 
                    (store["id"], start_date_str, end_date_str),
                    fetch_type='one'
                )
                count = result[0] if result else 0
                logger.info(f"Verificação final: {count} produtos salvos no banco para o período {start_date_str} a {end_date_str}")
                
                # Confirmação de sucesso se salvou pelo menos um produto
                success = count > 0
            except Exception as verify_error:
                logger.error(f"Erro na verificação de contagem: {str(verify_error)}")
                # Considera sucesso se chegou até aqui sem erros graves
                success = True
            
            driver.quit()
            return success
                
        except Exception as extraction_error:
            logger.error(f"Erro durante extração/salvamento de dados: {str(extraction_error)}")
            driver.quit()
            return False
        
    except Exception as general_error:
        logger.error(f"Erro geral ao atualizar dados da Dropi: {str(general_error)}")
        try:
            driver.quit()
        except:
            pass
        return False

# === FUNÇÕES PARA O LAYOUT MELHORADO ===

def display_sidebar_filters(store):
    """Exibe os filtros na barra lateral para a loja selecionada."""
    st.sidebar.markdown(f"## Filtros para {store['name']}")
    
    # Seção Shopify
    with st.sidebar.expander("Filtros Shopify", expanded=True):
        st.sidebar.markdown("### Shopify")
        # Filtro de Data
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
        st.sidebar.markdown("#### Plataforma de Anúncio")
        url_categories = get_url_categories(store["id"], start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
        if url_categories:
            category_options = ["Todos"] + url_categories
            selected_category = st.sidebar.selectbox("", category_options)
        else:
            selected_category = "Todos"
        
        # Botão para atualizar dados
        update_shopify = st.sidebar.button("Atualizar Dados Shopify", use_container_width=True)
    
    # Seção Dropi
    with st.sidebar.expander("Filtros Dropi", expanded=True):
        # Substituir as opções de período por seleção de data
        st.sidebar.markdown("### Dropi")
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
        update_dropi = st.sidebar.button("Atualizar Dados Dropi", use_container_width=True)
    
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

def display_shopify_data(data, selected_category):
    """Exibe os dados da Shopify em tabelas colapsáveis com produto, imagem, total de pedidos, valor total e URL."""
    
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
            # Como URLs e imagens podem ser diferentes para mesmo produto, pegamos a primeira URL e imagem para cada produto
            url_mapping = {}
            image_mapping = {}
            for _, row in filtered_data.iterrows():
                product = row['product']
                url = row.get('product_url', '')
                image = row.get('product_image_url', '')
                
                if product not in url_mapping and url:
                    url_mapping[product] = url
                    
                if product not in image_mapping and image:
                    image_mapping[product] = image
            
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
            
            # Adicionar coluna de URL e imagem
            product_data['url'] = product_data['product'].map(url_mapping)
            product_data['image'] = product_data['product'].map(image_mapping)
            
            # Exibir tabela com dados agrupados
            with st.expander("Tabela de Produtos Shopify", expanded=True):
                # Selecionar apenas as colunas que queremos exibir (removendo total_value que é redundante)
                display_df = product_data[['image', 'product', 'total_orders', 'valor_formatado', 'url']]
                
                st.dataframe(
                    display_df,
                    column_config={
                        "image": st.column_config.ImageColumn("Imagem", help="Imagem do produto"),
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
    #else:
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

def display_dropi_data(store_id, start_date_str, end_date_str):
    """Exibe os dados da Dropi em tabelas colapsáveis e gráficos para um intervalo específico de datas."""
    conn = get_db_connection()
    
    # Consulta com filtro exato por intervalo de datas
    query = """
        SELECT * FROM dropi_metrics 
        WHERE store_id = ? 
          AND date_start = ? 
          AND date_end = ?
    """
    
    if is_railway_environment():
        query = query.replace("?", "%s")
    
    cursor = conn.cursor()
    cursor.execute(query, (store_id, start_date_str, end_date_str))
    
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
    
    logger.info(f"Encontrados {len(data_df)} registros Dropi para o período {start_date_str} a {end_date_str}")
    
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
        if start_date_str == end_date_str:
            period_text = f"Mostrando {len(data_df)} produtos para a data: {start_date_str} (Valores em {currency_to})"
        else:
            period_text = f"Mostrando {len(data_df)} produtos para o período: {start_date_str} a {end_date_str} (Valores em {currency_to})"
        
        with st.expander("Produtos Dropi", expanded=True):
            st.info(period_text)
            
            # Primeiro, criar uma cópia do DataFrame sem a coluna 'date'
            display_df = data_df.drop(columns=['date'], errors='ignore')
            
            # Reorganizar colunas para mostrar imagem primeiro se existir
            if 'image_url' in display_df.columns:
                cols = display_df.columns.tolist()
                cols.remove('image_url')
                cols = ['image_url'] + cols
                display_df = display_df[cols]
            
            st.dataframe(
                display_df,
                column_config={
                    "store_id": None,  # Ocultar esta coluna
                    "image_url": st.column_config.ImageColumn("Imagem", help="Imagem do produto"),
                    "date_start": st.column_config.TextColumn("Data Inicial"),
                    "date_end": st.column_config.TextColumn("Data Final"),
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
        if start_date_str == end_date_str:
            st.info(f"Não há dados disponíveis da Dropi para a data {start_date_str}.")
        else:
            st.info(f"Não há dados disponíveis da Dropi para o período {start_date_str} a {end_date_str}.")
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
    """Exibe gráfico interativo para os dados Dropi com barras empilhadas por produto."""
    if not data.empty:
        # Preparar dados para gráfico
        chart_data = data[['product', 'orders_count', 'transit_count', 'delivered_count']].copy()
        
        # Ordenar por número de pedidos para melhor visualização
        chart_data = chart_data.sort_values('orders_count', ascending=False)
        
        with st.expander("Gráfico de Produtos Dropi", expanded=True):
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

def display_effectiveness_table(store_id, start_date_str, end_date_str):
    """Display a table with effectiveness metrics based on Dropi data for a specific date range."""    
    # Debug info
    logger.info(f"Buscando dados de efetividade para store_id={store_id}, período: {start_date_str} a {end_date_str}")
    
    conn = get_db_connection()
    
    # Get Dropi data for the specific date range including image URLs
    dropi_query = """
        SELECT product, SUM(orders_count) as orders_count, SUM(delivered_count) as delivered_count, 
               MAX(image_url) as image_url
        FROM dropi_metrics 
        WHERE store_id = ? AND date_start = ? AND date_end = ?
        GROUP BY product
    """
    
    if is_railway_environment():
        dropi_query = dropi_query.replace("?", "%s")
    
    cursor = conn.cursor()
    cursor.execute(dropi_query, (store_id, start_date_str, end_date_str))
    
    # Converter resultados para DataFrame
    columns = ["product", "orders_count", "delivered_count", "image_url"]
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
    
    # Estilo para tabelas com fundo branco, incluindo cabeçalhos
    st.markdown("""
    <style>
    /* Tabelas e componentes relacionados com fundo branco */
    .stDataFrame, .stDataEditor, .stTable, 
    .stDataFrame table, .stDataEditor table, .stTable table {
        background-color: white !important;
    }
    
    /* Cabeçalhos com fundo branco */
    .stDataFrame thead tr, .stDataEditor thead tr, .stTable thead tr,
    .stDataFrame th, .stDataEditor th, .stTable th {
        background-color: white !important;
        color: black !important;
    }
    
    /* Remover cores alternadas de linhas */
    .stDataFrame tbody tr, .stDataEditor tbody tr, .stTable tbody tr {
        background-color: white !important;
    }
    
    /* Remover cores de fundo padrão de células */
    .stDataFrame td, .stDataEditor td, .stTable td {
        background-color: white !important;
    }
    </style>
    """, unsafe_allow_html=True)
    
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
        
        # Informar data dos dados
        if start_date_str == end_date_str:
            st.info(f"Mostrando {len(dropi_data)} produtos para a data: {start_date_str}")
        else:
            st.info(f"Mostrando {len(dropi_data)} produtos para o período: {start_date_str} a {end_date_str}")
        
        # Função para aplicar cores de fundo por linha
        def get_row_color(effectiveness):
            if effectiveness <= 40.0:
                return '#ffcccc'  # Vermelho claro
            elif effectiveness < 60.0:
                return '#ffff99'  # Amarelo claro
            else:
                return '#ccffcc'  # Verde claro
        
        # Adicionar coluna com a cor para cada linha baseada na efetividade
        dropi_data['_row_color'] = dropi_data['effectiveness'].apply(get_row_color)
        
        # Função para aplicar a formatação condicional em todas as células da linha
        def highlight_rows(row):
            color = row['_row_color']
            return [f'background-color: {color}'] * len(row)
        
        # Preparar DataFrame para visualização
        view_columns = ['image_url', 'product', 'orders_count', 'delivered_count', 
                      'effectiveness', 'general_effectiveness', '_row_color']
        view_df = dropi_data[view_columns].copy()
        
        # Aplicar estilo com cores
        styled_view = view_df.style.apply(highlight_rows, axis=1)
        
        # Exibir tabela estilizada completa (não editável)
        st.dataframe(
            styled_view,
            column_config={
                "image_url": st.column_config.ImageColumn("Imagem", help="Imagem do produto"),
                "product": "Produto",
                "orders_count": st.column_config.NumberColumn("Pedidos"),
                "delivered_count": st.column_config.NumberColumn("Entregues"),
                "effectiveness": st.column_config.NumberColumn(
                    "Efetividade (%)",
                    format="%.1f%%",
                    help="Calculado como (Entregues / Pedidos) * 100"
                ),
                "general_effectiveness": st.column_config.NumberColumn(
                    "Efetividade Geral (%)",
                    format="%.1f%%",
                    help="Valor definido manualmente"
                ),
                "_row_color": None  # Ocultar coluna de cor
            },
            hide_index=True,
            use_container_width=True
        )
        
        # Seção para edição da Efetividade Geral - Minimizada por padrão
        with st.expander("Editar Efetividade Geral", expanded=False):
            # Preparar DataFrame para edição
            edit_columns = ['product', 'general_effectiveness', '_row_color']
            edit_df = dropi_data[edit_columns].copy()
            
            # Aplicar o mesmo estilo de cores à tabela editável
            styled_edit = edit_df.style.apply(highlight_rows, axis=1)
            
            # Exibir tabela editável com cores por efetividade
            edited_df = st.data_editor(
                edit_df,
                column_config={
                    "product": "Produto",
                    "general_effectiveness": st.column_config.NumberColumn(
                        "Efetividade Geral (%)",
                        format="%.1f%%",
                        help="Valor definido manualmente"
                    ),
                    "_row_color": None  # Ocultar coluna de cor
                },
                disabled=["product"],
                hide_index=True,
                use_container_width=True,
                key="edit_effectiveness_table"
            )
        
        # Check if any general_effectiveness values have been edited
        if not edited_df.equals(edit_df):
            # Find rows where general_effectiveness has changed
            for idx, row in edited_df.iterrows():
                product = row['product']
                new_value = row['general_effectiveness']
                
                # Get the old value (if exists)
                try:
                    old_value = edit_df.loc[edit_df['product'] == product, 'general_effectiveness'].iloc[0]
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
        # MODIFICAÇÃO: Removida a mensagem de aviso que estava duplicada
        pass  # Não exibe nenhuma mensagem quando não há dados
        
# Inicializar banco de dados
init_db()

# Atualizar esquema da tabela dropi_metrics
try:
    from db_utils import update_dropi_metrics_schema
    update_dropi_metrics_schema()
except Exception as e:
    st.error(f"Erro ao atualizar esquema do banco: {str(e)}")

# Usar a loja já selecionada pela barra lateral principal
selected_store = st.session_state.get("selected_store")

# Dashboard principal quando uma loja é selecionada
if selected_store:
    # Mostrar o dashboard para a loja selecionada
    store_dashboard(selected_store)
else:
    # Tela inicial quando nenhuma loja está selecionada
    st.write("Selecione uma loja no menu lateral ou cadastre uma nova para começar.")