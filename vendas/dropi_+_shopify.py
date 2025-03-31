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
        save_effectiveness, is_railway_environment, update_dropi_metrics_schema_for_duplicates
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
    
    # Verificar e adicionar campo is_custom para lojas
    try:
        c.execute("SELECT is_custom FROM stores LIMIT 1")
    except sqlite3.OperationalError:
        st.info("Atualizando estrutura do banco de dados para personalização...")
        c.execute("ALTER TABLE stores ADD COLUMN is_custom INTEGER DEFAULT 0")
        st.success("Banco de dados atualizado com sucesso para personalização!")
    
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
    
    # Criar tabela para dados personalizados
    c.execute("""
        CREATE TABLE IF NOT EXISTS custom_product_data (
            store_id TEXT,
            product TEXT,
            custom_id TEXT,
            custom_provider TEXT,
            last_updated TEXT,
            PRIMARY KEY (store_id, product)
        )
    """)
    
    # Confirmar todas as alterações e fechar a conexão
    conn.commit()
    conn.close()

# Funções para manipular dados personalizados
def get_custom_product_data(store_id):
    """Obtém os dados personalizados dos produtos de uma loja."""
    try:
        query = "SELECT product, custom_id, custom_provider FROM custom_product_data WHERE store_id = ?"
        
        if is_railway_environment():
            query = query.replace("?", "%s")
            
        result = execute_query(query, (store_id,), fetch_type='all')
        
        custom_data = {}
        if result:
            for row in result:
                product = row[0]
                custom_data[product] = {
                    "custom_id": row[1],
                    "custom_provider": row[2]
                }
        
        return custom_data
    except Exception as e:
        logger.error(f"Erro ao obter dados personalizados: {str(e)}")
        return {}

def save_custom_product_data(store_id, product, custom_id, custom_provider):
    """Salva dados personalizados para um produto."""
    try:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        data = {
            "store_id": store_id,
            "product": product,
            "custom_id": custom_id,
            "custom_provider": custom_provider,
            "last_updated": current_time
        }
        
        execute_upsert("custom_product_data", data, ["store_id", "product"])
        return True
    except Exception as e:
        logger.error(f"Erro ao salvar dados personalizados: {str(e)}")
        return False

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

def select_date_range(driver, start_date, end_date, logger):
    """Select a specific date range in the Product Sold report with enhanced support for recent dates."""
    try:
        # Formatação das datas para exibição no formato esperado pelo Dropi (DD/MM/YYYY)
        start_date_formatted = start_date.strftime("%d/%m/%Y")
        end_date_formatted = end_date.strftime("%d/%m/%Y")
        
        logger.info(f"Tentando selecionar intervalo de datas: {start_date_formatted} a {end_date_formatted}")
        
        # Capturar screenshot para diagnóstico
        driver.save_screenshot("before_date_select.png")
        
        # Função para verificar se o calendário está aberto
        def is_calendar_open():
            try:
                calendar_elements = driver.find_elements(By.XPATH, 
                    "//div[contains(@class, 'p-datepicker') or contains(@class, 'daterangepicker') or contains(@class, 'calendar')]")
                return len(calendar_elements) > 0
            except:
                return False
        
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
            # Adicionar seletores mais genéricos para encontrar qualquer elemento de data
            "//input[contains(@placeholder, 'fecha') or contains(@placeholder, 'date')]",
            "//div[contains(@class, 'date')]",
            "//div[contains(@class, 'calendar')]"
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
                    
                    # Verificar se o calendário apareceu
                    time.sleep(2)
                    if is_calendar_open():
                        logger.info("Calendário aberto com sucesso")
                        clicked = True
                        break
                    else:
                        logger.warning("Calendário não apareceu após o clique. Tentando outra abordagem.")
                except Exception as e:
                    logger.warning(f"Clique direto no seletor {selector} falhou: {str(e)}")
                    
                # Tentar JavaScript click como alternativa
                element = driver.find_element(By.XPATH, selector)
                driver.execute_script("arguments[0].click();", element)
                logger.info(f"Clicou via JavaScript no seletor de data: {selector}")
                
                # Verificar se o calendário apareceu
                time.sleep(2)
                if is_calendar_open():
                    logger.info("Calendário aberto com sucesso via JavaScript")
                    clicked = True
                    break
                else:
                    logger.warning("Calendário não apareceu após o clique via JavaScript. Tentando outro seletor.")
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
                        
                        # Verificar se o calendário apareceu
                        time.sleep(2)
                        if is_calendar_open():
                            logger.info(f"Calendário aberto com sucesso após clicar no elemento {i+1}")
                            clicked = True
                            break
                        else:
                            logger.warning(f"Calendário não apareceu após clicar no elemento {i+1}")
                    except Exception as e:
                        logger.warning(f"Não conseguiu clicar no elemento {i+1}: {str(e)}")
                        try:
                            driver.execute_script("arguments[0].click();", elem)
                            logger.info(f"Clicou via JavaScript no potencial elemento de data {i+1}")
                            
                            # Verificar se o calendário apareceu
                            time.sleep(2)
                            if is_calendar_open():
                                logger.info(f"Calendário aberto com sucesso após clicar via JS no elemento {i+1}")
                                clicked = True
                                break
                            else:
                                logger.warning(f"Calendário não apareceu após clicar via JS no elemento {i+1}")
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
                month_year_elements = driver.find_elements(By.XPATH, 
                    "//div[contains(@class, 'p-datepicker-title') or contains(@class, 'datepicker-title') or contains(@class, 'calendar-title')]")
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
            # Regex mais flexível para extrair mês e ano
            match = re.search(r'(\w+)[\s,]+(\d{4})', current_text)
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
                # Tenta com uma substring parcial para maior flexibilidade
                for month_name, month_num in {**month_map, **spanish_month_map}.items():
                    if month_name.lower() in current_month_str.lower():
                        current_month = month_num
                        break
                
            if not current_month:
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
                
            # Se não estiver no mês desejado, clicar no botão de mês anterior/próximo
            logger.info(f"Mês atual não é o desejado. Tentando navegar.")
            
            # Comparar datas para saber se precisa avançar ou retroceder
            def should_go_forward(current_text):
                if not current_text:
                    return False  # Por segurança, preferimos não avançar quando não temos certeza
                
                month_map = {
                    'January': 1, 'February': 2, 'March': 3, 'April': 4,
                    'May': 5, 'June': 6, 'July': 7, 'August': 8,
                    'September': 9, 'October': 10, 'November': 11, 'December': 12,
                    'Enero': 1, 'Febrero': 2, 'Marzo': 3, 'Abril': 4,
                    'Mayo': 5, 'Junio': 6, 'Julio': 7, 'Agosto': 8,
                    'Septiembre': 9, 'Octubre': 10, 'Noviembre': 11, 'Diciembre': 12
                }
                
                import re
                match = re.search(r'(\w+)[\s,]+(\d{4})', current_text)
                if not match:
                    return False
                
                current_month_str = match.group(1)
                current_year_str = match.group(2)
                
                # Determinar o mês atual
                current_month = None
                for month_name, month_num in month_map.items():
                    if month_name.lower() in current_month_str.lower():
                        current_month = month_num
                        break
                
                if not current_month:
                    return False
                
                current_year = int(current_year_str)
                desired_month = int(start_date.strftime("%m"))
                desired_year = int(start_date.strftime("%Y"))
                
                # Calcular se precisa avançar (True) ou retroceder (False)
                if current_year < desired_year:
                    return True
                elif current_year > desired_year:
                    return False
                else:  # Mesmo ano
                    return current_month < desired_month
            
            # Determinar se devemos avançar ou retroceder
            go_forward = should_go_forward(current_month_year)
            
            # Selecionar os botões apropriados
            if go_forward:
                logger.info("Tentando navegar para o próximo mês")
                nav_button_selectors = [
                    "//button[contains(@class, 'next')]",
                    "//a[contains(@class, 'next')]",
                    "//div[contains(@class, 'p-datepicker-next')]",
                    "//span[contains(@class, 'p-datepicker-next-icon')]/..",
                    "//div[contains(@class, 'datepicker-next')]",
                    "//i[contains(@class, 'next-icon')]/..",
                    "//button[contains(@class, 'forward') or contains(@class, 'adelante')]"
                ]
            else:
                logger.info("Tentando navegar para o mês anterior")
                nav_button_selectors = [
                    "//button[contains(@class, 'prev')]",
                    "//a[contains(@class, 'prev')]",
                    "//div[contains(@class, 'p-datepicker-prev')]",
                    "//span[contains(@class, 'p-datepicker-prev-icon')]/..",
                    "//div[contains(@class, 'datepicker-prev')]",
                    "//i[contains(@class, 'prev-icon')]/..",
                    "//button[contains(@class, 'back') or contains(@class, 'previo')]"
                ]
            
            button_clicked = False
            for selector in nav_button_selectors:
                try:
                    nav_elements = driver.find_elements(By.XPATH, selector)
                    if nav_elements:
                        nav_button = nav_elements[0]
                        driver.execute_script("arguments[0].style.border='3px solid purple'", nav_button)
                        driver.save_screenshot(f"nav_button_{attempt+1}.png")
                        
                        # Tentar clique direto
                        try:
                            nav_button.click()
                            logger.info(f"Clicou no botão de navegação, tentativa {attempt+1}")
                            button_clicked = True
                            break
                        except:
                            # Tentar via JavaScript
                            driver.execute_script("arguments[0].click();", nav_button)
                            logger.info(f"Clicou via JavaScript no botão de navegação, tentativa {attempt+1}")
                            button_clicked = True
                            break
                except Exception as e:
                    logger.warning(f"Erro ao tentar clicar no botão {selector}: {str(e)}")
            
            if not button_clicked:
                logger.warning(f"Não conseguiu clicar no botão de navegação na tentativa {attempt+1}")
                break
                
            # Esperar a atualização do calendário
            time.sleep(2)
            attempt += 1
        
        if not correct_month_found:
            logger.warning("Não foi possível navegar até o mês desejado após múltiplas tentativas")
            # Vamos tentar selecionar os dias mesmo assim, no mês atual
            
        # Capturar screenshot após navegação entre meses
        driver.save_screenshot("after_month_navigation.png")
        
        # Melhor estratégia para selecionar dia no calendário
        def select_day(day_number, description):
            """
            Método aprimorado para selecionar um dia específico no calendário,
            capaz de lidar com diferentes estruturas HTML dos dias.
            """
            logger.info(f"Tentando selecionar {description}: dia {day_number}")
            day_str = str(day_number)
            
            # Capture o estado atual do calendário
            driver.save_screenshot(f"calendar_before_{description}.png")
            
            # Listar todos os seletores possíveis para o dia (do mais específico para o mais genérico)
            day_selectors = [
                # Lidar com dias com classe 'p-highlight' (selecionados)
                f"//span[contains(@class, 'p-highlight') and text()='{day_str}']",
                f"//td[contains(@class, 'p-highlight')]//span[text()='{day_str}']",
                
                # Lidar com dias normais sem highlight
                f"//span[contains(@class, 'p-element') and text()='{day_str}']",
                f"//td//span[text()='{day_str}']",
                
                # Estruturas genéricas adicionais
                f"//table[contains(@class, 'p-datepicker-calendar')]//span[normalize-space()='{day_str}']",
                f"//div[contains(@class, 'day') and normalize-space()='{day_str}']",
                f"//td[normalize-space()='{day_str}']",
                
                # Qualquer elemento com o texto do dia que não esteja desativado
                f"//*[normalize-space()='{day_str}' and not(contains(@class, 'disabled'))]"
            ]
            
            # Tentar cada seletor
            for selector in day_selectors:
                try:
                    logger.info(f"Tentando seletor: {selector}")
                    day_elements = driver.find_elements(By.XPATH, selector)
                    
                    if day_elements:
                        logger.info(f"Encontrados {len(day_elements)} elementos para o dia {day_str}")
                        
                        # Tentar cada elemento encontrado
                        for i, day_elem in enumerate(day_elements):
                            try:
                                # Verificar se o elemento está visível
                                if not day_elem.is_displayed():
                                    logger.info(f"Elemento {i+1} não está visível, pulando")
                                    continue
                                
                                # Verificar se o elemento está na parte visível do mês atual
                                class_attr = day_elem.get_attribute("class") or ""
                                if "disabled" in class_attr or "hidden" in class_attr or "other-month" in class_attr:
                                    logger.info(f"Elemento {i+1} está desabilitado ou é de outro mês, pulando")
                                    continue
                                
                                # Destacar o elemento para diagnóstico
                                driver.execute_script("arguments[0].style.border='3px solid green'", day_elem)
                                driver.save_screenshot(f"{description}_candidate_{i+1}.png")
                                
                                # Tentar diferentes métodos de clique
                                try:
                                    # 1. Clique direto
                                    day_elem.click()
                                    logger.info(f"Clicou no dia {day_str} (elemento {i+1})")
                                    return True
                                except Exception as e1:
                                    logger.warning(f"Clique direto falhou: {str(e1)}")
                                    
                                    try:
                                        # 2. Clique via JavaScript
                                        driver.execute_script("arguments[0].click();", day_elem)
                                        logger.info(f"Clicou via JavaScript no dia {day_str} (elemento {i+1})")
                                        return True
                                    except Exception as e2:
                                        logger.warning(f"Clique via JavaScript falhou: {str(e2)}")
                                        
                                        try:
                                            # 3. Ações encadeadas
                                            from selenium.webdriver.common.action_chains import ActionChains
                                            actions = ActionChains(driver)
                                            actions.move_to_element(day_elem).click().perform()
                                            logger.info(f"Clicou via ActionChains no dia {day_str} (elemento {i+1})")
                                            return True
                                        except Exception as e3:
                                            logger.warning(f"Clique via ActionChains falhou: {str(e3)}")
                            except Exception as e:
                                logger.warning(f"Erro ao processar elemento {i+1}: {str(e)}")
                except Exception as e:
                    logger.warning(f"Erro ao usar seletor {selector}: {str(e)}")
            
            # Se todos os seletores específicos falharem, tentar uma abordagem mais genérica
            try:
                # Procurar qualquer elemento visível com o texto do dia
                all_elements = driver.find_elements(By.XPATH, f"//*[contains(text(), '{day_str}')]")
                logger.info(f"Abordagem genérica: encontrados {len(all_elements)} elementos contendo '{day_str}'")
                
                for i, elem in enumerate(all_elements):
                    try:
                        if not elem.is_displayed():
                            continue
                            
                        text = elem.text.strip()
                        # Verificar se o texto é exatamente o número do dia
                        if text == day_str:
                            driver.execute_script("arguments[0].style.border='3px solid blue'", elem)
                            driver.save_screenshot(f"{description}_generic_{i+1}.png")
                            
                            # Tentar clique
                            try:
                                elem.click()
                                logger.info(f"Clicou no dia {day_str} (abordagem genérica)")
                                return True
                            except:
                                driver.execute_script("arguments[0].click();", elem)
                                logger.info(f"Clicou via JS no dia {day_str} (abordagem genérica)")
                                return True
                    except:
                        continue
            except Exception as e:
                logger.warning(f"Abordagem genérica falhou: {str(e)}")
                
            # Se chegou aqui, não conseguiu selecionar o dia
            logger.error(f"Não foi possível selecionar o dia {day_str} para {description}")
            return False
        
        # Tentar selecionar a data inicial
        start_day = int(start_date.strftime("%d"))
        start_day_selected = select_day(start_day, "data_inicial")
        
        # Aguardar processamento da seleção da data inicial
        time.sleep(3)
        driver.save_screenshot("after_start_date_select.png")
        
        # Tentar selecionar a data final
        end_day = int(end_date.strftime("%d"))
        end_day_selected = select_day(end_day, "data_final")
        
        # Aguardar processamento da seleção da data final
        time.sleep(3)
        driver.save_screenshot("after_end_date_select.png")
        
        # Tentar confirmar a seleção se houver botão de aplicar/confirmar
        try:
            # Lista expandida de possíveis botões de confirmação
            confirm_buttons_xpaths = [
                "//button[contains(text(), 'Apply') or contains(text(), 'Aplicar')]",
                "//button[contains(text(), 'OK') or contains(text(), 'Ok')]",
                "//button[contains(text(), 'Done') or contains(text(), 'Concluir')]",
                "//button[contains(text(), 'Confirm') or contains(text(), 'Confirmar')]",
                "//button[contains(@class, 'confirm') or contains(@class, 'apply')]",
                "//button[contains(@class, 'btn-primary') or contains(@class, 'btn-success')]",
                "//span[contains(text(), 'Aplicar')]/parent::button",
                "//span[contains(text(), 'Apply')]/parent::button"
            ]
            
            for xpath in confirm_buttons_xpaths:
                try:
                    elements = driver.find_elements(By.XPATH, xpath)
                    if elements:
                        confirm_button = elements[0]
                        logger.info(f"Botão de confirmação encontrado: {confirm_button.text}")
                        # Destacar o botão
                        driver.execute_script("arguments[0].style.border='3px solid green'", confirm_button)
                        driver.save_screenshot("confirm_button.png")
                        
                        # Tentar clicar
                        try:
                            confirm_button.click()
                            logger.info("Clicou no botão de confirmação")
                            break
                        except:
                            # Tentar via JavaScript
                            driver.execute_script("arguments[0].click();", confirm_button)
                            logger.info("Clicou via JavaScript no botão de confirmação")
                            break
                except Exception as e:
                    logger.warning(f"Erro ao tentar usar seletor de botão {xpath}: {str(e)}")
        except Exception as e:
            logger.info(f"Não encontrou botão de confirmação: {str(e)}, continuando...")
        
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
                    "profits": 0.0,
                    "image_url": image_url  # Adicionando URL da imagem
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

def save_dropi_metrics_to_db(store_id, date_str, products_data, start_date_str=None, end_date_str=None):
    """Save Dropi product metrics to the database with date interval support."""
    # Se as datas de início e fim não foram fornecidas, use a data de referência para ambas
    if not start_date_str:
        start_date_str = date_str
    if not end_date_str:
        end_date_str = date_str
    
    # Verificar e atualizar o esquema para permitir produtos duplicados
    update_dropi_metrics_schema_for_duplicates()
    
    # Remove dados existentes para o período
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if is_railway_environment():
            cursor.execute("""
                DELETE FROM dropi_metrics 
                WHERE store_id = %s AND date_start = %s AND date_end = %s
            """, (store_id, start_date_str, end_date_str))
        else:
            cursor.execute("""
                DELETE FROM dropi_metrics 
                WHERE store_id = ? AND date_start = ? AND date_end = ?
            """, (store_id, start_date_str, end_date_str))
        
        conn.commit()
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
    
    # Inserir os dados - cada produto com um ID de instância único
    saved_count = 0
    import uuid
    
    for product in products_data:
        try:
            product_name = product.get("product", "")
            if not product_name:
                continue
                
            # Gerar ID único para cada instância de produto
            product_instance_id = str(uuid.uuid4())
            
            conn = get_db_connection()
            cursor = conn.cursor()
            
            if is_railway_environment():
                cursor.execute("""
                    INSERT INTO dropi_metrics (
                        store_id, date, date_start, date_end, product, product_instance_id, provider, stock,
                        orders_count, orders_value, transit_count, transit_value,
                        delivered_count, delivered_value, profits, image_url
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """, (
                    store_id, date_str, start_date_str, end_date_str, 
                    product_name, product_instance_id, product.get("provider", ""), product.get("stock", 0),
                    product.get("orders_count", 0), product.get("orders_value", 0),
                    product.get("transit_count", 0), product.get("transit_value", 0),
                    product.get("delivered_count", 0), product.get("delivered_value", 0),
                    product.get("profits", 0), product.get("image_url", "")
                ))
            else:
                cursor.execute("""
                    INSERT INTO dropi_metrics (
                        store_id, date, date_start, date_end, product, product_instance_id, provider, stock,
                        orders_count, orders_value, transit_count, transit_value,
                        delivered_count, delivered_value, profits, image_url
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    store_id, date_str, start_date_str, end_date_str, 
                    product_name, product_instance_id, product.get("provider", ""), product.get("stock", 0),
                    product.get("orders_count", 0), product.get("orders_value", 0),
                    product.get("transit_count", 0), product.get("transit_value", 0),
                    product.get("delivered_count", 0), product.get("delivered_value", 0),
                    product.get("profits", 0), product.get("image_url", "")
                ))
            
            conn.commit()
            saved_count += 1
            cursor.close()
            conn.close()
        except Exception as e:
            logger.error(f"Erro ao salvar produto {product.get('product', '')}: {str(e)}")
            if conn:
                try:
                    conn.rollback()
                    conn.close()
                except:
                    pass
    
    logger.info(f"Total de {saved_count} produtos salvos com sucesso de {len(products_data)}")
    return saved_count > 0

# Função para exibir tabela de produtos Dropi com campos personalizáveis
def display_dropi_table_with_custom_fields(store_id, dropi_data, currency_to):
    """Exibe a tabela de produtos Dropi com campos personalizáveis."""
    # Obter dados personalizados
    custom_data = get_custom_product_data(store_id)
    
    # Criar uma cópia do DataFrame para edição
    display_df = dropi_data.drop(columns=['date'], errors='ignore')
    
    # Ordenar por orders_count (maior para menor)
    display_df = display_df.sort_values('orders_count', ascending=False)
    
    # Adicionar colunas personalizadas
    display_df['custom_id'] = ""
    
    # Preencher com valores salvos
    for idx, row in display_df.iterrows():
        product = row['product']
        if product in custom_data:
            display_df.at[idx, 'custom_id'] = custom_data[product].get('custom_id', '')
            # Se o fornecedor personalizado estiver definido, substituir o valor original
            if custom_data[product].get('custom_provider'):
                display_df.at[idx, 'provider'] = custom_data[product].get('custom_provider', '')
    
    # Reorganizar colunas
    if 'image_url' in display_df.columns:
        cols = display_df.columns.tolist()
        cols.remove('image_url')
        # Colocar custom_id logo após o produto e antes do provider
        new_cols = []
        for col in cols:
            new_cols.append(col)
            if col == 'product':
                new_cols.append('custom_id')
        # Remover custom_id da sua posição original
        new_cols.remove('custom_id')
        # Colocar image_url como primeira coluna
        cols = ['image_url'] + new_cols
        display_df = display_df[cols]
    else:
        # Ordenar colunas sem imagem
        cols = display_df.columns.tolist()
        # Colocar custom_id logo após o produto
        new_cols = []
        for col in cols:
            new_cols.append(col)
            if col == 'product':
                new_cols.append('custom_id')
        # Remover custom_id da sua posição original
        new_cols.remove('custom_id')
        display_df = display_df[new_cols]
    
    # Criar editor de dados
    edited_df = st.data_editor(
        display_df,
        column_config={
            "store_id": None,  # Ocultar esta coluna
            "image_url": st.column_config.ImageColumn("Imagem", help="Imagem do produto"),
            "date_start": st.column_config.TextColumn("Data Inicial"),
            "date_end": st.column_config.TextColumn("Data Final"),
            "product": "Produto",
            "custom_id": st.column_config.TextColumn("ID", help="Identificador personalizado"),
            "provider": st.column_config.TextColumn("Fornecedor", help="Fornecedor do produto"),
            "stock": "Estoque",
            "orders_count": "Pedidos",
            "orders_value": st.column_config.NumberColumn(f"Valor Pedidos ({currency_to})", format="%.2f"),
            "transit_count": "Em Trânsito",
            "transit_value": st.column_config.NumberColumn(f"Valor Trânsito ({currency_to})", format="%.2f"),
            "delivered_count": "Entregues",
            "delivered_value": st.column_config.NumberColumn(f"Valor Entregues ({currency_to})", format="%.2f"),
            "profits": st.column_config.NumberColumn(f"Lucros ({currency_to})", format="%.2f")
        },
        disabled=["store_id", "image_url", "date_start", "date_end", "product", 
                 "stock", "orders_count", "orders_value", "transit_count", 
                 "transit_value", "delivered_count", "delivered_value", "profits"],
        use_container_width=True,
        key="custom_dropi_products_table"
    )
    
    # Verificar se houve alterações e salvar
    if not edited_df.equals(display_df):
        st.info("Detectadas alterações nos dados personalizados. Salvando...")
        for idx, row in edited_df.iterrows():
            product = row['product']
            custom_id = row['custom_id']
            custom_provider = row['provider']
            
            # Verificar se é diferente do que estava no display_df
            original_id = display_df.loc[display_df['product'] == product, 'custom_id'].iloc[0]
            original_provider = display_df.loc[display_df['product'] == product, 'provider'].iloc[0]
            
            if custom_id != original_id or custom_provider != original_provider:
                success = save_custom_product_data(store_id, product, custom_id, custom_provider)
                if success:
                    st.success(f"Dados personalizados salvos para: {product}")
                else:
                    st.error(f"Erro ao salvar dados personalizados para: {product}")
    
    return edited_df

# Atualizar função update_dropi_data_silent para preservar dados personalizados
def update_dropi_data_silent(store, start_date, end_date):
    """Atualiza os dados da Dropi sem exibir feedback de progresso."""
    # Configurar o driver Selenium
    driver = setup_selenium(headless=True)
    
    if not driver:
        return False
    
    try:
        # Converter datas para strings
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        
        # Data de referência é a mesma da final (mantido para compatibilidade)
        date_str = end_date_str
        
        logger.info(f"Buscando dados Dropi para o período: {start_date_str} a {end_date_str}")
        
        # Fazer login no Dropi
        success = login(driver, store["dropi_username"], store["dropi_password"], logger, store["dropi_url"])
        
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
        
        # Verificar se a loja está no modo personalizado
        is_custom = store.get("is_custom", False)
        
        if is_custom:
            # Obter os dados personalizados antes de limpar
            custom_data = get_custom_product_data(store["id"])
            
            # Para cada produto nos novos dados, preservar os valores personalizados
            for product in product_data:
                product_name = product.get("product", "")
                if product_name in custom_data:
                    # Usar o fornecedor personalizado se existir
                    if custom_data[product_name].get("custom_provider"):
                        product["provider"] = custom_data[product_name]["custom_provider"]
        
        # Limpar dados antigos e salvar os novos - agora com datas inicial e final
        save_dropi_metrics_to_db(store["id"], date_str, product_data, start_date_str, end_date_str)
        
        # Verificar após salvar (depuração)
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
            logger.info(f"Verificação: {count} produtos salvos no banco para o período {start_date_str} a {end_date_str}")
        except Exception as e:
            logger.error(f"Erro na verificação de contagem: {str(e)}")
        
        driver.quit()
        return True
            
    except Exception as e:
        logger.error(f"Erro ao atualizar dados da Dropi: {str(e)}")
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
            
            # Ordenar por total_orders (maior para menor)
            product_data = product_data.sort_values('total_orders', ascending=False)
            
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
                    use_container_width=True,
                    key="shopify_products_table"
                )
        else:
            st.warning("Nenhum produto encontrado com o filtro selecionado")

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
    
    # Get Dropi data for the specific date range including image URLs - não agrupa mais por produto
    dropi_query = """
        SELECT product, product_instance_id, orders_count, delivered_count, image_url, provider, stock
        FROM dropi_metrics 
        WHERE store_id = ? AND date_start = ? AND date_end = ?
    """
    
    if is_railway_environment():
        dropi_query = dropi_query.replace("?", "%s")
    
    cursor = conn.cursor()
    cursor.execute(dropi_query, (store_id, start_date_str, end_date_str))
    
    # Converter resultados para DataFrame
    columns = ["product", "product_instance_id", "orders_count", "delivered_count", "image_url", "provider", "stock"]
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
    
    # Estilo para tabelas com fundo branco
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
        # Criar identificador único para cada produto, combinando nome e id da instância
        dropi_data['product_full_id'] = dropi_data['product'] + "___" + dropi_data['product_instance_id']
        
        # Calculate effectiveness
        dropi_data['effectiveness'] = 0.0
        for idx, row in dropi_data.iterrows():
            if row['orders_count'] > 0:
                dropi_data.at[idx, 'effectiveness'] = (row['delivered_count'] / row['orders_count']) * 100
        
        # Add general effectiveness column from saved values using left join on product name
        # (mantém o id de instância separado)
        dropi_data = pd.merge(
            dropi_data, 
            effectiveness_data[['product', 'general_effectiveness']], 
            on='product', 
            how='left'
        )
        
        # Ordenar por efetividade (maior para menor)
        dropi_data = dropi_data.sort_values('effectiveness', ascending=False)
        
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
        
        # Criar uma coluna de produto formatada que inclui o estoque para distinguir produtos com mesmo nome
        dropi_data['product_display'] = dropi_data.apply(
            lambda row: f"{row['product']} (Estq: {row['stock']})", axis=1)
        
        # Preparar DataFrame para visualização
        view_columns = ['image_url', 'product_display', 'stock', 'orders_count', 'delivered_count', 
                      'effectiveness', 'general_effectiveness', '_row_color', 'product_full_id']
        view_df = dropi_data[view_columns].copy()
        
        # Aplicar estilo com cores
        styled_view = view_df.style.apply(highlight_rows, axis=1)
        
        # Exibir tabela estilizada completa (não editável)
        st.dataframe(
            styled_view,
            column_config={
                "image_url": st.column_config.ImageColumn("Imagem", help="Imagem do produto"),
                "product_display": "Produto",
                "stock": "Estoque",
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
                "_row_color": None,  # Ocultar coluna de cor
                "product_full_id": None  # Ocultar ID completo
            },
            hide_index=True,
            use_container_width=True,
            key="effectiveness_table"
        )
        
        # Seção para edição da Efetividade Geral - Minimizada por padrão
        with st.expander("Editar Efetividade Geral", expanded=False):
            # Preparar DataFrame para edição usando product_full_id para identificação única
            edit_columns = ['product_display', 'general_effectiveness', '_row_color', 'product']
            edit_df = dropi_data[edit_columns].copy()
            
            # Aplicar o mesmo estilo de cores à tabela editável
            styled_edit = edit_df.style.apply(highlight_rows, axis=1)
            
            # Exibir tabela editável com cores por efetividade
            edited_df = st.data_editor(
                edit_df,
                column_config={
                    "product_display": "Produto",
                    "general_effectiveness": st.column_config.NumberColumn(
                        "Efetividade Geral (%)",
                        format="%.1f%%",
                        help="Valor definido manualmente"
                    ),
                    "_row_color": None,  # Ocultar coluna de cor
                    "product": None  # Ocultar nome original do produto
                },
                disabled=["product_display"],
                hide_index=True,
                use_container_width=True,
                key="edit_effectiveness_table"
            )
        
        # Check if any general_effectiveness values have been edited
        if not edited_df.equals(edit_df):
            for idx, row in edited_df.iterrows():
                product = row['product']
                new_value = row['general_effectiveness']
                
                try:
                    old_value = edit_df.loc[edit_df['product'] == product, 'general_effectiveness'].iloc[0]
                    if pd.notna(new_value) and (pd.isna(old_value) or new_value != old_value):
                        save_effectiveness(store_id, product, new_value)
                except:
                    if pd.notna(new_value):
                        save_effectiveness(store_id, product, new_value)
        
            if st.button("Salvar Todas as Alterações", key="save_effectiveness"):
                for idx, row in edited_df.iterrows():
                    if pd.notna(row['general_effectiveness']):
                        save_effectiveness(store_id, row['product'], row['general_effectiveness'])
                st.success("Valores de efetividade geral salvos com sucesso!")
    else:
        pass  # Não exibe nenhuma mensagem quando não há dados

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
    
    # Título principal com estilo aprimorado
    st.markdown(f'<h1>Métricas de Produtos {store["name"]}</h1>', unsafe_allow_html=True)
    
    # Definir valores padrão para datas - no início da função para garantir disponibilidade
    default_start_date = datetime.today() - timedelta(days=7)
    default_end_date = datetime.today()
    update_shopify = False
    update_dropi = False
    
    # IMPORTANTE: Definir todas as variáveis de data aqui no início
    # Isso evitará que dropi_start_date_str seja acessada antes de ser definida
    dropi_start_date = default_start_date
    dropi_end_date = default_end_date
    dropi_start_date_str = dropi_start_date.strftime("%Y-%m-%d")
    dropi_end_date_str = dropi_end_date.strftime("%Y-%m-%d")
    
    # Estilo CSS melhorado para criar uma interface mais atraente e colorida
    st.markdown("""
    <style>
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
    
    /* Linha única de controles */
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
    
    /* Botões mais atraentes */
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
    </style>
    """, unsafe_allow_html=True)
    
    # ========== SEÇÃO SHOPIFY (LAYOUT ALINHADO EM UMA LINHA) ==========
    # Adicionar CSS para alinhar elementos em uma linha
    st.markdown("""
    <style>
    .single-line-container {
        display: flex;
        flex-direction: row;
        align-items: center;
        gap: 20px;
        padding: 10px;
        background-color: white;
        border-radius: 8px;
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.05);
        margin-bottom: 15px;
    }
    .logo-container {
        flex: 0 0 140px;
    }
    .date-container {
        flex: 0 0 200px;
    }
    .platform-container {
        flex: 0 0 200px;
    }
    .button-container {
        flex: 1;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Container para todos os elementos em linha única
    st.markdown("""
    <div class="single-line-container">
        <div class="logo-container">
            <img src="https://cdn.shopify.com/shopifycloud/brochure/assets/brand-assets/shopify-logo-primary-logo-456baa801ee66a0a435671082365958316831c9960c480451dd0330bcdae304f.svg" width="120">
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Usando Streamlit para o restante dos controles em uma única linha
    shopify_controls = st.columns([1, 1, 1.5, 1.5])
    
    # Gerar chaves únicas adicionando o store_id às chaves
    shopify_start_key = f"shopify_start_{store['id']}"
    shopify_end_key = f"shopify_end_{store['id']}"
    
    # Data inicial
    with shopify_controls[0]:
        st.write("De:")
        start_date = st.date_input(
            "",
            default_start_date,
            key=shopify_start_key,
            format="DD/MM/YYYY",
            label_visibility="collapsed"
        )
    
    # Data final
    with shopify_controls[1]:
        st.write("Até:")
        end_date = st.date_input(
            "",
            default_end_date,
            key=shopify_end_key,
            format="DD/MM/YYYY",
            label_visibility="collapsed"
        )
    
    # Strings de data formatadas para uso nas consultas
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    
    # Obter categorias de URL com base nas datas selecionadas
    url_categories = get_url_categories(store["id"], start_date_str, end_date_str)
    
    # Filtro de plataforma de anúncio
    with shopify_controls[2]:
        st.write("Plataforma de Anúncio:")
        # Verificar se temos categorias
        shopify_cat_key = f"shopify_cat_{store['id']}"
        if url_categories and len(url_categories) > 0:
            category_options = ["Todos"] + url_categories
            selected_category = st.selectbox(
                "",
                options=category_options,
                index=0,
                key=shopify_cat_key,
                label_visibility="collapsed"
            )
        else:
            selected_category = "Todos"
            st.selectbox(
                "",
                options=["Todos"],
                key=shopify_cat_key,
                label_visibility="collapsed"
            )
    
    # Botão de atualização
    with shopify_controls[3]:
        st.write("&nbsp;", unsafe_allow_html=True)  # Espaçamento para alinhar com outros elementos
        # Adicionar botão de atualização
        shopify_update_key = f"shopify_update_{store['id']}"
        update_shopify_direct = st.button(
            "Atualizar Dados Shopify", 
            key=shopify_update_key,
            use_container_width=True
        )
        
    # Flag para atualizar dados
    if update_shopify_direct:
        update_shopify = True

    # Atualizar dados se o botão foi clicado
    if update_shopify:
        # Atualizar dados da Shopify
        with st.spinner("Atualizando dados da Shopify..."):
            product_urls, product_images = get_shopify_products(URL, HEADERS)
            orders = get_shopify_orders(URL, HEADERS, start_date_str, end_date_str)
            
            if orders:
                product_total, product_processed, product_delivered, product_url_map, product_value, product_image_map = process_shopify_products(orders, product_urls, product_images)
                
                # Limpar dados antigos para esse período
                try:
                    query = "DELETE FROM product_metrics WHERE store_id = ? AND date BETWEEN ? AND ?"
                    execute_query(query, (store["id"], start_date_str, end_date_str))
                except Exception as e:
                    st.error(f"Erro ao limpar dados antigos: {str(e)}")
                
                # Salvar os novos dados
                save_metrics_to_db(store["id"], start_date_str, product_total, product_processed, product_delivered, product_url_map, product_value, product_image_map)
                
                st.success("Dados da Shopify atualizados com sucesso!")
                
    # Recuperar dados atualizados para o intervalo de datas
    conn = get_db_connection()
    query = f"""
        SELECT * FROM product_metrics 
        WHERE store_id = '{store["id"]}' AND date BETWEEN '{start_date_str}' AND '{end_date_str}'
    """
    shopify_data = pd.read_sql_query(query, conn)
    conn.close()

    # Mostrar mensagem de "não há dados" logo abaixo do logo se não houver dados
    if shopify_data.empty:
        st.markdown('<div class="info-box">Não há dados disponíveis para o intervalo selecionado</div>', unsafe_allow_html=True)
        # Exibir zeros se não houver dados para o filtro selecionado
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Pedidos", "0")
        with col2:
            st.metric("Valor Total", "$0.00")
    else:
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

    # Mostrar apenas a tabela, sem divisão em colunas
    display_shopify_data(shopify_data, selected_category)

    # Fechando a seção principal Shopify
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Linha divisória entre as seções
    st.markdown('<hr>', unsafe_allow_html=True)
    
    # ========== SEÇÃO DROPI (LAYOUT ALINHADO EM UMA LINHA) ==========
    # Container para todos os elementos em linha única
    st.markdown("""
    <div class="single-line-container">
        <div class="logo-container">
            <img src="https://d39ru7awumhhs2.cloudfront.net/mexico/brands/1/logo/169517993216951799321rMLQGRSO8qfZSk4hhXNZPjPxX601y0WjAovHOll.png" width="120">
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Layout em uma única linha para os controles Dropi
    dropi_controls = st.columns([1, 1, 3])
    
    # Gerar chaves únicas adicionando o store_id às chaves
    dropi_start_key = f"dropi_start_{store['id']}"
    dropi_end_key = f"dropi_end_{store['id']}"
    
    # Data inicial
    with dropi_controls[0]:
        st.write("De:")
        dropi_start_date = st.date_input(
            "",
            default_start_date,
            key=dropi_start_key,
            format="DD/MM/YYYY",
            label_visibility="collapsed"
        )
    
    # Data final
    with dropi_controls[1]:
        st.write("Até:")
        dropi_end_date = st.date_input(
            "",
            default_end_date,
            key=dropi_end_key,
            format="DD/MM/YYYY",
            label_visibility="collapsed"
        )
    
    # Botão de atualização
    with dropi_controls[2]:
        st.write("&nbsp;", unsafe_allow_html=True)  # Espaçamento para alinhar com outros elementos
        # Adicionar botão de atualização com chave única
        dropi_update_key = f"dropi_update_{store['id']}"
        update_dropi_direct = st.button(
            "Atualizar Dados Dropi", 
            key=dropi_update_key,
            use_container_width=True
        )
        
    # Atualizar as strings de data após os inputs terem sido processados
    dropi_start_date_str = dropi_start_date.strftime("%Y-%m-%d")
    dropi_end_date_str = dropi_end_date.strftime("%Y-%m-%d")
    
    # Flag para atualizar dados
    if update_dropi_direct:
        update_dropi = True

    # Atualizar dados da Dropi se o botão foi clicado
    if update_dropi:
        # Atualizar dados da Dropi
        with st.spinner("Atualizando dados da Dropi..."):
            success = update_dropi_data_silent(
                store,
                dropi_start_date,
                dropi_end_date
            )
    
        if success:
            st.success("Dados da Dropi atualizados com sucesso!")
        else:
            st.error("Erro ao atualizar dados da Dropi.")
    
    # ========== SEÇÃO DE ANÁLISE DE EFETIVIDADE ==========
    # A seção de análise de efetividade agora vem LOGO APÓS os filtros de Dropi
    # e ANTES da exibição de dados do Dropi
    st.markdown('<h4>ANÁLISE DE EFETIVIDADE</h4>', unsafe_allow_html=True)
    
    # Exibir tabela de efetividade para o intervalo selecionado
    display_effectiveness_table(store["id"], dropi_start_date_str, dropi_end_date_str)
    
    # Linha divisória entre as seções
    st.markdown('<hr>', unsafe_allow_html=True)

    # ========== EXIBIÇÃO DE DADOS DROPI ==========
    # Buscar dados da Dropi
    conn = get_db_connection()
    query = """
        SELECT * FROM dropi_metrics 
        WHERE store_id = ? 
          AND date_start = ? 
          AND date_end = ?
    """

    if is_railway_environment():
        query = query.replace("?", "%s")

    cursor = conn.cursor()
    cursor.execute(query, (store["id"], dropi_start_date_str, dropi_end_date_str))

    # Converter resultados para DataFrame
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()

    # Obter informações da moeda da loja
    currency_info = get_store_currency(store["id"])
    currency_from = currency_info["from"]
    currency_to = currency_info["to"]

    # Converter para DataFrame
    dropi_data = pd.DataFrame(data, columns=columns)
    conn.close()

    # Mensagem quando não há dados disponíveis
    if dropi_data.empty:
        if dropi_start_date_str == dropi_end_date_str:
            st.markdown(f'<div class="info-box">Não há dados disponíveis da Dropi para a data {dropi_start_date_str}.</div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div class="info-box">Não há dados disponíveis da Dropi para o período {dropi_start_date_str} a {dropi_end_date_str}.</div>', unsafe_allow_html=True)
    else:
        # Exibir informação do período logo abaixo do logo
        if dropi_start_date_str == dropi_end_date_str:
            period_text = f"Mostrando {len(dropi_data)} produtos para a data: {dropi_start_date_str} (Valores em {currency_to})"
        else:
            period_text = f"Mostrando {len(dropi_data)} produtos para o período: {dropi_start_date_str} a {dropi_end_date_str} (Valores em {currency_to})"
        
        st.markdown(f'<div class="info-box">{period_text}</div>', unsafe_allow_html=True)

        # Obter taxa de conversão
        exchange_rate = get_exchange_rate(currency_from, currency_to)

        # Converter valores monetários
        for col in ['orders_value', 'transit_value', 'delivered_value', 'profits']:
            if col in dropi_data.columns:
                dropi_data[col] = dropi_data[col] * exchange_rate

        # Summary statistics
        total_orders = dropi_data["orders_count"].sum()
        total_orders_value = dropi_data["orders_value"].sum()
        total_transit = dropi_data["transit_count"].sum()
        total_transit_value = dropi_data["transit_value"].sum()
        total_delivered = dropi_data["delivered_count"].sum()
        total_delivered_value = dropi_data["delivered_value"].sum()
        total_profits = dropi_data["profits"].sum()
        
        # Display metrics in three columns with Dropi-specific styling
        st.markdown('<div class="dropi-metrics">', unsafe_allow_html=True)
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Pedidos", f"{total_orders}", f"{currency_to} {total_orders_value:,.2f}")
            
        with col2:
            st.metric("Em Trânsito", f"{total_transit}", f"{currency_to} {total_transit_value:,.2f}")
            
        with col3:
            st.metric("Entregues", f"{total_delivered}", f"{currency_to} {total_delivered_value:,.2f}")
        
        st.markdown('</div>', unsafe_allow_html=True)

    # Exibir apenas a tabela de produtos Dropi
    if not dropi_data.empty:
        st.subheader("Produtos Dropi")
    
        # Verificar se a loja está no modo personalizado
        if store.get("is_custom", False):
            # Usar a função de tabela personalizável
            display_dropi_table_with_custom_fields(store["id"], dropi_data, currency_to)
        else:
            # Primeiro, criar uma cópia do DataFrame sem a coluna 'date'
            display_df = dropi_data.drop(columns=['date'], errors='ignore')
        
            # Ordenar por orders_count (maior para menor)
            display_df = display_df.sort_values('orders_count', ascending=False)
        
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
                use_container_width=True,
                key="dropi_products_table"
            )

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