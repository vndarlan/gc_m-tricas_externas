import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, date
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
from selenium.webdriver.common.keys import Keys

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
    """Navigate to the Product Sold report in Dropi with improved menu detection."""
    try:
        # Esperar que a página carregue completamente após o login
        time.sleep(5)
        
        # Capturar screenshot para diagnóstico
        driver.save_screenshot("post_login.png")
        logger.info(f"URL atual: {driver.current_url}")
        
        # Se já estiver na página de relatório, não precisamos navegar
        if "reports/product" in driver.current_url or "product-report" in driver.current_url:
            logger.info("Já estamos na página de relatório de produtos")
            return True
        
        # NOVO: Tentar encontrar usando javascript primeiro (mais confiável)
        try:
            logger.info("Tentando encontrar menu Reports usando JavaScript")
            found = driver.execute_script("""
                // Primeiro tentar encontrar o menu Reports/Reportes
                var reportLinks = [];
                
                // Procurar por texto
                document.querySelectorAll('a, span, div').forEach(function(el) {
                    var text = el.textContent.trim().toLowerCase();
                    if (text === 'reports' || text === 'reportes' || text.includes('report')) {
                        reportLinks.push(el);
                    }
                });
                
                // Se encontrou algum, clicar no primeiro
                if (reportLinks.length > 0) {
                    // Clicar no próprio elemento ou no elemento pai se for um span
                    var clickElement = reportLinks[0];
                    if (clickElement.tagName.toLowerCase() === 'span') {
                        clickElement = clickElement.parentElement;
                    }
                    clickElement.click();
                    return true;
                }
                
                return false;
            """)
            
            if found:
                logger.info("Menu Reports encontrado e clicado via JavaScript")
                time.sleep(3)
            else:
                logger.warning("Não foi possível encontrar menu Reports via JavaScript")
        except Exception as e:
            logger.warning(f"Erro ao tentar JavaScript para menu: {str(e)}")
        
        # Tentar os métodos tradicionais como fallback
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
        
        # NOVO: Verificar se já chegamos à página de relatório após clicar no menu
        if "reports" in driver.current_url:
            logger.info("Já na seção de relatórios, verificando se temos o relatório de produtos")
            # Verificar se o relatório de produtos está disponível na página atual
            try:
                product_report_element = driver.find_element(By.XPATH, 
                    "//*[contains(text(), 'Product Sold') or contains(text(), 'Productos Vendidos')]")
                product_report_element.click()
                logger.info("Clicou no relatório de produtos diretamente da página de relatórios")
                time.sleep(3)
                return True
            except:
                logger.info("Não encontrou o relatório na página atual, continuando navegação")
        
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
        
        # Tentar encontrar via JavaScript como último recurso
        try:
            if "product-report" not in driver.current_url:
                logger.info("Tentando navegar para Product Sold via JavaScript")
                found = driver.execute_script("""
                    var productSoldLinks = [];
                    
                    // Procurar por links com texto relevante
                    document.querySelectorAll('a, span, div').forEach(function(el) {
                        var text = el.textContent.trim().toLowerCase();
                        if (text.includes('product sold') || text.includes('productos vendidos')) {
                            productSoldLinks.push(el);
                        }
                    });
                    
                    // Se encontrou algum, clicar no primeiro
                    if (productSoldLinks.length > 0) {
                        var clickElement = productSoldLinks[0];
                        if (clickElement.tagName.toLowerCase() === 'span') {
                            clickElement = clickElement.parentElement;
                        }
                        clickElement.click();
                        return true;
                    }
                    
                    return false;
                """)
                
                if found:
                    logger.info("Link Product Sold encontrado e clicado via JavaScript")
                    time.sleep(3)
        except Exception as e:
            logger.warning(f"Erro ao tentar JavaScript para Product Sold: {str(e)}")
        
        # Confirmar que estamos na página correta
        driver.save_screenshot("product_sold_page.png")
        logger.info(f"URL após navegar: {driver.current_url}")
        
        # Verificar se há elementos que indicam sucesso
        page_loaded = False
        check_elements = ["Rango de fecha", "Date Range", "producto", "Vendidos", "product", "filter"]
        
        for check_elem in check_elements:
            try:
                driver.find_element(By.XPATH, f"//*[contains(text(), '{check_elem}')]")
                page_loaded = True
                logger.info(f"Página confirmada pelo elemento: {check_elem}")
                break
            except:
                pass
        
        # NOVO: Verificar também pelo URL
        if "product-report" in driver.current_url or "productos" in driver.current_url:
            logger.info("Página confirmada pelo URL")
            page_loaded = True
        
        if page_loaded:
            return True
        else:
            logger.error("Não foi possível confirmar se estamos na página correta")
            return False
            
    except Exception as e:
        logger.error(f"Erro ao navegar para Product Sold: {str(e)}")
        return False

def extract_product_data(driver, logger):
    """Extract product data from the Product Sold report with improved empty data handling."""
    try:
        logger.info("Iniciando extração de dados dos produtos")
        driver.save_screenshot("product_page_before_extraction.png")
        
        # Aumentar tempo de espera para garantir carregamento completo
        wait_time = 10
        logger.info(f"Aguardando {wait_time} segundos para garantir carregamento completo dos dados")
        time.sleep(wait_time)
        
        # NOVO: Verificação específica para "sem dados" - verificar múltiplos padrões
        no_data_patterns = [
            "//div[contains(text(), 'No hay datos')]", 
            "//div[contains(text(), 'Sin resultados')]",
            "//div[contains(text(), 'No data')]",
            "//div[contains(text(), 'No se encontraron')]",
            "//div[contains(text(), 'No products')]",
            "//div[contains(text(), 'Sin productos')]",
            "//td[contains(text(), 'No data')]",
            "//p[contains(text(), 'No hay datos')]"
        ]
        
        for pattern in no_data_patterns:
            try:
                elements = driver.find_elements(By.XPATH, pattern)
                if elements:
                    logger.info(f"Detectada mensagem 'sem dados': '{elements[0].text}'")
                    driver.save_screenshot("no_data_found.png")
                    # Retornar lista vazia para indicar que não há dados
                    return []
            except:
                pass
        
        # NOVO: Verificar se a página está vazia ou tem apenas cabeçalhos
        try:
            # Pegar todos os elementos com texto na página
            text_elements = driver.find_elements(By.XPATH, "//*[string-length(text()) > 5]")
            
            # Filtrar apenas os que têm texto significativo
            significant_text = [elem.text for elem in text_elements if len(elem.text.strip()) > 5]
            
            # Se houver poucos elementos com texto, pode indicar uma página sem dados
            if len(significant_text) < 10:
                logger.warning(f"Página com poucos elementos de texto ({len(significant_text)}), pode indicar sem dados")
                logger.info(f"Textos encontrados: {significant_text}")
                
                # Verificar se há apenas elementos de interface (filtros, cabeçalhos)
                interface_terms = ["filtro", "filter", "date", "fecha", "rango", "range", "tienda", "store"]
                
                # Contar quantos elementos de texto parecem ser da interface
                interface_elements = 0
                for text in significant_text:
                    if any(term in text.lower() for term in interface_terms):
                        interface_elements += 1
                
                # Se quase todos os elementos forem da interface, provavelmente não há dados
                if interface_elements >= len(significant_text) * 0.7:
                    logger.info(f"Página contém principalmente elementos de interface ({interface_elements}/{len(significant_text)})")
                    driver.save_screenshot("likely_no_data.png")
                    
                    # Tentar uma verificação final - buscar por tabelas vazias
                    tables = driver.find_elements(By.TAG_NAME, "table")
                    if tables:
                        for table in tables:
                            rows = table.find_elements(By.TAG_NAME, "tr")
                            # Se tem só uma linha (cabeçalho) ou nenhuma, tabela vazia
                            if len(rows) <= 1:
                                logger.info("Tabela vazia encontrada")
                                return []
                    else:
                        # Sem tabelas e poucos elementos significativos - provavelmente sem dados
                        logger.info("Nenhuma tabela encontrada na página, provavelmente sem dados")
                        return []
            
        except Exception as e:
            logger.warning(f"Erro ao verificar se a página está vazia: {str(e)}")
        
        # Continuar com o processo regular de extração se não está vazio
        # (O restante da função seria o mesmo do seu código de extração de produtos)
        
        products_data = []
        ignore_texts = [
            "Informe de productos", 
            "Reporte de productos",
            "Productos vendidos",
            "Productos en transito",
            "Resumen",
            "Total",
            "Filtrar",
            "Rango de fecha",
            "Date Range"
        ]
        
        # Usar múltiplos seletores para aumentar a chance de encontrar os cards
        product_card_selectors = [
            "//div[contains(@class, 'product-item') or contains(@class, 'product-card')]",
            "//div[.//div[contains(text(), 'Stock:') or contains(text(), 'Proveedor:')]]",
            "//div[.//img]/following-sibling::div",
            "//div[contains(@class, 'card') and not(contains(@class, 'header')) and not(contains(@class, 'filter'))]",
            "//table//tr[position() > 1]",
            "//div[.//div[contains(@class, 'product-name')] and .//div[contains(@class, 'product-stats')]]"
        ]
        
        # Tentar cada seletor até encontrar cards de produtos
        product_cards = []
        for selector in product_card_selectors:
            try:
                logger.info(f"Tentando seletor: {selector}")
                cards = driver.find_elements(By.XPATH, selector)
                
                if cards and len(cards) > 0:
                    # Verificar se parece um card de produto válido
                    for card in cards:
                        card_text = card.text.strip()
                        # Verificar comprimento mínimo e presença de termos relacionados a produtos
                        if (len(card_text) > 30 and 
                            ("Stock" in card_text or "Proveedor" in card_text or 
                             "ordenes" in card_text or "productos" in card_text)):
                            product_cards.append(card)
                
                if len(product_cards) > 0:
                    logger.info(f"Encontrados {len(product_cards)} cards de produtos usando o seletor: {selector}")
                    break
            except Exception as e:
                logger.warning(f"Erro ao usar o seletor {selector}: {str(e)}")
        
        # Se não encontrou nenhum card, assume que não há dados
        if not product_cards:
            logger.warning("Nenhum card de produto encontrado. Assumindo que não há dados disponíveis.")
            return []
        
        # Processar cada card encontrado (resto da função continua como antes)
        # (Parte omitida para brevidade, já está no código completo anterior)
        
        logger.info(f"Total de {len(products_data)} produtos extraídos com sucesso")
        return products_data
        
    except Exception as e:
        logger.error(f"Erro geral ao extrair dados dos produtos: {str(e)}")
        # Capturar screenshot em caso de erro
        try:
            driver.save_screenshot("error_extracting_products.png")
        except:
            pass
        return []

def select_date_range(driver, start_date, end_date, logger):
    """Select a specific date range in the Product Sold report with improved date type handling."""
    try:
        # Formatação das datas para exibição no formato esperado pelo Dropi (DD/MM/YYYY)
        start_date_formatted = start_date.strftime("%d/%m/%Y")
        end_date_formatted = end_date.strftime("%d/%m/%Y")
        
        logger.info(f"Tentando selecionar intervalo de datas: {start_date_formatted} a {end_date_formatted}")
        
        # Capturar screenshot para diagnóstico
        driver.save_screenshot("before_date_select.png")
        
        # CORREÇÃO: Garantir que estamos trabalhando com datetime.date consistentemente
        # Converter datetime.datetime para datetime.date se necessário
        import datetime
        
        if isinstance(start_date, datetime.datetime):
            start_date_obj = start_date.date()
        else:
            start_date_obj = start_date
            
        if isinstance(end_date, datetime.datetime):
            end_date_obj = end_date.date()
        else:
            end_date_obj = end_date
            
        # Converter a data atual para datetime.date para comparações consistentes
        today = date.today()
        
        # Verificar se há opções pré-definidas que podemos usar em vez de selecionar datas manualmente
        if (today - start_date_obj).days <= 1 and (today - end_date_obj).days <= 1:
            # Tentar usar a opção "Hoy" (Hoje)
            try:
                hoy_elements = driver.find_elements(By.XPATH, "//div[text()='Hoy' or text()='Today']")
                if hoy_elements:
                    logger.info("Usando opção pré-definida 'Hoy/Today'")
                    hoy_elements[0].click()
                    time.sleep(3)
                    driver.save_screenshot("selected_today.png")
                    return True
            except Exception as e:
                logger.warning(f"Não foi possível usar a opção 'Hoy': {str(e)}")
        
        if (today - start_date_obj).days <= 7 and (today - end_date_obj).days <= 1:
            # Tentar usar a opção "Últimos 7 días"
            try:
                last7_elements = driver.find_elements(By.XPATH, "//div[contains(text(), 'ltimos 7 d') or contains(text(), 'Last 7 d')]")
                if last7_elements:
                    logger.info("Usando opção pré-definida 'Últimos 7 días/Last 7 days'")
                    last7_elements[0].click()
                    time.sleep(3)
                    driver.save_screenshot("selected_last7days.png")
                    return True
            except Exception as e:
                logger.warning(f"Não foi possível usar a opção 'Últimos 7 días': {str(e)}")
        
        # Tentar clicar no seletor de datas
        date_clicked = False
        date_picker_selectors = [
            "//div[contains(@class, 'date-picker')]",
            "//input[contains(@class, 'date')]",
            "//span[contains(@class, 'p-calendar')]",
            "//div[contains(text(), 'Rango de fecha')]",
            "//div[@class='date']"
        ]
        
        for selector in date_picker_selectors:
            try:
                elements = driver.find_elements(By.XPATH, selector)
                if elements:
                    logger.info(f"Tentando clicar no seletor de data: {selector}")
                    elements[0].click()
                    time.sleep(2)
                    driver.save_screenshot("date_selector_clicked.png")
                    date_clicked = True
                    break
            except Exception as e:
                logger.warning(f"Não foi possível clicar no seletor {selector}: {str(e)}")
        
        # Se não conseguiu clicar, tentar JavaScript
        if not date_clicked:
            try:
                logger.info("Tentando abrir calendário via JavaScript")
                driver.execute_script("""
                    // Procurar elementos de calendário
                    var dateElements = document.querySelectorAll('.date-picker, .date, .p-calendar, [class*="date"]');
                    if (dateElements.length > 0) {
                        dateElements[0].click();
                        return true;
                    }
                    return false;
                """)
                time.sleep(2)
                driver.save_screenshot("date_js_clicked.png")
                date_clicked = True
            except Exception as e:
                logger.warning(f"JavaScript para clicar no calendário falhou: {str(e)}")
        
        if not date_clicked:
            logger.warning("Não foi possível encontrar ou clicar no seletor de data")
            return False
        
        # Aguardar o popup do calendário aparecer
        time.sleep(3)
        driver.save_screenshot("calendar_popup.png")
        
        # CORREÇÃO: Função melhorada para selecionar dias específicos com verificação de classes
        def select_calendar_day(day_number, check_if_selected=False):
            logger.info(f"Tentando selecionar dia {day_number}")
            
            # NOVO: Verificar se o dia já está selecionado
            if check_if_selected:
                pre_selected = driver.find_elements(
                    By.XPATH, 
                    f"//span[contains(@class, 'p-highlight') and text()='{day_number}']"
                )
                if pre_selected:
                    logger.info(f"Dia {day_number} já está selecionado")
                    return True
            
            # Lista abrangente de seletores para encontrar o dia no calendário
            selectors = [
                # Seletor específico baseado no HTML que você forneceu
                f"//span[@class='p-ripple p-element ng-tns-c43-70 ng-star-inserted' and text()='{day_number}']",
                # Seletores mais genéricos
                f"//span[contains(@class, 'p-element') and text()='{day_number}']",
                f"//td[contains(@class, 'p-datepicker-today')]/span[text()='{day_number}']",
                f"//span[text()='{day_number}' and not(contains(@class, 'disabled'))]",
                f"//td[text()='{day_number}']",
                f"//a[text()='{day_number}']"
            ]
            
            for selector in selectors:
                try:
                    elements = driver.find_elements(By.XPATH, selector)
                    if elements:
                        # Filtrar para remover dias desabilitados
                        valid_days = [elem for elem in elements if 
                                    "disabled" not in elem.get_attribute("class") and 
                                    "hidden" not in elem.get_attribute("class")]
                        
                        if valid_days:
                            # Usar JavaScript para clicar, é mais confiável
                            driver.execute_script("arguments[0].click();", valid_days[0])
                            logger.info(f"Selecionado dia {day_number} via JavaScript")
                            time.sleep(1)
                            driver.save_screenshot(f"day_{day_number}_selected.png")
                            return True
                except Exception as e:
                    logger.warning(f"Erro ao tentar selecionar dia {day_number} com selector {selector}: {str(e)}")
            
            # NOVO: Abordagem usando JavaScript mais agressivo
            try:
                logger.info(f"Tentando selecionar dia {day_number} com JavaScript direto")
                success = driver.execute_script(f"""
                    // Tentar encontrar o dia {day_number} em qualquer formato no calendário
                    var dayElements = [];
                    
                    // Procurar spans com texto exato
                    document.querySelectorAll('span').forEach(function(el) {{
                        if (el.textContent.trim() === '{day_number}') dayElements.push(el);
                    }});
                    
                    // Procurar em células de tabela também
                    document.querySelectorAll('td').forEach(function(el) {{
                        if (el.textContent.trim() === '{day_number}') dayElements.push(el);
                    }});
                    
                    // Se encontrou elementos, tentar clicar no primeiro válido
                    for (var i = 0; i < dayElements.length; i++) {{
                        var el = dayElements[i];
                        if (!el.className.includes('disabled') && !el.className.includes('hidden')) {{
                            el.click();
                            return true;
                        }}
                    }}
                    
                    return false;
                """)
                if success:
                    logger.info(f"Dia {day_number} selecionado com JavaScript")
                    time.sleep(1)
                    driver.save_screenshot(f"day_{day_number}_js_selected.png")
                    return True
            except Exception as e:
                logger.warning(f"Tentativa com JavaScript direto falhou: {str(e)}")
            
            logger.warning(f"Não foi possível selecionar o dia {day_number}")
            return False
        
        # Selecionar dia inicial
        start_day = start_date.day
        start_selected = select_calendar_day(start_day, check_if_selected=True)
        
        # Esperar para garantir que a primeira seleção foi processada
        time.sleep(2)
        
        # Selecionar dia final (se diferente)
        end_day = end_date.day
        end_selected = True  # Assumir true por padrão
        
        if start_day != end_day:
            end_selected = select_calendar_day(end_day)
            time.sleep(2)
        
        # NOVO: Se não conseguiu selecionar as datas pelo calendário, 
        # tentar enviar as datas como texto formatado
        if not start_selected or not end_selected:
            try:
                logger.info("Tentando inserir datas diretamente como texto")
                
                # Procurar campos de input
                input_fields = driver.find_elements(By.XPATH, "//input[contains(@class, 'date') or contains(@class, 'p-calendar')]")
                
                if input_fields:
                    # Limpar e inserir a data de início
                    input_fields[0].clear()
                    input_fields[0].send_keys(start_date_formatted)
                    input_fields[0].send_keys(Keys.TAB)  # Tabular para confirmar
                    logger.info(f"Inseriu data inicial como texto: {start_date_formatted}")
                    
                    # Se houver um segundo campo e datas diferentes, inserir a data final
                    if len(input_fields) > 1 and start_date_obj != end_date_obj:
                        input_fields[1].clear()
                        input_fields[1].send_keys(end_date_formatted)
                        input_fields[1].send_keys(Keys.TAB)
                        logger.info(f"Inseriu data final como texto: {end_date_formatted}")
                    
                    start_selected = True
                    end_selected = True
                    time.sleep(2)
                    driver.save_screenshot("dates_inserted_as_text.png")
                else:
                    # Tentar outra abordagem usando JavaScript para inserir as datas
                    js_success = driver.execute_script(f"""
                        var dateInputs = document.querySelectorAll('input[type="date"], input.date, input.p-calendar');
                        if (dateInputs.length > 0) {{
                            dateInputs[0].value = '{start_date_formatted}';
                            var event = new Event('input', {{ bubbles: true }});
                            dateInputs[0].dispatchEvent(event);
                            
                            if (dateInputs.length > 1) {{
                                dateInputs[1].value = '{end_date_formatted}';
                                dateInputs[1].dispatchEvent(event);
                            }}
                            return true;
                        }}
                        return false;
                    """)
                    
                    if js_success:
                        logger.info("Datas inseridas via JavaScript")
                        start_selected = True
                        end_selected = True
                        time.sleep(2)
                        driver.save_screenshot("dates_inserted_js.png")
            except Exception as e:
                logger.warning(f"Erro ao tentar inserir datas como texto: {str(e)}")
        
        # Procurar e clicar em botões de confirmação/aplicar
        try:
            confirm_buttons = driver.find_elements(By.XPATH, 
                "//button[contains(text(), 'Aplicar') or contains(text(), 'Apply') or contains(text(), 'OK') or contains(@class, 'confirm')]")
            
            if confirm_buttons:
                confirm_buttons[0].click()
                logger.info(f"Clicou no botão de confirmação: {confirm_buttons[0].text}")
                time.sleep(2)
            else:
                # Tentar com JavaScript para fechar o calendário / confirmar
                driver.execute_script("""
                    // Tentar encontrar e clicar em algum botão de confirmação
                    var buttons = document.querySelectorAll('button');
                    for (var i = 0; i < buttons.length; i++) {
                        var btn = buttons[i];
                        if (btn.textContent.includes('Aplicar') || 
                            btn.textContent.includes('Apply') || 
                            btn.textContent.includes('OK') || 
                            btn.className.includes('confirm')) {
                            btn.click();
                            return;
                        }
                    }
                    
                    // Se não encontrou botão, tentar clicar fora do calendário para fechá-lo
                    var body = document.querySelector('body');
                    if (body) {
                        var event = new MouseEvent('click', {
                            bubbles: true,
                            cancelable: true,
                            view: window
                        });
                        body.dispatchEvent(event);
                    }
                """)
                logger.info("Tentou confirmar via JavaScript")
                time.sleep(2)
        except Exception as e:
            logger.warning(f"Erro ao tentar confirmar seleção: {str(e)}")
        
        # Tentar clicar em qualquer botão de filtro
        try:
            filter_buttons = driver.find_elements(By.XPATH, 
                "//button[contains(text(), 'Filtrar') or contains(text(), 'Filter') or contains(text(), 'Buscar')]")
            
            if filter_buttons:
                filter_buttons[0].click()
                logger.info(f"Clicou no botão de filtro: {filter_buttons[0].text}")
                time.sleep(5)  # Espera mais longa para carregar dados
        except Exception as e:
            logger.warning(f"Erro ao tentar filtrar: {str(e)}")
        
        # Verificar resultado final
        driver.save_screenshot("after_date_selection.png")
        
        # Retornar sucesso se pelo menos uma data foi selecionada
        return start_selected or end_selected
        
    except Exception as e:
        logger.error(f"Erro ao selecionar intervalo de datas: {str(e)}")
        try:
            driver.save_screenshot("date_selection_error.png")
        except:
            pass
        return False

def extract_product_data(driver, logger):
    """Extract product data from the Product Sold report with improved robustness."""
    try:
        logger.info("Iniciando extração de dados dos produtos com método aprimorado")
        driver.save_screenshot("product_page_before_extraction.png")
        
        # Aumentar significativamente o tempo de espera para garantir carregamento completo
        wait_time = 10
        logger.info(f"Aguardando {wait_time} segundos para garantir carregamento completo dos dados")
        time.sleep(wait_time)
        
        # Verificar se há mensagem de "sem dados" na página
        try:
            no_data_elements = driver.find_elements(By.XPATH, 
                "//div[contains(text(), 'No hay datos') or contains(text(), 'Sin resultados') or contains(text(), 'No data')]")
            
            if no_data_elements:
                logger.info(f"Detectada mensagem de 'sem dados': {no_data_elements[0].text}")
                driver.save_screenshot("no_data_found.png")
                return []
        except:
            pass
            
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
            "Rango de fecha",
            "Date Range"
        ]
        
        # NOVO: Usar múltiplos seletores para aumentar a chance de encontrar os cards
        product_card_selectors = [
            # Seletor mais específico para cards com classes relacionadas a produtos
            "//div[contains(@class, 'product-item') or contains(@class, 'product-card')]",
            # Seletor para cards que contêm informações típicas de produtos
            "//div[.//div[contains(text(), 'Stock:') or contains(text(), 'Proveedor:')]]",
            # Seletor para cards que contêm imagens de produtos seguido de informações
            "//div[.//img]/following-sibling::div",
            # Seletor menos específico para qualquer card que possa conter produtos
            "//div[contains(@class, 'card') and not(contains(@class, 'header')) and not(contains(@class, 'filter'))]",
            # Seletor para linhas ou cards em uma tabela de produtos
            "//table//tr[position() > 1]",
            # Seletor extremamente específico para o padrão de produto Dropi
            "//div[.//div[contains(@class, 'product-name')] and .//div[contains(@class, 'product-stats')]]"
        ]
        
        # Tentar cada seletor até encontrar cards de produtos
        product_cards = []
        for selector in product_card_selectors:
            try:
                logger.info(f"Tentando seletor: {selector}")
                cards = driver.find_elements(By.XPATH, selector)
                
                if cards and len(cards) > 0:
                    # Verificar se parece um card de produto válido
                    for card in cards:
                        card_text = card.text.strip()
                        # Verificar comprimento mínimo e presença de termos relacionados a produtos
                        if (len(card_text) > 30 and 
                            ("Stock" in card_text or "Proveedor" in card_text or 
                             "ordenes" in card_text or "productos" in card_text)):
                            product_cards.append(card)
                
                if len(product_cards) > 0:
                    logger.info(f"Encontrados {len(product_cards)} cards de produtos usando o seletor: {selector}")
                    break
            except Exception as e:
                logger.warning(f"Erro ao usar o seletor {selector}: {str(e)}")
        
        # Se não encontrou cards com seletores específicos, tentar capturar elementos genéricos
        if not product_cards:
            logger.info("Tentando método alternativo para identificar produtos")
            try:
                # Primeiro, pegar todos os elementos div que têm tamanho razoável
                all_divs = driver.find_elements(By.XPATH, "//div[string-length(text()) > 50]")
                logger.info(f"Encontrados {len(all_divs)} divs com conteúdo substancial")
                
                # Filtrar apenas os que tem indicações de ser um produto
                potential_products = []
                for div in all_divs:
                    text = div.text
                    if ("Stock:" in text or "Proveedor:" in text) and not any(ignore in text for ignore in ignore_texts):
                        potential_products.append(div)
                
                if potential_products:
                    logger.info(f"Identificados {len(potential_products)} potenciais produtos via método alternativo")
                    product_cards = potential_products
            except Exception as e:
                logger.warning(f"Método alternativo também falhou: {str(e)}")
        
        # NOVO: Verificação final para determinar se realmente temos produtos
        if not product_cards:
            logger.warning("Nenhum card de produto encontrado na página")
            
            # Capturar HTML da página para análise posterior
            page_source = driver.page_source
            with open("page_source_no_products.html", "w", encoding="utf-8") as f:
                f.write(page_source)
            
            # Capturar screenshot final
            driver.save_screenshot("no_products_found.png")
            return []
        
        logger.info(f"Processando {len(product_cards)} cards de produtos encontrados")
        driver.save_screenshot("products_found.png")
        
        # Processar cada card de produto individualmente
        for i, card in enumerate(product_cards):
            try:
                # Verificar se este é realmente um card de produto válido
                card_text = card.text
                
                # Para debug: salvar o texto do card
                logger.info(f"Card #{i+1} texto completo:\n{card_text}")
                
                # Ignorar cards vazios ou inválidos
                if not card_text or len(card_text) < 20:
                    logger.info(f"Ignorando card #{i+1}: texto muito curto ou vazio")
                    continue
                
                # Verificar se não é um cabeçalho ou outro elemento não-produto
                if any(ignore_text.lower() in card_text.lower() for ignore_text in ignore_texts):
                    logger.info(f"Ignorando card #{i+1}: contém texto de cabeçalho/título")
                    continue
                
                # Verificações adicionais para validar que é um produto real
                if "Stock:" not in card_text and "Proveedor:" not in card_text:
                    logger.info(f"Ignorando card #{i+1}: não contém informações de produto")
                    continue
                
                # Extrair nome do produto (geralmente o primeiro texto substancial no card)
                lines = card_text.split('\n')
                if not lines:
                    continue
                    
                # Procurar nome do produto (pode não ser a primeira linha se houver cabeçalhos)
                product_name = ""
                for line in lines:
                    if line and len(line) > 3 and not any(ignore_text.lower() in line.lower() for ignore_text in ignore_texts):
                        product_name = line.strip()
                        break
                
                if not product_name:
                    logger.info(f"Ignorando card #{i+1}: não foi possível identificar o nome do produto")
                    continue
                
                # Debug
                logger.info(f"Processando produto: '{product_name}'")
                
                # Tentar extrair a imagem do produto
                image_url = ""
                try:
                    img_elements = driver.execute_script("""
                        var card = arguments[0];
                        // Procurar no card atual e elementos próximos
                        var imgs = [];
                        // 1. No próprio card
                        Array.from(card.querySelectorAll('img')).forEach(img => imgs.push(img));
                        // 2. No elemento pai
                        if (card.parentElement) {
                            Array.from(card.parentElement.querySelectorAll('img')).forEach(img => imgs.push(img));
                        }
                        // 3. Nos elementos irmãos anteriores
                        var sibling = card.previousElementSibling;
                        while(sibling) {
                            Array.from(sibling.querySelectorAll('img')).forEach(img => imgs.push(img));
                            sibling = sibling.previousElementSibling;
                        }
                        return imgs.length > 0 ? imgs[0].src : '';
                    """, card)
                    
                    if image_url:
                        logger.info(f"Imagem encontrada: {image_url}")
                except Exception as e:
                    logger.warning(f"Erro ao extrair imagem: {str(e)}")
                
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
                
                # MELHORADO: Extração de dados usando regex
                # Fornecedor
                provider_pattern = r'Proveedor:[\s]*([^\n]+)'
                provider_match = re.search(provider_pattern, card_text, re.IGNORECASE)
                if provider_match:
                    product_data["provider"] = provider_match.group(1).strip()
                
                # Estoque
                stock_pattern = r'Stock:[\s]*(\d+)'
                stock_match = re.search(stock_pattern, card_text, re.IGNORECASE)
                if stock_match:
                    product_data["stock"] = int(stock_match.group(1))
                
                # Pedidos - busca por múltiplos padrões
                orders_patterns = [
                    r'(\d+)[\s]*ordenes',
                    r'Orders:[\s]*(\d+)',
                    r'Pedidos:[\s]*(\d+)'
                ]
                
                for pattern in orders_patterns:
                    orders_match = re.search(pattern, card_text, re.IGNORECASE)
                    if orders_match:
                        product_data["orders_count"] = int(orders_match.group(1))
                        break
                
                # Valor de pedidos - busca múltiplos padrões
                if product_data["orders_count"] > 0:
                    orders_value_patterns = [
                        r'ordenes[\s\n]*\$[\s]*([\d.,]+)',
                        r'Orders[\s\n]*\$[\s]*([\d.,]+)',
                        r'Pedidos[\s\n]*\$[\s]*([\d.,]+)',
                        r'\$[\s]*([\d.,]+)[\s\n]*[Oo]rdenes'
                    ]
                    
                    for pattern in orders_value_patterns:
                        value_match = re.search(pattern, card_text, re.IGNORECASE)
                        if value_match:
                            value_str = value_match.group(1).replace(',', '').replace('.', '')
                            # Considerar dois dígitos do final como centavos
                            try:
                                value = float(value_str) / 100
                                product_data["orders_value"] = value
                                break
                            except:
                                logger.warning(f"Não foi possível converter valor: {value_str}")
                
                # Em trânsito - múltiplos padrões
                transit_patterns = [
                    r'(\d+)[\s]*productos[\s\n]*[Ee]n[\s]*[Tt]ransito',
                    r'[Ee]n[\s]*[Tt]ransito[\s\n]*(\d+)[\s]*productos',
                    r'[Tt]ransit:[\s]*(\d+)',
                    r'[Ee]n[\s]*[Tt]ransporte:[\s]*(\d+)'
                ]
                
                for pattern in transit_patterns:
                    transit_match = re.search(pattern, card_text, re.IGNORECASE)
                    if transit_match:
                        product_data["transit_count"] = int(transit_match.group(1))
                        break
                
                # Valor em trânsito - múltiplos padrões
                if product_data["transit_count"] > 0:
                    transit_value_patterns = [
                        r'[Tt]ransito[\s\n]*[\s\S]{0,20}\$[\s]*([\d.,]+)',
                        r'[Tt]ransit[\s\n]*[\s\S]{0,20}\$[\s]*([\d.,]+)'
                    ]
                    
                    for pattern in transit_value_patterns:
                        value_match = re.search(pattern, card_text, re.IGNORECASE)
                        if value_match:
                            value_str = value_match.group(1).replace(',', '').replace('.', '')
                            try:
                                value = float(value_str) / 100
                                product_data["transit_value"] = value
                                break
                            except:
                                logger.warning(f"Não foi possível converter valor de trânsito: {value_str}")
                
                # Entregados - múltiplos padrões
                delivered_patterns = [
                    r'(\d+)[\s]*productos[\s\n]*[Ee]ntregados',
                    r'[Ee]ntregados[\s\n]*(\d+)[\s]*productos',
                    r'[Dd]elivered:[\s]*(\d+)',
                    r'[Ee]ntregues:[\s]*(\d+)'
                ]
                
                for pattern in delivered_patterns:
                    delivered_match = re.search(pattern, card_text, re.IGNORECASE)
                    if delivered_match:
                        product_data["delivered_count"] = int(delivered_match.group(1))
                        break
                
                # Valor entregados - múltiplos padrões
                if product_data["delivered_count"] > 0:
                    delivered_value_patterns = [
                        r'[Ee]ntregados[\s\n]*[\s\S]{0,20}\$[\s]*([\d.,]+)',
                        r'[Dd]elivered[\s\n]*[\s\S]{0,20}\$[\s]*([\d.,]+)'
                    ]
                    
                    for pattern in delivered_value_patterns:
                        value_match = re.search(pattern, card_text, re.IGNORECASE)
                        if value_match:
                            value_str = value_match.group(1).replace(',', '').replace('.', '')
                            try:
                                value = float(value_str) / 100
                                product_data["delivered_value"] = value
                                break
                            except:
                                logger.warning(f"Não foi possível converter valor entregado: {value_str}")
                
                # Ganâncias/Lucros - múltiplos padrões
                profits_patterns = [
                    r'[Gg]anancias[\s\n]*\$[\s]*([\d.,]+)',
                    r'[Pp]rofits[\s\n]*\$[\s]*([\d.,]+)',
                    r'[Ll]ucros?[\s\n]*\$[\s]*([\d.,]+)'
                ]
                
                for pattern in profits_patterns:
                    profits_match = re.search(pattern, card_text, re.IGNORECASE)
                    if profits_match:
                        value_str = profits_match.group(1).replace(',', '').replace('.', '')
                        try:
                            value = float(value_str) / 100
                            product_data["profits"] = value
                            break
                        except:
                            logger.warning(f"Não foi possível converter valor de lucro: {value_str}")
                
                # NOVO: Uma verificação menos rigorosa - aceita qualquer produto com pelo menos um valor
                valid_metrics_count = sum([
                    product_data["orders_count"] > 0,
                    product_data["transit_count"] > 0,
                    product_data["delivered_count"] > 0,
                    product_data["stock"] > 0
                ])
                
                if valid_metrics_count >= 1 or len(product_data["provider"]) > 0:
                    products_data.append(product_data)
                    logger.info(f"Produto '{product_name}' processado com sucesso")
                else:
                    logger.warning(f"Produto '{product_name}' ignorado por não ter dados significativos")
                
            except Exception as e:
                logger.error(f"Erro ao processar card #{i+1}: {str(e)}")
                # Continuar processando os outros cards
        
        logger.info(f"Total de {len(products_data)} produtos extraídos com sucesso")
        return products_data
        
    except Exception as e:
        logger.error(f"Erro geral ao extrair dados dos produtos: {str(e)}")
        # Capturar screenshot em caso de erro
        try:
            driver.save_screenshot("error_extracting_products.png")
        except:
            pass
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
        
def update_dropi_data_silent(store, start_date, end_date):
    """Atualiza os dados da Dropi com melhor tratamento para datas recentes."""
    # Configurar o driver Selenium
    driver = None
    max_retries = 3
    retry_count = 0
    success = False
    
    while retry_count < max_retries and not success:
        try:
            retry_count += 1
            logger.info(f"Tentativa #{retry_count} de atualizar dados Dropi")
            
            # Configurar o driver com cada tentativa
            driver = setup_selenium(headless=True)
            
            if not driver:
                logger.error("Falha ao inicializar o driver Selenium")
                time.sleep(2)  # Esperar um pouco antes de tentar novamente
                continue
            
            # CORREÇÃO: Garantir que as datas estão no formato correto (datetime.date)
            import datetime
            if isinstance(start_date, datetime.datetime):
                start_date_obj = start_date.date()
            else:
                start_date_obj = start_date
                
            if isinstance(end_date, datetime.datetime):
                end_date_obj = end_date.date()
            else:
                end_date_obj = end_date
            
            # Converter datas para strings
            start_date_str = start_date_obj.strftime("%Y-%m-%d")
            end_date_str = end_date_obj.strftime("%Y-%m-%d")
            
            # Data de referência é a mesma da final (mantido para compatibilidade)
            date_str = end_date_str
            
            logger.info(f"Buscando dados Dropi para o período: {start_date_str} a {end_date_str}")
            
            # NOVO: Verificar se são datas muito recentes
            today = datetime.date.today()
            is_recent_date = (today - start_date_obj).days <= 7 and (today - end_date_obj).days <= 1
            
            if is_recent_date:
                logger.info("Período selecionado é recente (últimos 7 dias). Usando tratamento especial.")
            
            # Fazer login no Dropi
            login_success = False
            login_retries = 3
            
            for login_attempt in range(login_retries):
                try:
                    logger.info(f"Tentativa de login #{login_attempt+1}")
                    login_success = login(driver, store["dropi_username"], store["dropi_password"], logger, store["dropi_url"])
                    if login_success:
                        logger.info("Login bem-sucedido!")
                        break
                    else:
                        logger.warning(f"Falha no login (tentativa {login_attempt+1})")
                        # Esperar mais tempo entre tentativas de login
                        time.sleep(3)
                except Exception as e:
                    logger.error(f"Erro durante login (tentativa {login_attempt+1}): {str(e)}")
                    time.sleep(3)
            
            if not login_success:
                logger.error("Falha em todas as tentativas de login")
                if driver:
                    try:
                        driver.quit()
                    except:
                        pass
                time.sleep(5)  # Esperar mais tempo antes da próxima tentativa completa
                continue
            
            # Navegar para o relatório de produtos vendidos
            if not navigate_to_product_sold(driver, logger):
                logger.error("Falha ao navegar para o relatório de produtos vendidos")
                if driver:
                    try:
                        driver.quit()
                    except:
                        pass
                time.sleep(3)
                continue
            
            # NOVO: Tratamento especial para períodos recentes
            # Para datas recentes, tentaremos primeiro usando períodos predefinidos
            if is_recent_date and (today - start_date_obj).days <= 1 and (today - end_date_obj).days <= 1:
                # É "hoje" - tentar usar a opção predefinida
                try:
                    hoy_elements = driver.find_elements(By.XPATH, "//div[text()='Hoy' or text()='Today']")
                    if hoy_elements:
                        logger.info("Usando opção predefinida 'Hoy/Today' para período atual")
                        hoy_elements[0].click()
                        time.sleep(3)
                        driver.save_screenshot("selected_today_option.png")
                        # Considerar isso como uma seleção bem-sucedida
                        date_selection = True
                    else:
                        # Tentar seleção normal
                        date_selection = select_date_range(driver, start_date, end_date, logger)
                except Exception as e:
                    logger.warning(f"Erro ao tentar usar opção 'Hoy': {str(e)}")
                    date_selection = select_date_range(driver, start_date, end_date, logger)
            elif is_recent_date and (today - start_date_obj).days <= 7:
                # Últimos 7 dias - tentar usar a opção predefinida
                try:
                    last7_elements = driver.find_elements(By.XPATH, 
                                                      "//div[contains(text(), 'ltimos 7 d') or contains(text(), 'Last 7 d')]")
                    if last7_elements:
                        logger.info("Usando opção predefinida 'Últimos 7 días' para período recente")
                        last7_elements[0].click()
                        time.sleep(3)
                        driver.save_screenshot("selected_last7days_option.png")
                        # Considerar isso como uma seleção bem-sucedida
                        date_selection = True
                    else:
                        # Tentar seleção normal
                        date_selection = select_date_range(driver, start_date, end_date, logger)
                except Exception as e:
                    logger.warning(f"Erro ao tentar usar opção 'Últimos 7 días': {str(e)}")
                    date_selection = select_date_range(driver, start_date, end_date, logger)
            else:
                # Para outros períodos, usar a seleção normal
                date_selection = select_date_range(driver, start_date, end_date, logger)
            
            if not date_selection:
                logger.error("Falha ao selecionar intervalo de datas")
                if driver:
                    try:
                        driver.quit()
                    except:
                        pass
                time.sleep(3)
                continue
            
            # NOVO: Se chegou até aqui, considerar a seleção de datas bem-sucedida
            # Agora precisamos verificar se há dados ou não
            
            # Tentar clicar em qualquer botão de filtro/busca que possa existir
            try:
                filter_buttons = driver.find_elements(By.XPATH, 
                    "//button[contains(text(), 'Filtrar') or contains(text(), 'Filter') or contains(text(), 'Buscar')]")
                if filter_buttons:
                    filter_buttons[0].click()
                    logger.info(f"Clicou no botão de filtro: {filter_buttons[0].text}")
                    time.sleep(5)  # Esperar mais para carregar dados
            except:
                pass
            
            # Extrair dados dos produtos
            product_data = extract_product_data(driver, logger)
            
            # Fechar o driver após extração de dados
            if driver:
                try:
                    driver.quit()
                    driver = None
                except:
                    pass
            
            # Para períodos recentes, é comum não ter dados - tratar isso como sucesso
            if is_recent_date and not product_data:
                logger.info("Período recente sem dados disponíveis - considerando como atualização bem-sucedida")
                
                # Salvar um registro vazio para indicar que verificamos, mas não há dados
                save_result = save_dropi_metrics_to_db(store["id"], date_str, [], start_date_str, end_date_str)
                
                if save_result:
                    logger.info("Registro de 'sem dados' salvo com sucesso para o período recente")
                    return True
                else:
                    logger.error("Falha ao salvar registro de 'sem dados'")
                    return False
            
            # Se não encontrou produtos e não é período recente, tentar novamente
            if not product_data and retry_count < max_retries:
                logger.warning(f"Nenhum produto encontrado na tentativa {retry_count}. Tentando novamente...")
                time.sleep(3)
                continue
            
            # Salvar os dados encontrados
            save_result = save_dropi_metrics_to_db(store["id"], date_str, product_data, start_date_str, end_date_str)
            
            if save_result:
                logger.info(f"Dados salvos com sucesso! {len(product_data)} produtos encontrados")
                success = True
                return True
            else:
                logger.error("Falha ao salvar dados no banco")
                if retry_count < max_retries:
                    time.sleep(3)
                    continue
                else:
                    return False
                
        except Exception as e:
            logger.error(f"Erro na tentativa {retry_count} de atualizar dados: {str(e)}")
            
            if driver:
                try:
                    driver.save_screenshot(f"error_retry_{retry_count}.png")
                    driver.quit()
                    driver = None
                except:
                    pass
            
            if retry_count < max_retries:
                # Esperar antes de tentar novamente
                wait_time = 3 * retry_count  # Tempo crescente entre tentativas
                logger.info(f"Aguardando {wait_time} segundos antes da próxima tentativa...")
                time.sleep(wait_time)
            else:
                return False
    
    # Se chegou aqui, todas as tentativas falharam
    return success
    
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
    
    # Adicionar containers de seção principal para agrupar conteúdo
    #st.markdown('<div class="main-section">', unsafe_allow_html=True)
    
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

    # Crie colunas para a tabela e o gráfico lado a lado
    shopify_col1, shopify_col2 = st.columns(2)

    with shopify_col1:
        # Tabela de produtos
        display_shopify_data(shopify_data, selected_category)

    with shopify_col2:
        # Gráfico
        display_shopify_chart(shopify_data, selected_category)

    # Fechando a seção principal Shopify
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Linha divisória entre as seções
    st.markdown('<hr>', unsafe_allow_html=True)
    
    # Abrindo uma nova seção principal para Dropi
    #st.markdown('<div class="main-section">', unsafe_allow_html=True)

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

    # Crie colunas para a tabela e o gráfico lado a lado
    dropi_col1, dropi_col2 = st.columns(2)

    with dropi_col1:
        # Exibir tabela de produtos Dropi
        if not dropi_data.empty:
            st.subheader("Produtos Dropi")
            
            # Primeiro, criar uma cópia do DataFrame sem a coluna 'date'
            display_df = dropi_data.drop(columns=['date'], errors='ignore')
            
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

    with dropi_col2:
        # Exibir gráfico interativo para os dados Dropi
        if not dropi_data.empty:
            # Preparar dados para gráfico
            chart_data = dropi_data[['product', 'orders_count', 'transit_count', 'delivered_count']].copy()
            
            # Ordenar por número de pedidos para melhor visualização
            chart_data = chart_data.sort_values('orders_count', ascending=False)
            
            st.subheader("Gráfico de Produtos Dropi")
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

    # Fechando a seção principal Dropi
    st.markdown('</div>', unsafe_allow_html=True)
    
    # ========== SEÇÃO DE EFETIVIDADE (Parte de Dropi) ==========
    st.markdown('<hr>', unsafe_allow_html=True)
    
    # Abrindo nova seção para análise de efetividade
    #st.markdown('<div class="main-section">', unsafe_allow_html=True)
    st.markdown('<h4>ANÁLISE DE EFETIVIDADE</h4>', unsafe_allow_html=True)

    # Exibir tabela de efetividade para o intervalo selecionado
    display_effectiveness_table(store["id"], dropi_start_date_str, dropi_end_date_str)
    
    # Fechando a seção de análise de efetividade
    st.markdown('</div>', unsafe_allow_html=True)

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