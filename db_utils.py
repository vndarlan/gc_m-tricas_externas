import os
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

def get_db_connection():
    """Retorna uma conexão com o banco de dados PostgreSQL."""
    # Use a URL do banco fornecida pelo Railway ou do arquivo .env local
    database_url = os.getenv("DATABASE_URL")
    
    if not database_url:
        raise ValueError("DATABASE_URL não encontrada nas variáveis de ambiente")
    
    # Conectar ao PostgreSQL
    conn = psycopg2.connect(database_url)
    conn.autocommit = False  # Gerenciar transações explicitamente
    
    return conn

def init_db():
    """Inicializa as tabelas no banco de dados PostgreSQL."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Criar tabelas usando sintaxe PostgreSQL
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stores (
            id TEXT PRIMARY KEY,
            name TEXT,
            shop_name TEXT,
            access_token TEXT,
            dropi_url TEXT,
            dropi_username TEXT,
            dropi_password TEXT,
            currency_from TEXT DEFAULT 'MXN',
            currency_to TEXT DEFAULT 'BRL'
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS product_metrics (
            store_id TEXT,
            date TEXT,
            product TEXT,
            total_orders INTEGER,
            processed_orders INTEGER,
            delivered_orders INTEGER,
            total_value FLOAT DEFAULT 0,
            product_url TEXT,
            PRIMARY KEY (store_id, date, product)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dropi_metrics (
            store_id TEXT,
            date TEXT,
            product TEXT,
            provider TEXT,
            stock INTEGER,
            orders_count INTEGER,
            orders_value FLOAT,
            transit_count INTEGER, 
            transit_value FLOAT,
            delivered_count INTEGER,
            delivered_value FLOAT,
            profits FLOAT,
            PRIMARY KEY (store_id, date, product)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS product_effectiveness (
            store_id TEXT,
            product TEXT,
            general_effectiveness FLOAT,
            last_updated TEXT,
            PRIMARY KEY (store_id, product)
        )
    """)
    
    conn.commit()
    conn.close()