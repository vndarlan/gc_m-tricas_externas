import os
import psycopg2
import sqlite3
import streamlit as st

def get_db_connection():
    """Retorna uma conexão com o banco de dados (PostgreSQL no Railway, SQLite localmente)."""
    # Verificar se estamos no Railway (ambiente de produção)
    database_url = os.getenv("DATABASE_URL")
    
    if database_url:
        # Estamos no Railway, usar PostgreSQL
        try:
            conn = psycopg2.connect(database_url)
            return conn
        except Exception as e:
            st.error(f"Erro ao conectar ao PostgreSQL: {str(e)}")
            raise e
    else:
        # Ambiente local, usar SQLite
        try:
            conn = sqlite3.connect("dashboard.db")
            return conn
        except Exception as e:
            st.error(f"Erro ao conectar ao SQLite: {str(e)}")
            raise e

def init_db():
    """Inicializa as tabelas no banco de dados."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Verificar se estamos usando PostgreSQL
        if os.getenv("DATABASE_URL"):
            # PostgreSQL
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
        else:
            # SQLite
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
                    total_value REAL DEFAULT 0,
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
                    orders_value REAL,
                    transit_count INTEGER, 
                    transit_value REAL,
                    delivered_count INTEGER,
                    delivered_value REAL,
                    profits REAL,
                    PRIMARY KEY (store_id, date, product)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS product_effectiveness (
                    store_id TEXT,
                    product TEXT,
                    general_effectiveness REAL,
                    last_updated TEXT,
                    PRIMARY KEY (store_id, product)
                )
            """)
    except Exception as e:
        st.error(f"Erro ao criar tabelas: {str(e)}")
        conn.rollback()
        raise e
    
    conn.commit()
    conn.close()
    
    st.success("Banco de dados inicializado com sucesso!")