import os
import psycopg2
import sqlite3
import streamlit as st
from datetime import datetime
import logging

# Configuração de logger
logger = logging.getLogger("db_utils")

def is_railway_environment():
    """Verifica se estamos rodando no Railway."""
    return os.getenv("RAILWAY_ENVIRONMENT") is not None or os.getenv("DATABASE_URL") is not None

def get_db_connection():
    """Retorna uma conexão com o banco de dados."""
    if is_railway_environment():
        # Ambiente Railway - PostgreSQL
        try:
            database_url = os.getenv("DATABASE_URL")
            if not database_url:
                st.error("DATABASE_URL não encontrada no ambiente Railway")
                raise ValueError("DATABASE_URL não encontrada")
                
            conn = psycopg2.connect(database_url)
            return conn
        except Exception as e:
            st.error(f"Erro ao conectar ao PostgreSQL: {str(e)}")
            raise e
    else:
        # Ambiente local - SQLite
        try:
            conn = sqlite3.connect("dashboard.db")
            return conn
        except Exception as e:
            st.error(f"Erro ao conectar ao SQLite: {str(e)}")
            raise e

def execute_query(query, params=None, fetch_type=None):
    """
    Executa uma query adaptando-a ao banco de dados correto.
    
    Args:
        query: A consulta SQL com placeholders '?'
        params: Tupla de parâmetros para a consulta
        fetch_type: 'one', 'all', ou None para não buscar resultados
        
    Returns:
        Resultados da consulta ou None se fetch_type=None
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Adaptar a query para o banco correto
        if is_railway_environment():
            # PostgreSQL usa %s como placeholder
            adapted_query = query.replace("?", "%s")
        else:
            # SQLite mantém ? como placeholder
            adapted_query = query
        
        # Executar a consulta
        if params:
            cursor.execute(adapted_query, params)
        else:
            cursor.execute(adapted_query)
        
        # Buscar resultados se necessário
        if fetch_type == 'one':
            result = cursor.fetchone()
        elif fetch_type == 'all':
            result = cursor.fetchall()
        else:
            result = None
            conn.commit()
        
        return result
    except Exception as e:
        logger.error(f"Erro ao executar query: {str(e)}")
        conn.rollback()
        raise e
    finally:
        conn.close()

def execute_upsert(table, data, keys):
    """
    Executa uma operação UPSERT (INSERT or UPDATE) adaptada ao banco de dados.
    
    Args:
        table: Nome da tabela
        data: Dicionário com os dados a serem inseridos/atualizados
        keys: Lista de colunas que formam a chave primária
    """
    columns = list(data.keys())
    values = list(data.values())
    
    placeholders = ["?"] * len(columns)
    
    # Construir a consulta base
    query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
    
    # Adaptar para UPSERT conforme o banco
    if is_railway_environment():
        # PostgreSQL
        conflict_cols = ", ".join(keys)
        update_cols = [c for c in columns if c not in keys]
        update_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
        
        query = query.replace("?", "%s")
        query += f" ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_clause}"
    else:
        # SQLite
        conflict_cols = ", ".join(keys)
        update_cols = [c for c in columns if c not in keys]
        update_clause = ", ".join([f"{c} = excluded.{c}" for c in update_cols])
        
        query += f" ON CONFLICT({conflict_cols}) DO UPDATE SET {update_clause}"
    
    # Executar
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute(query, values)
        conn.commit()
    except Exception as e:
        logger.error(f"Erro no UPSERT: {str(e)}")
        conn.rollback()
        raise e
    finally:
        conn.close()

def init_db():
    """Inicializa as tabelas no banco de dados."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Tabela para armazenar as lojas
        if is_railway_environment():
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
                    currency_to TEXT DEFAULT 'BRL',
                    is_custom BOOLEAN DEFAULT FALSE
                )
            """)
            
            # Verificar se a coluna is_custom existe
            try:
                cursor.execute("""
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = 'stores' AND column_name = 'is_custom'
                """)
                if not cursor.fetchone():
                    cursor.execute("ALTER TABLE stores ADD COLUMN is_custom BOOLEAN DEFAULT FALSE")
            except Exception as e:
                logger.error(f"Erro ao verificar coluna is_custom: {str(e)}")
            
            # Tabela para métricas de produtos
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
            
            # Tabela para métricas da DroPi
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
            
            # Tabela para métricas de efetividade
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS product_effectiveness (
                    store_id TEXT,
                    product TEXT,
                    general_effectiveness FLOAT,
                    last_updated TEXT,
                    PRIMARY KEY (store_id, product)
                )
            """)
            
            # Tabela para valores personalizados
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS custom_product_data (
                    store_id TEXT,
                    product TEXT,
                    custom_id TEXT,
                    custom_provider TEXT,
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
                    currency_to TEXT DEFAULT 'BRL',
                    is_custom INTEGER DEFAULT 0
                )
            """)
            
            # Verificar se a coluna is_custom existe
            try:
                cursor.execute("PRAGMA table_info(stores)")
                columns = [column[1] for column in cursor.fetchall()]
                if 'is_custom' not in columns:
                    cursor.execute("ALTER TABLE stores ADD COLUMN is_custom INTEGER DEFAULT 0")
            except Exception as e:
                logger.error(f"Erro ao verificar coluna is_custom: {str(e)}")
            
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
            
            # Tabela para valores personalizados
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS custom_product_data (
                    store_id TEXT,
                    product TEXT,
                    custom_id TEXT,
                    custom_provider TEXT,
                    last_updated TEXT,
                    PRIMARY KEY (store_id, product)
                )
            """)
        
        conn.commit()
        logger.info("Banco de dados inicializado com sucesso")
    except Exception as e:
        logger.error(f"Erro ao inicializar banco: {str(e)}")
        conn.rollback()
        raise e
    finally:
        conn.close()

# Funções específicas para operações comuns
def load_stores():
    """Carrega a lista de lojas cadastradas."""
    try:
        # Inicializa o banco se necessário
        init_db()
        
        # Consulta as lojas
        result = execute_query("SELECT id, name FROM stores", fetch_type='all')
        return result or []
    except Exception as e:
        logger.error(f"Erro ao carregar lojas: {str(e)}")
        return []

def get_store_details(store_id):
    """Obtém os detalhes de uma loja específica."""
    try:
        result = execute_query(
            "SELECT name, shop_name, access_token, dropi_url, dropi_username, dropi_password, currency_from, currency_to, is_custom FROM stores WHERE id = ?", 
            (store_id,), 
            fetch_type='one'
        )
        
        if result:
            return {
                "name": result[0],
                "shop_name": result[1],
                "access_token": result[2],
                "dropi_url": result[3] or "https://app.dropi.mx/",
                "dropi_username": result[4],
                "dropi_password": result[5],
                "currency_from": result[6] or "MXN",
                "currency_to": result[7] or "BRL",
                "is_custom": bool(result[8]),
                "id": store_id
            }
        return None
    except Exception as e:
        logger.error(f"Erro ao obter detalhes da loja: {str(e)}")
        return None

def save_store(name, shop_name, access_token, dropi_url="", dropi_username="", dropi_password="", currency_from="MXN", currency_to="BRL", is_custom=False):
    """Salva uma nova loja no banco de dados."""
    import uuid
    store_id = str(uuid.uuid4())
    
    data = {
        "id": store_id,
        "name": name,
        "shop_name": shop_name,
        "access_token": access_token,
        "dropi_url": dropi_url,
        "dropi_username": dropi_username,
        "dropi_password": dropi_password,
        "currency_from": currency_from,
        "currency_to": currency_to,
        "is_custom": is_custom
    }
    
    try:
        execute_upsert("stores", data, ["id"])
        return store_id
    except Exception as e:
        logger.error(f"Erro ao salvar loja: {str(e)}")
        st.error(f"Erro ao salvar loja: {str(e)}")
        return None

def get_store_currency(store_id):
    """Obtém as configurações de moeda da loja."""
    try:
        result = execute_query(
            "SELECT currency_from, currency_to FROM stores WHERE id = ?", 
            (store_id,), 
            fetch_type='one'
        )
        
        if result:
            return {
                "from": result[0] or "MXN",
                "to": result[1] or "BRL"
            }
        return {"from": "MXN", "to": "BRL"}  # Valores padrão
    except Exception as e:
        logger.error(f"Erro ao obter configurações de moeda: {str(e)}")
        return {"from": "MXN", "to": "BRL"}  # Valores padrão em caso de erro

def save_effectiveness(store_id, product, value):
    """Salva valor de efetividade para um produto."""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    data = {
        "store_id": store_id,
        "product": product,
        "general_effectiveness": value,
        "last_updated": current_time
    }
    
    try:
        execute_upsert("product_effectiveness", data, ["store_id", "product"])
        return True
    except Exception as e:
        logger.error(f"Erro ao salvar efetividade: {str(e)}")
        return False
    
def update_dropi_metrics_schema():
    """Atualiza o esquema da tabela dropi_metrics para suportar intervalos de data."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Verificar se as colunas já existem
        if is_railway_environment():
            # PostgreSQL
            cursor.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'dropi_metrics' AND column_name = 'date_start'
            """)
        else:
            # SQLite
            cursor.execute("PRAGMA table_info(dropi_metrics)")
            columns = [column[1] for column in cursor.fetchall()]
            column_exists = 'date_start' in columns
            
            if column_exists:
                conn.close()
                return  # As colunas já existem, não faz nada
        
        # Criar nova tabela com as colunas adicionais
        if is_railway_environment():
            # PostgreSQL usa ALTER TABLE para adicionar colunas
            cursor.execute("ALTER TABLE dropi_metrics ADD COLUMN IF NOT EXISTS date_start TEXT")
            cursor.execute("ALTER TABLE dropi_metrics ADD COLUMN IF NOT EXISTS date_end TEXT")
            
            # Atualizar os dados existentes
            cursor.execute("UPDATE dropi_metrics SET date_start = date, date_end = date WHERE date_start IS NULL")
        else:
            # SQLite - Criar tabela temporária, migrar dados e renomear
            cursor.execute("""
                CREATE TABLE dropi_metrics_new (
                    store_id TEXT,
                    date TEXT,
                    date_start TEXT,
                    date_end TEXT,
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
            
            # Copiar dados antigos para a nova tabela
            cursor.execute("""
                INSERT INTO dropi_metrics_new 
                SELECT store_id, date, date, date, product, provider, stock, 
                      orders_count, orders_value, transit_count, transit_value, 
                      delivered_count, delivered_value, profits
                FROM dropi_metrics
            """)
            
            # Substituir a tabela antiga pela nova
            cursor.execute("DROP TABLE dropi_metrics")
            cursor.execute("ALTER TABLE dropi_metrics_new RENAME TO dropi_metrics")
        
        conn.commit()
        logger.info("Esquema da tabela dropi_metrics atualizado com sucesso")
    except Exception as e:
        logger.error(f"Erro ao atualizar esquema: {str(e)}")
        conn.rollback()
    finally:
        conn.close()

def delete_store_by_id(store_id):
    """
    Remove uma loja e todos os seus dados relacionados do banco de dados.
    
    Args:
        store_id: ID da loja a ser removida
        
    Returns:
        Tuple: (sucesso, mensagem)
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Armazenar contadores para relatório
    deleted_counts = {
        "product_metrics": 0,
        "dropi_metrics": 0,
        "product_effectiveness": 0,
        "stores": 0
    }
    
    try:
        # Iniciar transação explicitamente
        if is_railway_environment():
            cursor.execute("BEGIN")
        else:
            cursor.execute("BEGIN TRANSACTION")
        
        # 1. Excluir registros da tabela product_metrics
        if is_railway_environment():
            cursor.execute("DELETE FROM product_metrics WHERE store_id = %s", (store_id,))
        else:
            cursor.execute("DELETE FROM product_metrics WHERE store_id = ?", (store_id,))
        deleted_counts["product_metrics"] = cursor.rowcount
        
        # 2. Excluir registros da tabela dropi_metrics
        if is_railway_environment():
            cursor.execute("DELETE FROM dropi_metrics WHERE store_id = %s", (store_id,))
        else:
            cursor.execute("DELETE FROM dropi_metrics WHERE store_id = ?", (store_id,))
        deleted_counts["dropi_metrics"] = cursor.rowcount
        
        # 3. Excluir registros da tabela product_effectiveness
        if is_railway_environment():
            cursor.execute("DELETE FROM product_effectiveness WHERE store_id = %s", (store_id,))
        else:
            cursor.execute("DELETE FROM product_effectiveness WHERE store_id = ?", (store_id,))
        deleted_counts["product_effectiveness"] = cursor.rowcount
        
        # 4. Finalmente, excluir a loja
        if is_railway_environment():
            cursor.execute("DELETE FROM stores WHERE id = %s", (store_id,))
        else:
            cursor.execute("DELETE FROM stores WHERE id = ?", (store_id,))
        deleted_counts["stores"] = cursor.rowcount
        
        # Confirmar a transação
        conn.commit()
        
        # Verificar se a loja foi realmente excluída
        if deleted_counts["stores"] == 0:
            return False, f"Loja com ID {store_id} não foi encontrada no banco de dados."
        
        # Preparar relatório de sucesso
        total_deleted = sum(deleted_counts.values())
        detail_msg = ", ".join([f"{table}: {count}" for table, count in deleted_counts.items()])
        
        return True, f"Loja excluída com sucesso! Total de {total_deleted} registros removidos ({detail_msg})"
        
    except Exception as e:
        # Reverter em caso de erro
        try:
            conn.rollback()
        except:
            pass
        logger.error(f"Erro ao excluir loja: {str(e)}")
        return False, f"Erro ao excluir loja: {str(e)}"
        
    finally:
        # Garantir que a conexão seja fechada
        try:
            conn.close()
        except:
            pass