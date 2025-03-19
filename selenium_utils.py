from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import os

def setup_selenium_for_cloud(headless=True):
    """Configura o Selenium para funcionar em ambiente cloud."""
    chrome_options = Options()
    
    # Configurações essenciais para ambiente cloud
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-gpu")
    
    # Tamanho da janela
    chrome_options.add_argument("--window-size=1920,1080")
    
    # Configurações adicionais de performance
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--dns-prefetch-disable")
    
    # No Railway/ambientes cloud, o ChromeDriver geralmente está no PATH
    driver = webdriver.Chrome(options=chrome_options)
    
    return driver