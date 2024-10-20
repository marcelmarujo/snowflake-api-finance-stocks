# ingestão da API para o Snowflake (Camada RAW/Bronze)
import os
import requests
import snowflake.connector
from dotenv import load_dotenv

# Carregar variáveis do arquivo .env
load_dotenv()

# Carregar parâmetros sensíveis do .env
api_key = os.getenv('API_KEY')
user = os.getenv('SNOWFLAKE_USER')
password = os.getenv('SNOWFLAKE_PASSWORD')
account = os.getenv('SNOWFLAKE_ACCOUNT')
warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
database = os.getenv('SNOWFLAKE_DATABASE')
schema = os.getenv('SNOWFLAKE_SCHEMA')

# Função para coletar dados de várias ações
def get_stock_data(symbol, api_key):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=5min&apikey={api_key}'
    response = requests.get(url)
    return response.json()

# Lista de ações para consultar
stock_symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA'] 

# Conectar ao Snowflake
conn = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    warehouse=warehouse,
    database=database,
    schema=schema,
    login_timeout=60
)
cursor = conn.cursor()

# Criar tabela no snowflake
cursor.execute("""
    CREATE OR REPLACE TABLE finance_data (
        symbol STRING,
        price FLOAT,
        volume FLOAT,
        date TIMESTAMP
    );
""")

# Coletar dados de várias US STOCKS e inserir no Snowflake
for stock_symbol in stock_symbols:
    data = get_stock_data(stock_symbol, api_key)
    
    if 'Time Series (5min)' in data:
        for timestamp, values in data['Time Series (5min)'].items():
            price = float(values['1. open'])
            volume = float(values['5. volume'])
            cursor.execute(f"INSERT INTO finance_data (symbol, price, volume, date) VALUES ('{stock_symbol}', {price}, {volume}, '{timestamp}')")
    else:
        print(f"Erro ao coletar dados de {stock_symbol}")

# Fechar a conexão
conn.commit()
cursor.close()
conn.close()

print("Dados inseridos com sucesso!")
