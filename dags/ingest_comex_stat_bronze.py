from airflow.sdk import dag, task
from datetime import datetime
import duckdb
import requests
import os
import pandas as pd

# Configurações de caminho
BASE_PATH = "/usr/local/airflow/include/data/bronze"

@dag(
    dag_id='ingest_comex_stat_bronze',
    schedule='@monthly',
    start_date=datetime(2026, 1, 1), # Ajustado para o ano atual
    catchup=False,
    tags=['bronze', 'comex', 'ingestion'],
)
def comex_stat_ingestion():

    @task()
    def download_comex_data():
        """
        Downloads the 2026 Import CSV from Comex Stat and saves it to Bronze.
        """
        url = "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/IMP_2026.csv"
        file_path = os.path.join(BASE_PATH, "IMP_2026.csv")
        
        print(f"Downloading from: {url}")
        
        # Fazendo o download
        response = requests.get(url, stream=True, verify=False)
        response.raise_for_status() # Garante que se o site cair, a Task falha
        
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f"File successfully saved to: {file_path}")
        return file_path


    @task()
    def check_bronze_data(file_path):
        """
        Reads the first 5 rows of the downloaded CSV to check its structure.
        """
        
        print(f"Checking data from: {file_path}")
        
        # O 'sep' e o 'encoding' são o 'pulo do gato' para dados do governo BR
        df = pd.read_csv(file_path, sep=';', encoding='latin-1', nrows=5)
        
        print("-------------------- DATA SAMPLE --------------------")
        print(df.to_string())
        print("-----------------------------------------------------")
        print(f"Columns found: {list(df.columns)}")
        
        return "Check complete!"
    
    @task()
    def transform_bronze_to_silver(csv_path):

        # Definindo onde o Parquet vai morar
        # Note que trocamos 'bronze' por 'silver' no caminho
        silver_path = csv_path.replace('bronze', 'silver').replace('.csv', '.parquet')
        
        # Criando a pasta silver se ela não existir (prevenção!)
        os.makedirs(os.path.dirname(silver_path), exist_ok=True)

        print(f"Transforming {csv_path} to {silver_path}...")

        # O SQL que faz tudo: Renomeia, Filtra e Converte
        query = f"""
            COPY (
                SELECT 
                    CO_ANO AS year,
                    CO_MES AS month,
                    SG_UF_NCM AS state,
                    CO_PAIS AS country_code,
                    VL_FOB AS value_usd,
                    ROUND(VL_FOB * 5.25, 2) AS value_brl, 
                    'IMPORT' AS flow_type
                FROM read_csv_auto('{csv_path}', sep=';', encoding='latin-1')
                WHERE VL_FOB > 0 -- Limpeza básica
            ) TO '{silver_path}' (FORMAT 'PARQUET');
        """
        
        duckdb.sql(query)
        print(f"Silver layer success! File: {silver_path}")
        return silver_path


    # 1. Baixa o dado
    csv_file = download_comex_data()

    # 2. Faz o check (opcional, mas bom pra log)
    check_bronze_data(csv_file)

    # 3. TRANSFORMA EM SILVER!
    transform_bronze_to_silver(csv_file)


comex_stat_ingestion()

