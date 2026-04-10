from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task
from datetime import datetime
import duckdb
from ncm.client import FetchNcm
import os
import pandas as pd
import requests

# Configurações de caminho
BASE_PATH = "/usr/local/airflow/include/data/bronze"

@dag(
    dag_id='ingest_comex_stat_bronze_silver',
    schedule='@yearly',
    start_date=datetime(2015, 12, 31), #ano inicial dos dados disponíveis
    catchup=True,
    tags=['bronze', 'comex', 'ingestion'],
)
def comex_stat_ingestion():

    @task()
    def download_comex_data(**context):
        """
        Downloads the {year} Import CSV from Comex Stat and saves it to Bronze.
        """
        year = context['data_interval_start'].year
        url = f"https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/IMP_{year}.csv"
        file_path = os.path.join(BASE_PATH, f"IMP_{year}.csv")
        
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

        
        # 1. Busca a tabela de NCM e transforma em DataFrame
        fetch_ncm = FetchNcm()
        ncm_data = fetch_ncm.get_all(only_ncm_8_digits=True)
        ncm_dicts = [
            {'CO_NCM': n.codigo_ncm, 'NO_NCM_POR': n.descricao_ncm} 
            for n in ncm_data.ncm_list
        ]
        df_ncm = pd.DataFrame(ncm_dicts)

        # O SQL que faz tudo: Renomeia, Filtra e Converte
        query = f"""
            COPY (
                SELECT 
                    imp.CO_ANO AS year,
                    imp.CO_MES AS month,
                    imp.SG_UF_NCM AS state,
                    imp.CO_PAIS AS country_code,
                    imp.VL_FOB AS value_usd,
                    'IMPORT' AS flow_type,
                    imp.CO_NCM AS ncm_code,
                    ncm.NO_NCM_POR as ncm_description
                FROM read_csv_auto('{csv_path}', sep=';', encoding='latin-1') imp
                LEFT JOIN df_ncm AS ncm
                ON CAST(imp.CO_NCM AS VARCHAR) = CAST(ncm.CO_NCM AS VARCHAR)
                WHERE imp.VL_FOB > 0 -- Limpeza básica
            ) TO '{silver_path}' (FORMAT 'PARQUET');
        """

        
        duckdb.sql(query)
        print(f"Silver layer success! File: {silver_path}")
        return silver_path
    
    # Bronze
    csv_file = download_comex_data()
    check_bronze_data(csv_file)
    
    # Silver
    parquet_silver = transform_bronze_to_silver(csv_file)
    
    # Task para chamar a DAG Gold
    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_comex_stat_gold",
        trigger_dag_id="comex_stat_gold", # O dag_id que está no outro arquivo
        wait_for_completion=True, # Espera a Gold terminar antes de dar essa como sucesso
        conf={"target_year": "{{ data_interval_start.year }}"}
    )
    parquet_silver >> trigger_gold


comex_stat_ingestion()
