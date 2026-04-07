from airflow.sdk import dag, task
from datetime import datetime
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


    path = download_comex_data()
    check_bronze_data(path)


comex_stat_ingestion()

