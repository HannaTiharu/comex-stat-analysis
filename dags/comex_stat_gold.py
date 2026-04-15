from airflow.sdk import dag, task
from datetime import datetime
import duckdb
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import matplotlib.ticker as ticker
import os
import pandas as pd
import seaborn as sns

# Configurações de caminho
BASE_PATH = "/usr/local/airflow/include/data/gold"

@dag(
    dag_id='comex_stat_gold',
    schedule='@yearly',
    start_date=datetime(2015, 12, 31), #ano inicial dos dados disponíveis
    catchup=False,
    tags=['gold', 'comex'],
)
def comex_stat_gold_processing():

    @task()
    def create_ncm_summary_sp(**context):

        year = context['dag_run'].conf.get('target_year', context['data_interval_start'].year)
        output_path = f"/usr/local/airflow/include/data/gold/ncm_ranking_per_state_{year}.parquet"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Lista de NCMs de SP para o ano específico.
        duckdb.sql(f"""
            COPY(
                SELECT 
                    year,
                    state,
                    ncm_code,
                    max(ncm_description) as ncm_description,
                    SUM(value_usd) / 1000000.0 as total_usd -- Em MILHÕES
                FROM '/usr/local/airflow/include/data/silver/IMP_{year}.parquet' 
                WHERE state = 'SP'
                GROUP BY year, state, ncm_code
                ORDER BY year ASC, state ASC, total_usd DESC
            ) TO '{output_path}' (FORMAT 'parquet')
        """)

        print(f"Arquivo Gold gerado em: {output_path}")
        
        return output_path
    
    
    @task()
    def create_annual_bar_chart_race(): 
        """
        O argumento 'wait_for_data' serve para o Airflow segurar essa task
        até que a Silver esteja pronta, mesmo que você não use a variável
        dentro da função (já que você usa o '*' no DuckDB).
        """
    # 1. Busca todos os anos na Silver
        query = """
            SELECT 
                year, 
                state, 
                SUM(value_usd) / 1000000000.0 as total_usd -- Em BILHÕES
            FROM read_parquet('/usr/local/airflow/include/data/silver/*.parquet')
            GROUP BY 1, 2
            ORDER BY year ASC, total_usd DESC
        """
        df = duckdb.query(query).to_df()
        anos = sorted(df['year'].unique())
        if not anos:
            print("Nenhum dado encontrado para gerar a animação.")
            return None
        
        # --- LÓGICA DE CORES FIXAS ---
        # Cada estado terá sempre a mesma cor da paleta Set3
        unique_states = sorted(df['state'].unique())
        cmap = plt.get_cmap('Set3')
        state_colors = {state: cmap(i / len(unique_states)) for i, state in enumerate(unique_states)}
            
        # --- Configurações de Design ---
        plt.style.use('seaborn-v0_8-whitegrid') # Um estilo claro, limpo e com grade suave
        fig, ax = plt.subplots(figsize=(10, 6), dpi=100)
        fig.patch.set_facecolor('white') # Garante que o fundo da figura é branco
        ax.set_facecolor('white')        # Garante que o fundo do gráfico é branco
        
        # --- Definição da Paleta de Cores (Agradáveis) ---
        # Usamos 'Pastel1' ou 'Set3' para tons suaves e coloridos
        cmap = plt.get_cmap('Set3')
        
        def format_usd_billions(x, pos):
            """Formata os números do eixo X (ex: 20.9B)"""
            if x >= 1: return f'{x:.1f}B'
            if x > 0: return f'{x*1000:.0f}M'
            return '0'
        
        formatter = plt.FuncFormatter(format_usd_billions)

        def update(year):
            ax.clear()
            dff = df[df['year'] == year].head(10).iloc[::-1]
            
            # Atribui a cor fixa do estado
            colors = [state_colors[state] for state in dff['state']]
            bars = ax.barh(dff['state'], dff['total_usd'], color=colors, edgecolor='#AAAAAA', linewidth=0.5)
            
            # --- O ANO GIGANTE NO FUNDO ---
            ax.text(0.95, 0.2, f'{year}', 
                    transform=ax.transAxes, 
                    fontsize=80, 
                    fontweight='bold', 
                    color='#DDDDDD', # Cinza muito suave
                    ha='right', va='center', 
                    alpha=0.6) # Leve transparência
            
            # --- LEGENDA DAS BARRAS  ---
            for i, bar in enumerate(bars):
                # 1. Nome do Estado (Negrito, dentro da barra)
                state_name = dff.iloc[i]['state']
                ax.text(1, bar.get_y() + bar.get_height()/2, # Pequeno pad (1B)
                        f'{state_name}', 
                        va='center', ha='left', fontsize=12, color='black', fontweight='bold')
                
                #2. Valor em USD (Negrito, dentro da barra, à direita do nome)
                width = bar.get_width()
                ax.text(width + 1, bar.get_y() + bar.get_height()/2, 
                        f'US$ {width:.1f}B', 
                        va='center', ha='left', fontsize=11, color='#444444', fontweight='normal')
            
            # --- EIXO X TRAVADO (0 a 100B) ---
            ax.set_xlim(0, 100)
            ax.xaxis.set_major_locator(ticker.MultipleLocator(10)) # Marcações de 10 em 10
            ax.xaxis.set_major_formatter(ticker.FuncFormatter(format_usd_billions))
            
            # Título Dinâmico (Letras Pretas)
            ax.set_title(f'Top 10 Estados Importadores ({df["year"].min()} à {df["year"].max()})', fontsize=16, fontweight='bold', color='#333333', pad=20)
            
            
            # Customização do Eixo X e Y (Letras Pretas e Suaves)
            ax.xaxis.set_major_formatter(formatter)
            ax.set_xlabel('Total Importado (USD em Bilhões)', fontsize=10, color='#555555')
            
            # Configura o básico (tamanho e cor) para ambos
            ax.tick_params(axis='x', labelsize=9, labelcolor='#555555')
            ax.tick_params(axis='y', labelsize=12, labelcolor='black')
            
            # Remove Eixo Y (Labels estão nas barras) e bordas
            ax.set_yticks([]) # Remove os labels 'SP', 'RJ' do eixo
            for spine in ['top', 'right', 'left']: ax.spines[spine].set_visible(False)
            ax.grid(axis='x', linestyle='--', alpha=0.3, color='#DDDDDD')
            ax.set_axisbelow(True) # Grade fica atrás das barras e do texto gigante
            
            # Remove as bordas do gráfico 
            for spine in ['top', 'right']:
                ax.spines[spine].set_visible(False)
                
            ax.grid(axis='x', linestyle='--', alpha=0.3, color='#DDDDDD')
            

        ani = animation.FuncAnimation(fig, update, frames=anos, interval=1000, repeat=False)
        
        # Salva como GIF (mais fácil para o LinkedIn)
        output = "/usr/local/airflow/include/data/graphics/annual_race.gif"
        ani.save(output, writer='ffmpeg')
        return output
    

    @task()
    def create_temporal_serie_sp_ncm_30043929(wait_path):    
        """
        Cria a série temporal de SP para o NCM 30043929 (NCM que inclui o medicamento GLP-1):
            Entram medicamentos prontos para uso que contêm hormônios de base proteica ou polipeptídica (exceto insulinas), 
            como os modernos análogos de GLP-1 para diabetes e obesidade.
        """    
        # 1. Leitura dos dados da Gold usando DuckDB
        # O DuckDB lê todos os arquivos da pasta Gold de uma vez
        query = """
            SELECT 
                year, 
                SUM(total_usd) as total_usd_millions
            FROM read_parquet('/usr/local/airflow/include/data/gold/*.parquet')
            WHERE ncm_code = '30043929'
            GROUP BY year
            ORDER BY year ASC
        """
        df = duckdb.query(query).to_df()

        if df.empty:
            print("Nenhum dado encontrado para o NCM 30043929.")
            return None

        # --- Configuração Visual ---
        plt.style.use('seaborn-v0_8-whitegrid')
        fig, ax = plt.subplots(figsize=(12, 6), dpi=120)
        fig.patch.set_facecolor('#FAFAFA')
        ax.set_facecolor('#FAFAFA')

        # Plotagem da linha com gradiente ou sombra (estilo moderno)
        line, = ax.plot(df['year'], df['total_usd_millions'], 
                        color='#2E7D32', marker='o', linewidth=3, 
                        markersize=8, markerfacecolor='white', markeredgewidth=2)

        # Preenchimento suave abaixo da linha (Area Chart)
        ax.fill_between(df['year'], df['total_usd_millions'], color='#4CAF50', alpha=0.1)

        # --- Títulos e Rótulos ---
        ax.set_title(f'Importação de NCM 3004.39.29 pelo Estado de SP ({df["year"].min()}-{df["year"].max()})', 
                    fontsize=16, fontweight='bold', pad=20, color='#333333')
        ax.set_xlabel('Ano', fontsize=12, color='#666666')
        ax.set_ylabel('Valor Total (USD em Milhões)', fontsize=12, color='#666666')

        # Ajuste de eixos para anos inteiros
        ax.set_xticks(df['year'])
        
        # Limpeza de bordas (Spines)
        for spine in ['top', 'right', 'left']:
            ax.spines[spine].set_visible(False)

        # Adicionando rótulos de valores sobre os pontos
        for x, y in zip(df['year'], df['total_usd_millions']):
            ax.annotate(f'US$ {y:.1f}M', 
                        xy=(x, y), xytext=(0, 10), 
                        textcoords='offset points', ha='center', 
                        fontsize=10, fontweight='bold', color='#2E7D32')

        plt.tight_layout()
        
        # Salva o resultado
        output_img = "/usr/local/airflow/include/data/graphics/ncm_30043929_series.png"
        os.makedirs(os.path.dirname(output_img), exist_ok=True)
        plt.savefig(output_img, facecolor=fig.get_facecolor())
        print(f"Série temporal salva em: {output_img}")
        
        return output_img

    
    # Gold
    ncm_summary = create_ncm_summary_sp()

    # cria o gif da corrida dos estados
    create_annual_bar_chart_race()

    # cria a serie temporal de SP para o ncm 30043929
    create_temporal_serie_sp_ncm_30043929(ncm_summary)



comex_stat_gold_processing()
