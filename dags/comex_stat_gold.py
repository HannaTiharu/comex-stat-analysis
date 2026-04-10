from airflow.sdk import dag, task
from datetime import datetime
import duckdb
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import matplotlib.ticker as ticker
import os
import pandas as pd
from ncm.client import FetchNcm

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
    def create_gold_summary(**context):

        year = context['dag_run'].conf.get('target_year', context['data_interval_start'].year)
        output_path = f"/usr/local/airflow/include/data/gold/ncm_ranking_per_state_{year}.parquet"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # 4. Faz a mágica no DuckDB
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
        # Criamos um mapeamento: cada estado terá sempre a mesma cor da paleta Set3
        unique_states = sorted(df['state'].unique())
        cmap = plt.get_cmap('Set3')
        state_colors = {state: cmap(i / len(unique_states)) for i, state in enumerate(unique_states)}
            
        # --- Configurações de Design (Modo Suave) ---
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

        # --- A Mágica do Desenho (Suave e Colorido) ---
        def update(year):
            ax.clear()
            dff = df[df['year'] == year].head(10).iloc[::-1]
            
            # Atribui a cor fixa do estado
            colors = [state_colors[state] for state in dff['state']]
            
            bars = ax.barh(dff['state'], dff['total_usd'], color=colors, edgecolor='#AAAAAA', linewidth=0.5)
            
            # --- O ANO GIGANTE NO FUNDO (A sua referência!) ---
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
            ax.set_xlabel('Total Importado (USD)', fontsize=10, color='#555555')
            
            # Configura o básico (tamanho e cor) para ambos
            ax.tick_params(axis='x', labelsize=9, labelcolor='#555555')
            ax.tick_params(axis='y', labelsize=12, labelcolor='black')
            
            # Remove Eixo Y (Labels agora estão nas barras) e bordas
            ax.set_yticks([]) # Remove os labels 'SP', 'RJ' do eixo
            for spine in ['top', 'right', 'left']: ax.spines[spine].set_visible(False)
            ax.grid(axis='x', linestyle='--', alpha=0.3, color='#DDDDDD')
            ax.set_axisbelow(True) # Grade fica atrás das barras e do texto gigante
            
            # Remove as bordas do gráfico (deixa mais limpo)
            for spine in ['top', 'right']:
                ax.spines[spine].set_visible(False)
                
            ax.grid(axis='x', linestyle='--', alpha=0.3, color='#DDDDDD')
            

        ani = animation.FuncAnimation(fig, update, frames=anos, interval=1000, repeat=False)
        
        # Salva como GIF (mais fácil para o LinkedIn)
        output = "/usr/local/airflow/include/data/graphics/annual_race.gif"
        ani.save(output, writer='pillow')
        return output

    
    # Gold
    create_gold_summary()

    # cria o grafico  
    create_annual_bar_chart_race()


comex_stat_gold_processing()
