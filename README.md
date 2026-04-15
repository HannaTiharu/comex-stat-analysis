# 🌬️ Comex Stat Data Pipeline: From Raw Data to Market Insights 🦆

[![Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=Apache-Airflow&logoColor=white)](https://airflow.apache.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?style=for-the-badge&logo=duckdb&logoColor=black)](https://duckdb.org/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)](https://pandas.pydata.org/)

This project features a complete end-to-end ETL pipeline using Brazilian foreign trade open data (**Comex Stat**). The goal is to transform millions of raw records into **Bronze**, **Silver**, and **Gold** layers, uncovering market trends hidden within technical NCM (Nomenclature Common of the Mercosur) codes.

---

## 🔍 The Insight: The GLP-1 (Mounjaro) Import Explosion
While auditing the **Gold Layer**, I identified a significant statistical anomaly in **NCM 3004.39.29** within the State of São Paulo. Under the generic description "Others," this item surged to the **2nd position in the 2026 import ranking** (as of March), surpassed only by Diesel Oil.

Further investigation revealed that this code serves as the tax classification for modern protein-based hormonal medications. Specifically, it tracks the influx of **GLP-1 analogues** (such as Mounjaro), reflecting a staggering **~700% growth** in import value between 2024 and 2025 in SP.

---

## 🛠️ Data Architecture

The pipeline follows the **Medallion Architecture** to ensure data quality and scalability:

1.  **Bronze Layer**: Raw ingestion of `.csv` files from the Federal Government.
2.  **Silver Layer**: Data cleaning, schema enforcement, and conversion to partitioned `.parquet` files.
3.  **Gold Layer**: Business Intelligence (BI) aggregations and automated visualization generation.

### Tech Stack
* **Orchestration**: Apache Airflow (via Astro CLI)
* **Processing Engine**: DuckDB (In-process OLAP for extreme performance)
* **Language**: Python
* **Visualization**: Matplotlib & Seaborn

---

## 📊 Automated Visualizations

### 1. Historical Import Ranking (State-wise)
<img width="1000" height="600" alt="annual_race" src="https://github.com/user-attachments/assets/7a8b77d5-32cb-4521-a438-c362f4c2e00e" />


### 2. Time Series Analysis: NCM 3004.39.29
<img width="1440" height="720" alt="ncm_30043929_series" src="https://github.com/user-attachments/assets/139a6257-5685-49d4-85bd-ab332f6dcb0c" />
*Figure 1: High-fidelity line chart highlighting the exponential growth of polypeptide hormonal medications in São Paulo.*

---

## 🚀 Getting Started

1.  **Prerequisites**: Install the [Astro CLI](https://www.astronomer.io/docs/cloud/stable/develop/astro-cli-install).
2.  **Clone the repository**:
    ```bash
    git clone [https://github.com/your-username/comex-stat-analysis.git](https://github.com/your-username/comex-stat-analysis.git)
    ```
3.  **Start the environment**:
    ```bash
    astro dev start
    ```
4.  **Run the Pipeline**: Access the Airflow UI at `localhost:8080` and trigger the `comex_stat_gold` DAG.

---

## 🧑‍💻 Author
**Hanna** - Software Engineer & Data Enthusiast.
[LinkedIn](https://br.linkedin.com/in/hanna-tiharu) | [GitHub](https://github.com/HannaTiharu)
