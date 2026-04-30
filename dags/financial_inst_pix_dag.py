import os
from datetime import datetime
from pathlib import Path
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import SQLExecuteQueryOperator

# Imports das funções de cada etapa do pipeline
from src.extract_data import extract_pix_data, extract_bcb_reference
from src.transform_data import process_transformation
from src.load import save_to_silver, load_to_db

# Configurações lidas do ambiente (Docker + .env)
DB_URL = os.getenv("DB_URL")
PATH_PARQUET = os.getenv("PARQUET_PATH", "/opt/airflow/data/dados_instituicoes.parquet")
PATH_SQL = os.getenv("SQL_SCRIPT_PATH", "/opt/airflow/scripts_sql/create_table.sql")
BCB_BASE_URL = os.getenv("BCB_BASE_URL")
BCB_FILTERS = os.getenv("BCB_FILTERS")

@dag(
    dag_id='pipeline_instituicoes_financeiras_v3',
    start_date=datetime(2026, 4, 1),
    schedule='@daily',
    catchup=False,
    tags=['financeiro', 'pix', 'etl']
)
def financial_etl():

    # Tarefa 1: Limpeza da tabela (Garante que a carga seja limpa)
    # Usa a conexão 'postgres_default' injetada via variável de ambiente no Docker
    clean_table = SQLExecuteQueryOperator(
        task_id='truncate_target_table',
        postgres_conn_id='postgres_default',
        sql="TRUNCATE TABLE financial_inst_pix;"
    )

    # Tarefa 2: Extração
    @task
    def extract():
        # Obtém a referência necessária (a data ou valor dinâmico da função)
        ref_date = extract_bcb_reference()
        
        # Realiza a extração principal usando as variáveis do .env e a referência
        return extract_pix_data(
            base_url=BCB_BASE_URL, 
            filters=BCB_FILTERS, 
            reference=ref_date
        )

    # Tarefa 3: Transformação
    @task
    def transform(raw_data):
        return process_transformation(raw_data)

    # Tarefa 4: Carga Dupla (Silver Layer + Banco de Dados)
    @task
    def load(transformed_data):
        save_to_silver(transformed_data, PATH_PARQUET)
        
        load_to_db(
            db_url=DB_URL, 
            data=transformed_data, 
            sql_path=PATH_SQL
        )

    # Fluxo de Execução
    dados_brutos = extract()
    dados_processados = transform(dados_brutos)
    
    # A limpeza do banco antes da carga final
    clean_table >> load(dados_processados)

financial_etl()