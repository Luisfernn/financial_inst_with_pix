import os
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


from src.extract_data import extract_pix_data, extract_bcb_reference
from src.transform_data import process_transformation
from src.load import save_to_silver, load_to_db

# Configurações do ambiente
URL_PIX = os.getenv("URL_PIX")
DB_URL = os.getenv("DB_URL")
PATH_PARQUET = os.getenv("PARQUET_PATH", "/opt/airflow/data/dados_instituicoes.parquet")
PATH_SQL = os.getenv("SQL_SCRIPT_PATH", "/opt/airflow/sql_scripts/create_table.sql")

@dag(
    dag_id='pipeline_instituicoes_financeiras_v3',
    start_date=datetime(2026, 4, 1),
    schedule='@daily',
    catchup=False,
    tags=['financeiro', 'pix', 'etl']
)
def financial_etl():

    # Limpeza da tabela de destino antes de cada carga
    clean_table = SQLExecuteQueryOperator(
        task_id='truncate_target_table',
        conn_id='postgres_pix',
        sql="TRUNCATE TABLE financial_inst_pix;"
    )

    # Extração 
    @task
    def extract():
        extract_bcb_reference()
        extract_pix_data(url=URL_PIX)

    # Transformação (Lê do disco)
    @task
    def transform():
        # Caminhos fixos onde as funções de extração salvam os arquivos
        path_pix = '/opt/airflow/data/bronze/pix_data.json'
        path_bcb = '/opt/airflow/data/bronze/bcb_reference.json'
        
        return process_transformation(path_pix, path_bcb)

    # Carga
    @task
    def load(transformed_data):
        save_to_silver(transformed_data, PATH_PARQUET)
    
        # Em vez de usar a variável global DB_URL, usa o Hook.
        hook = PostgresHook(postgres_conn_id='postgres_pix')
        
        # 3. O Hook extrai a URI de conexão formatada corretamente para o SQLAlchemy/Pandas
        airflow_db_url = hook.get_uri()
        
        # 4. Executa a carga usando a URL provida pelo Airflow
        load_to_db(db_url=airflow_db_url, parquet_path=PATH_PARQUET, sql_path=PATH_SQL)

    # Fluxo
    # A extração deve ocorrer antes da transformação
    dados_processados = transform()
    
    # Execução
    extract() >> dados_processados
    clean_table >> load(dados_processados)

financial_etl()