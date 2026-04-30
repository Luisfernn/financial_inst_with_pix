import os
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

db_url = os.getenv("DB_URL")
parquet_path = os.getenv("PARQUET_PATH")
sql_path = os.getenv("SQL_SCRIPT_PATH")


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def save_to_silver(df: pd.DataFrame, output_path: Path):
    """
    Salva o DataFrame resultante da transformação na camada Silver em formato Parquet.
    """
       
    try:
        if df is None or df.empty:
            logging.error("Dados inválidos ou vazios. Abortando carga.")
            return

        # Adiciona metadados de auditoria
        df['processed_at'] = datetime.now()

        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False, compression='snappy')
        
        logging.info(f"Carga Silver concluída com sucesso: {output_path} | Shape: {df.shape}")

    except Exception as e:
        logging.error(f"Erro ao salvar na camada Silver: {e}")
        raise



def load_to_db (db_url, parquet_path, sql_path):
    """
    Carrega o DataFrame resultante da transformação para um banco de dados relacional.
    """

    try:
        engine = create_engine(db_url)

        with open (sql_path, 'r', encoding='utf-8') as f:
            create_table_script = f.read()

        df = pd.read_parquet(parquet_path)

        with engine.begin() as conn:
            logging.info("Conectado! Verificando estrutura da tabela...")
            conn.execute(text(create_table_script))
            logging.info(f"Estrutura da tabela verificada. Iniciando carga de {len(df)}...")

            df.to_sql(
                'financial_inst_pix',
                con=conn, 
                if_exists='append', 
                index=False,
                method='multi'
            )

            logging.info("Sucesso! Dados carregados no banco 'pix_db' sem erros.")

    except Exception as e:
        logging.error(f"Falha no carregamento. O banco permanece intacto. Erro: {e}") 


if __name__ == "__main__":

    save_to_silver()
    load_to_db(db_url, parquet_path, sql_path)