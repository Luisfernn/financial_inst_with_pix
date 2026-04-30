import os
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine, text

db_url = os.getenv("DB_URL")
parquet_path = os.getenv("PARQUET_PATH")
sql_path = os.getenv("SQL_SCRIPT_PATH")
load_mode = os.getenv("LOAD_MODE", 'append')


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
    Orquestra a carga de dados no banco: conexão, criação e inserção.
    """
    
    engine = create_engine(db_url)

    try:
        logging.info("Criando tabela no banco de dados...")
        with open (sql_path, 'r', encoding='utf-8') as f:
            create_table_script = f.read()
        
        with engine.begin() as conn:
            conn.execute(text(create_table_script))

    except Exception as e:
        logging.error(f"Erro ao criar tabela: {e}")
        raise e        
        

    try:
        logging.info("Lendo arquivo e inserindo dados...")
        df = pd.read_parquet(parquet_path)
        
        logging.info(f"Colunas do DataFrame: {list(df.columns)}")
        
        with engine.begin() as conn:
            df.to_sql('financial_inst_pix', con=conn, if_exists=load_mode, index=False)
            
        logging.info("Carga concluída com sucesso!")

    except Exception as e:
        logging.error(f"Erro na inserção: {e}")
        raise e   


if __name__ == "__main__":

    save_to_silver()
    load_to_db(db_url, parquet_path, sql_path)