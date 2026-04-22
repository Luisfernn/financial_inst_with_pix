import pandas as pd
import logging
from pathlib import Path
from datetime import datetime

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