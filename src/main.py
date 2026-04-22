import os
import logging
from pathlib import Path
from dotenv import load_dotenv

from src.extract import extract_pix_data, extract_bcb_reference
from src.transform import process_transformation
from src.load import save_to_silver

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

BASE_DIR = Path(__file__).parent
BRONZE_PIX = BASE_DIR / 'data' / 'bronze' / 'pix_data.json'
BRONZE_BCB = BASE_DIR / 'data' / 'bronze' / 'bcb_reference.json'
SILVER_FINAL = BASE_DIR / 'data' / 'silver' / 'final_institutions.parquet'

URL_PIX = os.getenv("URL_PIX")
URL_BCB = os.getenv("URL_BCB")

def run_pipeline():
    try:
        logging.info("Iniciando Pipeline...")

        # 1. Extração
        extract_pix_data(URL_PIX)
        extract_bcb_reference(URL_BCB)

        # 2. Transformação (Aqui o main chama a função que faz tudo)
        df_silver = process_transformation(BRONZE_PIX, BRONZE_BCB)

        # 3. Load
        save_to_silver(df_silver, SILVER_FINAL)

        logging.info("Pipeline concluída com sucesso!")

    except Exception as e:
        logging.error(f"❌ Erro: {e}")

if __name__ == "__main__":
    run_pipeline()