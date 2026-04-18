import pandas as pd
from pathlib import Path
import json

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

path_name_pix = Path(__file__).parent.parent / 'data' / 'pix_data.json'
path_name_bcb = Path(__file__).parent.parent / 'data' / 'bcb_reference.json'


# Criando os DataFrames a partir dos arquivos JSON extraídos

def create_pix_dataframe(path: str) -> pd.DataFrame:
    """
    Cria um DataFrame a partir do arquivo JSON contendo os dados de PIX.
    """

    logging.info(f"Criando DataFrame do PIX a partir do arquivo JSON...")

    if not path_name_pix.exists():
        raise FileNotFoundError(f"O arquivo {path} não foi encontrado.")
    
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    df_pix = pd.json_normalize(data)

    logging.info(f"DataFrame do PIX criado com sucesso. Número de linhas: {len(df_pix)}, Número de colunas: {len(df_pix.columns)}")  

    return df_pix


def create_bcb_dataframe(path: str) -> pd.DataFrame:
    """
    Cria um DataFrame a partir do arquivo JSON contendo os dados de referência das instituições supervisionadas pelo Banco Central.
    """

    logging.info(f"Criando DataFrame referência do Banco Central a partir do arquivo JSON...")

    if not path_name_bcb.exists():
        raise FileNotFoundError(f"O arquivo {path} não foi encontrado.")
    
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    df_ref_bcb = pd.json_normalize(data)

    logging.info(f"DataFrame do Banco Central criado com sucesso. Número de linhas: {len(df_ref_bcb)}, Número de colunas: {len(df_ref_bcb.columns)}")    

    return df_ref_bcb


# Preparando os DataFrames para o merge

