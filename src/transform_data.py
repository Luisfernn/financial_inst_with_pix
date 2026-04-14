import pandas as pd
from pathlib import Path
import json

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

path_name = Path(__file__).parent.parent / 'data' / 'pix_data.json'


def create_dataframe(path_name: str) -> pd.DataFrame:
    """
    Cria um DataFrame a partir do arquivo JSON contendo os dados de PIX.
    """

    logging.info(f"Criando DataFrame a partir do arquivo JSON...")
    path = path_name


    if not path_name.exists():
        raise FileNotFoundError(f"O arquivo {path_name} não foi encontrado.")
    
    with open(path) as f:
        data = json.load(f)

    df = pd.json_normalize(data)

    logging.info(f"DataFrame criado com sucesso. Número de linhas: {len(df)}, Número de colunas: {len(df.columns)}")  

    return df
    