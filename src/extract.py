import requests
import json

from pathlib import Path

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

url = "https://brasilapi.com.br/api/pix/v1/participants"

def extract_pix_data(url: str) -> list:
    """
    Extrai os dados de PIX a partir da API e salva em um arquivo JSON.
    """


    response = requests.get(url)

    data = response.json()

    if response.status_code != 200:
        logging.error(f"Falha na requisição para {url}. Status code: {response.status_code}")
        return []

    if not data:
        logging.warning(f"Nenhum dado encontrado em {url}.")
        return []    


    output_path = 'data' / 'pix_data.json' 
    outputh_dir = Path(output_path).parent
    outputh_dir.mkdir(parents=True, exist_ok=True)

    with open (output_path, 'w') as f:
        json.dump(data, f, indent=4)

    logging.info(f"Dados extraídos e salvos em {output_path}")    

    return data


extract_pix_data(url)