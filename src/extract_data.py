import requests
import json

from pathlib import Path

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

url_apibrasil = "https://brasilapi.com.br/api/pix/v1/participants"

def extract_pix_data(url: str) -> list:
    """
    Extrai os dados de PIX a partir da API e salva em um arquivo JSON.
    """

    try:
        logging.info(f"Extraindo dados de API Brasil: {url}...")

        response = requests.get(url, timeout=15)
        response.raise_for_status()

        data = response.json()

        if not data:
            logging.warning("Nenhum dado encontrado na resposta da API.")
            return []

        output_path = Path('data') / 'pix_data.json' 
        output_dir = Path(output_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)

        with open (output_path, 'w') as f:
            json.dump(data, f, indent=4)


        logging.info(f"Dados extraídos e salvos em {output_path}")    
        return data
    
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao extrair dados da API: {e}")
        return []


extract_pix_data(url_apibrasil)