import requests
import json

from pathlib import Path

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

url_apibrasil = "https://brasilapi.com.br/api/pix/v1/participants"
url_bcb = "https://olinda.bcb.gov.br/olinda/servico/BcBase/versao/v2/odata/EntidadesSupervisionadas(dataBase=@dataBase)?@dataBase='04-16-2026'&$top=10000&$format=json&$select=nomeDoPais,nomeEntidadeInteresse,codigoCNPJ8,codigoTipoEntidadeSupervisionada,descricaoTipoEntidadeSupervisionada,nomeFantasia,indicadorEsferaPublica"

def extract_pix_data(url: str) -> list:
    """
    Extrai os dados de PIX a partir da API e salva em um arquivo JSON.
    """

    try:
        logging.info(f"Extraindo dados de API Brasil...")

        response = requests.get(url, timeout=15)
        response.raise_for_status()

        data = response.json()

        if not data:
            logging.warning("Nenhum dado encontrado na resposta da API.")
            return []

        output_path = Path('data') / 'bronze' / 'pix_data.json' 
        output_dir = Path(output_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)

        with open (output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)


        logging.info(f"Dados extraídos e salvos em {output_path}")    
        return data
    
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao extrair dados da API: {e}")
        return []
    

def extract_bcb_reference(url: str) -> list:
    """
    Extrai os dados de referência das instituições supervisionadas pelo Banco Central e salva em um arquivo JSON.
    """

    try:
        logging.info(f"Extraindo dados da API do Banco Central...")

        response = requests.get(url_bcb, timeout=15)
        response.raise_for_status()

        data = response.json().get('value', [])

        if not data:
            logging.warning("Nenhum dado encontrado na resposta da API.")
            return []
        
        output_path = Path('data') / 'bronze' / 'bcb_reference.json'
        output_dir = Path(output_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)

        with open (output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

        logging.info(f"Dados extraídos e salvos em {output_path}")
        return data
    
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao extrair dados da API: {e}")
        return []



if __name__ == "__main__":

    extract_pix_data(url_apibrasil)
    extract_bcb_reference(url_bcb)