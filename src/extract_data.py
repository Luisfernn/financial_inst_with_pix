import os
import requests
import json

from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

url_apibrasil = "https://brasilapi.com.br/api/pix/v1/participants"

def get_last_business_day(date_ref=None):
    """
    Retorna o último dia útil formatado para a API do BCB (MM-DD-YYYY).
    Aceita strings do Airflow (YYYY-MM-DD) ou objetos datetime.
    """
    # 1. Se receber a string do Airflow (YYYY-MM-DD), converte para objeto datetime
    if isinstance(date_ref, str):
        try:
            date_ref = datetime.strptime(date_ref, '%Y-%m-%d')
        except ValueError:
            # Caso você passe no formato brasileiro por erro, por exemplo
            date_ref = datetime.strptime(date_ref, '%d-%m-%Y')
    
    # 2. Se não vier nada, usa o "agora"
    if date_ref is None:
        date_ref = datetime.now()

    # 3. Lógica de fim de semana
    if date_ref.weekday() == 5:  # Sábado -> Sexta
        date_ref = date_ref - timedelta(days=1)
    elif date_ref.weekday() == 6:  # Domingo -> Sexta
        date_ref = date_ref - timedelta(days=2)
    
    # 4. Formatação específica para a URL (Mês-Dia-Ano)
    return date_ref.strftime('%m-%d-%Y')



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
    


def extract_bcb_reference(data_customizada: str = None) -> list:
    """
    Extrai os dados de referência das instituições supervisionadas pelo Banco Central e salva em um arquivo JSON.
    """
    
    base_url = os.getenv("BCB_BASE_URL")
    filtros = os.getenv("BCB_FILTERS")

    data_formatada = get_last_business_day(data_customizada)

    url_final = f"{base_url}'{data_formatada}'{filtros}"

    try:
        logging.info(f"Extraindo dados da API do Banco Central...")

        response = requests.get(url_final, timeout=15)
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
    extract_bcb_reference()