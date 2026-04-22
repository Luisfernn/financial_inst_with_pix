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

    logging.info(f"DataFrame do PIX criado com sucesso. Shape: {df_pix.shape}")  

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

    logging.info(f"DataFrame do Banco Central criado com sucesso. Shape: {df_ref_bcb.shape}")    

    return df_ref_bcb


# Preparando dados da fonge original para o merge


def prepare_pix_data (df: pd.DataFrame) -> pd.DataFrame:

    """
    Realiza a limpeza, slicing e tipagem dos dados brutos do PIX (primeira fonte)
    """

    logging.info("Iniciando a preparação do DataFrame do PIX...")

    if df.empty:
        logging.warning("O DataFrame do PIX está vazio. Nenhuma preparação será realizada.")
        return df

    steps = {
        "Dtype (inicio_operacao)": lambda d: pd.to_datetime(d['inicio_operacao']),
        "Z fill (ispb)": lambda d: d['ispb'].fillna('0').astype(str).str.zfill(8),
        "Limpeza (strip nomes)": lambda d: d['nome'].str.strip()
    }

    for step_name, operation in steps.items():
        try:
            logging.info(f"Executando etapa: {step_name}")
            if step_name == "Dtype (inicio_operacao)":
                df['inicio_operacao'] = operation(df)
            elif step_name == "Z fill (ispb)":
                df['ispb'] = operation(df)   
            elif step_name == "Limpeza (strip nomes)":
                df['nome'] = operation(df)    

        except Exception as e:
            logging.error(f"Erro na etapa '{step_name}': {e}")
            raise

    logging.info(f"Preparação do DataFrame do PIX concluída com sucesso. Shape: {df.shape}") 

    return df   



# Preparando dados referência para o merge


def prepare_bcb_data (df: pd.DataFrame) -> pd.DataFrame:

    """
    Renomeia colunas, remove espaços vazios, converte str vazias para NaN, ordena e remove duplicatas de CNPJ
    """

    logging.info("Iniciando a preparação dos dados referência do BCB")

    if df.empty:
        logging.warning("O DataFrame do BCB está vazio. Nenhuma preparação será realizada.")
        return df
    

    df = df.rename(columns={
        'nomeDoPais': 'nome_pais',
        'nomeEntidadeInteresse': 'nome_entidade_interesse',
        'codigoCNPJ8': 'ispb',
        'codigoTipoEntidadeSupervisionada': 'codigo_tipo_entidade',
        'descricaoTipoEntidadeSupervisionada': 'categoria',
        'nomeFantasia': 'nome_fantasia',
        'indicadorEsferaPublica': 'indicador_esfera_publica'
    })

    try:
        etapa = "Z fill e tratamento de NaN em ['ispb']"
        df['ispb'] = df['ispb'].fillna('0').astype(str).str.zfill(8)

        etapa = "Remover espaços vazios em '[nome_fantasia']"
        df['nome_fantasia'] = df['nome_fantasia'].str.strip()

        etapa = "Substituir str vazias por NaN em ['nome_fantasia']"
        df['nome_fantasia'] = df['nome_fantasia'].replace(r'^\s*$', pd.NA, regex=True)
        
        etapa = "Ordenar linhas iguais com NaN"
        df = df.sort_values(by='nome_fantasia', na_position='last')

        etapa = "Drop duplicatas"
        df = df.drop_duplicates(subset='ispb', keep='first')

    except Exception as e:
        logging.error(f"Erro na etapa {etapa}: {e}")
        raise    

    logging.info(f"Preparação do DataFrame do BCB concluída com sucesso. Shape: {df.shape}")

    return df


df_pix = create_pix_dataframe(path_name_pix)
df_bcb = create_bcb_dataframe(path_name_bcb)

df_pix_ready = prepare_pix_data(df_pix)
df_bcb_ready = prepare_bcb_data(df_bcb)
