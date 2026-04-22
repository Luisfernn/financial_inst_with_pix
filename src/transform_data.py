import pandas as pd
from pathlib import Path
import json
import re

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

path_name_pix = Path(__file__).parent.parent / 'data' / 'bronze' / 'pix_data.json'
path_name_bcb = Path(__file__).parent.parent / 'data' / 'bronze' / 'bcb_reference.json'


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

    df_bcb = pd.json_normalize(data)

    logging.info(f"DataFrame do Banco Central criado com sucesso. Shape: {df_bcb.shape}")    

    return df_bcb


# Preparando dados da fonge original para o merge


def prepare_pix_data (df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza a limpeza, tipagem, remove espaços em branco e renomeia colunas dos dados brutos do PIX
    """

    logging.info("Iniciando a preparação do DataFrame do PIX...")

    if df.empty:
        logging.warning("O DataFrame do PIX está vazio. Nenhuma preparação será realizada.")
        return df

    steps = {
        "Dtype (inicio_operacao)": lambda d: pd.to_datetime(d['inicio_operacao']),
        "Z fill (ispb)": lambda d: d['ispb'].fillna('0').astype(str).str.zfill(8),
        "Renomear coluna (nome_juridico)": lambda d: d.rename(columns={'nome': 'nome_juridico'}),
        "Limpeza (strip nomes)": lambda d: d['nome_juridico'].str.strip(),
        
    }

    for step_name, operation in steps.items():
        try:
            logging.info(f"Executando etapa: {step_name}")
            if step_name == "Dtype (inicio_operacao)":
                df['inicio_operacao'] = operation(df)
            elif step_name == "Z fill (ispb)":
                df['ispb'] = operation(df) 
            elif step_name == "Renomear coluna (nome_juridico)":
                df = operation(df)      
            elif step_name == "Limpeza (strip nomes)":
                df['nome_juridico'] = operation(df)    
           

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
        'nomeDoPais': 'pais_sede',
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


def merge_data(df_pix: pd.DataFrame, df_bcb: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza o merge dos DataFrames do PIX e do Banco Central utilizando a coluna 'ispb' como chave.
    """

    logging.info("Iniciando o merge dos DataFrames do Pix e do Banco Central...")

    if df_pix.empty:
        logging.warning("O DataFrame do PIX está vazio. O merge resultará em um DataFrame vazio.")
        return pd.DataFrame()
    
    if df_bcb.empty:
        logging.warning("O DataFrame do Banco Central está vazio. O merge resultará em um DataFrame vazio.")
        return pd.DataFrame()
    
    try:
        df_merged = pd.merge(df_pix, df_bcb, on='ispb', how='left', suffixes=('_pix', '_bcb'))
        logging.info(f"Merge concluído com sucesso. Shape do DataFrame final: {df_merged.shape}")
        return df_merged
    except Exception as e:
        logging.error(f"Erro ao realizar o merge: {e}")
        raise


def clean_short_names(text):
    """
    Limpa nomes do tipo de instituição e siglas de nomes jurídicos removendo sufixos comuns como S.A., COOP,  etc.
    """

    if pd.isna(text):
        return text
    
    text = str(text).upper()

    patterns = r'\b(S\.?A\.?|LTDA|COOP|SCD|CFI|CCB)\b|S/A'
    text = re.sub(patterns, '', text)
    
    return re.sub(r'\s+', ' ', text).strip()


def refine_final_data(df: pd.DataFrame) -> pd.DataFrame:    
    """
    Realiza a refinação final do DataFrame resultante do merge, criando coluna de pesquisa de nomes e definindo colunas finais.
    """

    logging.info("Iniciando refinação final do DataFrame...")

    if df.empty:
        logging.warning("O DataFrame final está vazio. Nenhuma refinação será realizada.")
        return df
    
    try:
        # Aplica bussiness rule de limpeza textual
        df['nome_reduzido'] = df['nome_reduzido'].apply(clean_short_names)

        # Cria coluna de busca de nome facilitado, preenchendo com a prioridade nome_fantasia -> nome_reduzido
        df['nome_busca'] = df['nome_fantasia'].fillna(df['nome_reduzido'])
        
        # Aplica a limpeza textual na coluna de busca para garantir consistência
        df['nome_busca'] = df['nome_busca'].apply(clean_short_names)
        
        # Colunas que serão mantidas no DataFrame final
        cols_to_keep = ['ispb', 'nome_juridico', 'nome_busca', 'categoria', 'pais_sede', 'inicio_operacao']

        df_silver = df[cols_to_keep].copy()

        logging.info(f"Refinação concluída. Registro 1: {df_silver['nome_busca'].iloc[0]}")
        return df_silver
    
    except Exception as e:
        logging.error(f"Erro durante a refinação final: {e}")
        raise


df_pix = create_pix_dataframe(path_name_pix)
df_bcb = create_bcb_dataframe(path_name_bcb)

df_pix_ready = prepare_pix_data(df_pix)
df_bcb_ready = prepare_bcb_data(df_bcb)

df_merged = merge_data(df_pix_ready, df_bcb_ready)
