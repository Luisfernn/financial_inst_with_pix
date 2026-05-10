# Instituições Financeiras com PIX 🏦

Pipeline ETL que extrai dados de todos os participantes do PIX registrados no Banco Central do Brasil, enriquece com uma segunda fonte oficial e carrega os resultados em um banco PostgreSQL — totalmente orquestrada com Apache Airflow e containerizada com Docker.

---

## Tech Stack

![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-3.2.1-017CEE?logo=apacheairflow)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Docker-336791?logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)
![UV](https://img.shields.io/badge/UV-Gerenciador%20de%20Pacotes-black)

---

## Arquitetura

O projeto segue a **Arquitetura Medallion** com duas camadas ativas:

```
APIs (JSON)
    │
    ▼
┌─────────┐     Arquivos JSON brutos da API Brasil e API Olinda BCB
│ BRONZE  │
└─────────┘
    │
    ▼  limpeza · enriquecimento · merge · conversão
┌─────────┐     Arquivo Parquet + tabela PostgreSQL
│ SILVER  │
└─────────┘
```

### Fluxo da Pipeline

```
extract_pix_data()          → API Brasil      → bronze/pix_data.json
extract_bcb_reference()     → API Olinda BCB  → bronze/bcb_reference.json
        │
        ▼
process_transformation()    → LEFT JOIN + enriquecimento + novas colunas
        │
        ▼
save_to_silver()            → silver/final_institutions.parquet
load_to_db()                → PostgreSQL (container Docker)
```

---

## Fontes de Dados

| Fonte | Descrição |
|-------|-----------|
| [API Brasil — Participantes PIX](https://brasilapi.com.br/docs#tag/PIX) | Lista de todas as instituições financeiras registradas como participantes do PIX |
| [API Olinda — BCB](https://olinda.bcb.gov.br/olinda/servico/IFDATA/versao/v1/odata/) | Dados oficiais do BCB com informações adicionais das instituições (nome fantasia, CNPJ, tipo de entidade, etc.) |

### Data Enrichment

Um `LEFT JOIN` entre as duas fontes possibilita a criação de novas colunas usando os dados do BCB. Um exemplo é a coluna `nome_busca`, pensada para facilitar a busca pela instituição no banco — ela prioriza o nome fantasia do BCB quando disponível:

```
nome_busca = nome_fantasia (BCB) > nome_reduzido (API Brasil)
```

### URL do BCB — Lógica de Dia Útil

A API do BCB exige uma data de referência. Uma função dedicada verifica se a data de execução da pipeline é um dia útil; caso não seja, ela busca automaticamente o último dia útil válido. A URL final é montada dinamicamente:

```python
url_final = f"{BCB_BASE_URL}'{data_formatada}'{BCB_FILTERS}"
```

---

## Estrutura do Projeto

```
financial_inst_with_pix/
├── dags/                    # Definição da DAG do Airflow
├── notebooks/               # Análises exploratórias
├── sql_scripts/
│   └── create_table.sql     # Script de criação da tabela
├── src/
│   ├── extract_data.py      # Extração das duas APIs
│   ├── transform_data.py    # Transformações e enriquecimento
│   └── load.py              # Salvar em Parquet e carregar no PostgreSQL
├── main.py                  # Ponto de entrada local da pipeline
├── docker-compose.yaml      # Containers Airflow + PostgreSQL
├── pyproject.toml           # Dependências (gerenciadas com UV)
└── .env / .env.local        # Variáveis de ambiente (não commitadas)
```

---

## DAG

A pipeline roda automaticamente todos os dias via DAG do Airflow:

```python
schedule='@daily'
```

A DAG espelha o fluxo do `main.py`: extração → transformação → salvar em silver → carregar no banco.

---

## Como Executar

### Pré-requisitos

- [Docker](https://docs.docker.com/get-docker/) e Docker Compose
- [UV](https://github.com/astral-sh/uv) (gerenciador de pacotes Python)

### Setup

**1. Clone o repositório**
```bash
git clone https://github.com/Luisfernn/financial_inst_with_pix.git
cd financial_inst_with_pix
```

**2. Crie o arquivo `.env`** (veja [Variáveis de Ambiente](#variáveis-de-ambiente) abaixo)

**3. Suba os containers**
```bash
docker compose up -d
```

Pronto. O Airflow estará disponível em `http://localhost:8080` e a DAG será disparada automaticamente conforme o agendamento.

### Execução local (sem Docker)

```bash
cp .env.local .env  # usar o arquivo de env local
uv run main.py
```

---

## Variáveis de Ambiente

O projeto utiliza dois arquivos `.env`:

| Arquivo | Uso |
|---------|-----|
| `.env` | Execução via Docker (Airflow + PostgreSQL) |
| `.env.local` | Execução local via `main.py` |

A diferença é que o `.env.local` não inclui as variáveis de conexão do Airflow (`AIRFLOW_CONN_*`).

### Referência das Variáveis

```dotenv
# ── Airflow ────────────────────────────────────────────────────────
AIRFLOW_UID=                        # UID do usuário Docker do Airflow (ex: 50000)

# ── Fontes de Dados ────────────────────────────────────────────────
URL_PIX=                            # Endpoint da API Brasil para participantes PIX
BCB_BASE_URL=                       # URL base da API Olinda BCB (antes da data)
BCB_FILTERS=                        # Parâmetros de query: formato, colunas, limite de linhas
                                    # ex: &$top=10000&$format=json&$select=nomeFantasia,...

# ── Banco de Dados ─────────────────────────────────────────────────
AIRFLOW_CONN_POSTGRES_DEFAULT=      # Conexão Postgres interna do Airflow (usada pelo próprio Airflow)
AIRFLOW_CONN_POSTGRES_PIX=          # Conexão com o banco dedicado ao PIX (os dados da pipeline vão aqui)
DB_URL=                             # URL de conexão SQLAlchemy para execução local

# ── Caminhos da Pipeline ───────────────────────────────────────────
PARQUET_PATH=                       # Caminho de saída do arquivo .parquet silver
SQL_SCRIPT_PATH=                    # Caminho do script .sql de criação da tabela
PROJECT_ROOT=                       # Caminho absoluto da pasta raiz do projeto

# ── Configuração de Carga ──────────────────────────────────────────
LOAD_MODE=replace                   # Modo if_exists do pandas to_sql: 'replace' ou 'append'
```

> **Sobre a configuração do banco:** dois bancos PostgreSQL compartilham o mesmo servidor Docker. `AIRFLOW_CONN_POSTGRES_DEFAULT` é usado pelo Airflow internamente para seus próprios metadados. `AIRFLOW_CONN_POSTGRES_PIX` aponta para um banco separado onde os dados da pipeline são armazenados de fato, mantendo o estado interno do Airflow isolado dos dados do projeto.

---

## Dependências

Gerenciadas com [UV](https://github.com/astral-sh/uv). Principais pacotes:

```
apache-airflow>=3.2.1
apache-airflow-providers-postgres>=6.6.3
pandas>=3.0.2
pyarrow>=24.0.0
fastparquet>=2026.3.0
sqlalchemy>=2.0.49
psycopg2-binary>=2.9.11
requests>=2.33.1
python-dotenv>=1.2.2
```

---

## Licença

[MIT](LICENSE)