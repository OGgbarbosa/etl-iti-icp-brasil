# 🏛️ ETL ITI - ICP-Brasil (Databricks Lakehouse & Asset Bundles)

> ⚠️ **Status do Projeto:** 🚧 **Em Desenvolvimento (*Work in Progress*)**  
> Pipeline de engenharia de dados voltado à extração, normalização, modelagem dimensional e carga de dados públicos de entidades e certificados digitais da Infraestrutura de Chaves Públicas Brasileira (ICP-Brasil) no Databricks Unity Catalog.

---

## 1. 📌 Introdução e Visão Geral da Arquitetura

O presente projeto tem por finalidade realizar a extração automatizada, o processamento e a disponibilização analítica dos dados abertos governamentais disponibilizados pelo **Instituto Nacional de Tecnologia da Informação (ITI)**. O fluxo abrange entidades certificadoras (Autoridades Certificadoras — ACs, Autoridades de Registro — ARs, Autoridades de Carimbo do Tempo — ACTs e Prestadores de Serviço de Suporte — PSS).

A arquitetura de dados segue o padrão **Medalhão** no **Databricks Lakehouse**, garantindo rastreabilidade, governança via **Unity Catalog** e qualidade em cada camada de processamento:

```text
                  [ API Oficial do ITI / ICP-Brasil ]
                                  │
                                  ▼ (Módulo de Extração & Flatten)
┌──────────────────────────────────────────────────────────────────────────────────┐
│ Camada 0_raw (Volumes do Unity Catalog)                                          │
│ └── Volume: /Volumes/lakehouse_iti/0_raw/raw/entidades.json                      │
└─────────────────────────────────┬────────────────────────────────────────────────┘
                                  │
                                  ▼ (Conversão CSV & Statement Execution API / DLT)
┌──────────────────────────────────────────────────────────────────────────────────┐
│ Camada 1_bronze (Volumes & Tabelas Delta)                                        │
│ ├── Volume: /Volumes/lakehouse_iti/1_bronze/raw/entidades.csv                    │
│ └── Tabela Delta: lakehouse_iti.1_bronze.entidades (com metadados e auditoria)   │
└─────────────────────────────────┬────────────────────────────────────────────────┘
                                  │
                                  ▼ (Transformações, Deduplicação & PySpark Serverless)
┌──────────────────────────────────────────────────────────────────────────────────┐
│ Camada 2_silver (Tabelas Delta Normalizadas)                                     │
│ ├── Tabela Delta: lakehouse_iti.2_silver.tbl_entidades                           │
│ ├── Tabela Delta: lakehouse_iti.2_silver.tbl_enderecos                           │
│ └── Tabela Delta: lakehouse_iti.2_silver.tbl_hierarquia                          │
└─────────────────────────────────┬────────────────────────────────────────────────┘
                                  │
                                  ▼ (Modelagem Dimensional & Visões Analíticas)
┌──────────────────────────────────────────────────────────────────────────────────┐
│ Camada 3_gold (Tabelas Delta & Visões)                                           │
│ └── Modelagem para consumo analítico, auditoria e conformidade regulatória       │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. 🗂️ Estrutura de Diretórios do Projeto

A organização de diretórios e arquivos do repositório está estruturada conforme a seguir:

```text
etl-iti-icp-brasil/
├── .github/                             # Automações de Integração e Entrega Contínuas (CI/CD)
│   └── workflows/
│       ├── python-cicd-validado.yml     # Pipeline de validação estática, testes e deploy Databricks
│       └── python-package.yml           # Pipeline secundária de testes
│
├── .vscode/                             # Configurações de ambiente de desenvolvimento local
│   ├── settings.json                    # Definições do interpretador e linters
│   └── __builtins__.pyi                 # Declarações de tipagem global para Spark e Databricks
│
├── databricks.yml                       # Configuração declarativa do Databricks Asset Bundle (DAB)
├── pyproject.toml                       # Especificação do projeto e gerenciamento de dependências
├── uv.lock                              # Registro determinístico de versões de dependências
│
├── resources/                           # Definições declarativas de recursos no Databricks
│   └── ITI_ICP_BRASIL_etl.pipeline.yml  # Pipeline declarativa Delta Live Tables (DLT)
│
├── src/                                 # Código-fonte principal da aplicação
│   ├── ITI_ICP_BRASIL/                  # Pacote Python para extração, tratamento e carga
│   │   ├── __init__.py                  # Inicialização do módulo Python
│   │   ├── main.py                      # Ponto de entrada de execução do pacote
│   │   ├── assets/                      # Módulo de comunicação e consumo de APIs externas
│   │   │   ├── __init__.py
│   │   │   └── url_iti.py               # Extração de dados da API oficial de entidades do ITI
│   │   ├── processamento/               # Módulo de transformação e normalização de dados
│   │   │   ├── __init__.py
│   │   │   └── flatten.py               # Algoritmo de desaninhamento recursivo de estruturas JSON
│   │   ├── exportacao/                  # Módulo de carga e persistência de dados
│   │   │   ├── __init__.py
│   │   │   ├── upload_raw.py            # Upload de dados brutos (JSON) para Volume Raw
│   │   │   ├── upload_bronze.py         # Conversão para CSV, upload no Volume Bronze e criação de Tabela Delta
│   │   │   ├── upload_silver.py         # Criação das tabelas Silver via SQL Statement Execution API
│   │   │   └── upload_silver_pyspark.py # Pipeline Silver com PySpark DataFrame API & Databricks Connect Serverless
│   │   ├── config/                      # Configurações gerais e parâmetros de ambiente
│   │   └── output/                      # Utilitários de escrita e geração de relatórios
│   │
│   └── ITI_ICP_BRASIL_etl/              # Pipelines declarativas DLT gerenciadas pelo Databricks
│       ├── README.md
│       └── transformations/             # Scripts de transformação entre as camadas Medalhão
│           └── .gitkeep
│
├── tests/                               # Suíte de testes automatizados
│   ├── conftest.py                      # Configurações globais e fixtures do pytest (Spark/Connect)
│   └── test_package.py                  # Testes unitários do pacote ITI_ICP_BRASIL
│
├── fixtures/                            # Conjuntos de dados estáticos para validação e testes
│   └── .gitkeep
│
├── .gitignore                           # Regras de exclusão de arquivos no controle de versão
└── README.md                            # Documentação técnica principal do projeto
```

---

## 3. 🛠️ Tecnologias e Especificações Técnicas

O projeto utiliza ferramentas de padrões modernos de Engenharia de Dados em Nuvem:

- **Plataforma e Orquestração:** [Databricks Asset Bundles (DABs)](https://docs.databricks.com/dev-tools/bundles/index.html)
- **Motor de Computação Distribuída:** Apache Spark / PySpark & [Delta Live Tables (DLT)](https://docs.databricks.com/delta-live-tables/index.html)
- **Execução Local Remota:** [Databricks Connect](https://docs.databricks.com/dev-tools/databricks-connect/python/index.html) com computação **Serverless** (`DatabricksSession.builder.serverless(True)`)
- **Armazenamento e Governança:** Databricks Unity Catalog (`Volumes` gerenciados e Tabelas Delta)
- **SDK de Integração:** [Databricks SDK para Python](https://docs.databricks.com/dev-tools/sdk-python.html) (`WorkspaceClient`, `StatementExecutionAPI`, `VolumesAPI`, `FilesAPI`)
- **Gerenciador de Dependências e Ambientes:** [Astral uv](https://docs.astral.sh/uv/) / [Hatchling](https://hatch.pypa.io/)
- **Análise Estática de Código e Formatação:** [Ruff](https://astral.sh/ruff)
- **Framework de Testes Automatizados:** [pytest](https://docs.pytest.org/) com fixtures do Databricks Connect
- **Integração e Entrega Contínuas (CI/CD):** GitHub Actions com validação de linters, testes unitários e deploy do bundle

---

## 4. 🚀 Guia de Instalação e Configuração do Ambiente

### 4.1. Pré-requisitos
- **Interpretador Python:** Versão `>=3.10, <3.13`
- **Gerenciador UV:** [Documentação de Instalação do UV](https://docs.astral.sh/uv/getting-started/installation/)
- **Databricks CLI:** [Documentação de Instalação da Databricks CLI v0.200+](https://docs.databricks.com/dev-tools/cli/databricks-cli.html)

### 4.2. Inicialização do Ambiente Virtual

Execute a sincronização determinística do ambiente e instalação de dependências de desenvolvimento:

```bash
uv sync --dev
```

### 4.3. Autenticação no Databricks

Configure as credenciais do Databricks no seu arquivo `~/.databrickscfg` ou utilize o comando da CLI:

```bash
databricks auth login --host https://dbc-15e61da2-fb6a.cloud.databricks.com
```

---

## 5. ⚙️ Execução das Etapas de ETL e Ingestão de Dados

### 5.1. Ingestão da Camada Raw (JSON para Volume)

Executa a extração da API oficial do ITI, aplica o desaninhamento estrutural (*flatten*) e envia o arquivo JSON para o Volume do Unity Catalog:

```bash
uv run python src/ITI_ICP_BRASIL/exportacao/upload_raw.py
```
- **Destino:** `/Volumes/lakehouse_iti/0_raw/raw/entidades.json`

### 5.2. Ingestão da Camada Bronze (Conversão CSV & Tabela Delta)

Converte o JSON da camada Raw para CSV no Volume Bronze e cria/atualiza a tabela Delta correspondente com metadados de auditoria:

```bash
uv run python src/ITI_ICP_BRASIL/exportacao/upload_bronze.py
```
- **Volume Destino:** `/Volumes/lakehouse_iti/1_bronze/raw/entidades.csv`
- **Tabela Delta Destino:** `lakehouse_iti.1_bronze.entidades` (campos adicionais: `nome_arquivo`, `data_insercao`)

### 5.3. Processamento da Camada Silver (Tabelas Delta Normalizadas)

A camada Silver separa a estrutura desnormalizada da Bronze em tabelas relacionais limpas, tipadas e deduplicadas. Estão disponíveis duas abordagens:

#### A) Via PySpark com Databricks Connect Serverless (Recomendado)
Processa os dados distribuídos utilizando a DataFrame API do PySpark conectando-se diretamente à computação Serverless do Databricks:

```bash
uv run python src/ITI_ICP_BRASIL/exportacao/upload_silver_pyspark.py
```

Tabelas Delta geradas:
- **`lakehouse_iti.2_silver.tbl_entidades`**: Entidades com limpeza de strings, formatação de CNPJ (`LPAD` de 14 dígitos), padronização de datas (`data_credenciamento`), situação textual (`Credenciada` / `Em Credenciamento`) e deduplicação por chave primária (`id_entidade`).
- **`lakehouse_iti.2_silver.tbl_enderecos`**: Endereços com extração de números (`regexp_extract` + `try_cast`), tratamento de CEPs com preservação de zeros à esquerda e deduplicação.
- **`lakehouse_iti.2_silver.tbl_hierarquia`**: Relações hierárquicas entre entidades (`id_entidade_filho`, `id_entidade_pai`, `nivel_pai`).

#### B) Via SQL Statement Execution API
Executa instruções DDL/DML diretamente em um SQL Warehouse via Databricks SDK:

```bash
uv run python src/ITI_ICP_BRASIL/exportacao/upload_silver.py
```

### 5.4. Comandos do Databricks Asset Bundle (DAB)

```bash
# Validação sintática das configurações e declarações do bundle
databricks bundle validate

# Deploy em ambiente de desenvolvimento (dev)
databricks bundle deploy

# Deploy em ambiente de produção (prod)
databricks bundle deploy --target prod

# Execução da pipeline declarativa no workspace Databricks
databricks bundle run
```

---

## 6. 🧪 Qualidade de Software e Testes Automatizados

Para garantir a confiabilidade, manutenibilidade e conformidade das diretrizes de desenvolvimento:

```bash
# Execução da suíte de testes unitários
uv run pytest

# Execução do linter e verificação de boas práticas (Ruff)
uv run ruff check .

# Aplicação de correções e formatação automática
uv run ruff format .
```
