# ETL ITI - ICP-Brasil (Databricks Lakehouse & Asset Bundles)

> **Status do Projeto:** Em Desenvolvimento (Work in Progress)  
> Pipeline de ingestao, desaninhamento e carga dos dados publicos de entidades e certificados digitais do ITI / ICP-Brasil no Databricks Unity Catalog.

---

Pipeline de Engenharia de Dados e ETL para processamento e ingestao dos dados publicos da estrutura do **ITI (Instituto Nacional de Tecnologia da Informacao) / ICP-Brasil**, estruturado utilizando **Databricks Asset Bundles (DABs)**, **Delta Live Tables (DLT)**, **Unity Catalog Volumes** e **PySpark**.

---

## Visao Geral da Arquitetura

O projeto realiza a extracao dos dados abertos da infraestrutura de chaves publicas brasileira (ACs, ARs, ACTs, PSS e certificados), tratando estruturas JSON hierarquicas e realizando a ingestao automatizada na arquitetura Medalhao do **Databricks Lakehouse**:

```text
[ API Oficial ITI ] 
       │
       ▼ (requests + flatten)
[ 0_raw / Volumes ] ──► Armazenamento de arquivos brutos (JSON / CSV)
       │
       ▼ (Delta Live Tables / Spark)
[ 1_bronze ] ──────────► Tabelas Delta brutas com metadados de ingestao
       │
       ▼ (Transformacoes & Validacoes de Qualidade)
[ 2_silver ] ──────────► Entidades tratadas, deduplicadas e padronizadas
       │
       ▼ (Modelagem Dimensional / Agregacoes)
[ 3_gold ] ────────────► Visoes analiticas, metricas de conformidade e relatorios
```

---

## Estrutura do Projeto

```text
etl-iti-icp-brasil/
├── .github/                             # Automacoes de CI/CD (GitHub Actions)
│   └── workflows/
│       ├── python-cicd-validado.yml     # Pipeline de testes, linter e validacao Databricks
│       └── python-package.yml           # Pipeline auxiliar
│
├── .vscode/                             # Configuracoes do VS Code e stubs Spark/Databricks
│   ├── settings.json
│   └── __builtins__.pyi
│
├── databricks.yml                       # Configuracao do Databricks Asset Bundle (DAB)
├── pyproject.toml                       # Gerenciamento de dependencias Python (uv / hatchling)
├── uv.lock                              # Lockfile de dependencias com versoes fixadas
│
├── resources/                           # Recursos declarativos do Databricks
│   └── ITI_ICP_BRASIL_etl.pipeline.yml  # Definicao da pipeline Delta Live Tables (DLT)
│
├── src/                                 # Codigo-fonte da aplicacao e pipelines
│   ├── ITI_ICP_BRASIL/                  # Pacote Python principal de extracao e tratamento
│   │   ├── __init__.py
│   │   ├── main.py                      # Entry point de execucao do pacote
│   │   ├── assets/                      # Modulo de extracao de dados e chamadas a API
│   │   │   ├── __init__.py
│   │   │   └── url_iti.py               # Extracao de detalhes e entidades do ITI
│   │   ├── processamento/               # Modulo de transformacao e normalizacao de dados
│   │   │   ├── __init__.py
│   │   │   └── flatten.py               # Algoritmo de achatamento recursivo de JSON aninhado
│   │   ├── exportacao/                  # Modulo de carga e integracao com Databricks
│   │   │   ├── __init__.py
│   │   │   └── upload_raw.py            # Upload de dados brutos para Volumes do Unity Catalog
│   │   ├── config/                      # Configuracoes de conexao e variaveis
│   │   └── output/                      # Utilitarios de saida e escrita
│   │
│   └── ITI_ICP_BRASIL_etl/              # Pipelines declarativas DLT no Databricks
│       ├── README.md
│       └── transformations/             # Scripts de transformacao (Raw -> Bronze -> Silver -> Gold)
│           └── .gitkeep
│
├── tests/                               # Testes automatizados (Unit & Integration tests)
│   ├── conftest.py                      # Fixtures e inicializacao de sessao Spark/Connect
│   └── test_package.py                  # Testes unitarios do pacote
│
├── fixtures/                            # Dados de teste e mocks
│   └── .gitkeep
│
├── .gitignore                           # Regras de exclusao do Git
└── README.md                            # Documentacao principal do projeto
```

---

## Stack Tecnologica

- **Orquestracao & Plataforma:** [Databricks Asset Bundles (DABs)](https://docs.databricks.com/dev-tools/bundles/index.html)
- **Engine de Processamento:** Apache Spark / PySpark & [Delta Live Tables (DLT)](https://docs.databricks.com/delta-live-tables/index.html)
- **Governanca & Armazenamento:** Databricks Unity Catalog (Volumes gerenciados e Delta Tables)
- **SDK & Integracoes:** [Databricks SDK para Python](https://docs.databricks.com/dev-tools/sdk-python.html) (WorkspaceClient)
- **Gerenciador de Pacotes:** [Astral uv](https://docs.astral.sh/uv/) / [Hatchling](https://hatch.pypa.io/)
- **Qualidade de Codigo & Linter:** [Ruff](https://astral.sh/ruff)
- **Testes:** [pytest](https://docs.pytest.org/) com Databricks Connect (databricks-connect)
- **CI/CD:** GitHub Actions com execucao em matriz multi-versao (Python 3.10, 3.11 e 3.12)

---

## Como Comecar

### 1. Pre-requisitos
- **Python:** Versao `>=3.10, <3.13`
- **Gerenciador UV:** [Instalar o UV](https://docs.astral.sh/uv/getting-started/installation/)
- **Databricks CLI:** [Instalar Databricks CLI v0.200+](https://docs.databricks.com/dev-tools/cli/databricks-cli.html)

### 2. Configurar o Ambiente Local

Sincronize as dependencias virtuais com `uv` diretamente na raiz do projeto:

```bash
# Criar ambiente virtual e instalar dependencias de desenvolvimento
uv sync --dev
```

### 3. Autenticacao no Databricks

Configure a autenticacao com o seu workspace:

```bash
databricks auth login --host https://dbc-15e61da2-fb6a.cloud.databricks.com
```

---

## Execucao e Ingestao de Dados

### Ingestao Manual da API para a Camada Raw (Volume Unity Catalog)

Para extrair os dados da API oficial do ITI e enviar o JSON bruto para o Volume `/Volumes/lakehouse_iti/0_raw/raw/entidades.json`:

```bash
uv run python src/ITI_ICP_BRASIL/exportacao/upload_raw.py
```

### Comandos do Databricks Asset Bundle:

```bash
# Validar sintaxe das configuracoes e schemas do bundle
databricks bundle validate

# Deploy em ambiente de desenvolvimento
databricks bundle deploy

# Deploy em ambiente de producao
databricks bundle deploy --target prod

# Executar pipeline no Databricks
databricks bundle run
```

---

## Testes e Qualidade de Codigo

Execute a suite de testes unitarios e o linter:

```bash
# Executar testes unitarios com pytest
uv run pytest

# Executar linter e checagem de estilo de codigo
uv run ruff check .

# Formatar codigo automaticamente
uv run ruff format .
```
