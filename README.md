# ðŸ›ï¸ ETL ITI - ICP-Brasil (Databricks Lakehouse & Asset Bundles)

> âš ï¸ **Status do Projeto:** ðŸš§ **Em Desenvolvimento (Work in Progress)**  
> Pipeline de ingestÃ£o, desaninhamento e carga dos dados pÃºblicos de entidades e certificados digitais do ITI / ICP-Brasil no Databricks Unity Catalog.

---

Pipeline de Engenharia de Dados e ETL para processamento e ingestÃ£o dos dados pÃºblicos da estrutura do **ITI (Instituto Nacional de Tecnologia da InformaÃ§Ã£o) / ICP-Brasil**, estruturado utilizando **Databricks Asset Bundles (DABs)**, **Delta Live Tables (DLT)**, **Unity Catalog Volumes** e **PySpark**.

---

## ðŸ“Œ VisÃ£o Geral da Arquitetura

O projeto realiza a extraÃ§Ã£o dos dados abertos da infraestrutura de chaves pÃºblicas brasileira (ACs, ARs, ACTs, PSS e certificados), tratando estruturas JSON hierÃ¡rquicas e realizando a ingestÃ£o automatizada na arquitetura MedalhÃ£o do **Databricks Lakehouse**:

```text
[ API Oficial ITI ] 
       â”‚
       â–¼ (requests + flatten)
[ 0_raw / Volumes ] â”€â”€â–º Armazenamento de arquivos brutos (JSON / CSV)
       â”‚
       â–¼ (Delta Live Tables / Spark)
[ 1_bronze ] â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â–º Tabelas Delta brutas com metadados de ingestÃ£o
       â”‚
       â–¼ (TransformaÃ§Ãµes & ValidaÃ§Ãµes de Qualidade)
[ 2_silver ] â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â–º Entidades tratadas, deduplicadas e padronizadas
       â”‚
       â–¼ (Modelagem Dimensional / AgregaÃ§Ãµes)
[ 3_gold ] â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â–º VisÃµes analÃ­ticas, mÃ©tricas de conformidade e relatÃ³rios
```

---

## ðŸ—‚ï¸ Estrutura do Projeto

```text
etl-iti-icp-brasil/
â”œâ”€â”€ .github/                             # AutomaÃ§Ãµes de CI/CD (GitHub Actions)
â”‚   â””â”€â”€ workflows/
â”‚       â”œâ”€â”€ python-cicd-validado.yml     # Pipeline de testes, linter e validaÃ§Ã£o Databricks
â”‚       â””â”€â”€ python-package.yml           # Pipeline auxiliar
â”‚
â”œâ”€â”€ .vscode/                             # ConfiguraÃ§Ãµes do VS Code e stubs Spark/Databricks
â”‚   â”œâ”€â”€ settings.json
â”‚   â””â”€â”€ __builtins__.pyi
â”‚
â”œâ”€â”€ databricks.yml                       # ConfiguraÃ§Ã£o do Databricks Asset Bundle (DAB)
â”œâ”€â”€ pyproject.toml                       # Gerenciamento de dependÃªncias Python (uv / hatchling)
â”œâ”€â”€ uv.lock                              # Lockfile de dependÃªncias com versÃµes fixadas
â”‚
â”œâ”€â”€ resources/                           # Recursos declarativos do Databricks
â”‚   â””â”€â”€ ITI_ICP_BRASIL_etl.pipeline.yml  # DefiniÃ§Ã£o da pipeline Delta Live Tables (DLT)
â”‚
â”œâ”€â”€ src/                                 # CÃ³digo-fonte da aplicaÃ§Ã£o e pipelines
â”‚   â”œâ”€â”€ ITI_ICP_BRASIL/                  # Pacote Python principal de extraÃ§Ã£o e tratamento
â”‚   â”‚   â”œâ”€â”€ __init__.py
â”‚   â”‚   â”œâ”€â”€ main.py                      # Entry point de execuÃ§Ã£o do pacote
â”‚   â”‚   â”œâ”€â”€ assets/                      # MÃ³dulo de extraÃ§Ã£o de dados e chamadas Ã  API
â”‚   â”‚   â”‚   â”œâ”€â”€ __init__.py
â”‚   â”‚   â”‚   â””â”€â”€ url_iti.py               # ExtraÃ§Ã£o de detalhes e entidades do ITI
â”‚   â”‚   â”œâ”€â”€ processamento/               # MÃ³dulo de transformaÃ§Ã£o e normalizaÃ§Ã£o de dados
â”‚   â”‚   â”‚   â”œâ”€â”€ __init__.py
â”‚   â”‚   â”‚   â””â”€â”€ flatten.py               # Algoritmo de achatamento recursivo de JSON aninhado
â”‚   â”‚   â”œâ”€â”€ exportacao/                  # MÃ³dulo de carga e integraÃ§Ã£o com Databricks
â”‚   â”‚   â”‚   â”œâ”€â”€ __init__.py
â”‚   â”‚   â”‚   â””â”€â”€ upload_raw.py            # Upload de dados brutos para Volumes do Unity Catalog
â”‚   â”‚   â”œâ”€â”€ config/                      # ConfiguraÃ§Ãµes de conexÃ£o e variÃ¡veis
â”‚   â”‚   â””â”€â”€ output/                      # UtilitÃ¡rios de saÃ­da e escrita
â”‚   â”‚
â”‚   â””â”€â”€ ITI_ICP_BRASIL_etl/              # Pipelines declarativas DLT no Databricks
â”‚       â”œâ”€â”€ README.md
â”‚       â””â”€â”€ transformations/             # Scripts de transformaÃ§Ã£o (Raw -> Bronze -> Silver -> Gold)
â”‚           â””â”€â”€ .gitkeep
â”‚
â”œâ”€â”€ tests/                               # Testes automatizados (Unit & Integration tests)
â”‚   â”œâ”€â”€ conftest.py                      # Fixtures e inicializaÃ§Ã£o de sessÃ£o Spark/Connect
â”‚   â””â”€â”€ test_package.py                  # Testes unitÃ¡rios do pacote
â”‚
â”œâ”€â”€ fixtures/                            # Dados de teste e mocks
â”‚   â””â”€â”€ .gitkeep
â”‚
â”œâ”€â”€ .gitignore                           # Regras de exclusÃ£o do Git
â””â”€â”€ README.md                            # DocumentaÃ§Ã£o principal do projeto
```

---

## ðŸ› ï¸ Stack TecnolÃ³gica

- **OrquestraÃ§Ã£o & Plataforma:** [Databricks Asset Bundles (DABs)](https://docs.databricks.com/dev-tools/bundles/index.html)
- **Engine de Processamento:** Apache Spark / PySpark & [Delta Live Tables (DLT)](https://docs.databricks.com/delta-live-tables/index.html)
- **GovernanÃ§a & Armazenamento:** Databricks Unity Catalog (`Volumes` gerenciados e Delta Tables)
- **SDK & IntegraÃ§Ãµes:** [Databricks SDK para Python](https://docs.databricks.com/dev-tools/sdk-python.html) (`WorkspaceClient`)
- **Gerenciador de Pacotes:** [Astral uv](https://docs.astral.sh/uv/) / [Hatchling](https://hatch.pypa.io/)
- **Qualidade de CÃ³digo & Linter:** [Ruff](https://astral.sh/ruff)
- **Testes:** [pytest](https://docs.pytest.org/) com Databricks Connect (`databricks-connect`)
- **CI/CD:** GitHub Actions com execuÃ§Ã£o em matriz multi-versÃ£o (Python 3.10, 3.11 e 3.12)

---

## ðŸš€ Como ComeÃ§ar

### 1. PrÃ©-requisitos
- **Python:** VersÃ£o `>=3.10, <3.13`
- **Gerenciador UV:** [Instalar o UV](https://docs.astral.sh/uv/getting-started/installation/)
- **Databricks CLI:** [Instalar Databricks CLI v0.200+](https://docs.databricks.com/dev-tools/cli/databricks-cli.html)

### 2. Configurar o Ambiente Local

Sincronize as dependÃªncias virtuais com `uv` diretamente na raiz do projeto:

```bash
# Criar ambiente virtual e instalar dependÃªncias de desenvolvimento
uv sync --dev
```

### 3. AutenticaÃ§Ã£o no Databricks

Configure a autenticaÃ§Ã£o com o seu workspace:

```bash
databricks auth login --host https://dbc-15e61da2-fb6a.cloud.databricks.com
```

---

## âš™ï¸ ExecuÃ§Ã£o e IngestÃ£o de Dados

### ðŸ“¥ IngestÃ£o Manual da API para a Camada Raw (Volume Unity Catalog)

Para extrair os dados da API oficial do ITI e enviar o JSON bruto para o Volume `/Volumes/lakehouse_iti/0_raw/raw/entidades.json`:

```bash
uv run python src/ITI_ICP_BRASIL/exportacao/upload_raw.py
```

### ðŸš€ Comandos do Databricks Asset Bundle:

```bash
# Validar sintaxe das configuraÃ§Ãµes e schemas do bundle
databricks bundle validate

# Deploy em ambiente de desenvolvimento
databricks bundle deploy

# Deploy em ambiente de produÃ§Ã£o
databricks bundle deploy --target prod

# Executar pipeline no Databricks
databricks bundle run
```

---

## ðŸ§ª Testes e Qualidade de CÃ³digo

Execute a suÃ­te de testes unitÃ¡rios e o linter:

```bash
# Executar testes unitÃ¡rios com pytest
uv run pytest

# Executar linter e checagem de estilo de cÃ³digo
uv run ruff check .

# Formatar cÃ³digo automaticamente
uv run ruff format .
```

