# 🏛️ ETL ITI - ICP-Brasil (Databricks Asset Bundles)

> ⚠️ **Status do Projeto:** 🚧 **Em Desenvolvimento (Work in Progress)**  
> Novas funcionalidades, ingestões e pipelines de dados do ITI / ICP-Brasil estão sendo implementadas ativamente.

---

Pipeline de Engenharia de Dados e ETL para processamento e ingestão de dados públicos do **ITI (Instituto Nacional de Tecnologia da Informação) / ICP-Brasil**, estruturado utilizando **Databricks Asset Bundles (DABs)**, **Delta Live Tables (DLT)** e **PySpark**.

---

## 📌 Visão Geral

Este repositório contém a infraestrutura como código (IaC) e o código de transformação de dados para ingestão, processamento e modelagem dos dados de certificados e entidades do ecossistema ICP-Brasil no **Databricks Lakehouse**, garantindo governança via **Unity Catalog**, versionamento rigoroso e ciclo de vida de desenvolvimento automatizado (CI/CD).

---

## 🗂️ Estrutura do Projeto

```text
etl-iti-icp-brasil/
├── .github/                             # Automações e CI/CD (GitHub Actions)
│   └── workflows/
├── .vscode/                             # Configurações do VS Code e stubs de tipagem Spark
│   ├── settings.json
│   └── __builtins__.pyi
│
├── databricks.yml                       # Configuração principal do Databricks Asset Bundle
├── pyproject.toml                       # Gerenciamento de dependências Python (uv / hatchling)
├── uv.lock                              # Lockfile de dependências com versões exatas
│
├── resources/                           # Declaração de recursos e orquestrações Databricks
│   └── ITI_ICP_BRASIL_etl.pipeline.yml  # Definição da pipeline Delta Live Tables (DLT)
│
├── src/                                 # Código-fonte Python e pipelines
│   ├── ITI_ICP_BRASIL/                  # Biblioteca Python compartilhada
│   │   ├── __init__.py
│   │   ├── main.py                      # Entry point de execução do pacote
│   │   ├── config/                      # Configurações de conexões e parâmetros
│   │   └── output/                      # Utilitários de saída e escrita
│   │
│   └── ITI_ICP_BRASIL_etl/              # Pipeline declarativa DLT
│       ├── README.md
│       └── transformations/             # Scripts de transformações (Bronze -> Silver -> Gold)
│           └── .gitkeep
│
├── tests/                               # Testes automatizados (Unit & Integration tests)
│   ├── conftest.py                      # Fixtures e inicialização de sessão Spark/Connect
│   └── test_package.py                  # Teste unitário base do pacote
│
├── fixtures/                            # Mock data e fixtures para testes
│   └── .gitkeep
│
├── .gitignore                           # Regras de exclusão do Git
└── README.md                            # Documentação principal do projeto
```

---

## 🛠️ Stack Tecnológica

- **Orquestração & Plataforma:** [Databricks Asset Bundles (DABs)](https://docs.databricks.com/dev-tools/bundles/index.html)
- **Engine de Processamento:** Apache Spark / PySpark & [Delta Live Tables (DLT)](https://docs.databricks.com/delta-live-tables/index.html)
- **Governança de Dados:** Databricks Unity Catalog (`catalog` e `schema` configuráveis por ambiente)
- **Gerenciador de Pacotes:** [Astral uv](https://docs.astral.sh/uv/) / [Hatchling](https://hatch.pypa.io/)
- **Qualidade de Código & Linter:** [Ruff](https://astral.sh/ruff)
- **Testes:** [pytest](https://docs.pytest.org/) com Databricks Connect (`databricks-connect`)

---

## 🚀 Como Começar

### 1. Pré-requisitos
- **Python:** Versão `>=3.10, <3.13`
- **Gerenciador UV:** [Instalar o UV](https://docs.astral.sh/uv/getting-started/installation/)
- **Databricks CLI:** [Instalar Databricks CLI v0.200+](https://docs.databricks.com/dev-tools/cli/databricks-cli.html)

### 2. Configurar o Ambiente Local

Sincronize as dependências virtuais com `uv` diretamente na raiz do projeto:

```bash
# Criar ambiente virtual e instalar dependências de desenvolvimento
uv sync --dev
```

### 3. Autenticação no Databricks

```bash
databricks auth login --host https://dbc-15e61da2-fb6a.cloud.databricks.com
```

---

## ⚙️ Ciclo de Desenvolvimento & Deploy

O projeto conta com alvos de deploy parametrizados (`dev` e `prod`) em [databricks.yml](file:///c:/Users/gbarb/Documents/PITON/icp_brasil/databricks.yml):

| Alvo (`target`) | Modo | Catálogo / Schema | Descrição |
| :--- | :--- | :--- | :--- |
| **`dev`** *(default)* | Development | `workspace` / `${workspace.current_user.short_name}` | Recursos prefixados com `[dev user]`, triggers e schedules pausados. |
| **`prod`** | Production | `workspace` / `prod` | Deploy para o path centralizado de produção com execução agendada. |

### Comandos do Databricks Bundle:

```bash
# Validar sintaxe das configurações e schemas
databricks bundle validate

# Deploy em ambiente de desenvolvimento (padrão)
databricks bundle deploy

# Deploy em ambiente de produção
databricks bundle deploy --target prod

# Executar pipeline no Databricks
databricks bundle run
```

---

## 🧪 Testes e Qualidade de Código

Execute a suíte de testes unitários e o linter diretamente na raiz:

```bash
# Executar testes unitários com pytest
uv run pytest

# Executar linter e checagem de estilo de código
uv run ruff check .

# Formatar código automaticamente
uv run ruff format .
```
