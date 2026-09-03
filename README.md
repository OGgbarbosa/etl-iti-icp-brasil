# 🏛️ ETL ITI - ICP-Brasil (Databricks Lakehouse & Asset Bundles)

> ⚠️ **Status do Projeto:** 🚧 **Em Desenvolvimento (*Work in Progress*)**  
> Pipeline de engenharia de dados voltado à extração, normalização e carga de dados públicos de entidades e certificados digitais da Infraestrutura de Chaves Públicas Brasileira (ICP-Brasil) no Databricks Unity Catalog.

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
│ └── Armazenamento de arquivos semiestruturados brutos (.json / .csv)             │
└─────────────────────────────────┬────────────────────────────────────────────────┘
                                  │
                                  ▼ (Delta Live Tables / Apache Spark)
┌──────────────────────────────────────────────────────────────────────────────────┐
│ Camada 1_bronze (Tabelas Delta)                                                  │
│ └── Ingestão bruta com esquema tipado e metadados de auditoria                   │
└─────────────────────────────────┬────────────────────────────────────────────────┘
                                  │
                                  ▼ (Transformações, Limpeza & Qualidade)
┌──────────────────────────────────────────────────────────────────────────────────┐
│ Camada 2_silver (Tabelas Delta)                                                  │
│ └── Entidades normalizadas, deduplicadas e validadas                             │
└─────────────────────────────────┬────────────────────────────────────────────────┘
                                  │
                                  ▼ (Modelagem Dimensional & Agregações)
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
│   │   │   └── upload_raw.py            # Upload de dados brutos para Volumes do Unity Catalog
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
- **Armazenamento e Governança:** Databricks Unity Catalog (`Volumes` gerenciados e Tabelas Delta)
- **SDK de Integração:** [Databricks SDK para Python](https://docs.databricks.com/dev-tools/sdk-python.html) (`WorkspaceClient`)
- **Gerenciador de Dependências e Ambientes:** [Astral uv](https://docs.astral.sh/uv/) / [Hatchling](https://hatch.pypa.io/)
- **Análise Estática de Código e Formatação:** [Ruff](https://astral.sh/ruff)
- **Framework de Testes Automatizados:** [pytest](https://docs.pytest.org/) com Databricks Connect (`databricks-connect`)
- **Integração e Entrega Contínuas (CI/CD):** GitHub Actions com matriz de execução multi-versão (Python 3.10, 3.11 e 3.12)

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

Realize a autenticação segura com o workspace de destino:

```bash
databricks auth login --host https://dbc-15e61da2-fb6a.cloud.databricks.com
```

---

## 5. ⚙️ Execução das Etapas de ETL e Ingestão de Dados

### 5.1. Ingestão da Camada Raw (Volumes do Unity Catalog)

Para executar a chamada à API oficial do ITI, realizar o desaninhamento das estruturas hierárquicas e persistir o arquivo bruto no Volume `/Volumes/lakehouse_iti/0_raw/raw/entidades.json`:

```bash
uv run python src/ITI_ICP_BRASIL/exportacao/upload_raw.py
```

### 5.2. Comandos do Databricks Asset Bundle (DAB)

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
