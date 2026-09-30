# 🤖 Databricks Genie Space - Inteligência Analítica ICP-Brasil

Este diretório contém todos os artefatos declarativos, scripts de automação, definições de governança semântica e manuais de regras de negócio para o **Databricks Genie Space** do ecossistema **ICP-Brasil**.

O Genie atua como o analista conversacional do Lakehouse, permitindo que usuários de negócio, analistas e diretores realizem perguntas em linguagem natural (português) e recebam consultas SQL precisas, dados tabulares e respostas executivas consolidadas.

---

## 📌 1. Identificadores do Espaço no Databricks

* **Nome do Espaço**: `Genie - Inteligência Analítica ICP-Brasil`
* **Space ID**: `01f1bc8146741edaab32c05d8d67fda9`
* **SQL Warehouse ID**: `e054bea1d2fa6e66` (Serverless / Auto-start)
* **Catálogo & Schema Padrão**: `lakehouse_iti.3_gold`
* **Status**: 🟢 **Ativo, implantado e validado via Genie API**

---

## 📂 2. Estrutura Organizada em Subpastas

```text
resources/genie/
├── README.md                      # Guia arquitetural e operacional do módulo (este arquivo)
│
├── config/                        # Especificações e definições declarativas do espaço
│   ├── ITI_ICP_BRASIL_genie.yml   # Definição do recurso Genie Space (DAB)
│   └── genie_space_definition.json # Exportação serializada versionada (JSON v2) do espaço
│
├── docs/                          # Manuais de negócio e diretrizes semânticas
│   └── REGRAS_NEGOCIO_GENIE.md    # Glossário ITI, filtros obrigatórios, padrões SQL e inteligência setorial
│
├── sql/                           # Scripts SQL de governança do Unity Catalog
│   └── comments_gold_genie.sql    # 38 instruções DDL de comentários em tabelas e colunas Gold
│
└── scripts/                       # Automação de deploy e aplicação de governança
    ├── deploy_genie.py            # Script idempotente de deploy/atualização contínua via Databricks SDK
    └── apply_comments.py          # Executor em lote dos comentários via Statement Execution API
```

---

## 🏛️ 3. Tabelas Mapeadas no Espaço (9 Tabelas Gold)

O espaço consome exclusivamente dados curados e auditados da camada **Gold (`lakehouse_iti.3_gold`)**:

| Tabela | Categoria | Finalidade no Genie |
| :--- | :--- | :--- |
| `dim_entidade` | Dimensão | Cadastro mestre com dados geográficos (UF, Região), CNPJs higienizados e status operacional (`CREDENCIADA`). |
| `dim_hierarquia` | Dimensão | Grafo de subordinação direta pai-filho entre entidades credenciadas (da AC Raiz às ARs). |
| `fato_metricas_entidades` | Fato Analítico | Tabela recursiva pré-agregada com a quantidade de ARs (`NR_AGREGADOS_AR`), ACs N1 e ACs N2 vinculadas a cada autoridade. |
| `fato_emissao_mensal` | Fato Histórico | Séries temporais de emissão mensal e estoque de certificados válidos (`MENSAL CORRENTE`, `HISTORICO ATIVOS`, `HISTORICO EMITIDOS`). |
| `fato_distribuicao_geografica` | Fato Espacial | Distribuição territorial de certificados emitidos e de ARs ativas por UF e Macrorregião brasileira. |
| `fato_segmentacao_certificados` | Fato Segmentado | Emissões fatiadas por tecnologia (A1 em software vs A3 em hardware/nuvem) e perfil de titular (PF, PJ, Equipamento). |
| `fato_infraestrutura_credenciamento` | Fato Operacional | Volumetria e ritmo de novos credenciamentos de Autoridades de Registro (ARs). |
| `kpi_resumo_executivo` | KPI Executivo | Indicadores agregados consolidados e comparativos percentuais para respostas rápidas de diretoria. |
| `dim_inteligencia_mercado` | Inteligência Externa | Notícias, comunicados, artigos técnicos e notas regulatórias coletadas de 5 portais (ANCD, Crypto ID, ABRID, AR Federal, Convergência Digital). |

---

## 🛠️ 4. Como Executar e Manter

### 4.1. Deploy ou Atualização do Genie Space
Para criar ou atualizar o espaço no workspace com as instruções, tabelas e perguntas de exemplo mais recentes:
```powershell
$env:PYTHONPATH="src"
.\.venv\Scripts\python.exe resources/genie/scripts/deploy_genie.py
```
> O script é **idempotente**: localiza o espaço existente pelo título e atualiza o payload serializado, evitando duplicidades no workspace.  
> **Dica**: Use o parâmetro opcional `--with-comments` para aplicar os comentários semânticos e implantar o Genie em uma única etapa:
> ```powershell
> .\.venv\Scripts\python.exe resources/genie/scripts/deploy_genie.py --with-comments
> ```

### 4.2. Aplicação de Comentários no Unity Catalog
Para reaplicar ou sincronizar todos os 38 comentários semânticos de tabelas e colunas no Unity Catalog:
```powershell
$env:PYTHONPATH="src"
.\.venv\Scripts\python.exe resources/genie/scripts/apply_comments.py
```

### 4.3. Coleta de Inteligência Externa (RSS)
Para forçar uma rodada de coleta de notícias dos 5 portais de referência e atualizar a tabela `dim_inteligencia_mercado`:
```powershell
$env:PYTHONPATH="src"
.\.venv\Scripts\python.exe -m ITI_ICP_BRASIL.processamento.inteligencia_externa
```

---

## 💡 5. Exemplos de Perguntas e Respostas Validadas

O espaço foi testado e validado ponta a ponta via **Genie API**:

### Exemplo 1: Topologia da Rede Credenciada
* **Pergunta do Usuário**: *"Quais são as 5 maiores ACs em número de ARs subordinadas?"*
* **SQL Gerado pelo Genie**:
  ```sql
  SELECT ID_ENTIDADE, DS_ENTIDADE, SG_UF, DS_TIPO, NR_AGREGADOS_AR
  FROM lakehouse_iti.3_gold.fato_metricas_entidades
  WHERE DS_TIPO IN ('AC 1º NÍVEL', 'AC 2º NÍVEL') AND DS_SITUACAO = 'CREDENCIADA'
  ORDER BY NR_AGREGADOS_AR DESC
  LIMIT 5;
  ```
* **Resposta do Genie**:
  > *"As 5 maiores ACs em número de ARs subordinadas são todas credenciadas e incluem:*
  > *- **AC RFB** (DF) com **1.642 ARs***
  > *- **AC JUS** (DF) com **1.011 ARs***
  > *- **AC SAFEWEB** (RS) com **455 ARs***
  > *- **AC SAFEWEB RFB** (RS) com **450 ARs***
  > *- **AC SAFEWEB CD** (RS) com **438 ARs***
  > *A AC com maior número de ARs subordinadas é a **AC RFB**."*

---

### Exemplo 2: Inteligência Setorial Externa
* **Pergunta do Usuário**: *"O que a ANCD publicou recentemente sobre as emissões da ICP-Brasil?"*
* **SQL Gerado pelo Genie**:
  ```sql
  SELECT dim_inteligencia_mercado.DS_TITULO, dim_inteligencia_mercado.DT_PUBLICACAO, dim_inteligencia_mercado.DS_URL_ORIGEM
  FROM lakehouse_iti.3_gold.dim_inteligencia_mercado
  WHERE dim_inteligencia_mercado.NM_FONTE = 'ANCD'
    AND (
      (dim_inteligencia_mercado.DS_TITULO ILIKE '%emiss%' AND dim_inteligencia_mercado.DS_TITULO ILIKE '%ICP-Brasil%')
      OR (dim_inteligencia_mercado.DS_TAGS ILIKE '%emiss%' AND dim_inteligencia_mercado.DS_TAGS ILIKE '%ICP-Brasil%')
    )
  ORDER BY dim_inteligencia_mercado.DT_PUBLICACAO DESC
  LIMIT 5;
  ```
* **Resposta do Genie**:
  > *"A ANCD publicou recentemente dois destaques sobre as emissões da ICP-Brasil: em setembro de 2026, noticiou o crescimento das emissões de certificados digitais em agosto, e em julho de 2026, destacou o aumento das emissões e da base de certificados ativos no primeiro semestre do ano. Ambos os conteúdos reforçam a tendência de crescimento nas emissões de certificados digitais pela ICP-Brasil."*
