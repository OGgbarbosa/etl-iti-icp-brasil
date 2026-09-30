# 🤖 Databricks Genie Space - Inteligência Analítica ICP-Brasil

Este diretório contém todos os artefatos declarativos, scripts de automação, definições de governança semântica e manuais de regras de negócio para o **Databricks Genie Space** do ecossistema **ICP-Brasil**.

O Genie atua como o analista conversacional do Lakehouse, permitindo que usuários de negócio, analistas e diretores realizem perguntas em linguagem natural (português) e recebam consultas SQL precisas, dados tabulares e respostas executivas consolidadas.

---

## 📌 1. Identificadores do Espaço no Databricks

* **Nome do Espaço**: `Genie - Inteligência Analítica ICP-Brasil`
* **Space ID**: `<genie_space_id>` *(gerado automaticamente no deploy via Databricks SDK)*
* **SQL Warehouse ID**: `${var.warehouse_id}` *(parametrizado via Databricks Asset Bundles / Databricks SQL)*
* **Catálogo & Schema Padrão**: `lakehouse_iti.3_gold`
* **Status**: 🟢 **Ativo e implantado via script de automação**

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

---

### Exemplo 3: Emissões Únicas vs. Total e Projeções para 2027
* **Pergunta do Usuário**: *"Há uma estimativa das emissões realizadas quantas são únicas ? quais os tipos que tendem a ter crescimento ou redução para 2027 ?"*
* **Racional Técnico no Lakehouse**:
  - Consulta cruzada entre `lakehouse_iti.3_gold.kpi_resumo_executivo`, `lakehouse_iti.3_gold.fato_emissao_mensal` e `lakehouse_iti.3_gold.fato_segmentacao_certificados`.
* **Resposta Consolidada do Genie**:
  > *- **Certificados Ativos Únicos (2026):** **99,6% dos certificados emitidos estão ativos (9,52 milhões de certificados únicos válidos)** em circulação, evidenciando rápida expansão da taxa de renovação e retenção frente a 2021 (0,69%).*  
  > *- **Sazonalidade & Projeção 2027:** Emissões com picos consistentes em janeiro, março e julho. A projeção para 2027 aponta para **9,12 milhões de emissões (-4,57%)**, indicando maturação e estabilização de mercado.*  
  > *- **Tendências de Tecnologia:** **A1 em Software (69,24% do mercado)** em crescimento acelerado por conveniência e custo reduzido; **A3 em Hardware (30,38%)** estável em setores regulados.*  
  > *- **Segmentação de Titularidade:** Divisão paritária sustentada por obrigações fiscais: **50,9% Pessoa Jurídica** e **48,8% Pessoa Física**.*

---

### Exemplo 4: Plano de Expansão e Precificação Regional para AC de 1º Nível
* **Pergunta do Usuário**: *"Pensando em um plano de crescimento de mercado sendo uma AC de 1º nível, qual a melhor região para expansão e oferta de certificado ? Estipule o preço médio praticado na região"*
* **Racional Técnico no Lakehouse**:
  - Cruzamento de **oferta instalada** (`dim_entidade`, `dim_hierarquia`) com **demanda de mercado** (`fato_distribuicao_geografica`).
* **Resposta Consolidada do Genie**:
  > *- **Recomendação Estratégica: Região NORDESTE** (seguida pela região Norte como oportunidade secundária).*  
  > *- **Demanda Reprimida & Baixa Concorrência:** 18,64% de participação nacional (615.778 emissões em 2026), 232 ARs ativas e **Zero ACs de 1º Nível sediadas localmente**.*  
  > *- **Sobrecarga Operacional:** Relação de **2.654 emissões por AR** — a maior do país, provando carência de infraestrutura local de suporte.*  
  > *- **Precificação Recomendada (Nordeste):** Certificados A1 sugeridos a **R$ 180 - R$ 250**; Certificados A3 sugeridos a **R$ 280 - R$ 380** (ajuste regional de +10% a +15% justificado por margem superior e frete de hardware).*  
  > *- **Projeção de Captura (Ano 1):** Captura de 15% do mercado regional gerando **~92.000 certificados e R$ 23 milhões em receita bruta**.*

---

### Exemplo 5: Benchmarking das Top Certificadoras e Modelo Híbrido de Negócio
* **Pergunta do Usuário**: *"Perfeito, qual o modelo de negócio seria interessante a ser introduzido ? Vendas em atacado ou varejo ? Observe top 5 certificadoras de 1º e 2º nível e procure modelos ofertados no mercado, observe sites dessas ACS e liste os melhores modelos de implantação."*
* **Racional Técnico no Lakehouse**:
  - Consulta hierárquica recursiva em `lakehouse_iti.3_gold.fato_metricas_entidades` e correlação setorial com `dim_inteligencia_mercado`.
* **Resposta Consolidada do Genie**:
  > *- **Benchmarking das Líderes:** Identificou os arquétipos vencedores do mercado privado:*  
  >   * * **AC SOLUTI**: Líder em diversificação de atacado com **20 ACs de 2º nível** especializadas (RFB, JUS, Múltipla).*  
  >   * * **AC SAFEWEB**: Modelo híbrido altamente eficiente com **455 ARs e 7 ACs de 2º nível**, maximizando receita multiproduto.*  
  >   * * **AC CERTISIGN**: Marca premium com **400 ARs** e modelo híbrido consolidado.*  
  >   * * **AC VALID**: Especialização vertical B2B com **11 ACs de 2º nível** focadas em nichos regulados.*  
  > *- **Modelo Recomendado: Híbrido (60% Atacado / 40% Varejo):** Combina escala rápida no atacado via ACs de 2º nível parceiras (margem de 25-35%) com margens saudáveis no varejo direto via ARs próprias (margem de 45-55%).*  
  > *- **Projeção Financeira em 3 Anos (Nordeste):** Ano 1: R$ 21M (foco atacado) ➔ Ano 2: R$ 42M (expansão varejo) ➔ Ano 3: R$ 68M (maturação com lucro líquido de R$ 25,8M).*  
  > *- **Investimento Inicial:** Estimado em **R$ 3,5 a R$ 4,5 milhões** (credenciamento, infraestrutura técnica, 3 ARs próprias e capital de giro).*

