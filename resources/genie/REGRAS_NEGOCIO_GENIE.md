# Regras de Negócio e Diretrizes Semânticas para o Databricks Genie - ICP-Brasil Lakehouse

Este documento consolida as diretrizes operacionais, o glossário semântico e as regras canônicas de consulta SQL para o espaço conversacional **Databricks Genie**, calibrado estritamente com as tabelas e dados da camada **Gold (`lakehouse_iti.3_gold`)**.

---

## 1. Glossário e Taxonomia da ICP-Brasil

| Termo / Sigla | Descrição no Negócio | Representação no Lakehouse (`DS_TIPO` / `DS_NIVEL`) |
| :--- | :--- | :--- |
| **ICP-Brasil** | Infraestrutura de Chaves Públicas Brasileira. | Conjunto de todas as entidades do Lakehouse. |
| **AC Raiz** | Autoridade máxima e âncora de confiança da cadeia, operada pelo ITI. | `DS_TIPO = 'AUTORIDADE CERTIFICADORA'` e `DS_NIVEL = 0`. |
| **AC 1º Nível** | AC intermediária subordinada diretamente à AC Raiz. | `DS_TIPO = 'AUTORIDADE CERTIFICADORA'` e `DS_NIVEL = 1`. |
| **AC 2º Nível** | AC emissora final subordinada a uma AC de 1º Nível (vincula ARs). | `DS_TIPO = 'AUTORIDADE CERTIFICADORA'` e `DS_NIVEL = 2`. |
| **AR (Autoridade de Registro)** | Ponto de atendimento físico ou remoto que identifica e valida os titulares. É a ponta da infraestrutura física. | `DS_TIPO = 'AUTORIDADE DE REGISTRO'` (`DS_NIVEL = 3`). |
| **ACT (Carimbo do Tempo)** | Autoridade que atesta a data e hora exata de documentos eletrônicos. | `DS_TIPO = 'AUTORIDADE DE CARIMBO DO TEMPO'`. |
| **PSS** | Prestador de Serviço de Suporte técnico/infraestrutura. | `DS_TIPO = 'PRESTADOR DE SERVICO DE SUPORTE'`. |
| **A1** | Certificado digital de software (validade de 1 ano, armazenado no computador). | `DS_TIPO_CERTIFICADO = 'A1'`. |
| **A3** | Certificado digital criptográfico em hardware (token, smartcard ou nuvem, validade de até 3 ou 5 anos). | `DS_TIPO_CERTIFICADO = 'A3'`. |

---

## 2. Regras de Filtragem e Padrões de Consulta

### 2.1. Entidades Ativas e Operacionais
* **ATENÇÃO CRÍTICA**: No Lakehouse, a situação operacional de entidades ativas/vigentes é **`CREDENCIADA`** (e **NUNCA** `'ATIVA'`).
* Quando o usuário solicitar *"entidades ativas"*, *"ARs ativas"*, *"redes operacionais"* ou *"quantas entidades existem hoje"*, utilize:
  ```sql
  WHERE DS_SITUACAO = 'CREDENCIADA'
  ```
* Se a pergunta for especificamente sobre Autoridades de Registro (ARs):
  ```sql
  WHERE DS_SITUACAO = 'CREDENCIADA'
    AND DS_TIPO = 'AUTORIDADE DE REGISTRO'
  ```

### 2.2. Coluna Canônica de Quantidade e Volumetria
* **NÃO EXISTE** a coluna `NR_QUANTIDADE` nas tabelas fato.
* O valor métrico quantitativo (contagem de certificados, emissões ou entidades) chama-se **`VL_METRICA`** em todas as tabelas fato (`fato_emissao_mensal`, `fato_distribuicao_geografica`, `fato_segmentacao_certificados`, `fato_infraestrutura_credenciamento`, `kpi_resumo_executivo`).

### 2.3. Séries Temporais e Tipos de Emissão (`fato_emissao_mensal`)
* **Novas emissões ocorridas em um mês específico**:
  ```sql
  SELECT DT_ANO, DT_MES_ANO, SUM(VL_METRICA) AS TOTAL_EMISSOES_MES
  FROM lakehouse_iti.3_gold.fato_emissao_mensal
  WHERE DS_TIPO_SERIE = 'MENSAL CORRENTE'
  GROUP BY DT_ANO, DT_MES_ANO
  ORDER BY DT_MES_ANO;
  ```
* **Estoque acumulado de certificados válidos/ativos no ano**:
  ```sql
  WHERE DS_TIPO_SERIE = 'HISTORICO ATIVOS' AND DS_GRANULARIDADE = 'ANUAL'
  ```
* **Total consolidado de certificados emitidos no ano**:
  ```sql
  WHERE DS_TIPO_SERIE = 'HISTORICO EMITIDOS' AND DS_GRANULARIDADE = 'ANUAL'
  ```

---

## 3. Oferta vs. Demanda (Infraestrutura Instalada x Mercado Consumidor)

Ao correlacionar **capacidade de atendimento (oferta)** com o **volume de mercado (demanda)**:

### 3.1. Oferta (Infraestrutura / Presença Física de ARs)
* **Via Cadastro Mestre (`dim_entidade`)**:
  ```sql
  SELECT 
      SG_UF,
      DS_REGIAO,
      COUNT(DISTINCT ID_ENTIDADE) AS QTD_ARS_OFERTA
  FROM lakehouse_iti.3_gold.dim_entidade
  WHERE DS_SITUACAO = 'CREDENCIADA'
    AND DS_TIPO = 'AUTORIDADE DE REGISTRO'
  GROUP BY SG_UF, DS_REGIAO;
  ```
* **Via Tabela Fato Geográfica (`fato_distribuicao_geografica`)**:
  ```sql
  WHERE DS_METRICA = 'TOTAL AR ESTADO' 
    AND DS_TIPO_OBJETO = 'ENTIDADES'
  ```

### 3.2. Demanda (Volume de Emissões de Certificados)
* **Por UF / Região Geográfica (`fato_distribuicao_geografica`)**:
  ```sql
  SELECT 
      SG_UF,
      DS_REGIAO,
      SUM(VL_METRICA) AS TOTAL_EMISSOES_DEMANDA
  FROM lakehouse_iti.3_gold.fato_distribuicao_geografica
  WHERE DS_METRICA = 'EMISSAO ANUAL' -- ou 'EMISSAO MENSAL'
    AND DS_TIPO_OBJETO = 'CERTIFICADOS'
  GROUP BY SG_UF, DS_REGIAO;
  ```

---

## 4. Topologia e Subordinação da Árvore de Confiança

Para perguntas do tipo *"Quais ACs têm mais ARs vinculadas?"* ou *"Quantas entidades estão abaixo de uma autoridade?"*:
* **Utilize preferencialmente a tabela pré-calculada `lakehouse_iti.3_gold.fato_metricas_entidades`**:
  * `NR_AGREGADOS_AR`: Total de ARs subordinadas à AC em toda a sua cadeia descendente.
  * `NR_AGREGADOS_AC_NIVEL_1`: Quantidade de ACs de 1º Nível vinculadas.
  * `NR_AGREGADOS_AC_NIVEL_2`: Quantidade de ACs de 2º Nível vinculadas.
* Exemplo de ranking das 5 maiores ACs:
  ```sql
  SELECT 
      ID_ENTIDADE,
      DS_ENTIDADE,
      SG_UF,
      NR_AGREGADOS_AR
  FROM lakehouse_iti.3_gold.fato_metricas_entidades
  WHERE DS_TIPO = 'AUTORIDADE CERTIFICADORA'
    AND DS_SITUACAO = 'CREDENCIADA'
  ORDER BY NR_AGREGADOS_AR DESC
  LIMIT 5;
  ```

---

## 5. Segmentação de Mercado (`fato_segmentacao_certificados`)

* **Por Tipo de Certificado (A1 vs. A3)**:
  ```sql
  WHERE DS_CATEGORIA_CORTE = 'TIPO CERTIFICADO'
    AND DS_TIPO_CERTIFICADO IN ('A1', 'A3')
  ```
* **Por Titular / Usuário (Pessoa Física vs. Pessoa Jurídica vs. Aplicação)**:
  ```sql
  WHERE DS_CATEGORIA_CORTE IN ('TIPO USO', 'TIPO TITULAR')
    AND DS_TIPO_USUARIO IN ('PESSOA FISICA', 'PESSOA JURIDICA', 'EQUIPAMENTO/APLICACAO')
  ```

---

## 6. Formatação e Tratamento de CNPJs

* A coluna `NR_CNPJ` na tabela `lakehouse_iti.3_gold.dim_entidade` é textual (`STRING`) contendo exatamente **14 dígitos** preenchidos com zeros à esquerda (`LPAD`).
* Quando uma pergunta em linguagem natural incluir CNPJ formatado com pontuação (ex: `00.394.460/0058-87` ou `00394460005887`), o Genie deve higienizar os caracteres antes da busca:
  ```sql
  WHERE NR_CNPJ = LPAD(REGEXP_REPLACE(:cnpj_input, '[^0-9]', ''), 14, '0')
  ```

---

## 7. KPIs Executivos Rápidos (`kpi_resumo_executivo`)

Para resumos instantâneos da diretoria sem necessidade de recalcular séries completas:
* `DS_INDICADOR`:
  * `'CERTIFICADOS ATIVOS'`: Estoque geral ativo.
  * `'CERTIFICADOS EMITIDOS'`: Total emitido consolidado.
  * `'AUTORIDADE REGISTRO'`, `'AUTORIDADE CERTIFICADORA 1'`, `'AUTORIDADE CERTIFICADORA 2'`: Contagem de entidades da rede.
* `DS_TIPO_INDICADOR`:
  * `'ACUMULADO ATUAL'`: Valor consolidado no ano corrente.
  * `'COMPARATIVO PERCENTUAL'`: Variação percentual versus período anterior.
  * `'PROJECAO'`: Estimativa de encerramento do exercício.

---

## 8. Inteligência Setorial e Fontes Externas

O Genie dispõe de uma camada de inteligência com notícias, análises de mercado, novidades regulatórias e tendências tecnológicas coletadas de 5 fontes de referência do setor:
1. **ANCD** (`https://ancd.org.br/`): Associação Nacional de Certificação Digital — dados do setor, relatórios institucionais e regulatórios.
2. **AR Federal Blog** (`https://arfederal.com.br/blog/`): Operações práticas de ARs, emissão remota e dia a dia de credenciamento.
3. **Crypto ID** (`https://cryptoid.com.br/`): Maior portal de identificação digital, criptografia pós-quântica, biometria e cibersegurança do Brasil.
4. **ABRID** (`https://www.abrid.org.br/`): Associação Brasileira das Empresas de Tecnologia em Identificação Digital.
5. **Convergência Digital** (`https://convergenciadigital.com.br/`): Notícias governamentais, políticas públicas de TIC e debates regulatórios.

### 8.1. Consulta Direta via Função Analítica (Genie Tool)
Quando o usuário perguntar sobre notícias, tendências, regulação, DREX ou opiniões de entidades setoriais, invoque a ferramenta canônica:
```sql
SELECT 
    NM_FONTE,
    DS_TITULO,
    DS_RESUMO,
    DS_URL_ORIGEM,
    DT_PUBLICACAO
FROM lakehouse_iti.3_gold.fn_consultar_inteligencia_setorial(:termo_busca, :nome_fonte);
```

### 8.2. Cruzamento de Séries Históricas com Fatos de Mercado
Sempre que uma pergunta exigir contextualizar o **porquê** de oscilações na emissão ou credenciamento (ex: *"Por que as emissões aumentaram no 1º semestre de 2026?"*), o Genie deve:
1. Consultar a série quantitativa em `lakehouse_iti.3_gold.fato_emissao_mensal`.
2. Correlacionar com as matérias e comunicados na tabela `lakehouse_iti.3_gold.dim_inteligencia_mercado` filtrando pelo período e palavras-chave.
3. Citar a fonte externa oficial (`NM_FONTE`) e a URL correspondente (`DS_URL_ORIGEM`).
