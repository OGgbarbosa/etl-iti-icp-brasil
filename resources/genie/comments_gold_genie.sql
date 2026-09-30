-- =====================================================================
-- 1. Dimensão Entidades (Rede Instalada / Cadastro Mestre)
-- =====================================================================
COMMENT ON TABLE lakehouse_iti.3_gold.dim_entidade IS 
'Dimensão mestre de entidades físicas e lógicas credenciadas na ICP-Brasil. Cada linha representa uma autoridade com localização geográfica e situação operacional.';

ALTER TABLE lakehouse_iti.3_gold.dim_entidade ALTER COLUMN DS_TIPO 
COMMENT 'Classificação do tipo de entidade: "AR" (Autoridade de Registro), "AC 1º NÍVEL", "AC 2º NÍVEL" ou "AC RAIZ".';

ALTER TABLE lakehouse_iti.3_gold.dim_entidade ALTER COLUMN DS_SITUACAO 
COMMENT 'Status operacional da autoridade. Valores possíveis: "CREDENCIADA" ou "EM CREDENCIAMENTO". Para autoridades vigentes/ativas em operação, filtrar sempre por DS_SITUACAO = "CREDENCIADA".';

ALTER TABLE lakehouse_iti.3_gold.dim_entidade ALTER COLUMN SG_UF 
COMMENT 'Sigla da Unidade Federativa (estado brasileiro) onde a entidade está sediada, com 2 caracteres em maiúsculo (ex: "SP", "RJ", "DF").';

ALTER TABLE lakehouse_iti.3_gold.dim_entidade ALTER COLUMN DS_REGIAO 
COMMENT 'Macrorregião geográfica brasileira derivada da UF em maiúsculo: "SUDESTE", "SUL", "NORDESTE", "CENTRO-OESTE" ou "NORTE".';


-- =====================================================================
-- 2. Dimensão Hierarquia (Topologia da Cadeia de Confiança)
-- =====================================================================
COMMENT ON TABLE lakehouse_iti.3_gold.dim_hierarquia IS 
'Grafo de subordinação direta pai-filho entre entidades credenciadas da ICP-Brasil, mapeando desde a AC Raiz até as ARs na ponta.';

ALTER TABLE lakehouse_iti.3_gold.dim_hierarquia ALTER COLUMN ID_ENTIDADE_PAI 
COMMENT 'Identificador único da autoridade superior/ancestral direta.';

ALTER TABLE lakehouse_iti.3_gold.dim_hierarquia ALTER COLUMN ID_ENTIDADE 
COMMENT 'Identificador único da autoridade subordinada direta.';

ALTER TABLE lakehouse_iti.3_gold.dim_hierarquia ALTER COLUMN DS_NIVEL 
COMMENT 'Nível hierárquico numérico da entidade filha na cadeia (0 = Raiz, 1 = AC N1, 2 = AC N2, 3 = AR).';


-- =====================================================================
-- 3. Fato Métricas da Cadeia (Hierarquia e Agregações Recursivas)
-- =====================================================================
COMMENT ON TABLE lakehouse_iti.3_gold.fato_metricas_entidades IS 
'Métricas calculadas via CTE recursiva consolidando a árvore de subordinação da ICP-Brasil. Cada linha resume o total de entidades filhas e agregadas subordinadas a uma autoridade pai.';

ALTER TABLE lakehouse_iti.3_gold.fato_metricas_entidades ALTER COLUMN NR_AGREGADOS_AR 
COMMENT 'Quantidade total de Autoridades de Registro (ARs) vinculadas direta ou indiretamente abaixo desta autoridade em toda a árvore de confiança.';

ALTER TABLE lakehouse_iti.3_gold.fato_metricas_entidades ALTER COLUMN NR_AGREGADOS_AC_NIVEL_1 
COMMENT 'Quantidade total de Autoridades Certificadoras de 1º Nível vinculadas abaixo desta autoridade.';

ALTER TABLE lakehouse_iti.3_gold.fato_metricas_entidades ALTER COLUMN NR_AGREGADOS_AC_NIVEL_2 
COMMENT 'Quantidade total de Autoridades Certificadoras de 2º Nível vinculadas abaixo desta autoridade.';


-- =====================================================================
-- 4. Fato Emissão Mensal (Demanda e Séries Históricas)
-- =====================================================================
COMMENT ON TABLE lakehouse_iti.3_gold.fato_emissao_mensal IS 
'Série histórica com a volumetria de emissões e certificados ativos da ICP-Brasil agregada temporalmente por mês e ano.';

ALTER TABLE lakehouse_iti.3_gold.fato_emissao_mensal ALTER COLUMN DS_GRANULARIDADE 
COMMENT 'Nível temporal do registro: "MENSAL" (valores específicos mês a mês) ou "ANUAL" (séries históricas consolidadas anuais).';

ALTER TABLE lakehouse_iti.3_gold.fato_emissao_mensal ALTER COLUMN DS_TIPO_SERIE 
COMMENT 'Classificação da métrica de emissão. Valores: "HISTORICO ATIVOS" (estoque cumulativo de certificados válidos), "HISTORICO EMITIDOS" (total emitido consolidado) ou "MENSAL CORRENTE" (emissões ocorridas no mês).';

ALTER TABLE lakehouse_iti.3_gold.fato_emissao_mensal ALTER COLUMN VL_METRICA 
COMMENT 'Contagem quantitativa absoluta de certificados digitais correspondente ao tipo de série, mês e ano.';


-- =====================================================================
-- 5. Fato Distribuição Geográfica (Presença Territorial)
-- =====================================================================
COMMENT ON TABLE lakehouse_iti.3_gold.fato_distribuicao_geografica IS 
'Distribuição geográfica de certificados emitidos e de Autoridades de Registro ativas agrupadas por Unidade Federativa (UF) e Região.';

ALTER TABLE lakehouse_iti.3_gold.fato_distribuicao_geografica ALTER COLUMN DS_METRICA 
COMMENT 'Nome da métrica geográfica: "EMISSAO MENSAL", "EMISSAO ANUAL" ou "TOTAL AR ESTADO".';

ALTER TABLE lakehouse_iti.3_gold.fato_distribuicao_geografica ALTER COLUMN DS_TIPO_OBJETO 
COMMENT 'Tipo de objeto mensurado: "CERTIFICADOS" (para emissões) ou "ENTIDADES" (para total de ARs no estado).';

ALTER TABLE lakehouse_iti.3_gold.fato_distribuicao_geografica ALTER COLUMN VL_METRICA 
COMMENT 'Valor numérico quantitativo da métrica para o estado e período especificados.';


-- =====================================================================
-- 6. Fato Segmentação de Certificados (Produto e Titular)
-- =====================================================================
COMMENT ON TABLE lakehouse_iti.3_gold.fato_segmentacao_certificados IS 
'Segmentação das emissões de certificados digitais por tecnologia de armazenamento (A1 em software vs A3 em hardware/nuvem) e perfil de usuário.';

ALTER TABLE lakehouse_iti.3_gold.fato_segmentacao_certificados ALTER COLUMN DS_CATEGORIA_CORTE 
COMMENT 'Categoria de segmentação: "TIPO CERTIFICADO" (A1 vs A3), "TIPO USO" ou "TIPO TITULAR".';

ALTER TABLE lakehouse_iti.3_gold.fato_segmentacao_certificados ALTER COLUMN DS_TIPO_CERTIFICADO 
COMMENT 'Tipo tecnológico do certificado: "A1" (software, validade 1 ano) ou "A3" (hardware/token/cartão/nuvem, validade prolongada).';

ALTER TABLE lakehouse_iti.3_gold.fato_segmentacao_certificados ALTER COLUMN DS_TIPO_USUARIO 
COMMENT 'Perfil do titular solicitante: "PESSOA FISICA", "PESSOA JURIDICA" ou "EQUIPAMENTO/APLICACAO".';


-- =====================================================================
-- 7. Fato Infraestrutura e Novos Credenciamentos
-- =====================================================================
COMMENT ON TABLE lakehouse_iti.3_gold.fato_infraestrutura_credenciamento IS 
'Histórico e volumetria de novos credenciamentos de Autoridades de Registro (ARs) ao longo do tempo.';

ALTER TABLE lakehouse_iti.3_gold.fato_infraestrutura_credenciamento ALTER COLUMN DS_METRICA 
COMMENT 'Métrica de expansão da rede: "NOVOS CREDENCIAMENTOS".';

ALTER TABLE lakehouse_iti.3_gold.fato_infraestrutura_credenciamento ALTER COLUMN VL_METRICA 
COMMENT 'Quantidade absoluta de novos credenciamentos deferidos no período.';


-- =====================================================================
-- 8. KPI Resumo Executivo (Painel C-Level)
-- =====================================================================
COMMENT ON TABLE lakehouse_iti.3_gold.kpi_resumo_executivo IS 
'Indicadores-chave consolidados de desempenho (KPIs) para visão instantânea de diretoria, contendo totais e comparativos percentuais.';

ALTER TABLE lakehouse_iti.3_gold.kpi_resumo_executivo ALTER COLUMN DS_INDICADOR 
COMMENT 'Nome do indicador consolidado (ex: "CERTIFICADOS EMITIDOS", "CERTIFICADOS ATIVOS", "AUTORIDADE REGISTRO", "PROJECAO ANO ATUAL").';

ALTER TABLE lakehouse_iti.3_gold.kpi_resumo_executivo ALTER COLUMN DS_TIPO_INDICADOR 
COMMENT 'Tipo de métrica calculada: "ACUMULADO ATUAL", "COMPARATIVO PERCENTUAL", "COMPARATIVO ABSOLUTO" ou "PROJECAO".';


-- =====================================================================
-- 9. Dimensão Inteligência de Mercado e Fontes Externas
-- =====================================================================
COMMENT ON TABLE lakehouse_iti.3_gold.dim_inteligencia_mercado IS 
'Dimensão de inteligência externa e mercado. Contém notícias, artigos técnicos, notas regulatórias e posicionamentos institucionais coletados dos principais portais do setor (ANCD, Crypto ID, ABRID, AR Federal, Convergência Digital).';

ALTER TABLE lakehouse_iti.3_gold.dim_inteligencia_mercado ALTER COLUMN NM_FONTE 
COMMENT 'Nome do portal ou entidade representativa de origem: ANCD, CRYPTO ID, ABRID, AR FEDERAL ou CONVERGENCIA DIGITAL.';

ALTER TABLE lakehouse_iti.3_gold.dim_inteligencia_mercado ALTER COLUMN DS_TITULO 
COMMENT 'Manchete ou título principal da publicação externa.';

ALTER TABLE lakehouse_iti.3_gold.dim_inteligencia_mercado ALTER COLUMN DS_RESUMO 
COMMENT 'Texto resumido do conteúdo da publicação para consulta semântica e contextualização analítica.';

ALTER TABLE lakehouse_iti.3_gold.dim_inteligencia_mercado ALTER COLUMN DS_URL_ORIGEM 
COMMENT 'URL permanente da matéria original publicada para citação direta de fontes confiáveis.';

ALTER TABLE lakehouse_iti.3_gold.dim_inteligencia_mercado ALTER COLUMN DT_PUBLICACAO 
COMMENT 'Data e hora em que a publicação foi veiculada no portal de origem.';

ALTER TABLE lakehouse_iti.3_gold.dim_inteligencia_mercado ALTER COLUMN DS_TAGS 
COMMENT 'Termos-chave do ecossistema identificados no conteúdo (ex: ICP-Brasil, DREX, Certificado Digital, Biometria, Cibersegurança).';
