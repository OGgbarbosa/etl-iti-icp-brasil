-- =====================================================================
-- 1. Dimensão Entidades (Rede Instalada / Cadastro Mestre)
-- =====================================================================
COMMENT ON TABLE lakehouse_iti.3_gold.dim_entidade IS 
'Dimensão mestre de entidades físicas e lógicas credenciadas na ICP-Brasil. Cada linha representa uma autoridade com localização geográfica e situação operacional.';

ALTER TABLE lakehouse_iti.3_gold.dim_entidade ALTER COLUMN DS_TIPO 
COMMENT 'Descrição do tipo de entidade por extenso em caixa alta: "AUTORIDADE CERTIFICADORA", "AUTORIDADE DE REGISTRO", "AUTORIDADE DE CARIMBO DO TEMPO" ou "PRESTADOR DE SERVICO DE SUPORTE".';

ALTER TABLE lakehouse_iti.3_gold.dim_entidade ALTER COLUMN DS_SITUACAO 
COMMENT 'Status operacional da autoridade. Valores possíveis: "CREDENCIADA" ou "EM CREDENCIAMENTO". Para autoridades vigentes/ativas em operação, filtrar sempre por DS_SITUACAO = "CREDENCIADA".';

ALTER TABLE lakehouse_iti.3_gold.dim_entidade ALTER COLUMN SG_UF 
COMMENT 'Sigla da Unidade Federativa (estado brasileiro) onde a entidade está sediada, com 2 caracteres em maiúsculo (ex: "SP", "RJ", "DF").';

ALTER TABLE lakehouse_iti.3_gold.dim_entidade ALTER COLUMN DS_REGIAO 
COMMENT 'Macrorregião geográfica brasileira derivada da UF em maiúsculo: "SUDESTE", "SUL", "NORDESTE", "CENTRO-OESTE" ou "NORTE".';


-- =====================================================================
-- 2. Fato Emissão Mensal (Demanda e Séries Históricas)
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
-- 3. Fato Métricas da Cadeia (Hierarquia e Agregações)
-- =====================================================================
COMMENT ON TABLE lakehouse_iti.3_gold.fato_metricas_entidades IS 
'Métricas calculadas via CTE recursiva consolidando a árvore de subordinação da ICP-Brasil. Cada linha resume o total de entidades filhas e agregadas subordinadas a uma autoridade pai.';

ALTER TABLE lakehouse_iti.3_gold.fato_metricas_entidades ALTER COLUMN NR_AGREGADOS_AR 
COMMENT 'Quantidade total de Autoridades de Registro (ARs) vinculadas direta ou indiretamente abaixo desta autoridade em toda a árvore de confiança.';

ALTER TABLE lakehouse_iti.3_gold.fato_metricas_entidades ALTER COLUMN NR_AGREGADOS_AC_NIVEL_1 
COMMENT 'Quantidade total de Autoridades Certificadoras de 1º Nível vinculadas abaixo desta autoridade.';

ALTER TABLE lakehouse_iti.3_gold.fato_metricas_entidades ALTER COLUMN NR_AGREGADOS_AC_NIVEL_2 
COMMENT 'Quantidade total de Autoridades Certificadoras de 2º Nível vinculadas abaixo desta autoridade.';
