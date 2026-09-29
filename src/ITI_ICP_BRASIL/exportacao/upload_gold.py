from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

from ITI_ICP_BRASIL.config.config import obter_warehouse_id, warehouse_id
from ITI_ICP_BRASIL.config.logger import get_logger

logger = get_logger(__name__)


def garantir_schema_gold():
    w = WorkspaceClient()

    target_warehouse_id = warehouse_id or obter_warehouse_id(w)
    if not target_warehouse_id:
        logger.error("❌ Nenhum SQL Warehouse configurado ou disponível no workspace.")
        raise RuntimeError("Nenhum SQL Warehouse configurado ou disponível no workspace.")

    # Garante que o schema 3_gold existe
    try:
        w.schemas.get("lakehouse_iti.3_gold")
    except NotFound:
        logger.info("Schema 'lakehouse_iti.3_gold' não encontrado. Criando schema...")
        w.schemas.create(
            name="3_gold", 
            catalog_name="lakehouse_iti"
        )
        logger.info("✅ Schema 'lakehouse_iti.3_gold' criado com sucesso!")
        return w, target_warehouse_id
    else:
        logger.info("✅ Schema 'lakehouse_iti.3_gold' já existe!")
        return w, target_warehouse_id


def executar_statement_delta(w, warehouse_id: str, sql: str, tabela_destino: str):
    """Executa um statement SQL no Databricks SQL Warehouse com tratamento de erro e validação de status."""
    logger.info("🔄️ Criando/atualizando tabela Delta: %s...", tabela_destino)
    try:
        resposta = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            wait_timeout="50s",
        )
    except Exception as e:
        logger.error("❌ Erro de comunicação com o SQL Warehouse: %s", e)
        raise

    estado = resposta.status.state if resposta.status else None
    if not estado or estado.value != "SUCCEEDED":
        erro_msg = (
            resposta.status.error.message 
            if (resposta.status and resposta.status.error) 
            else f"Status final inválido: {estado}"
        )
        logger.error("❌ Falha ao criar tabela '%s': %s", tabela_destino, erro_msg)
        raise RuntimeError(f"Falha ao criar tabela '{tabela_destino}': {erro_msg}")

    logger.info("✅ Tabela Gold '%s' criada com sucesso!", tabela_destino)


def upload_gold_entidades(w=None, warehouse_id=None):
    if not w or not warehouse_id:
        w, warehouse_id = garantir_schema_gold()

    tabela_destino_entidades = "lakehouse_iti.3_gold.dim_entidade"

    sql_statement = """
    CREATE OR REPLACE TABLE lakehouse_iti.3_gold.dim_entidade AS
    SELECT 
        a.id_entidade as ID_ENTIDADE
        ,a.nome_entidade as DS_ENTIDADE
        ,a.descricao_tipo_entidade as DS_TIPO
        ,a.nivel_hierarquico as DS_NIVEL
        ,a.cnpj as NR_CNPJ
        ,a.telefone as NR_TELEFONE
        ,b.uf as SG_UF
        ,b.regiao as DS_REGIAO
        ,b.cidade as NM_CIDADE
        ,b.bairro as NM_BAIRRO
        ,b.cep as NR_CEP
        ,b.endereco_completo as DS_ENDERECO
        ,a.situacao as DS_SITUACAO
        ,a.data_credenciamento as DT_CREDENCIAMENTO
        ,current_timestamp() as DT_CARGA_DW

    FROM lakehouse_iti.`2_silver`.tbl_entidades a
        LEFT JOIN lakehouse_iti.`2_silver`.tbl_enderecos b
            ON a.id_entidade = b.id_entidade
    """
    executar_statement_delta(w, warehouse_id, sql_statement, tabela_destino_entidades)


def upload_gold_hierarquia(w=None, warehouse_id=None):
    if not w or not warehouse_id:
        w, warehouse_id = garantir_schema_gold()

    tabela_destino_hierarquia = "lakehouse_iti.3_gold.dim_hierarquia"

    sql = """
    CREATE OR REPLACE TABLE lakehouse_iti.3_gold.dim_hierarquia AS
    SELECT 
        a.id_entidade_pai as ID_ENTIDADE_PAI
        ,a.id_entidade as ID_ENTIDADE
        ,a.nivel_hierarquia_filho as DS_NIVEL
        ,current_timestamp() as DT_CARGA_DW
    
    FROM lakehouse_iti.`2_silver`.tbl_hierarquia a
    """
    executar_statement_delta(w, warehouse_id, sql, tabela_destino_hierarquia)


def upload_gold_metricas_entidades(w=None, warehouse_id=None):
    if not w or not warehouse_id:
        w, warehouse_id = garantir_schema_gold()

    tabela_destino_metricas = "lakehouse_iti.3_gold.fato_metricas_entidades"

    sql = """
    CREATE OR REPLACE TABLE lakehouse_iti.3_gold.fato_metricas_entidades AS
        WITH RECURSIVE hierarquia_completa as 
        (
        SELECT 
            id_entidade_pai AS id_ancestral
            ,id_entidade
            ,nivel_hierarquia_filho
        FROM lakehouse_iti.`2_silver`.tbl_hierarquia

    UNION ALL

        SELECT 
            h.id_ancestral
            ,f.id_entidade
            ,f.nivel_hierarquia_filho
        FROM hierarquia_completa h
            JOIN lakehouse_iti.`2_silver`.tbl_hierarquia f ON h.id_entidade = f.id_entidade_pai
        )
    SELECT 
        h.id_ancestral AS ID_ENTIDADE
        ,b.nome_entidade AS DS_ENTIDADE
        ,b.descricao_tipo_entidade AS DS_TIPO
        ,b.situacao AS DS_SITUACAO
        ,c.uf AS SG_UF
        ,c.regiao AS DS_REGIAO
        ,COALESCE(COUNT(DISTINCT CASE WHEN h.nivel_hierarquia_filho = 1 THEN h.id_entidade END), 0) AS NR_AGREGADOS_AC_NIVEL_1
        ,COALESCE(COUNT(DISTINCT CASE WHEN h.nivel_hierarquia_filho = 2 THEN h.id_entidade END), 0) AS NR_AGREGADOS_AC_NIVEL_2
        ,COALESCE(COUNT(DISTINCT CASE WHEN h.nivel_hierarquia_filho = 3 THEN h.id_entidade END), 0) AS NR_AGREGADOS_AR
        ,current_timestamp() as DT_CARGA_DW

    FROM hierarquia_completa h
        LEFT JOIN lakehouse_iti.`2_silver`.tbl_entidades b ON h.id_ancestral = b.id_entidade
        LEFT JOIN lakehouse_iti.`2_silver`.tbl_enderecos c ON b.id_entidade = c.id_entidade
    GROUP BY 
        h.id_ancestral
        ,b.nome_entidade
        ,b.descricao_tipo_entidade
        ,b.situacao
        ,c.uf
        ,c.regiao
    """
    executar_statement_delta(w, warehouse_id, sql, tabela_destino_metricas)


def upload_gold_fato_emissao_mensal(w=None, warehouse_id=None):
    if not w or not warehouse_id:
        w, warehouse_id = garantir_schema_gold()

    tabela_destino_fato_emissao = "lakehouse_iti.3_gold.fato_emissao_mensal"

    sql = """
        CREATE OR REPLACE TABLE `lakehouse_iti`.`3_gold`.`fato_emissao_mensal`
        CLUSTER BY (DT_ANO, DS_TIPO_SERIE)
        AS
        SELECT 
            CD_CHAVE_INDICADOR
            ,DT_ANO
            ,DT_MES_ANO
            ,CASE 
                WHEN DS_FLAG LIKE '%HISTORICO%' THEN 'ANUAL'
                ELSE 'MENSAL' 
            END AS DS_GRANULARIDADE
            ,CASE 
                WHEN DS_FLAG = 'cerHISTORICO_ANUAL_EMISSAO_ATIVOS' THEN 'HISTORICO ATIVOS'
                WHEN DS_FLAG = 'cerHISTORICO_ANUAL_EMISSAO_EMITIDOS' THEN 'HISTORICO EMITIDOS'
                WHEN DS_FLAG = 'cerEMISSAO_CORRENTE' THEN 'MENSAL CORRENTE'
                ELSE 'OUTROS' 
            END AS DS_TIPO_SERIE
            ,VL_METRICA
            ,current_timestamp() AS DT_CARGA_DW
        FROM `lakehouse_iti`.`2_silver`.`tbl_silver_numeros`
        WHERE DS_SUBORIGEM = 'CTE_CER'
        """
    executar_statement_delta(w, warehouse_id, sql, tabela_destino_fato_emissao)


def upload_gold_fato_distribuicao_geografica(w=None, warehouse_id=None):
    if not w or not warehouse_id:
        w, warehouse_id = garantir_schema_gold()

    tabela_destino_distribuicao = "lakehouse_iti.3_gold.fato_distribuicao_geografica"

    sql = """
    CREATE OR REPLACE TABLE `lakehouse_iti`.`3_gold`.`fato_distribuicao_geografica`
    CLUSTER BY (SG_UF, DT_ANO)
    AS
    SELECT  
        CD_CHAVE_INDICADOR
        ,DT_ANO
        ,DT_MES_ANO
        ,SG_UF
        ,DS_REGIAO
        ,CASE
            WHEN DS_FLAG = 'regEMISSAO_MENSAL' THEN 'EMISSAO MENSAL'
            WHEN DS_FLAG = 'regEMISSAO_ANUAL' THEN 'EMISSAO ANUAL'
            WHEN DS_FLAG = 'regAR_CORRENTE' THEN 'TOTAL AR ESTADO'
            ELSE 'OUTROS' 
           END AS DS_METRICA
        ,CASE 
            WHEN DS_FLAG = 'regAR_CORRENTE' THEN 'ENTIDADES'
            ELSE 'CERTIFICADOS' 
        END AS DS_TIPO_OBJETO
        ,VL_METRICA
        ,current_timestamp() AS DT_CARGA_DW
    FROM `lakehouse_iti`.`2_silver`.`tbl_silver_numeros`
    WHERE DS_SUBORIGEM = 'CTE_REG'
        """
    executar_statement_delta(w, warehouse_id, sql, tabela_destino_distribuicao)


def upload_gold_fato_segmentacao_certificados(w=None, warehouse_id=None):
    if not w or not warehouse_id:
        w, warehouse_id = garantir_schema_gold()

    tabela_destino_segmentacao = "lakehouse_iti.3_gold.fato_segmentacao_certificados"

    sql = """
    CREATE OR REPLACE TABLE `lakehouse_iti`.`3_gold`.`fato_segmentacao_certificados`
    CLUSTER BY (DT_ANO, DS_CATEGORIA_CORTE)
    AS
    SELECT 
        CD_CHAVE_INDICADOR
        ,DT_ANO
        ,DT_MES_ANO
        ,DS_REGIAO
        ,CASE
            WHEN DS_FLAG IN ('disTIPO', 'disCERTIFICADOS_TIPO') THEN 'TIPO CERTIFICADO'
            WHEN DS_FLAG = 'disUSO' THEN 'TIPO USO'
            WHEN DS_FLAG = 'disCERTIFICADOS_ASSINATURA' THEN 'TIPO TITULAR'
        ELSE 'OUTROS' END AS DS_CATEGORIA_CORTE
    ,CASE 
        WHEN DS_FLAG IN ('disTIPO', 'disCERTIFICADOS_TIPO') THEN
            CASE 
                WHEN DS_TIPO_CERTIFICADO IN ('A1', 'A3') THEN DS_TIPO_CERTIFICADO
                ELSE 'OUTROS' 
            END 
        ELSE NULL 
    END AS DS_TIPO_CERTIFICADO
    ,CASE 
        WHEN DS_FLAG IN ('disUSO', 'disCERTIFICADOS_ASSINATURA') THEN
        CASE 
            WHEN DS_USO LIKE '%Físic%' OR DS_TIPO_USUARIO LIKE '%Fisic%' THEN 'PESSOA FISICA'
            WHEN DS_USO LIKE '%Jurídic%' OR DS_TIPO_USUARIO LIKE '%Juridic%' THEN 'PESSOA JURIDICA'
            WHEN DS_USO LIKE '%Equipamento%' OR DS_TIPO_USUARIO LIKE '%Equipamento%' THEN 'EQUIPAMENTO/APLICACAO'
            ELSE COALESCE(UPPER(COALESCE(DS_USO, DS_TIPO_USUARIO)), 'APLICACAO')
        END ELSE NULL 
    END AS DS_TIPO_USUARIO
        ,VL_METRICA
        ,current_timestamp() AS DT_CARGA_DW
    FROM `lakehouse_iti`.`2_silver`.`tbl_silver_numeros` 
    WHERE DS_SUBORIGEM = 'CTE_DIS'
        """
    executar_statement_delta(w, warehouse_id, sql, tabela_destino_segmentacao)


def upload_gold_fato_infraestrutura_credenciamento(w=None, warehouse_id=None):
    if not w or not warehouse_id:
        w, warehouse_id = garantir_schema_gold()

    tabela_destino_credenciamento = "lakehouse_iti.3_gold.fato_infraestrutura_credenciamento"

    sql = """
    CREATE OR REPLACE TABLE `lakehouse_iti`.`3_gold`.`fato_infraestrutura_credenciamento`
    CLUSTER BY (DT_ANO, DT_MES_ANO)
    AS
    SELECT 
        CD_CHAVE_INDICADOR
        ,DT_ANO
        ,DT_MES_ANO
        ,'AUTORIDADE DE REGISTRO' AS DS_TIPO_ENTIDADE
        ,'NOVOS CREDENCIAMENTOS' AS DS_METRICA
        ,VL_METRICA
        ,current_timestamp() AS DT_CARGA_DW
    FROM `lakehouse_iti`.`2_silver`.`tbl_silver_numeros` 
    WHERE DS_FLAG = 'infCREDENCIAMENTO_AR'
        """
    executar_statement_delta(w, warehouse_id, sql, tabela_destino_credenciamento)


def upload_gold_kpi_resumo_executivo(w=None, warehouse_id=None):
    if not w or not warehouse_id:
        w, warehouse_id = garantir_schema_gold()

    tabela_destino_kpi_resumo_executivo = "lakehouse_iti.3_gold.kpi_resumo_executivo"

    sql = """
    CREATE OR REPLACE TABLE `lakehouse_iti`.`3_gold`.`kpi_resumo_executivo`
    CLUSTER BY (DS_TIPO_INDICADOR, DS_INDICADOR)
    AS
    SELECT
        CD_CHAVE_INDICADOR
        ,DT_ANO
        ,DT_MES_ANO
        ,CASE   
            WHEN DS_FLAG LIKE '%AC1%' THEN 'AUTORIDADE CERTIFICADORA 1'
            WHEN DS_FLAG LIKE '%AC2%' THEN 'AUTORIDADE CERTIFICADORA 2'
            WHEN DS_FLAG LIKE '%ACT%' THEN 'AUTORIDADE CARIMBO TEMPO' 
            WHEN DS_FLAG LIKE '%AGENTE_REGISTRO%' THEN 'AGENTE REGISTRO'
            WHEN DS_FLAG LIKE '%AUT_REG%' OR DS_FLAG LIKE '%AR%' THEN 'AUTORIDADE REGISTRO'
            WHEN DS_FLAG LIKE '%PSB%' THEN 'SERVICO BIOMETRICO'
            WHEN DS_FLAG LIKE '%PSC%' THEN 'SERVICO CONFIANCA'
            WHEN DS_FLAG LIKE '%PSS%' THEN 'SERVICO SUPORTE'
            WHEN DS_FLAG LIKE '%ATIVOS%' THEN 'CERTIFICADOS ATIVOS'
            WHEN DS_FLAG LIKE '%EMITIDOS%' OR DS_FLAG LIKE '%TOTAL_REL%' THEN 'CERTIFICADOS EMITIDOS'
            WHEN DS_FLAG LIKE '%PROJECAO%' THEN 'PROJECAO ANO ATUAL'
        ELSE 'OUTROS' END AS DS_INDICADOR
    ,CASE
        WHEN DS_FLAG LIKE '%REL_ANTERIOR%' THEN 'COMPARATIVO PERCENTUAL'
        WHEN DS_FLAG LIKE '%COMPARATIVO%' THEN 'COMPARATIVO ABSOLUTO'
        WHEN DS_FLAG LIKE '%PROJECAO%' THEN 'PROJECAO'
        ELSE 'ACUMULADO ATUAL' 
    END AS DS_TIPO_INDICADOR
    ,CASE
        WHEN DS_FLAG IN ('infHEADER_COMPARATIVO_AC2_REL_ANTERIOR', 'infHEADER_COMPARATIVO_AR_REL_ANTERIOR', 'infHEADER_TOTAL_REL_ANTERIOR') THEN 'PERCENTUAL'
    ELSE 'INTEIRO' END AS DS_TIPO_VALOR
    ,VL_METRICA
    ,current_timestamp() AS DT_CARGA_DW
    FROM `lakehouse_iti`.`2_silver`.`tbl_silver_numeros` 
    WHERE DS_SUBORIGEM = 'CTE_INF'
    AND DS_FLAG <> 'infCREDENCIAMENTO_AR'
        """
    executar_statement_delta(w, warehouse_id, sql, tabela_destino_kpi_resumo_executivo)


def upload_gold_todas(w=None, warehouse_id=None):
    """Executa a criação/atualização de todas as tabelas Gold."""
    if not w or not warehouse_id:
        w, warehouse_id = garantir_schema_gold()

    upload_gold_entidades(w, warehouse_id)
    upload_gold_hierarquia(w, warehouse_id)
    upload_gold_metricas_entidades(w, warehouse_id)

    upload_gold_fato_emissao_mensal(w, warehouse_id)
    upload_gold_fato_distribuicao_geografica(w, warehouse_id)
    upload_gold_fato_segmentacao_certificados(w, warehouse_id)
    upload_gold_fato_infraestrutura_credenciamento(w, warehouse_id)
    upload_gold_kpi_resumo_executivo(w, warehouse_id)


if __name__ == '__main__':
    upload_gold_todas()
