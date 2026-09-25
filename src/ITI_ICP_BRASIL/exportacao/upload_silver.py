from contextlib import suppress

import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType

from ITI_ICP_BRASIL.config.config import obter_warehouse_id, warehouse_id
from ITI_ICP_BRASIL.config.glossario import (
    flag,
    glossario,
    glossario_mes,
    mapa_anos,
)
from ITI_ICP_BRASIL.config.logger import get_logger

logger = get_logger(__name__)


def obter_spark():
    # 1. Sessão ativa nativa (quando executando dentro de um Job/Cluster no Databricks)
    with suppress(Exception):
        spark = SparkSession.getActiveSession()
        if spark is not None:
            return spark

    # 2. Databricks Connect Serverless (quando executando remotamente do ambiente local)
    with suppress(Exception):
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.serverless(True).getOrCreate()

    # 3. Fallback SparkSession padrão
    try:
        return SparkSession.builder.getOrCreate()
    except Exception as e:
        print(f"❌ Falha ao inicializar sessao Spark: {e}")
        return None

def garantir_schema_silver():
    w = WorkspaceClient()

    target_warehouse_id = warehouse_id or obter_warehouse_id(w)
    if not target_warehouse_id:
        logger.error("❌ Nenhum SQL Warehouse configurado ou disponível no workspace.")
        raise RuntimeError("Nenhum SQL Warehouse configurado ou disponível no workspace.")

    try:
        w.schemas.get("lakehouse_iti.2_silver")
    except NotFound:
        print("Schema 'lakehouse_iti.2_silver' não encontrado. Criando schema...")
        w.schemas.create(
            name="2_silver", 
            catalog_name="lakehouse_iti"
        )
        print("✅ Schema 'lakehouse_iti.2_silver' criado com sucesso!")
        return w, target_warehouse_id
    else:
        print("✅ Schema 'lakehouse_iti.2_silver' já existe!")
        return w, target_warehouse_id


def upload_silver_entidades(spark=None):
    if spark is None:
        spark = obter_spark()
    if not spark:
        print("⚠️ Nao foi possivel continuar sem sessao Spark ativa.")
        return

    tabela_origem = "lakehouse_iti.1_bronze.entidades"
    tabela_destino = "lakehouse_iti.2_silver.tbl_entidades"
    print(f"Processando tabela Delta via PySpark: {tabela_destino}...")

    try:
        df_bronze = spark.read.table(tabela_origem)

        df_entidades = (
            df_bronze
            .filter(F.col("id").isNotNull())
            .select(
                F.col("id").cast(LongType()).alias("id_entidade"),
                F.trim(F.regexp_replace(F.col("nome"), r"\s+", " ")).alias("nome_entidade"),
                F.lpad(F.regexp_replace(F.col("cnpj"), r"[^0-9]", ""), 14, "0").alias("cnpj"),
                F.upper(F.col("entidade")).alias("descricao_tipo_entidade"),
                F.col("nivel").cast(IntegerType()).alias("nivel_hierarquico"),
                F.col("situacao").cast(IntegerType()).alias("codigo_situacao"),
                F.when(F.col("situacao") == 4002, "CREDENCIADA").otherwise("EM CREDENCIAMENTO").alias("situacao"),
                F.to_date(F.col("dt_credenciamento"), "yyyy-MM-dd").alias("data_credenciamento"),
                F.regexp_replace(F.col("processo"), r"[^0-9]", "").try_cast(LongType()).alias("numero_processo"),
                F.col("telefone"),
                F.current_timestamp().alias("data_processamento"),
            )
            .dropDuplicates(["id_entidade"])
        )

        (
            df_entidades.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(tabela_destino)
        )
        print(f"✅ Tabela Silver '{tabela_destino}' criada com sucesso via PySpark!")

    except Exception as e:
        print(f"❌ Falha ao processar tabela '{tabela_destino}': {e}")


def upload_silver_enderecos(spark=None):
    if spark is None:
        spark = obter_spark()
    if not spark:
        print("⚠️ Nao foi possivel continuar sem sessao Spark ativa.")
        return

    tabela_origem = "lakehouse_iti.1_bronze.entidades"
    tabela_destino = "lakehouse_iti.2_silver.tbl_enderecos"
    print(f"Processando tabela Delta via PySpark: {tabela_destino}...")

    try:
        df_bronze = spark.read.table(tabela_origem)

        numero_tratado  = F.nullif(
            F.trim(F.regexp_replace(F.col('enderecos_0_numero'), r'(?i)N[º°\.]\s*|^\s*$', '')), 
            F.lit('')
        )

        cep_limpo = F.when(
            F.col('enderecos_0_cep').rlike(r'^\d{8}$'),
            F.regexp_replace(F.col('enderecos_0_cep'), r'(\d{5})(\d{3})', r'$1-$2')
        ).otherwise(F.col('enderecos_0_cep'))

        regiao = (
            F.when(F.col("enderecos_0_uf").isin("SP", "RJ", "MG", "ES"), "SUDESTE")
            .when(F.col("enderecos_0_uf").isin("PR", "SC", "RS"), "SUL")
            .when(F.col("enderecos_0_uf").isin("DF", "GO", "MT", "MS"), "CENTRO-OESTE")
            .when(F.col("enderecos_0_uf").isin("BA", "PE", "CE", "MA", "PB", "RN", "AL", "SE", "PI"), "NORDESTE")
            .when(F.col("enderecos_0_uf").isin("AM", "PA", "AC", "RO", "RR", "AP", "TO"), "NORTE")
            .otherwise("NÃO INFORMADO")
            )

        df_enderecos = (    
            df_bronze
            .filter(F.col("id").isNotNull())
            .select(
                F.col("id").cast(LongType()).alias("id_entidade"),
                F.concat_ws(", ",F.initcap(F.concat_ws(" ",F.coalesce(numero_tratado,F.col('enderecos_0_complemento')),F.col('enderecos_0_logradouro'))),F.initcap(F.col('enderecos_0_bairro')),F.initcap(F.col('enderecos_0_cidade')),F.upper(F.col('enderecos_0_uf')),cep_limpo).alias('endereco_completo'),
                F.upper(F.col("enderecos_0_logradouro")).alias("logradouro"),
                F.regexp_extract(F.col("enderecos_0_numero"), r"(\d+)", 1).try_cast(LongType()).alias("numero"),
                F.upper(F.col("enderecos_0_complemento")).alias("complemento"),
                F.upper(F.col("enderecos_0_bairro")).alias("bairro"),
                F.upper(F.col("enderecos_0_cidade")).alias("cidade"),
                F.upper(F.col("enderecos_0_uf")).alias("uf"),
                regiao.alias("regiao"), 
                F.regexp_replace(F.col("enderecos_0_cep"), r"[^0-9]", "").alias("cep"),
                F.current_timestamp().alias("data_processamento"),
            )
            .dropDuplicates(["id_entidade"])
        )

        (
            df_enderecos.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(tabela_destino)
        )
        print(f"✅ Tabela Silver '{tabela_destino}' criada com sucesso via PySpark!")

    except Exception as e:
        print(f"❌ Falha ao processar tabela '{tabela_destino}': {e}")

def upload_silver_hierarquia(spark=None):
    if spark is None:
        spark = obter_spark()
    if not spark:
        print("⚠️ Nao foi possivel continuar sem sessao Spark ativa.")
        return

    tabela_origem = "lakehouse_iti.1_bronze.entidades"
    tabela_destino = "lakehouse_iti.2_silver.tbl_hierarquia"
    print(f"Processando tabela Delta via PySpark: {tabela_destino}...")

    try:
        df_bronze = spark.read.table(tabela_origem)

        colunas_pai = [
            c for c in df_bronze.columns
            if c.startswith("ids_pai_") and c.endswith("id")
        ]

        cols_pai_cast = [F.col(c).cast(LongType()) for c in colunas_pai]

        array_pai = F.filter(F.array(*cols_pai_cast), lambda x: x.isNotNull())

        df_hierarquia = (
            df_bronze
            .filter(F.col("id").isNotNull())
            .withColumn('id_entidade_pai', F.explode(array_pai))
            .select(
                F.col("id_entidade_pai"),
                F.col("id").cast(LongType()).alias("id_entidade"),
                F.col("nivel").cast(IntegerType()).alias("nivel_hierarquia_filho"),
                F.current_timestamp().alias("data_processamento"),
            )
            .dropDuplicates(["id_entidade", 'id_entidade_pai'])
        )

        (
            df_hierarquia.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(tabela_destino)
        )
        print(f"✅ Tabela Silver '{tabela_destino}' criada com sucesso via PySpark!")

    except Exception as e:
        print(f"❌ Falha ao processar tabela '{tabela_destino}': {e}")


def upload_staging_silver_numeros(spark=None):
    if spark is None:
        spark = obter_spark()
    if not spark:
        print("⚠️ Nao foi possivel continuar sem sessao Spark ativa.")
        return

    tabela_origem = "lakehouse_iti.1_bronze.numeros"
    tabela_destino = "lakehouse_iti.2_silver.stg_silver_numeros"

    print(f"🔄️ Lendo tabela Bronze: {tabela_origem}...")
    df = spark.table(tabela_origem).toPandas()

    print("🔄️ Aplicando transformacoes em Pandas...")
    # Conversões de tipo
    df["indice"] = pd.to_numeric(df["indice"], errors="coerce").astype("Int64")
    df["value"] = pd.to_numeric(df["value"], errors="coerce").astype("Int64")
    df["qtdcertificado"] = pd.to_numeric(df["qtdcertificado"], errors="coerce").astype("Int64")
    df["count"] = pd.to_numeric(df["count"], errors="coerce").astype("Int64")

    # Enriquecimento com glossário e flags via dicionários
    df["glossario"] = df["indicador"].map(glossario).fillna(df["indicador"])
    df["flag"] = df["indicador"].map(flag).fillna("OUTROS")
    df["ano"] = pd.to_numeric(df["glossario"].map(mapa_anos), errors="coerce").astype("Int64").astype(object).where(lambda x: x.notna(), None)

    # Construção da data (apenas para séries mensais reais)
    eh_serie_mensal = df["flag"].isin(["cerEMISSAO_CORRENTE", "infCREDENCIAMENTO_AR"])
    mes_str = df["ano"].astype(str) + "-" + df["indice"].map(glossario_mes).astype(str) + "-01"
    df["mes"] = pd.to_datetime(mes_str.where(eh_serie_mensal, None), format="mixed", errors="coerce").dt.date

    df["data_processamento"] = pd.Timestamp.now()

    print(f"🔄️ Gravando tabela Silver no Unity Catalog: {tabela_destino}...")
    df_spark = spark.createDataFrame(df)
    (
        df_spark.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabela_destino)
    )
    print(f"✅ Tabela Silver '{tabela_destino}' criada com sucesso!")

def create_tabela_silver_numeros():
    w, warehouse_id = garantir_schema_silver()
    if not w or not warehouse_id:
        return

    tabela_destino_metricas = "lakehouse_iti.2_silver.tbl_silver_numeros"
    print(f"Criando/atualizando tabela Delta: {tabela_destino_metricas}...")

    sql = """
    CREATE TABLE IF NOT EXISTS `lakehouse_iti`.`2_silver`.`tbl_silver_numeros` 
    (
        CD_CHAVE_INDICADOR       STRING NOT NULL
        ,DS_ORIGEM               STRING
        ,DS_SUBORIGEM            STRING
        ,DS_INDICADOR            STRING
        ,DS_GLOSSARIO            STRING
        ,DS_FLAG                 STRING
        ,DS_TIPO_CERTIFICADO     STRING
        ,DS_TIPO_USUARIO         STRING
        ,DS_USO                  STRING
        ,DT_MES_ANO              DATE
        ,DT_ANO                  INT
        ,SG_UF                   STRING
        ,DS_REGIAO               STRING
        ,VL_METRICA              DOUBLE
        ,DT_INSERCAO             TIMESTAMP
        ,DT_ATUALIZACAO          TIMESTAMP
    )
    USING DELTA
    CLUSTER BY (DT_ANO, DS_FLAG);
    """

    resposta = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="50s",
    )

    estado = resposta.status.state if resposta.status else None
    if estado and estado.value in ["FAILED", "CANCELED", "CLOSED"]:
        erro_msg = resposta.status.error.message if resposta.status.error else "Erro desconhecido"
        print(f"❌ Falha ao criar tabela: {erro_msg}")
        return

    print("✅ Tabela silver 'lakehouse_iti.2_silver.tbl_silver_numeros' criada com sucesso!")

def merge_tabela_silver_numeros():
    w, warehouse_id = garantir_schema_silver()
    if not w or not warehouse_id:
        return

    tabela_destino_metricas = "lakehouse_iti.2_silver.tbl_silver_numeros"
    print(f"Criando/atualizando tabela Delta: {tabela_destino_metricas}...")

    sql = """
    MERGE INTO `lakehouse_iti`.`2_silver`.`tbl_silver_numeros` AS target
        USING (
            WITH CTE_CER AS
            (
                SELECT 
                    'iti_ext' AS DS_ORIGEM
                    ,'CTE_CER' AS DS_SUBORIGEM
                    ,indicador AS DS_INDICADOR
                    ,glossario AS DS_GLOSSARIO
                    ,flag AS DS_FLAG
                    ,NULL AS DS_TIPO_CERTIFICADO
                    ,NULL AS DS_TIPO_USUARIO
                    ,NULL AS DS_USO
                    ,CASE
                        WHEN flag LIKE 'cerHISTORICO_ANUAL_EMISSAO%' AND indice BETWEEN 0 AND 5 
                        THEN CAST(CONCAT(EXTRACT(YEAR FROM current_date()) - (5 - indice), '-01-01') AS DATE)
                        ELSE mes 
                    END AS DT_MES_ANO
                    ,CASE 
                        WHEN flag LIKE 'cerHISTORICO_ANUAL_EMISSAO%' AND indice BETWEEN 0 AND 5 
                        THEN EXTRACT(YEAR FROM current_date()) - (5 - indice)
                        ELSE CAST(ano AS INT) 
                    END AS DT_ANO
                    ,NULL AS SG_UF
                    ,NULL AS DS_REGIAO
                    ,COALESCE(CAST(valor AS DOUBLE), CAST(qtdcertificado AS DOUBLE)) AS VL_METRICA
                    ,data_processamento AS DT_ATUALIZACAO
                FROM `lakehouse_iti`.`2_silver`.`stg_silver_numeros` 
                WHERE flag LIKE '%cer%'
            ),

            CTE_REG AS
            (
                SELECT 
                    'iti_ext' AS DS_ORIGEM
                    ,'CTE_REG' AS DS_SUBORIGEM
                    ,indicador AS DS_INDICADOR
                    ,glossario AS DS_GLOSSARIO
                    ,flag AS DS_FLAG
                    ,NULL AS DS_TIPO_CERTIFICADO
                    ,NULL AS DS_TIPO_USUARIO
                    ,NULL AS DS_USO
                    ,CASE 
                        WHEN anomes IS NOT NULL THEN CAST(anomes AS DATE)
                        WHEN glossario IN ('cerDistribuicao Mapa ANO ATUAL', 'estVisao Geral Mapa ANO ATUAL') THEN CAST(CONCAT(EXTRACT(YEAR FROM current_date()), '-01-01') AS DATE)
                    ELSE NULL END AS DT_MES_ANO
                    ,CASE
                        WHEN anomes IS NOT NULL THEN CAST(LEFT(anomes, 4) AS INT)
                        WHEN glossario IN ('cerDistribuicao Mapa ANO ATUAL', 'estVisao Geral Mapa ANO ATUAL') THEN EXTRACT(YEAR FROM current_date())
                    ELSE NULL END AS DT_ANO
                    ,id AS SG_UF
                    ,CASE
                        WHEN id IN ('SP', 'RJ', 'MG', 'ES') THEN 'Sudeste'
                        WHEN id IN ('PR', 'SC', 'RS') THEN 'Sul'
                        WHEN id IN ('BA', 'SE', 'AL', 'PE', 'CE', 'PI', 'RN', 'PB') THEN 'Nordeste'
                        WHEN id IN ('AM', 'PA', 'TO', 'RR', 'AP', 'AC', 'RO') THEN 'Norte'
                        WHEN id IN ('MT', 'MS', 'GO', 'DF') THEN 'Centro Oeste'
                        ELSE reg 
                    END AS DS_REGIAO
                    ,CAST(COALESCE(count, value) AS DOUBLE) AS VL_METRICA
                    ,data_processamento AS DT_ATUALIZACAO
                FROM `lakehouse_iti`.`2_silver`.`stg_silver_numeros` 
                WHERE flag LIKE '%reg%'
            ),

            CTE_INF AS 
            (
                SELECT 
                    'iti_ext' AS DS_ORIGEM
                    ,'CTE_INF' AS DS_SUBORIGEM
                    ,indicador AS DS_INDICADOR
                    ,glossario AS DS_GLOSSARIO
                    ,flag AS DS_FLAG
                    ,NULL AS DS_TIPO_CERTIFICADO
                    ,NULL AS DS_TIPO_USUARIO
                    ,NULL AS DS_USO
                    ,CASE 
                        WHEN glossario IN ('estComparativo AR com o ANO PASSADO PERCENTUAL', 'estComparativo AC-2 com o ANO PASSADO PERCENTUAL') THEN CAST(CONCAT(EXTRACT(YEAR FROM current_date()) - 1, '-01-01') AS DATE)
                        WHEN glossario IN ('estComparativo AR com o ANO ATUAL', 'estComparativo AC-2 com o ANO ATUAL', 'cerVisao Geral Emitidos', 'cerVisao Geral Ativos (nao expirados, nao revogados)') THEN CAST(CONCAT(EXTRACT(YEAR FROM current_date()), '-01-01') AS DATE)
                        WHEN glossario = 'cerVisao Geral Projecao para ANO ATUAL' OR glossario LIKE '%PERCENTUAL%' OR glossario LIKE '%ACUMULADO%' THEN CAST(CONCAT(EXTRACT(YEAR FROM current_date()), '-01-01') AS DATE)
                    ELSE mes END AS DT_MES_ANO
                    ,CASE
                        WHEN glossario IN ('estComparativo AR com o ANO PASSADO PERCENTUAL', 'estComparativo AC-2 com o ANO PASSADO PERCENTUAL') THEN EXTRACT(YEAR FROM current_date()) - 1
                        WHEN glossario IN ('estComparativo AR com o ANO ATUAL', 'estComparativo AC-2 com o ANO ATUAL', 'cerVisao Geral Emitidos', 'cerVisao Geral Ativos (nao expirados, nao revogados)', 'cerVisao Geral Projecao para ANO ATUAL', 'cerVisao Geral Total de emissoes em relacao ao ano anterior PERCENTUAL') THEN EXTRACT(YEAR FROM current_date())
                        WHEN glossario LIKE '%ACUMULADO%' THEN EXTRACT(YEAR FROM current_date())
                    ELSE CAST(ano AS INT) END AS DT_ANO
                    ,NULL AS SG_UF
                    ,NULL AS DS_REGIAO
                    ,CAST(valor AS DOUBLE) AS VL_METRICA
                    ,data_processamento AS DT_ATUALIZACAO
                FROM `lakehouse_iti`.`2_silver`.`stg_silver_numeros` 
                WHERE flag LIKE '%inf%'
            ),

            CTE_DIS AS
            (
                SELECT 
                    'iti_ext' AS DS_ORIGEM
                    ,'CTE_DIS' AS DS_SUBORIGEM
                    ,indicador AS DS_INDICADOR
                    ,glossario AS DS_GLOSSARIO
                    ,flag AS DS_FLAG
                    ,COALESCE(tipo, tipo_certificado) AS DS_TIPO_CERTIFICADO
                    ,tipo_usuario AS DS_TIPO_USUARIO
                    ,uso AS DS_USO
                    ,CAST(CONCAT(COALESCE(ano, EXTRACT(YEAR FROM current_date())), '-01-01') AS DATE) AS DT_MES_ANO
                    ,COALESCE(CAST(ano AS INT), EXTRACT(YEAR FROM current_date())) AS DT_ANO
                    ,NULL AS SG_UF
                    ,reg AS DS_REGIAO
                    ,COALESCE(CAST(count AS DOUBLE), CAST(qtdcertificado AS DOUBLE), CAST(value AS DOUBLE)) AS VL_METRICA
                    ,data_processamento AS DT_ATUALIZACAO
                FROM `lakehouse_iti`.`2_silver`.`stg_silver_numeros`
                WHERE flag LIKE '%dis%'
            ),

            CONSOLIDADO AS
            (
                SELECT * FROM CTE_CER
                UNION ALL
                SELECT * FROM CTE_REG
                UNION ALL
                SELECT * FROM CTE_INF
                UNION ALL
                SELECT * FROM CTE_DIS
            )

            SELECT 
                sha2(
                concat_ws('||' 
                ,DS_ORIGEM
                ,DS_FLAG
                ,coalesce(cast(DT_MES_ANO as string), '') 
                ,coalesce(SG_UF, '')
                ,coalesce(DS_REGIAO, '')
                ,coalesce(DS_TIPO_CERTIFICADO, '') 
                ,coalesce(DS_TIPO_USUARIO, '') 
                ,coalesce(DS_USO, '')
                )
            , 256) AS CD_CHAVE_INDICADOR
            ,DS_ORIGEM
            ,DS_SUBORIGEM
            ,DS_INDICADOR
            ,DS_GLOSSARIO
            ,DS_FLAG
            ,DS_TIPO_CERTIFICADO
            ,DS_TIPO_USUARIO
            ,DS_USO
            ,DT_MES_ANO
            ,DT_ANO
            ,SG_UF
            ,DS_REGIAO
            ,VL_METRICA
            ,DT_ATUALIZACAO
            FROM CONSOLIDADO
        ) AS source
        ON target.CD_CHAVE_INDICADOR = source.CD_CHAVE_INDICADOR

        WHEN MATCHED AND (target.VL_METRICA <> source.VL_METRICA OR target.VL_METRICA IS NULL) THEN
            UPDATE SET 
            target.VL_METRICA     = source.VL_METRICA,
            target.DT_ATUALIZACAO = current_timestamp()
        WHEN NOT MATCHED THEN
            INSERT (
                CD_CHAVE_INDICADOR
                ,DS_ORIGEM
                ,DS_SUBORIGEM
                ,DS_INDICADOR
                ,DS_GLOSSARIO
                ,DS_FLAG
                ,DS_TIPO_CERTIFICADO
                ,DS_TIPO_USUARIO
                ,DS_USO
                ,DT_MES_ANO
                ,DT_ANO
                ,SG_UF
                ,DS_REGIAO
                ,VL_METRICA
                ,DT_INSERCAO
                ,DT_ATUALIZACAO
            )
            VALUES 
            (
                source.CD_CHAVE_INDICADOR
                ,source.DS_ORIGEM
                ,source.DS_SUBORIGEM
                ,source.DS_INDICADOR
                ,source.DS_GLOSSARIO
                ,source.DS_FLAG
                ,source.DS_TIPO_CERTIFICADO
                ,source.DS_TIPO_USUARIO
                ,source.DS_USO
                ,source.DT_MES_ANO
                ,source.DT_ANO
                ,source.SG_UF
                ,source.DS_REGIAO
                ,source.VL_METRICA
                ,current_timestamp()
                ,current_timestamp()
            );
    """

    resposta = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="50s",
    )

    estado = resposta.status.state if resposta.status else None
    if estado and estado.value in ["FAILED", "CANCELED", "CLOSED"]:
        erro_msg = resposta.status.error.message if resposta.status.error else "Erro desconhecido"
        print(f"❌ Falha ao criar tabela: {erro_msg}")
        return

    print("✅ Merge tabela silver 'lakehouse_iti.2_silver.tbl_silver_numeros' realizado com sucesso!")


if __name__ == "__main__":
    create_tabela_silver_numeros()
    upload_staging_silver_numeros()
    merge_tabela_silver_numeros()