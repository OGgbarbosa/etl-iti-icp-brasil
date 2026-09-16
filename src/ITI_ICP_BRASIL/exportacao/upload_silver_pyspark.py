from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType


def obter_spark():
    try:
        return DatabricksSession.builder.serverless(True).getOrCreate()
    except Exception as e:
        print(f"❌ Falha ao inicializar sessao Spark: {e}")
        return None


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
                F.when(F.col("situacao") == 4002, "Credenciada").otherwise("Em Credenciamento").alias("situacao"),
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

if __name__ == "__main__":
    upload_silver_entidades()
    upload_silver_enderecos()
    upload_silver_hierarquia()