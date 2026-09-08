from databricks.sdk.runtime import spark
import csv
import io
import json

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import VolumeType

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def upload_silver_entidades():
    
    w = WorkspaceClient()

    caminho_silver = "/Volumes/lakehouse_iti/2_silver/tb_entidades"

    tabela_destino = "lakehouse_iti.2_silver.tb_entidades"
    print(f"Criando/atualizando tabela Delta: {tabela_destino}...")

    # Busca o primeiro SQL Warehouse disponível
    warehouses = list(w.warehouses.list())
    if not warehouses:
        print("⚠️ Nenhum SQL Warehouse encontrado para criar a tabela automaticamente via SQL.")
        return
    warehouse_id = warehouses[0].id

    # Garante que o schema 2_silver existe
    try:
        w.schemas.get("lakehouse_iti.2_silver")
    except Exception:
        print("Schema 'lakehouse_iti.2_silver' não encontrado. Criando schema...")
        w.schemas.create(name="2_silver", catalog_name="lakehouse_iti")

    sql_statement = """
    CREATE OR REPLACE TABLE lakehouse_iti.2_silver.tb_entidades AS
    SELECT 
        CAST(id AS BIGINT) AS id_entidade,
        current_timestamp() as data_processamento
    FROM lakehouse_iti.1_bronze.entidades
    WHERE id IS NOT NULL;
    """

    print("Criando tabela Delta lakehouse_iti.2_silver.tb_entidades...")
    resposta = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql_statement,
        wait_timeout="50s",
    )

    estado = resposta.status.state if resposta.status else None
    if estado and estado.value in ["FAILED", "CANCELED", "CLOSED"]:
        erro_msg = resposta.status.error.message if resposta.status.error else "Erro desconhecido"
        print(f"❌ Falha ao criar tabela: {erro_msg}")
        return

    print("✅ Tabela Silver 'lakehouse_iti.2_silver.tb_entidades' criada com sucesso!")

if __name__ == "__main__":
    upload_silver_entidades()