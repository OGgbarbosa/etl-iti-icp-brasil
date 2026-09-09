from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound


def garantir_schema_silver():
    w = WorkspaceClient()

    # Busca o primeiro SQL Warehouse disponível
    warehouses = list(w.warehouses.list())
    if not warehouses:
        print("⚠️ Nenhum SQL Warehouse encontrado para criar a tabela automaticamente via SQL.")
        return None, None
    warehouse_id = warehouses[0].id

    # Garante que o schema 2_silver existe
    try:
        w.schemas.get("lakehouse_iti.2_silver")
    except NotFound:
        print("Schema 'lakehouse_iti.2_silver' não encontrado. Criando schema...")
        w.schemas.create(
            name="2_silver", 
            catalog_name="lakehouse_iti"
        )
        print("✅ Schema 'lakehouse_iti.2_silver' criado com sucesso!")
        return w, warehouse_id
    else:
        print("✅ Schema 'lakehouse_iti.2_silver' já existe!")
        return w, warehouse_id


def upload_silver_entidades():
    w, warehouse_id = garantir_schema_silver()
    if not w or not warehouse_id:
        return

    tabela_destino_entidades = "lakehouse_iti.2_silver.tb_entidades"
    print(f"Criando/atualizando tabela Delta: {tabela_destino_entidades}...")

    sql_statement = """
    CREATE OR REPLACE TABLE lakehouse_iti.2_silver.tb_entidades AS
    SELECT 
        CAST(id AS BIGINT) AS id_entidade,
        TRIM(REGEXP_REPLACE(nome, '\\s+', ' ')) AS nome_entidade,
        LPAD(REGEXP_REPLACE(cnpj, '[^0-9]', ''), 14, '0') AS cnpj,
        UPPER(tipo) as tipo_entidade,
        UPPER(entidade) as descricao_tipo_entidade,
        CAST(nivel AS INT) as nivel_hierarquico,
        CAST(situacao AS INT) as codigo_situacao,
        CASE WHEN situacao = 4002 THEN 'Credenciada' ELSE 'Em Credenciamento' END as situacao,
        TO_DATE(dt_credenciamento, 'yyyy-MM-dd') as data_credenciamento,
        CAST(processo as BIGINT) as numero_processo,
        telefone,
        current_timestamp() as data_processamento
    FROM lakehouse_iti.1_bronze.entidades
    WHERE id IS NOT NULL;
    """

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