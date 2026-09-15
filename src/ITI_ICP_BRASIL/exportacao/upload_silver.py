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

    tabela_destino_entidades = "lakehouse_iti.2_silver.tbl_entidades"
    print(f"Criando/atualizando tabela Delta: {tabela_destino_entidades}...")

    sql_statement = """
    CREATE OR REPLACE TABLE lakehouse_iti.2_silver.tbl_entidades AS
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

    print("✅ Tabela Silver 'lakehouse_iti.2_silver.tbl_entidades' criada com sucesso!")

def upload_silver_enderecos():
    w, warehouse_id = garantir_schema_silver()
    if not w or not warehouse_id:
        return

    tabela_destino_enderecos = "lakehouse_iti.2_silver.tbl_enderecos"
    print(f"Criando/atualizando tabela Delta: {tabela_destino_enderecos}...")

    sql = """
    CREATE OR REPLACE TABLE lakehouse_iti.2_silver.tbl_enderecos AS
    SELECT 
        CAST(id AS BIGINT) AS id_entidade,
        UPPER(enderecos_0_uf) as uf,
        UPPER(enderecos_0_cidade) as cidade,
        UPPER(enderecos_0_complemento) as complemento,
        UPPER(enderecos_0_logradouro) as logradouro,
        UPPER(enderecos_0_bairro) as bairro,
        TRY_CAST(enderecos_0_cep AS BIGINT) as cep,
        TRY_CAST(enderecos_0_numero AS BIGINT) as numero,
        current_timestamp() as data_processamento
    FROM lakehouse_iti.1_bronze.entidades
    WHERE id IS NOT NULL;
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

    print("✅ Tabela Silver 'lakehouse_iti.2_silver.tbl_enderecos' criada com sucesso!")

if __name__ == "__main__":
    upload_silver_entidades()
    upload_silver_enderecos()