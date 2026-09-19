from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound


def garantir_schema_gold():
    w = WorkspaceClient()

    # Busca o primeiro SQL Warehouse disponível
    warehouses = list(w.warehouses.list())
    if not warehouses:
        print("⚠️ Nenhum SQL Warehouse encontrado para criar a tabela automaticamente via SQL.")
        return None, None
    warehouse_id = warehouses[0].id

    # Garante que o schema 3_gold existe
    try:
        w.schemas.get("lakehouse_iti.3_gold")
    except NotFound:
        print("Schema 'lakehouse_iti.3_gold' não encontrado. Criando schema...")
        w.schemas.create(
            name="3_gold", 
            catalog_name="lakehouse_iti"
        )
        print("✅ Schema 'lakehouse_iti.3_gold' criado com sucesso!")
        return w, warehouse_id
    else:
        print("✅ Schema 'lakehouse_iti.3_gold' já existe!")
        return w, warehouse_id

def upload_gold_entidades():
    w, warehouse_id = garantir_schema_gold()
    if not w or not warehouse_id:
        return

    tabela_destino_entidades = "lakehouse_iti.3_gold.dim_entidade"
    print(f"Criando/atualizando tabela Delta: {tabela_destino_entidades}...")

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

    print("✅ Tabela Gold 'lakehouse_iti.3_gold.dim_entidade' criada com sucesso!")

def upload_gold_hierarquia():
    w, warehouse_id = garantir_schema_gold()
    if not w or not warehouse_id:
        return

    tabela_destino_hierarquia = "lakehouse_iti.3_gold.dim_hierarquia"
    print(f"Criando/atualizando tabela Delta: {tabela_destino_hierarquia}...")

    sql = """
    CREATE OR REPLACE TABLE lakehouse_iti.3_gold.dim_hierarquia AS
    SELECT 
        a.id_entidade_pai as ID_ENTIDADE_PAI
        ,a.id_entidade as ID_ENTIDADE
        ,a.nivel_hierarquia_filho as DS_NIVEL
        ,current_timestamp() as DT_CARGA_DW
    
    FROM lakehouse_iti.`2_silver`.tbl_hierarquia a
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

    print("✅ Tabela Gold 'lakehouse_iti.3_gold.dim_hierarquia' criada com sucesso!")
    
def upload_gold_metricas_entidades():
    w, warehouse_id = garantir_schema_gold()
    if not w or not warehouse_id:
        return

    tabela_destino_metricas = "lakehouse_iti.3_gold.fato_metricas_entidades"
    print(f"Criando/atualizando tabela Delta: {tabela_destino_metricas}...")

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

    print("✅ Tabela Gold 'lakehouse_iti.3_gold.fato_metricas_entidades' criada com sucesso!")