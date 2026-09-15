#1. dim_entidade (Visão enriquecida com endereço principal e tipo)
#2. dim_hierarquia_icp (Árvore de subordinação AC Raiz -> AC -> AR)
#3. fato_metricas_entidades (Totais agregados por UF, Tipo e Status)
#4. vw_conformidade_cadastral (Entidades sem CNPJ/endereço ou inativas)

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


if __name__ == "__main__":
    upload_gold_entidades()