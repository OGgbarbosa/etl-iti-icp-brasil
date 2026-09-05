import csv
import io
import json

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import VolumeType


def upload_volume_bronze():
    w = WorkspaceClient()

    caminho_raw = "/Volumes/lakehouse_iti/0_raw/raw/entidades.json"
    caminho_bronze = "/Volumes/lakehouse_iti/1_bronze/bronze/entidades.csv"

    nome_volume_completo = "lakehouse_iti.1_bronze.bronze"
    try:
        w.volumes.read(nome_volume_completo)
    except NotFound:
        print("Volume não encontrado. Criando volume lakehouse_iti.1_bronze.bronze...")
        w.volumes.create(
            catalog_name="lakehouse_iti",
            schema_name="1_bronze",
            name="bronze",
            volume_type=VolumeType.MANAGED,
        )

    print(f"Baixando dados do Volume Raw: {caminho_raw}")
    resposta = w.files.download(file_path=caminho_raw)
    dados = json.loads(resposta.contents.read().decode("utf-8"))

    if not dados:
        print("Nenhum dado encontrado para exportação.")
        return

    # Coleta todas as chaves existentes para o cabeçalho do CSV
    chaves = list({k: None for item in dados for k in item}.keys())

    buffer_csv = io.StringIO()
    writer = csv.DictWriter(buffer_csv, fieldnames=chaves)
    writer.writeheader()
    writer.writerows(dados)

    conteudo_bytes = buffer_csv.getvalue().encode("utf-8")

    print(f"Total de registros: {len(dados)}")
    print(f"Fazendo upload para o Volume Bronze: {caminho_bronze}...")

    w.files.upload(
        file_path=caminho_bronze,
        contents=io.BytesIO(conteudo_bytes),
        overwrite=True,
    )

    print("✅ Dados do JSON da camada Raw convertidos e salvos na camada Bronze com sucesso!")


def upload_tabela_bronze():

    w = WorkspaceClient()
    caminho_bronze = "/Volumes/lakehouse_iti/1_bronze/bronze/entidades.csv"

    # Criação/Carga da tabela Delta no Unity Catalog a partir do CSV salvo no Volume
    tabela_destino = "lakehouse_iti.1_bronze.entidades"
    print(f"Criando/atualizando tabela Delta: {tabela_destino}...")

    # Busca o primeiro SQL Warehouse disponível
    warehouses = list(w.warehouses.list())
    if not warehouses:
        print("⚠️ Nenhum SQL Warehouse encontrado para criar a tabela automaticamente via SQL.")
        return

    warehouse_id = warehouses[0].id

    sql_statement = f"""
    CREATE OR REPLACE TABLE {tabela_destino} AS
    SELECT 
        *,
        _FILE_NAME as nome_arquivo,
        current_timestamp as data_insercao
    FROM read_files(
        '{caminho_bronze}',
        format => 'csv',
        header => true,
        inferSchema => true
    );
    """

    w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql_statement,
        wait_timeout="50s"
    )

    print(f"✅ Tabela Delta '{tabela_destino}' criada/atualizada com sucesso no Unity Catalog!")


if __name__ == "__main__":
    upload_volume_bronze()
    upload_tabela_bronze()