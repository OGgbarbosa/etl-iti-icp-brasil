import csv
import io
import json

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import VolumeType

from ITI_ICP_BRASIL.config.config import (
    caminho_bronze_entidades,
    caminho_bronze_numeros,
    caminho_raw_entidades,
    caminho_raw_numeros,
    nome_volume_raw,
    tabela_destino_entidades,
    tabela_destino_numeros,
)
from ITI_ICP_BRASIL.config.logger import get_logger

logger = get_logger(__name__)

def upload_volume_bronze_iti_entidades():
    w = WorkspaceClient()

    caminho_raw = caminho_raw_entidades
    caminho_bronze = caminho_bronze_entidades
    nome_volume_completo = nome_volume_raw

    try:
        try:
            w.volumes.read(nome_volume_completo)
        except NotFound:
            logger.info(f"⚠️ Volume não encontrado. Criando volume {nome_volume_completo}...")
            w.volumes.create(
                catalog_name="lakehouse_iti",
                schema_name="1_bronze",
                name="raw",
                volume_type=VolumeType.MANAGED,
            )

        logger.info(f"⬇️  Baixando dados do Volume Raw: {caminho_raw}")
        try:
            resposta = w.files.download(file_path=caminho_raw)
            dados = json.loads(resposta.contents.read().decode("utf-8"))
        except Exception as e:
            logger.error(f"❌ Erro ao baixar dados do Volume Raw: {e}")
            raise

        if not dados:
            logger.warning("⚠️  Nenhum dado encontrado para exportação.")
            raise ValueError("Nenhum dado encontrado para exportação.")

    # Coleta todas as chaves existentes para o cabeçalho do CSV
        try:
            chaves = list({k: None for item in dados for k in item}.keys())

            buffer_csv = io.StringIO()
            writer = csv.DictWriter(buffer_csv, fieldnames=chaves)
            writer.writeheader()
            writer.writerows(dados)

            conteudo_bytes = buffer_csv.getvalue().encode("utf-8")

            logger.info(f"Total de registros: {len(dados)}")
        except Exception:
            logger.exception("❌ Erro ao converter os dados JSON para CSV")
            raise

        logger.info(f"⬆️  Fazendo upload para o Volume Bronze: {caminho_bronze}.")

        try:
            w.files.upload(
                file_path=caminho_bronze,
                contents=io.BytesIO(conteudo_bytes),
                overwrite=True,
            )
        except Exception:
            logger.exception("❌ Erro ao fazer upload para o Volume Bronze")
            raise

        logger.info("✅ Dados do JSON da camada Raw convertidos e salvos na camada Bronze com sucesso!")

    except Exception:
        logger.exception("❌ Falha na execução de upload_volume_bronze_iti_entidades.")
        raise


def upload_tabela_bronze_iti_entidades():
    w = WorkspaceClient()
    caminho_bronze = caminho_bronze_entidades
    try:
# Criação/Carga da tabela Delta no Unity Catalog a partir do CSV salvo no Volume
        try:
            tabela_destino = tabela_destino_entidades
            logger.info(f"🔄️ Criando/atualizando tabela Delta: {tabela_destino}...")
        except Exception:
            logger.exception("❌ Erro ao criar tabela Delta")
            raise
        
# Busca o primeiro SQL Warehouse disponível
        try:
            warehouses = list(w.warehouses.list())
            if not warehouses:
                logger.warning("⚠️ Nenhum SQL Warehouse encontrado para criar a tabela automaticamente via SQL.")
                raise RuntimeError("Nenhum SQL Warehouse encontrado para criar a tabela automaticamente via SQL.")

            warehouse_id = warehouses[0].id
        except Exception:
            logger.exception("❌ Erro ao buscar SQL Warehouse")
            raise

        try:
            sql_statement = f"""
            CREATE OR REPLACE TABLE {tabela_destino} AS
            SELECT 
                *,
                _metadata.file_name as nome_arquivo,
                current_timestamp() as data_insercao
            FROM read_files(
                '{caminho_bronze}',
                format => 'csv',
                header => true,
                inferSchema => true
            );
            """
        except Exception:
            logger.exception("❌ Erro ao criar tabela Delta")
            raise

        try:
            resposta = w.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=sql_statement,
                wait_timeout="50s",
            )
        except Exception as e:
            logger.error(f"❌ Erro ao criar tabela: {e}")
            raise

        try:
            estado = resposta.status.state if resposta.status else None
            if estado and estado.value in ["FAILED", "CANCELED", "CLOSED"]:
                erro_msg = resposta.status.error.message if resposta.status.error else "Erro desconhecido"
                logger.error(f"Falha ao criar tabela: {erro_msg}")
                raise RuntimeError(f"Falha ao criar tabela: {erro_msg}")
        except Exception:
            logger.exception("❌ Erro ao criar tabela Delta")
            raise

        logger.info(f"✅ Tabela Delta '{tabela_destino}' criada/atualizada com sucesso no Unity Catalog!")
    except Exception:
        logger.exception("❌ Falha na execução de upload_volume_bronze_iti_entidades.")
        raise
    
def upload_volume_bronze_iti_numeros():
    w = WorkspaceClient()

    caminho_raw = caminho_raw_numeros
    caminho_bronze = caminho_bronze_numeros

    nome_volume_completo = nome_volume_raw
    try:
        w.volumes.read(nome_volume_completo)
    except NotFound:
        logger.info(f"⚠️ Volume não encontrado. Criando volume {nome_volume_completo}...")
        w.volumes.create(
            catalog_name="lakehouse_iti",
            schema_name="1_bronze",
            name="raw",
            volume_type=VolumeType.MANAGED,
        )

    logger.info(f"⬇️  Baixando dados do Volume Raw: {caminho_raw}")
    try:
        resposta = w.files.download(file_path=caminho_raw)
        dados = json.loads(resposta.contents.read().decode("utf-8"))
    except Exception as e:
        logger.error(f"❌ Erro ao baixar dados do Volume Raw: {e}")
        return

    if not dados:
        logger.warning("⚠️  Nenhum dado encontrado para exportação.")
        return

    chaves = list({k: None for item in dados for k in item}.keys())

    buffer_csv = io.StringIO()
    writer = csv.DictWriter(buffer_csv, fieldnames=chaves)
    writer.writeheader()
    writer.writerows(dados)

    conteudo_bytes = buffer_csv.getvalue().encode("utf-8")

    logger.info(f"Total de registros: {len(dados)}")
    logger.info(f"⬆️  Fazendo upload para o Volume Bronze: {caminho_bronze}...")

    w.files.upload(
        file_path=caminho_bronze,
        contents=io.BytesIO(conteudo_bytes),
        overwrite=True,
    )

    logger.info("✅ Dados do JSON da camada Raw convertidos e salvos na camada Bronze com sucesso!")

def upload_tabela_bronze_iti_numeros():

    w = WorkspaceClient()
    caminho_bronze = caminho_bronze_numeros

    tabela_destino = tabela_destino_numeros
    logger.info(f"🔄️ Criando/atualizando tabela Delta: {tabela_destino}.")

    warehouses = list(w.warehouses.list())
    if not warehouses:
        logger.warning("⚠️ Nenhum SQL Warehouse encontrado para criar a tabela automaticamente via SQL.")
        return

    warehouse_id = warehouses[0].id

    sql_statement = f"""
    CREATE OR REPLACE TABLE {tabela_destino} AS
    SELECT 
        *,
        _metadata.file_name as nome_arquivo,
        current_timestamp() as data_insercao
    FROM read_files(
        '{caminho_bronze}',
        format => 'csv',
        header => true,
        inferSchema => true
    );
    """

    try:
        resposta = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql_statement,
            wait_timeout="50s",
        )
    except Exception as e:
        logger.error(f"❌ Erro ao criar tabela: {e}")
        return

    estado = resposta.status.state if resposta.status else None
    if estado and estado.value in ["FAILED", "CANCELED", "CLOSED"]:
        erro_msg = resposta.status.error.message if resposta.status.error else "Erro desconhecido"
        logger.error(f"❌ Falha ao criar tabela: {erro_msg}")
        return

    logger.info(f"✅ Tabela Delta '{tabela_destino}' criada/atualizada com sucesso no Unity Catalog!")

if __name__ == "__main__":
    upload_volume_bronze_iti_entidades()
    upload_volume_bronze_iti_numeros()
    upload_tabela_bronze_iti_entidades()
    upload_tabela_bronze_iti_numeros()
