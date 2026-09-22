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
    warehouse_id,
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
            logger.info("⚠️ Volume não encontrado. Criando volume %s...", nome_volume_completo)
            w.volumes.create(
                catalog_name="lakehouse_iti",
                schema_name="1_bronze",
                name="raw",
                volume_type=VolumeType.MANAGED,
            )

        logger.info("⬇️  Baixando dados do Volume Raw: %s", caminho_raw)
        try:
            resposta = w.files.download(file_path=caminho_raw)
            dados = json.loads(resposta.contents.read().decode("utf-8"))
        except Exception as e:
            logger.error("❌ Erro ao baixar dados do Volume Raw: %s", e)
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

            logger.info("Total de registros: %d", len(dados))
        except Exception:
            logger.exception("❌ Erro ao converter os dados JSON para CSV")
            raise

        logger.info("⬆️  Fazendo upload para o Volume Bronze: %s.", caminho_bronze)

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
    tabela_destino = tabela_destino_entidades

    try:
        logger.info("🔄️ Criando/atualizando tabela Delta: %s...", tabela_destino)

        if not warehouse_id:
            logger.error("❌ Nenhum SQL Warehouse configurado.")
            raise RuntimeError("Nenhum SQL Warehouse configurado.")

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
            logger.error("❌ Erro ao criar tabela: %s", e)
            raise

        estado = resposta.status.state if resposta.status else None
        if estado and estado.value in ["FAILED", "CANCELED", "CLOSED"]:
            erro_msg = resposta.status.error.message if resposta.status.error else "Erro desconhecido"
            logger.error("❌ Falha ao criar tabela: %s", erro_msg)
            raise RuntimeError(f"Falha ao criar tabela: {erro_msg}")

        logger.info("✅ Tabela Delta '%s' criada/atualizada com sucesso no Unity Catalog!", tabela_destino)
    except Exception:
        logger.exception("❌ Falha na execução de upload_tabela_bronze_iti_entidades.")
        raise


def upload_volume_bronze_iti_numeros():
    w = WorkspaceClient()

    caminho_raw = caminho_raw_numeros
    caminho_bronze = caminho_bronze_numeros
    nome_volume_completo = nome_volume_raw

    try:
        try:
            w.volumes.read(nome_volume_completo)
        except NotFound:
            logger.info("⚠️ Volume não encontrado. Criando volume %s...", nome_volume_completo)
            w.volumes.create(
                catalog_name="lakehouse_iti",
                schema_name="1_bronze",
                name="raw",
                volume_type=VolumeType.MANAGED,
            )

        logger.info("⬇️  Baixando dados do Volume Raw: %s", caminho_raw)
        try:
            resposta = w.files.download(file_path=caminho_raw)
            dados = json.loads(resposta.contents.read().decode("utf-8"))
        except Exception as e:
            logger.error("❌ Erro ao baixar dados do Volume Raw: %s", e)
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

            logger.info("Total de registros: %d", len(dados))
        except Exception:
            logger.exception("❌ Erro ao converter os dados JSON para CSV")
            raise

        logger.info("⬆️  Fazendo upload para o Volume Bronze: %s...", caminho_bronze)

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
        logger.exception("❌ Falha na execução de upload_volume_bronze_iti_numeros.")
        raise


def upload_tabela_bronze_iti_numeros():
    w = WorkspaceClient()
    caminho_bronze = caminho_bronze_numeros
    tabela_destino = tabela_destino_numeros

    try:
        logger.info("🔄️ Criando/atualizando tabela Delta: %s.", tabela_destino)

        if not warehouse_id:
            logger.error("❌ Nenhum SQL Warehouse configurado.")
            raise RuntimeError("Nenhum SQL Warehouse configurado.")

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
            logger.error("❌ Erro ao criar tabela: %s", e)
            raise

        estado = resposta.status.state if resposta.status else None
        if estado and estado.value in ["FAILED", "CANCELED", "CLOSED"]:
            erro_msg = resposta.status.error.message if resposta.status.error else "Erro desconhecido"
            logger.error("❌ Falha ao criar tabela: %s", erro_msg)
            raise RuntimeError(f"Falha ao criar tabela: {erro_msg}")

        logger.info("✅ Tabela Delta '%s' criada/atualizada com sucesso no Unity Catalog!", tabela_destino)
    except Exception:
        logger.exception("❌ Falha na execução de upload_tabela_bronze_iti_numeros.")
        raise


if __name__ == "__main__":
    upload_volume_bronze_iti_entidades()
    upload_volume_bronze_iti_numeros()
    upload_tabela_bronze_iti_entidades()
    upload_tabela_bronze_iti_numeros()
