import io
import json
import logging

from databricks.sdk import WorkspaceClient

from ITI_ICP_BRASIL.assets.url_iti import obter_dados_num, obter_entidade
from ITI_ICP_BRASIL.config.config import (
    volume_raw_entidades,
    volume_raw_numeros,
)

logger = logging.getLogger(__name__)


def upload_volume_iti_entidades():
    # Inicializa o cliente do Databricks usando a autenticação configurada
    try:
        w = WorkspaceClient()
    except Exception as e:
        logger.error("🟥 Erro ao conectar ao Databricks: %s", e)
        raise

    # Caminho do Volume no Unity Catalog
    caminho_volume_iti_entidades = volume_raw_entidades

    logger.info("🌐 Obtendo dados da API do ITI...")
    try:
        dados = obter_entidade()
    except Exception as e:
        logger.error("🟥 Erro ao obter dados da API: %s", e)
        raise

    logger.info("Total de registros obtidos: %d.", len(dados))
    logger.info("Fazendo upload para o Volume Databricks: %s", caminho_volume_iti_entidades)

    # Converte o JSON em bytes
    try:
        conteudo_bytes = json.dumps(dados, ensure_ascii=False, indent=2).encode("utf-8")
    except Exception as e:
        logger.error("🟥 Erro ao converter dados para JSON: %s", e)
        raise

    # Faz o upload diretamente para o Volume via API
    try:
        w.files.upload(
            file_path=caminho_volume_iti_entidades,
            contents=io.BytesIO(conteudo_bytes),
            overwrite=True
        )
        logger.info("✅ Dados salvos no Volume do Databricks com sucesso!")
    except Exception as e:
        logger.error("🟥 Erro ao fazer upload para o Volume: %s", e)
        raise


def upload_volume_iti_numeros():
    try:
        w = WorkspaceClient()
    except Exception as e:
        logger.error(f"🟥 Erro ao conectar ao Databricks: {e}")
        raise

    caminho_volume_iti_numeros = volume_raw_numeros

    logger.info("🌐 Obtendo dados da API do ITI...")
    try:
        dados = obter_dados_num()
    except Exception as e:
        logger.error(f"🟥 Erro ao obter dados da API: {e}")
        raise

    logger.info("Total de registros obtidos: %d.", len(dados))
    logger.info("Fazendo upload para o Volume Databricks: %s", caminho_volume_iti_numeros)
    try:
        conteudo_bytes = json.dumps(dados, ensure_ascii=False, indent=2).encode("utf-8")
    except Exception as e:
        logger.error("🟥 Erro ao converter dados para JSON: %s", e)
        raise

    try:
        w.files.upload(
            file_path=caminho_volume_iti_numeros,
            contents=io.BytesIO(conteudo_bytes),
            overwrite=True
        )
        logger.info("✅ Dados salvos no Volume do Databricks com sucesso!")
    except Exception as e:
        logger.error(f"🟥 Erro ao fazer upload para o Volume: {e}")
        raise
    
if __name__ == "__main__":
    upload_volume_iti_entidades ()
    upload_volume_iti_numeros()