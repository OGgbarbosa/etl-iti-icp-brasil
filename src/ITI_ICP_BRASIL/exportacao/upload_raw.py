import io
import json

from databricks.sdk import WorkspaceClient

from ITI_ICP_BRASIL.assets.url_iti import obter_dados_num, obter_entidade
from ITI_ICP_BRASIL.config.logger import get_logger

logger = get_logger(__name__)


def upload_para_volume():
    # Inicializa o cliente do Databricks usando a autenticação configurada
    w = WorkspaceClient()

    # Caminho do Volume no Unity Catalog
    caminho_volume = "/Volumes/lakehouse_iti/0_raw/raw/entidades.json"

    logger.info("Obtendo dados da API do ITI")
    dados = obter_entidade()

    logger.info("Total de entidades obtidas: %d", len(dados))
    logger.info("Fazendo upload para o Volume Databricks: %s", caminho_volume)

    # Converte o JSON em bytes
    conteudo_bytes = json.dumps(dados, ensure_ascii=False, indent=2).encode("utf-8")

    # Faz o upload diretamente para o Volume via API
    w.files.upload(
        file_path=caminho_volume,
        contents=io.BytesIO(conteudo_bytes),
        overwrite=True
    )

    logger.info("✅ Dados salvos no Volume do Databricks com sucesso!")


def upload_volume_iti_numeros():
    try:
        w = WorkspaceClient()
    except Exception as e:
        logger.error("🟥 Erro ao conectar ao Databricks: %s", e)
        raise

    caminho_volume_numeros = "/Volumes/lakehouse_iti/0_raw/raw/numeros.json"

    logger.info("🌐 Obtendo dados da API do ITI...")
    try:
        dados = obter_dados_num()
    except Exception as e:
        logger.error("🟥 Erro ao obter dados da API: %s", e)
        raise

    logger.info("Total de registros obtidos: %d.", len(dados))
    logger.info("Fazendo upload para o Volume Databricks: %s", caminho_volume_numeros)

    # Converte o JSON em bytes
    conteudo_bytes = json.dumps(dados, ensure_ascii=False, indent=2).encode("utf-8")

    # Faz o upload diretamente para o Volume via API
    try:
        w.files.upload(
            file_path=caminho_volume_numeros,
            contents=io.BytesIO(conteudo_bytes),
            overwrite=True
        )
        logger.info("✅ Dados salvos no Volume do Databricks com sucesso!")
    except Exception as e:
        logger.error("🟥 Erro ao fazer upload para o Volume: %s", e)
        raise


if __name__ == "__main__":
    upload_volume_iti_numeros()