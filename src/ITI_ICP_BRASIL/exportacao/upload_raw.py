import io
import json
import logging

from databricks.sdk import WorkspaceClient

from ITI_ICP_BRASIL.assets.url_iti import obter_dados_num, obter_entidade

logger = logging.getLogger(__name__)

def upload_para_volume():
    # Inicializa o cliente do Databricks usando a autenticação configurada
    try:
        w = WorkspaceClient()
    except Exception as e:
        logger.error("🟥 Erro ao conectar ao Databricks: %s", e)
        raise

    # Caminho do Volume no Unity Catalog
    caminho_volume = "/Volumes/lakehouse_iti/0_raw/raw/entidades.json"

<<<<<<< HEAD
    print("Obtendo dados da API do ITI")
    dados = obter_entidade()
=======
    logger.info("🌐 Obtendo dados da API do ITI...")
    try:
        dados = obter_entidade()
    except Exception as e:
        logger.error("🟥 Erro ao obter dados da API: %s", e)
        raise
>>>>>>> fd4a49a (feat(raw): adiciona logger na execucão da camada raw)

    print(f"Total de entidades obtidas: {len(dados)}")
    print(f"Fazendo upload para o Volume Databricks: {caminho_volume}")

    # Converte o JSON em bytes
    try:
        conteudo_bytes = json.dumps(dados, ensure_ascii=False, indent=2).encode("utf-8")
    except Exception as e:
        logger.error("🟥 Erro ao converter dados para JSON: %s", e)
        raise

    # Faz o upload diretamente para o Volume via API
<<<<<<< HEAD
    w.files.upload(
        file_path=caminho_volume,
        contents=io.BytesIO(conteudo_bytes),
        overwrite=True
    )

    print("✅ Dados salvos no Volume do Databricks com sucesso!")
=======
    try:
        w.files.upload(
            file_path=caminho_volume,
            contents=io.BytesIO(conteudo_bytes),
            overwrite=True
        )
        logger.info("✅ Dados salvos no Volume do Databricks com sucesso!")
    except Exception as e:
        logger.error("🟥 Erro ao fazer upload para o Volume: %s", e)
        raise

>>>>>>> fd4a49a (feat(raw): adiciona logger na execucão da camada raw)

def upload_volume_iti_numeros():
    try:
        w = WorkspaceClient()
    except Exception as e:
        logger.error(f"🟥 Erro ao conectar ao Databricks: {e}")
        raise

    caminho_volume_numeros = "/Volumes/lakehouse_iti/0_raw/raw/numeros.json"

    print("🌐 Obtendo dados da API do ITI")
    try:
        dados = obter_dados_num()
    except Exception as e:
        logger.error(f"🟥 Erro ao obter dados da API: {e}")
        raise

<<<<<<< HEAD
    print(f"Total de entidades obtidas: {len(dados)}.")
    print(f"Fazendo upload para o Volume Databricks: {caminho_volume_numeros}")
=======
    logger.info("Total de registros obtidos: %d.", len(dados))
    logger.info("Fazendo upload para o Volume Databricks: %s", caminho_volume_numeros)
    try:
        conteudo_bytes = json.dumps(dados, ensure_ascii=False, indent=2).encode("utf-8")
    except Exception as e:
        logger.error("🟥 Erro ao converter dados para JSON: %s", e)
        raise
>>>>>>> fd4a49a (feat(raw): adiciona logger na execucão da camada raw)

    try:
        w.files.upload(
            file_path=caminho_volume_numeros,
            contents=io.BytesIO(conteudo_bytes),
            overwrite=True
        )
        logger.info("✅ Dados salvos no Volume do Databricks com sucesso!")
    except Exception as e:
        logger.error(f"🟥 Erro ao fazer upload para o Volume: {e}")
        raise
<<<<<<< HEAD

if __name__ == "__main__":
    upload_volume_iti_numeros()
=======
    
>>>>>>> fd4a49a (feat(raw): adiciona logger na execucão da camada raw)
