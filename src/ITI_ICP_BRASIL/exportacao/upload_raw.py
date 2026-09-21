import io
import json
import logging

from databricks.sdk import WorkspaceClient

from ITI_ICP_BRASIL.assets.url_iti import obter_dados_num, obter_entidade

logger = logging.getLogger(__name__)

def upload_para_volume():
    # Inicializa o cliente do Databricks usando a autenticação configurada
    w = WorkspaceClient()

    # Caminho do Volume no Unity Catalog
    caminho_volume = "/Volumes/lakehouse_iti/0_raw/raw/entidades.json"

    print("Obtendo dados da API do ITI")
    dados = obter_entidade()

    print(f"Total de entidades obtidas: {len(dados)}")
    print(f"Fazendo upload para o Volume Databricks: {caminho_volume}")

    # Converte o JSON em bytes
    conteudo_bytes = json.dumps(dados, ensure_ascii=False, indent=2).encode("utf-8")

    # Faz o upload diretamente para o Volume via API
    w.files.upload(
        file_path=caminho_volume,
        contents=io.BytesIO(conteudo_bytes),
        overwrite=True
    )

    print("✅ Dados salvos no Volume do Databricks com sucesso!")

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

    print(f"Total de entidades obtidas: {len(dados)}.")
    print(f"Fazendo upload para o Volume Databricks: {caminho_volume_numeros}")

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
        logger.error(f"🟥 Erro ao fazer upload para o Volume: {e}")
        raise

if __name__ == "__main__":
    upload_volume_iti_numeros()