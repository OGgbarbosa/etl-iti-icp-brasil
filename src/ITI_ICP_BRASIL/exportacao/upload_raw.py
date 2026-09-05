import io
import json

from databricks.sdk import WorkspaceClient

from ITI_ICP_BRASIL.assets.url_iti import obter_entidade


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

if __name__ == "__main__":
    upload_para_volume()