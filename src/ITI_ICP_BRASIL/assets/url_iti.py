import requests

from ITI_ICP_BRASIL.config.config import URL_ENTIDADES, URL_NUMEROS
from ITI_ICP_BRASIL.processamento.flatten import flatten, flatten_num


def obter_entidade():
    response = requests.get(URL_ENTIDADES, timeout=30)
    response.raise_for_status()

    dados = response.json()

    try:
        return [flatten(item) for item in dados['entidades']]
    except Exception as e:
        print(f"Erro: {e}")
        return []

def obter_dados_num():
    response = requests.get(URL_NUMEROS, timeout=30)
    response.raise_for_status()
    dados = response.json()

    try:
        return flatten_num(dados)
    except Exception as e:
        print(f"Erro: {e}")
        return []