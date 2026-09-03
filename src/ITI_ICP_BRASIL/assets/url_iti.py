import requests
from ITI_ICP_BRASIL.processamento.flatten import flatten

url = 'https://estrutura.iti.gov.br/assets/jsons/details.json'

def obter_entidade():
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    dados = response.json()

    return [flatten(item) for item in dados['entidades']]