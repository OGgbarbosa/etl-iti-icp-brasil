import logging
from typing import Any

import requests

from ITI_ICP_BRASIL.config.config import URL_ENTIDADES, URL_NUMEROS
from ITI_ICP_BRASIL.processamento.flatten import flatten, flatten_num

logger = logging.getLogger(__name__)


def obter_entidade() -> list[dict[str, Any]]:
    try:
        response = requests.get(URL_ENTIDADES, timeout=30)
        response.raise_for_status()
        dados = response.json()
        return [flatten(item) for item in dados["entidades"]]
    except requests.RequestException as e:
        logger.error("Erro na requisição HTTP para %s: %s", URL_ENTIDADES, e)
        raise
    except (KeyError, TypeError) as e:
        logger.error("Erro na estrutura do JSON retornado por %s: %s", URL_ENTIDADES, e)
        raise
    except Exception as e:
        logger.error("Erro inesperado ao processar dados de %s: %s", URL_ENTIDADES, e)
        raise


def obter_dados_num() -> list[dict[str, Any]]:
    try:
        response = requests.get(URL_NUMEROS, timeout=30)
        response.raise_for_status()
        dados = response.json()
        return flatten_num(dados)
    except requests.RequestException as e:
        logger.error("Erro na requisição HTTP para %s: %s", URL_NUMEROS, e)
        raise
    except (KeyError, TypeError) as e:
        logger.error("Erro na estrutura do JSON retornado por %s: %s", URL_NUMEROS, e)
        raise
    except Exception as e:
        logger.error("Erro inesperado ao processar dados de %s: %s", URL_NUMEROS, e)
        raise