import os
from contextlib import suppress
from typing import Any

from dotenv import load_dotenv

load_dotenv()


def obter_warehouse_id(w: Any = None) -> str | None:
    """Retorna o ID do SQL Warehouse configurado via env, widgets ou busca no workspace."""
    # 1. Variável de ambiente (configurada via .env local ou ambiente do cluster)
    wh_id = os.getenv("DATABRICKS_WAREHOUSE_ID") or os.getenv("WAREHOUSE_ID")
    if wh_id:
        return wh_id

    # 2. Widgets do Databricks Jobs (quando executando via Workflow Job com parâmetros)
    with suppress(Exception):
        from databricks.sdk.runtime import dbutils

        wh_id = dbutils.widgets.get("warehouse_id")
        if wh_id:
            return wh_id

    # 3. Fallback dinâmico: busca o primeiro SQL Warehouse disponível no workspace
    with suppress(Exception):
        if w is None:
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
        warehouses = list(w.warehouses.list())
        if warehouses:
            return warehouses[0].id

    return None


# SQL Warehouse
warehouse_id = obter_warehouse_id()

# Assets
URL_ENTIDADES = "https://estrutura.iti.gov.br/assets/jsons/details.json"
URL_NUMEROS = "https://numeros.iti.gov.br/assets/paneljson/panels.json"

# Raw
volume_raw_entidades = "/Volumes/lakehouse_iti/0_raw/raw/entidades.json"
volume_raw_numeros = "/Volumes/lakehouse_iti/0_raw/raw/numeros.json"

# Bronze
caminho_raw_entidades = "/Volumes/lakehouse_iti/0_raw/raw/entidades.json"
caminho_raw_numeros = "/Volumes/lakehouse_iti/0_raw/raw/numeros.json"

caminho_bronze_entidades = "/Volumes/lakehouse_iti/1_bronze/raw/entidades.csv"
caminho_bronze_numeros = "/Volumes/lakehouse_iti/1_bronze/raw/numeros.csv"

nome_volume_raw = "lakehouse_iti.1_bronze.raw"

tabela_destino_entidades = "lakehouse_iti.1_bronze.entidades"
tabela_destino_numeros = "lakehouse_iti.1_bronze.numeros"

# Silver

# Gold
