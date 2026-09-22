from ITI_ICP_BRASIL.exportacao.upload_bronze import (
    upload_tabela_bronze,
    upload_volume_bronze,
)
from ITI_ICP_BRASIL.exportacao.upload_gold import (
    upload_gold_entidades,
    upload_gold_hierarquia,
    upload_gold_metricas_entidades,
)
from ITI_ICP_BRASIL.exportacao.upload_raw import upload_volume_iti_entidades, upload_volume_iti_numeros
from ITI_ICP_BRASIL.exportacao.upload_silver_pyspark import (
    upload_silver_enderecos,
    upload_silver_entidades,
    upload_silver_hierarquia,
)


def run_raw() -> None:
    """Extrai entidades da API pública do ITI e salva no Volume Raw."""
    upload_volume_iti_entidades()
    upload_volume_iti_numeros()


def run_bronze() -> None:
    """Converte os dados brutos para CSV e carrega a tabela Delta Bronze."""
    upload_volume_bronze()
    upload_tabela_bronze()


def run_silver() -> None:
    """Processa e padroniza as tabelas Silver (entidades, endereços e hierarquia)."""
    upload_silver_entidades()
    upload_silver_enderecos()
    upload_silver_hierarquia()


def run_gold() -> None:
    """Gera a modelagem dimensional Gold (dimensões e tabela fato com métricas recursivas)."""
    upload_gold_entidades()
    upload_gold_hierarquia()
    upload_gold_metricas_entidades()


def pipeline() -> None:
    """Executa o pipeline completo de ponta a ponta."""
    run_raw()
    run_bronze()
    run_silver()
    run_gold()