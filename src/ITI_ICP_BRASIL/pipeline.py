from ITI_ICP_BRASIL.assets.url_iti import obter_entidade
from ITI_ICP_BRASIL.exportacao.upload_bronze import (
    upload_tabela_bronze,
    upload_volume_bronze,
)
from ITI_ICP_BRASIL.exportacao.upload_gold import (
    upload_gold_entidades,
    upload_gold_hierarquia,
    upload_gold_metricas_entidades,
)
from ITI_ICP_BRASIL.exportacao.upload_raw import upload_para_volume
from ITI_ICP_BRASIL.exportacao.upload_silver_pyspark import (
    upload_silver_enderecos,
    upload_silver_entidades,
    upload_silver_hierarquia,
)


def pipeline():
    obter_entidade()
    upload_para_volume()
    upload_volume_bronze()
    upload_tabela_bronze()
    upload_silver_entidades()
    upload_silver_enderecos()
    upload_silver_hierarquia()
    upload_gold_entidades()
    upload_gold_hierarquia()
    upload_gold_metricas_entidades()

if __name__ == '__main__':
    pipeline()