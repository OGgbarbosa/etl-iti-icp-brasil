from datetime import datetime, timezone

glossario = {
    # cer: Séries de Emissão
    'cerVisGerHisAtivos': 'cerVisao Geral Ativos Ano a ano',
    'cerVisGerHistEmitidos': 'cerVisao Geral Emitidos Ano a ano',
    'cerVisGerCompMenAnoAtual': 'cerVisao Geral Mes a mes ANO ATUAL',
    'cerVisGerCompMenAnoPassado': 'cerVisao Geral Mes a mes ANO PASSADO',
    'cerVisGerCompMenAnoRetrasado': 'cerVisao Geral Mes a mes ANO RETRASADO',

    # reg: Distribuição Geográfica
    'certDistMapa': 'cerDistribuicao Mapa ANO ATUAL',
    'certDistCerMen': 'cerDistribuicao Mes a mes ',
    'estMap': 'estVisao Geral Mapa ANO ATUAL',

    # dis: Segmentação / Tipo e Uso
    'certDistCerTipo': 'cerDistribuicao Tipo ANO ATUAL',
    'certDistCerUso': 'cerDistribuicao Uso ANO ATUAL',
    'cerVisGerDisAssinatura': 'cerVisao Geral Assinatura certificado ACUMULADO',
    'cerVisGerDisTipo': 'cerVisao Geral Tipo certificado ACUMULADO',

    # inf: Infraestrutura e Cabeçalhos
    'cerVisGerAtivos': 'cerVisao Geral Ativos (nao expirados, nao revogados)',
    'cerVisGerEmitidos': 'cerVisao Geral Emitidos',
    'cerVisGerToEmRelAAnterior': 'cerVisao Geral Total de emissoes em relacao ao ano anterior PERCENTUAL',
    'cerVisGerProAAtual': 'cerVisao Geral Projecao para ANO ATUAL',
    'estAgtRegistro': 'estVisao Geral Agentes de Registro ACUMULADO',
    'estAcumuladoAc1': 'estVisao Geral AC-1 ACUMULADO',
    'estAcumuladoAc2': 'estVisao Geral AC-2 ACUMULADO',
    'estAcumuladoAutReg': 'estVisao Geral Autoridades de Registro ACUMULADO',
    'estAcumuladoPSS': 'estVisao Geral Prestadores de Servico de Suporte ACUMULADO',
    'estAcumuladoACT': 'estVisao Geral Autoridade de Carimbo do Tempo ACUMULADO',
    'estAcumuladoPSC': 'estVisao Geral Prestadores de Servico de Confianca ACUMULADO',
    'estAcumuladoPSB': 'estVisao Geral Prestadores de Servico Biometrico ACUMULADO',
    'estCompAC2': 'estComparativo AC-2 com o ANO ATUAL',
    'estCompAC2RelAnoAnterior': 'estComparativo AC-2 com o ANO PASSADO PERCENTUAL',
    'estCompAR': 'estComparativo AR com o ANO ATUAL',
    'estCompARRelAnoAnterior': 'estComparativo AR com o ANO PASSADO PERCENTUAL',
    'estCompMenAnoAtual': 'estComparativo AR credenciamento com o ANO ATUAL',
    'estCompMenAnoAnterior': 'estComparativo AR credenciamento com o ANO PASSADO',

    # meta: Metadados do Painel
    'manutencao': 'manutencao',
    'versao': 'versao',
    'finalizado': 'finalizado',
}

glossario_mes = {
0:'01',
1:'02',
2:'03',
3:'04',
4:'05',
5:'06',
6:'07',
7:'08',
8:'09',
9:'10',
10:'11',
11:'12',
}

ano_atual = datetime.now(timezone.utc).year
ano_anterior = ano_atual - 1
ano_retrasado = ano_atual - 2


mapa_anos = {
'cerVisao Geral Mes a mes ANO ATUAL': ano_atual,
'cerVisao Geral Mes a mes ANO PASSADO': ano_anterior,
'cerVisao Geral Mes a mes ANO RETRASADO': ano_retrasado,
'cerDistribuicao Mapa ANO ATUAL': ano_atual, # SOMENTE ANO
'cerDistribuicao Tipo ANO ATUAL': ano_atual, # SOMENTE ANO
'cerDistribuicao Uso ANO ATUAL': ano_atual, # SOMENTE ANO
'estVisao Geral Mapa ANO ATUAL': ano_atual, # SOMENTE ANO
'estComparativo AC-2 com o ANO ATUAL': ano_atual,
'estComparativo AR com o ANO ATUAL': ano_atual,
'estComparativo AR credenciamento com o ANO ATUAL': ano_atual,
'estComparativo AC-2 com o ANO PASSADO PERCENTUAL': ano_anterior,
'estComparativo AR com o ANO PASSADO PERCENTUAL': ano_anterior,
'estComparativo AR credenciamento com o ANO PASSADO': ano_anterior,
}

flag = {
    # cer: Séries de Emissão
    'cerVisGerHisAtivos': 'cerHISTORICO_ANUAL_EMISSAO_ATIVOS',
    'cerVisGerHistEmitidos': 'cerHISTORICO_ANUAL_EMISSAO_EMITIDOS',
    'cerVisGerCompMenAnoAtual': 'cerEMISSAO_CORRENTE',
    'cerVisGerCompMenAnoPassado': 'cerEMISSAO_CORRENTE',
    'cerVisGerCompMenAnoRetrasado': 'cerEMISSAO_CORRENTE',

    # reg: Distribuição Geográfica
    'certDistMapa': 'regEMISSAO_ANUAL',
    'certDistCerMen': 'regEMISSAO_MENSAL',
    'estMap': 'regAR_CORRENTE',

    # dis: Segmentação / Tipo e Uso
    'certDistCerTipo': 'disTIPO',
    'certDistCerUso': 'disUSO',
    'cerVisGerDisAssinatura': 'disCERTIFICADOS_ASSINATURA',
    'cerVisGerDisTipo': 'disCERTIFICADOS_TIPO',

    # inf: Infraestrutura e Cabeçalhos
    'cerVisGerAtivos': 'infHEADER_ATIVOS',
    'cerVisGerEmitidos': 'infHEADER_EMITIDOS',
    'cerVisGerToEmRelAAnterior': 'infHEADER_TOTAL_REL_ANTERIOR',
    'cerVisGerProAAtual': 'infHEADER_PROJECAO_ATUAL',
    'estAgtRegistro': 'infHEADER_ACUMULADO_AGENTE_REGISTRO',
    'estAcumuladoAc1': 'infHEADER_ACUMULADO_AC1',
    'estAcumuladoAc2': 'infHEADER_ACUMULADO_AC2',
    'estAcumuladoAutReg': 'infHEADER_ACUMULADO_AUT_REG',
    'estAcumuladoPSS': 'infHEADER_ACUMULADO_PSS',
    'estAcumuladoACT': 'infHEADER_ACUMULADO_ACT',
    'estAcumuladoPSC': 'infHEADER_ACUMULADO_PSC',
    'estAcumuladoPSB': 'infHEADER_ACUMULADO_PSB',
    'estCompAC2': 'infHEADER_COMPARATIVO_AC2',
    'estCompAC2RelAnoAnterior': 'infHEADER_COMPARATIVO_AC2_REL_ANTERIOR',
    'estCompAR': 'infHEADER_COMPARATIVO_AR',
    'estCompARRelAnoAnterior': 'infHEADER_COMPARATIVO_AR_REL_ANTERIOR',
    'estCompMenAnoAtual': 'infCREDENCIAMENTO_AR',
    'estCompMenAnoAnterior': 'infCREDENCIAMENTO_AR',
}







