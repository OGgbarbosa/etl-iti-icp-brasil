"""Script de Implantação e Publicação do Databricks Genie Space - ICP-Brasil.

Cria ou atualiza o espaço semântico conversacional 'Genie - Inteligência Analítica ICP-Brasil'
no workspace do Databricks via Databricks SDK (GenieAPI).

Configura:
1. Data Sources: 9 tabelas analíticas da camada Gold (dimensões, fatos, KPIs e inteligência de mercado).
2. Diretrizes e Instruções Globais (System Prompt): Regras de negócio, glossário do ITI, filtros obrigatórios
   (DS_SITUACAO = 'CREDENCIADA', VL_METRICA, séries temporais e higienização de CNPJs).
3. Exemplos Canônicos de SQL (Curated Queries): Consultas de referência para ranking de ACs, evolução temporal,
   distribuição geográfica e busca de notícias setoriais.
4. Perguntas de Amostra (Sample Questions): Consultas sugeridas na interface inicial do Genie.
5. Benchmark de Validação: Perguntas de referência com respostas SQL esperadas para validação contínua.
"""

import json
import uuid
from pathlib import Path

from databricks.sdk import WorkspaceClient

from ITI_ICP_BRASIL.config.config import obter_warehouse_id, warehouse_id
from ITI_ICP_BRASIL.config.logger import get_logger

logger = get_logger(__name__)

TITULO_GENIE = "Genie - Inteligência Analítica ICP-Brasil"
DESCRICAO_GENIE = (
    "Espaço analítico e conversacional para exploração de dados cadastrais, topologia da cadeia de confiança, "
    "séries temporais de emissão de certificados digitais e inteligência de mercado do ecossistema ICP-Brasil."
)

TABELAS_GOLD = [
    "lakehouse_iti.3_gold.dim_entidade",
    "lakehouse_iti.3_gold.dim_hierarquia",
    "lakehouse_iti.3_gold.fato_metricas_entidades",
    "lakehouse_iti.3_gold.fato_emissao_mensal",
    "lakehouse_iti.3_gold.fato_distribuicao_geografica",
    "lakehouse_iti.3_gold.fato_segmentacao_certificados",
    "lakehouse_iti.3_gold.fato_infraestrutura_credenciamento",
    "lakehouse_iti.3_gold.kpi_resumo_executivo",
    "lakehouse_iti.3_gold.dim_inteligencia_mercado",
]

INSTRUCOES_TEXTO = [
    (
        "REGRAS DE NEGOCIO OBRIGATORIAS:\n"
        "1. VALORES EXATOS DE DS_TIPO (dim_entidade e fato_metricas_entidades):\n"
        "   - 'AR': Autoridade de Registro (ponta física/remota de atendimento).\n"
        "   - 'AC 1º NÍVEL': Autoridade Certificadora de 1º Nível (subordinada à AC Raiz).\n"
        "   - 'AC 2º NÍVEL': Autoridade Certificadora de 2º Nível (emite para usuários finais e vincula ARs).\n"
        "   - 'AC RAIZ': Autoridade máxima operada pelo ITI.\n"
        "   * Para Autoridades de Registro (ARs), filtre sempre por: DS_TIPO = 'AR'.\n"
        "   * Para Autoridades Certificadoras (ACs), use: DS_TIPO IN ('AC 1º NÍVEL', 'AC 2º NÍVEL') ou DS_TIPO LIKE '%AC%'.\n"
        "2. ENTIDADES ATIVAS: No Lakehouse, a situacao operacional de autoridades ativas e 'CREDENCIADA'. "
        "NUNCA filtre por 'ATIVA'. Para ARs ativas/em operacao: WHERE DS_SITUACAO = 'CREDENCIADA' AND DS_TIPO = 'AR'.\n"
        "3. METRICA DE VOLUME: A coluna canonica quantitativa e 'VL_METRICA' (a coluna NR_QUANTIDADE nao existe).\n"
        "4. SERIES TEMPORAIS (fato_emissao_mensal):\n"
        "   - Novas emissoes ocorridas no mes: DS_TIPO_SERIE = 'MENSAL CORRENTE' (DS_GRANULARIDADE = 'MENSAL').\n"
        "   - Estoque cumulativo de certificados validos/ativos: DS_TIPO_SERIE = 'HISTORICO ATIVOS' (DS_GRANULARIDADE = 'ANUAL').\n"
        "   - Volume total emitido acumulado no ano: DS_TIPO_SERIE = 'HISTORICO EMITIDOS' (DS_GRANULARIDADE = 'ANUAL').\n"
        "5. TOPOLOGIA E HIERARQUIA: Para responder quais ACs tem mais ARs subordinadas ou o tamanho da cadeia de uma autoridade, "
        "utilize SEMPRE a tabela pre-agregada 'lakehouse_iti.3_gold.fato_metricas_entidades' "
        "(colunas NR_AGREGADOS_AR, NR_AGREGADOS_AC_NIVEL_1, NR_AGREGADOS_AC_NIVEL_2).\n"
        "6. CNPJs: A coluna NR_CNPJ possui exatamente 14 digitos com zeros a esquerda. "
        "Caso o usuario informe CNPJ pontuado, aplique LPAD(REGEXP_REPLACE(input, '[^0-9]', ''), 14, '0').\n"
        "7. INTELIGENCIA DE MERCADO E NOTICIAS SETORIAIS: Quando o usuario perguntar sobre noticias, fatos relevantes, "
        "posicionamento de entidades (ANCD, Crypto ID, ABRID, AR Federal, Convergencia Digital) ou contexto setorial, "
        "consulte a tabela 'lakehouse_iti.3_gold.dim_inteligencia_mercado' ou chame a funcao 'lakehouse_iti.3_gold.fn_consultar_inteligencia_setorial'."
    )
]

EXEMPLOS_SQL = [
    {
        "question": "Quais são as 5 maiores Autoridades Certificadoras em número de ARs subordinadas?",
        "sql": (
            "SELECT ID_ENTIDADE, DS_ENTIDADE, SG_UF, DS_TIPO, NR_AGREGADOS_AR\n"
            "FROM lakehouse_iti.3_gold.fato_metricas_entidades\n"
            "WHERE DS_TIPO IN ('AC 1º NÍVEL', 'AC 2º NÍVEL') AND DS_SITUACAO = 'CREDENCIADA'\n"
            "ORDER BY NR_AGREGADOS_AR DESC\n"
            "LIMIT 5;"
        ),
    },
    {
        "question": "Qual foi a evolução do total de emissões de certificados mês a mês no último ano?",
        "sql": (
            "SELECT DT_ANO, DT_MES_ANO, SUM(VL_METRICA) AS TOTAL_EMISSOES\n"
            "FROM lakehouse_iti.3_gold.fato_emissao_mensal\n"
            "WHERE DS_TIPO_SERIE = 'MENSAL CORRENTE'\n"
            "GROUP BY DT_ANO, DT_MES_ANO\n"
            "ORDER BY DT_MES_ANO DESC;"
        ),
    },
    {
        "question": "Como está distribuída a quantidade de ARs ativas por estado?",
        "sql": (
            "SELECT SG_UF, DS_REGIAO, COUNT(DISTINCT ID_ENTIDADE) AS TOTAL_ARS\n"
            "FROM lakehouse_iti.3_gold.dim_entidade\n"
            "WHERE DS_SITUACAO = 'CREDENCIADA' AND DS_TIPO = 'AR'\n"
            "GROUP BY SG_UF, DS_REGIAO\n"
            "ORDER BY TOTAL_ARS DESC;"
        ),
    },
    {
        "question": "Quais as notícias recentes sobre certificação digital e ICP-Brasil publicadas pela ANCD e Crypto ID?",
        "sql": (
            "SELECT NM_FONTE, DS_TITULO, DT_PUBLICACAO, DS_URL_ORIGEM\n"
            "FROM lakehouse_iti.3_gold.dim_inteligencia_mercado\n"
            "WHERE (LOWER(DS_TITULO) LIKE '%icp-brasil%' OR LOWER(DS_TAGS) LIKE '%icp-brasil%')\n"
            "  AND NM_FONTE IN ('ANCD', 'CRYPTO ID')\n"
            "ORDER BY DT_PUBLICACAO DESC\n"
            "LIMIT 5;"
        ),
    },
]

PERGUNTAS_AMOSTRA = [
    "Quais são as 5 maiores Autoridades Certificadoras por quantidade de ARs vinculadas?",
    "Qual o total de certificados digitais emitidos nos últimos 12 meses?",
    "Quantas Autoridades de Registro (ARs) credenciadas existem em São Paulo?",
    "Quais as principais notícias recentes publicadas pela ANCD e Crypto ID?",
    "Qual a proporção de emissões entre certificados A1 e A3?",
]

BENCHMARKS = [
    {
        "question": "Total de certificados emitidos no mês corrente",
        "sql": (
            "SELECT SUM(VL_METRICA) AS TOTAL_EMISSOES_MES\n"
            "FROM lakehouse_iti.3_gold.fato_emissao_mensal\n"
            "WHERE DS_TIPO_SERIE = 'MENSAL CORRENTE'\n"
            "  AND DT_MES_ANO = (SELECT MAX(DT_MES_ANO) FROM lakehouse_iti.3_gold.fato_emissao_mensal WHERE DS_TIPO_SERIE = 'MENSAL CORRENTE');"
        ),
    },
    {
        "question": "Quantidade de ARs credenciadas em São Paulo",
        "sql": (
            "SELECT COUNT(DISTINCT ID_ENTIDADE) AS TOTAL_ARS_SP\n"
            "FROM lakehouse_iti.3_gold.dim_entidade\n"
            "WHERE DS_SITUACAO = 'CREDENCIADA'\n"
            "  AND DS_TIPO = 'AR'\n"
            "  AND SG_UF = 'SP';"
        ),
    },
]


def gerar_payload_serialized_space() -> str:
    """Gera a especificação JSON do Genie Space no formato serialized_space versão 2."""
    def gen_id():
        return uuid.uuid4().hex

    sample_questions = sorted(
        [{"id": gen_id(), "question": [q]} for q in PERGUNTAS_AMOSTRA],
        key=lambda x: x["id"],
    )

    text_instructions = sorted(
        [{"id": gen_id(), "content": [inst]} for inst in INSTRUCOES_TEXTO],
        key=lambda x: x["id"],
    )

    example_question_sqls = sorted(
        [{"id": gen_id(), "question": [ex["question"]], "sql": [ex["sql"]]} for ex in EXEMPLOS_SQL],
        key=lambda x: x["id"],
    )

    benchmark_questions = sorted(
        [
            {
                "id": gen_id(),
                "question": [bm["question"]],
                "answer": [
                    {
                        "format": "SQL",
                        "content": [bm["sql"]],
                    }
                ],
            }
            for bm in BENCHMARKS
        ],
        key=lambda x: x["id"],
    )

    payload = {
        "version": 2,
        "config": {
            "sample_questions": sample_questions
        },
        "data_sources": {
            "tables": [{"identifier": tbl} for tbl in sorted(TABELAS_GOLD)]
        },
        "instructions": {
            "text_instructions": text_instructions,
            "example_question_sqls": example_question_sqls,
        },
        "benchmarks": {
            "questions": benchmark_questions
        },
    }
    return json.dumps(payload, ensure_ascii=False, indent=2)


def implantar_genie_space(
    w: WorkspaceClient | None = None, target_warehouse_id: str | None = None
) -> str:
    """Cria ou atualiza o Genie Space no workspace Databricks."""
    if not w:
        w = WorkspaceClient()
    if not target_warehouse_id:
        target_warehouse_id = warehouse_id or obter_warehouse_id(w)
        if not target_warehouse_id:
            raise RuntimeError("Nenhum SQL Warehouse ativo ou configurado para o Genie.")

    logger.info("Verificando espaços Genie existentes no workspace...")
    lista_resposta = w.genie.list_spaces()
    espacos_existentes = getattr(lista_resposta, "spaces", []) or []

    espaco_alvo = None
    for espaco in espacos_existentes:
        if getattr(espaco, "title", None) == TITULO_GENIE:
            espaco_alvo = espaco
            break

    serialized_payload = gerar_payload_serialized_space()

    # Salva também localmente em resources/genie/config para controle de versão
    json_path = Path(__file__).resolve().parent.parent / "config" / "genie_space_definition.json"
    json_path.write_text(serialized_payload, encoding="utf-8")
    logger.info("Definição do espaço salva localmente em: %s", json_path)

    if espaco_alvo:
        space_id = espaco_alvo.space_id
        logger.info("Espaço existente encontrado (ID: %s). Atualizando configurações...", space_id)
        espaco_atualizado = w.genie.update_space(
            space_id=space_id,
            title=TITULO_GENIE,
            description=DESCRICAO_GENIE,
            warehouse_id=target_warehouse_id,
            serialized_space=serialized_payload,
        )
        logger.info("✅ Genie Space atualizado com sucesso! ID: %s", espaco_atualizado.space_id)
        return espaco_atualizado.space_id
    else:
        logger.info("Criando novo Genie Space '%s'...", TITULO_GENIE)
        novo_espaco = w.genie.create_space(
            warehouse_id=target_warehouse_id,
            serialized_space=serialized_payload,
            title=TITULO_GENIE,
            description=DESCRICAO_GENIE,
        )
        logger.info("✅ Genie Space criado com sucesso! ID: %s", novo_espaco.space_id)
        return novo_espaco.space_id


if __name__ == "__main__":
    import sys

    if "--with-comments" in sys.argv:
        from resources.genie.scripts.apply_comments import aplicar_comentarios_genie

        logger.info("Aplicando comentários semânticos no Unity Catalog antes do deploy...")
        aplicar_comentarios_genie()

    sid = implantar_genie_space()
    print("\n🎉 Genie Space pronto para uso!")
    print(f"Space ID: {sid}")
