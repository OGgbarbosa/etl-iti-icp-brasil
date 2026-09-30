"""Módulo de Coleta e Ingestão de Inteligência Externa e Notícias Setoriais da ICP-Brasil.

Monitora e ingere publicações de fontes externas de referência no ecossistema:
- ANCD (Associação Nacional de Certificação Digital)
- AR Federal (Blog operacional de Autoridades de Registro)
- Crypto ID (Portal de criptografia, identidade digital e cibersegurança)
- ABRID (Associação Brasileira das Empresas de Tecnologia em Identificação Digital)
- Convergência Digital (Portal de telecomunicações, políticas públicas e segurança)

Cria e alimenta a tabela Gold 'lakehouse_iti.3_gold.dim_inteligencia_mercado'
e a função de catálogo 'lakehouse_iti.3_gold.fn_consultar_inteligencia_setorial'
utilizada pelo Databricks Genie como ferramenta semântica.
"""

import hashlib
import html
import re
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime

import requests
from databricks.sdk import WorkspaceClient

from ITI_ICP_BRASIL.config.config import obter_warehouse_id, warehouse_id
from ITI_ICP_BRASIL.config.logger import get_logger

logger = get_logger(__name__)

FONTES_INTELIGENCIA = [
    {
        "nome": "ANCD",
        "url_feed": "https://ancd.org.br/feed/",
        "portal": "https://ancd.org.br/",
        "categoria": "Associação e Políticas Setoriais",
    },
    {
        "nome": "AR FEDERAL",
        "url_feed": "https://arfederal.com.br/feed/",
        "portal": "https://arfederal.com.br/blog/",
        "categoria": "Operações de AR e Práticas de Mercado",
    },
    {
        "nome": "CRYPTO ID",
        "url_feed": "https://cryptoid.com.br/feed/",
        "portal": "https://cryptoid.com.br/",
        "categoria": "Segurança, Criptografia e Identidade Digital",
    },
    {
        "nome": "ABRID",
        "url_feed": "https://www.abrid.org.br/feed/",
        "portal": "https://www.abrid.org.br/",
        "categoria": "Indústria e Tecnologia de Identificação",
    },
    {
        "nome": "CONVERGENCIA DIGITAL",
        "url_feed": "https://convergenciadigital.com.br/feed/",
        "portal": "https://convergenciadigital.com.br/",
        "categoria": "Políticas Públicas e Telecomunicações",
    },
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}

TAGS_RELEVANTES = [
    "ICP-Brasil",
    "Certificado Digital",
    "Assinatura Eletrônica",
    "AR",
    "AC",
    "ITI",
    "DREX",
    "Identidade Digital",
    "Biometria",
    "Cibersegurança",
    "Criptografia",
    "LGPD",
]


def limpar_texto(texto_html: str, max_len: int = 600) -> str:
    """Remove marcações HTML, converte entidades e normaliza espaços."""
    if not texto_html:
        return ""
    texto = re.sub(r"<[^>]+>", " ", texto_html)
    texto = html.unescape(texto)
    texto = re.sub(r"\s+([.,!?;:])", r"\1", texto)
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto[:max_len]


def extrair_tags(texto: str) -> str:
    """Identifica termos-chave do ecossistema de identificação digital presentes no texto."""
    if not texto:
        return "Setorial"
    tags = [t for t in TAGS_RELEVANTES if re.search(rf"\b{re.escape(t)}\b", texto, re.IGNORECASE)]
    return ", ".join(tags) if tags else "Setorial"


def coletar_noticias_fontes_externas() -> list[dict]:
    """Coleta e padroniza as notícias e publicações recentes das fontes externas mapeadas."""
    artigos = []
    session = requests.Session()
    session.headers.update(HEADERS)

    for fonte in FONTES_INTELIGENCIA:
        try:
            logger.info("Coletando feed de inteligência: %s...", fonte["nome"])
            resp = session.get(fonte["url_feed"], timeout=12)
            if resp.status_code != 200:
                logger.warning("Fonte '%s' retornou status HTTP %d.", fonte["nome"], resp.status_code)
                continue

            root = ET.fromstring(resp.content)
            itens = root.findall(".//item")
            logger.info("Fonte '%s': %d publicações identificadas.", fonte["nome"], len(itens))

            for it in itens:
                title_elem = it.find("title")
                link_elem = it.find("link")
                date_elem = it.find("pubDate")
                desc_elem = it.find("description")

                titulo = limpar_texto(title_elem.text if title_elem is not None else "", 300)
                link = (link_elem.text or "").strip()
                resumo = limpar_texto(desc_elem.text if desc_elem is not None else "", 600)
                data_raw = date_elem.text if date_elem is not None else ""

                if not titulo or not link:
                    continue

                try:
                    dt = parsedate_to_datetime(data_raw).strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    dt = None

                id_hash = hashlib.md5(link.encode("utf-8")).hexdigest()
                tags = extrair_tags(f"{titulo} {resumo}")

                artigos.append({
                    "id_noticia": id_hash,
                    "nm_fonte": fonte["nome"],
                    "ds_categoria_fonte": fonte["categoria"],
                    "ds_titulo": titulo,
                    "ds_resumo": resumo,
                    "ds_url_origem": link,
                    "dt_publicacao": dt,
                    "ds_tags": tags,
                })
        except Exception as e:
            logger.warning("Falha ao coletar notícias da fonte '%s': %s", fonte["nome"], e)

    logger.info("Total de notícias coletadas de fontes externas: %d.", len(artigos))
    return artigos


def criar_tabela_inteligencia_mercado(w: WorkspaceClient, target_warehouse_id: str) -> None:
    """Cria a tabela Gold de inteligência externa no Lakehouse caso não exista."""
    ddl = """
    CREATE TABLE IF NOT EXISTS lakehouse_iti.3_gold.dim_inteligencia_mercado (
        ID_NOTICIA STRING,
        NM_FONTE STRING,
        DS_CATEGORIA_FONTE STRING,
        DS_TITULO STRING,
        DS_RESUMO STRING,
        DS_URL_ORIGEM STRING,
        DT_PUBLICACAO TIMESTAMP,
        DS_TAGS STRING,
        DT_CARGA_DW TIMESTAMP
    )
    CLUSTER BY (NM_FONTE, DT_PUBLICACAO);
    """
    logger.info("Garantindo estrutura da tabela 'lakehouse_iti.3_gold.dim_inteligencia_mercado'...")
    res = w.statement_execution.execute_statement(
        warehouse_id=target_warehouse_id,
        statement=ddl,
        wait_timeout="50s",
    )
    estado = res.status.state if res.status else None
    if not estado or estado.value != "SUCCEEDED":
        raise RuntimeError(f"Falha ao criar tabela dim_inteligencia_mercado: {estado}")


def carregar_noticias_inteligencia_mercado(
    w: WorkspaceClient, target_warehouse_id: str, noticias: list[dict]
) -> None:
    """Aplica MERGE idempotente dos artigos coletados na tabela Gold."""
    if not noticias:
        logger.info("Nenhuma notícia para inserir.")
        return

    def sql_escape(s):
        if s is None:
            return "NULL"
        return "'" + str(s).replace("'", "''") + "'"

    # Cria tabela temporária de staging ou batch merge
    values = []
    for n in noticias:
        dt_val = f"TIMESTAMP({sql_escape(n['dt_publicacao'])})" if n["dt_publicacao"] else "NULL"
        values.append(
            f"STRUCT({sql_escape(n['id_noticia'])} AS ID_NOTICIA, "
            f"{sql_escape(n['nm_fonte'])} AS NM_FONTE, "
            f"{sql_escape(n['ds_categoria_fonte'])} AS DS_CATEGORIA_FONTE, "
            f"{sql_escape(n['ds_titulo'])} AS DS_TITULO, "
            f"{sql_escape(n['ds_resumo'])} AS DS_RESUMO, "
            f"{sql_escape(n['ds_url_origem'])} AS DS_URL_ORIGEM, "
            f"{dt_val} AS DT_PUBLICACAO, "
            f"{sql_escape(n['ds_tags'])} AS DS_TAGS)"
        )

    array_sql = f"ARRAY({', '.join(values)})"

    merge_sql = f"""
    MERGE INTO lakehouse_iti.3_gold.dim_inteligencia_mercado AS target
    USING (
        SELECT 
            col.ID_NOTICIA,
            col.NM_FONTE,
            col.DS_CATEGORIA_FONTE,
            col.DS_TITULO,
            col.DS_RESUMO,
            col.DS_URL_ORIGEM,
            col.DT_PUBLICACAO,
            col.DS_TAGS,
            current_timestamp() AS DT_CARGA_DW
        FROM (SELECT EXPLODE({array_sql}) AS col)
    ) AS source
    ON target.ID_NOTICIA = source.ID_NOTICIA
    WHEN MATCHED THEN
        UPDATE SET 
            target.DS_TITULO = source.DS_TITULO,
            target.DS_RESUMO = source.DS_RESUMO,
            target.DS_TAGS = source.DS_TAGS,
            target.DT_CARGA_DW = source.DT_CARGA_DW
    WHEN NOT MATCHED THEN
        INSERT (ID_NOTICIA, NM_FONTE, DS_CATEGORIA_FONTE, DS_TITULO, DS_RESUMO, DS_URL_ORIGEM, DT_PUBLICACAO, DS_TAGS, DT_CARGA_DW)
        VALUES (source.ID_NOTICIA, source.NM_FONTE, source.DS_CATEGORIA_FONTE, source.DS_TITULO, source.DS_RESUMO, source.DS_URL_ORIGEM, source.DT_PUBLICACAO, source.DS_TAGS, source.DT_CARGA_DW);
    """

    logger.info("Executando MERGE de %d publicações na camada Gold...", len(noticias))
    res = w.statement_execution.execute_statement(
        warehouse_id=target_warehouse_id,
        statement=merge_sql,
        wait_timeout="50s",
    )
    estado = res.status.state if res.status else None
    if not estado or estado.value != "SUCCEEDED":
        raise RuntimeError(f"Falha ao executar MERGE em dim_inteligencia_mercado: {estado}")
    logger.info("✅ Notícias de inteligência externa mescladas com sucesso!")


def criar_funcao_inteligencia_setorial(w: WorkspaceClient, target_warehouse_id: str) -> None:
    """Cria a Unity Catalog Function utilizada pelo Genie como ferramenta analítica de inteligência externa."""
    ddl_func = """
    CREATE OR REPLACE FUNCTION lakehouse_iti.3_gold.fn_consultar_inteligencia_setorial(
        termo_busca STRING,
        nome_fonte STRING DEFAULT NULL
    )
    RETURNS TABLE (
        NM_FONTE STRING,
        DS_TITULO STRING,
        DS_RESUMO STRING,
        DS_URL_ORIGEM STRING,
        DT_PUBLICACAO TIMESTAMP
    )
    COMMENT 'Ferramenta de inteligência externa para o Genie. Busca artigos, notícias e publicações recentes dos portais ANCD, Crypto ID, ABRID, AR Federal e Convergência Digital com base em palavras-chave ou nome da fonte.'
    RETURN 
        SELECT 
            NM_FONTE,
            DS_TITULO,
            DS_RESUMO,
            DS_URL_ORIGEM,
            DT_PUBLICACAO
        FROM lakehouse_iti.3_gold.dim_inteligencia_mercado
        WHERE (termo_busca IS NULL OR LOWER(DS_TITULO) LIKE LOWER(CONCAT('%', termo_busca, '%')) OR LOWER(DS_RESUMO) LIKE LOWER(CONCAT('%', termo_busca, '%')))
          AND (nome_fonte IS NULL OR UPPER(NM_FONTE) LIKE UPPER(CONCAT('%', nome_fonte, '%')))
        ORDER BY DT_PUBLICACAO DESC
        LIMIT 10;
    """
    logger.info("Registrando função 'fn_consultar_inteligencia_setorial' no Unity Catalog...")
    res = w.statement_execution.execute_statement(
        warehouse_id=target_warehouse_id,
        statement=ddl_func,
        wait_timeout="50s",
    )
    estado = res.status.state if res.status else None
    if not estado or estado.value != "SUCCEEDED":
        raise RuntimeError(f"Falha ao registrar função fn_consultar_inteligencia_setorial: {estado}")
    logger.info("✅ Função de inteligência setorial criada no Unity Catalog com sucesso!")


def run_inteligencia_externa(
    w: WorkspaceClient | None = None, target_warehouse_id: str | None = None
) -> None:
    """Orquestra o ciclo completo de inteligência externa: coleta, DDL, MERGE e criação de UDF."""
    if not w or not target_warehouse_id:
        w = WorkspaceClient()
        target_warehouse_id = warehouse_id or obter_warehouse_id(w)
        if not target_warehouse_id:
            raise RuntimeError("SQL Warehouse não configurado.")

    noticias = coletar_noticias_fontes_externas()
    criar_tabela_inteligencia_mercado(w, target_warehouse_id)
    carregar_noticias_inteligencia_mercado(w, target_warehouse_id, noticias)
    criar_funcao_inteligencia_setorial(w, target_warehouse_id)


if __name__ == "__main__":
    run_inteligencia_externa()
