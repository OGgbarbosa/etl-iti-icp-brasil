from pathlib import Path

from databricks.sdk import WorkspaceClient

from ITI_ICP_BRASIL.config.config import obter_warehouse_id, warehouse_id
from ITI_ICP_BRASIL.config.logger import get_logger

logger = get_logger(__name__)


def aplicar_comentarios_genie():
    """Lê o arquivo comments_gold_genie.sql e aplica cada instrução via Statement Execution API."""
    w = WorkspaceClient()
    target_warehouse_id = warehouse_id or obter_warehouse_id(w)
    if not target_warehouse_id:
        logger.error("❌ Nenhum SQL Warehouse configurado ou disponível no workspace.")
        raise RuntimeError("SQL Warehouse não configurado para execução de comentários.")

    sql_file = Path(__file__).resolve().parent.parent / "sql" / "comments_gold_genie.sql"
    sql_content = sql_file.read_text(encoding="utf-8")

    # Divide os comandos por ponto e vírgula
    comandos = [c.strip() for c in sql_content.split(";") if c.strip()]

    logger.info("🚀 Aplicando %d comentários semânticos no Unity Catalog...", len(comandos))

    for idx, comando in enumerate(comandos, start=1):
        linhas = [l for l in comando.splitlines() if not l.strip().startswith("--")]
        stmt_limpo = "\n".join(linhas).strip()
        if not stmt_limpo:
            continue

        primeira_linha = stmt_limpo.splitlines()[0]
        logger.info("Executando [%d/%d]: %s", idx, len(comandos), primeira_linha)

        try:
            resposta = w.statement_execution.execute_statement(
                warehouse_id=target_warehouse_id,
                statement=stmt_limpo,
                wait_timeout="50s",
            )
            estado = resposta.status.state if resposta.status else None
            if not estado or estado.value != "SUCCEEDED":
                erro = (
                    resposta.status.error.message 
                    if (resposta.status and resposta.status.error) 
                    else f"Status: {estado}"
                )
                logger.error("❌ Falha no comando [%d]: %s", idx, erro)
                raise RuntimeError(f"Erro ao executar SQL: {erro}")
        except Exception as e:
            logger.error("❌ Erro ao executar [%s]: %s", primeira_linha, e)
            raise

    logger.info("✅ Todos os comentários semânticos para o Databricks Genie foram aplicados com sucesso!")


if __name__ == "__main__":
    aplicar_comentarios_genie()
