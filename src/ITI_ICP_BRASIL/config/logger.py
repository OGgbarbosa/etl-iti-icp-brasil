import contextlib
import logging
import os
import sys

DEFAULT_LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s - %(message)s"
DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def _configure_utf8_streams() -> None:
    """Garante que sys.stdout e sys.stderr suportem caracteres UTF-8 no Windows."""
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream and hasattr(stream, "reconfigure"):
            with contextlib.suppress(Exception):
                stream.reconfigure(encoding="utf-8", errors="replace")


class SafeStreamHandler(logging.StreamHandler):
    """StreamHandler defensivo que trata possíveis falhas de encoding no terminal Windows."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            super().emit(record)
        except UnicodeEncodeError:
            try:
                msg = self.format(record)
                stream = self.stream
                encoding = getattr(stream, "encoding", "utf-8") or "utf-8"
                safe_msg = msg.encode(encoding, errors="replace").decode(encoding)
                stream.write(safe_msg + self.terminator)
                self.flush()
            except Exception:
                self.handleError(record)


def setup_logging(
    level: str | None = None,
    log_format: str = DEFAULT_LOG_FORMAT,
    date_format: str = DEFAULT_DATE_FORMAT,
) -> None:
    """Configura o sistema de logging raiz para toda a aplicação.

    Evita duplicação de handlers caso já tenha sido configurado previamente.
    O nível de log pode ser sobrescrito pela variável de ambiente LOG_LEVEL.
    """
    _configure_utf8_streams()

    log_level_str = (level or os.getenv("LOG_LEVEL", "INFO")).upper()
    log_level = getattr(logging, log_level_str, logging.INFO)

    root_logger = logging.getLogger()

    # Se já houver handlers configurados, apenas ajusta o nível
    if root_logger.hasHandlers():
        root_logger.setLevel(log_level)
        return

    root_logger.setLevel(log_level)

    handler = SafeStreamHandler(sys.stdout)
    handler.setLevel(log_level)
    formatter = logging.Formatter(fmt=log_format, datefmt=date_format)
    handler.setFormatter(formatter)

    root_logger.addHandler(handler)


def get_logger(name: str | None = None) -> logging.Logger:
    """Retorna uma instância de logger configurada para o módulo solicitado.

    Garante que o logging raiz esteja inicializado antes de retornar o logger.
    """
    setup_logging()
    return logging.getLogger(name or "ITI_ICP_BRASIL")
