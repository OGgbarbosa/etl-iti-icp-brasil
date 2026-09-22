import logging

from ITI_ICP_BRASIL.config.logger import get_logger, setup_logging


def test_get_logger() -> None:
    """Verifica se get_logger retorna uma instância de Logger configurada."""
    logger = get_logger("teste_modulo")
    assert isinstance(logger, logging.Logger)
    assert logger.name == "teste_modulo"


def test_setup_logging_level() -> None:
    """Verifica se setup_logging configura adequadamente o nivel de log."""
    setup_logging(level="DEBUG")
    root_logger = logging.getLogger()
    assert root_logger.level == logging.DEBUG

    # Restaura para INFO
    setup_logging(level="INFO")
    assert root_logger.level == logging.INFO
