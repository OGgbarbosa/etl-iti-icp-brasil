import ITI_ICP_BRASIL


def test_package_import() -> None:
    """Verifica se o pacote ITI_ICP_BRASIL pode ser importado com sucesso."""
    assert ITI_ICP_BRASIL is not None


def test_config_warehouse_id() -> None:
    """Verifica se o warehouse_id está configurado conforme o projeto."""
    from ITI_ICP_BRASIL.config.config import warehouse_id

    assert warehouse_id is not None
    assert isinstance(warehouse_id, str)
    assert len(warehouse_id) > 0
