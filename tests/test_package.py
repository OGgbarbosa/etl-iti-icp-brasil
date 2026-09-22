import importlib

import pytest

import ITI_ICP_BRASIL
import ITI_ICP_BRASIL.config.config as config_module


def test_package_import() -> None:
    """Verifica se o pacote ITI_ICP_BRASIL pode ser importado com sucesso."""
    assert ITI_ICP_BRASIL is not None


def test_config_warehouse_id_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verifica se o warehouse_id é carregado a partir das variáveis de ambiente."""
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "test_warehouse_123")
    importlib.reload(config_module)
    assert config_module.warehouse_id == "test_warehouse_123"


def test_config_warehouse_id_fallback_none(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verifica o comportamento quando nenhuma variável de warehouse está definida."""
    monkeypatch.delenv("DATABRICKS_WAREHOUSE_ID", raising=False)
    monkeypatch.delenv("WAREHOUSE_ID", raising=False)
    monkeypatch.setattr("dotenv.load_dotenv", lambda *args, **kwargs: False)
    monkeypatch.setattr(
        "databricks.sdk.WorkspaceClient",
        lambda *args, **kwargs: (_ for _ in ()).throw(Exception("No auth")),
    )
    importlib.reload(config_module)
    assert config_module.warehouse_id is None

