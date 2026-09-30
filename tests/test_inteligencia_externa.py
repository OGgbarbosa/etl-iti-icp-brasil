from ITI_ICP_BRASIL.processamento.inteligencia_externa import (
    FONTES_INTELIGENCIA,
    extrair_tags,
    limpar_texto,
)


def test_fontes_inteligencia_configuracao():
    assert len(FONTES_INTELIGENCIA) == 5
    nomes = [f["nome"] for f in FONTES_INTELIGENCIA]
    assert "ANCD" in nomes
    assert "AR FEDERAL" in nomes
    assert "CRYPTO ID" in nomes
    assert "ABRID" in nomes
    assert "CONVERGENCIA DIGITAL" in nomes


def test_limpar_texto():
    raw = "<p>Olá <b>Mundo</b>! &amp; Bem-vindo.</p>"
    cleaned = limpar_texto(raw)
    assert cleaned == "Olá Mundo! & Bem-vindo."


def test_extrair_tags():
    texto = "Debate sobre o avanço do ICP-Brasil, DREX e Certificado Digital no setor."
    tags = extrair_tags(texto)
    assert "ICP-Brasil" in tags
    assert "DREX" in tags
    assert "Certificado Digital" in tags
