"""
Testes unitários para a Silver Layer.

Testa transformações de dados Bronze -> Silver.
"""

import json
import sqlite3
import pytest
import pandas as pd

from src.silver.transform import (
    parse_pub_date,
    normalizar_categoria,
    validar_url,
    analisar_sentimento,
    extrair_entidades,
    detectar_lingua,
    calcular_metricas_texto,
    transformar_artigo,
    criar_tabela_silver,
    processar_silver,
)

from src.utils.text_processing import (
    limpar_html,
    limpar_caracteres_especiais,
    normalizar_espacos,
    limpar_texto_completo,
    extrair_dominio,
    contar_palavras,
)

from src.db.loader import criar_tabela, carregar_csv


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def conn():
    """Conexão SQLite em memória com tabelas bronze e silver."""
    connection = sqlite3.connect(":memory:")
    criar_tabela(connection)
    criar_tabela_silver(connection)
    yield connection
    connection.close()


@pytest.fixture
def conn_com_dados(conn, tmp_path):
    """Conexão com dados de teste na tabela bronze."""
    df = pd.DataFrame([
        {
            "article_id": "test001",
            "title": "Portugal vence jogo importante",
            "description": "A seleção portuguesa conquistou vitória histórica.",
            "content": "O jogo decorreu em Lisboa com grande entusiasmo.",
            "source_id": "publico",
            "source_name": "Público",
            "source_url": "https://publico.pt",
            "creator": "João Silva",
            "pubDate": "2026-02-09 14:30:00",
            "category": "sports, top",
            "country": "portugal",
            "language": "portuguese",
            "link": "https://publico.pt/noticia1",
            "image_url": "https://publico.pt/img1.jpg",
        },
        {
            "article_id": "test002",
            "title": "Terrible news about economy crash",
            "description": "The market collapsed dramatically today.",
            "content": "Investors are worried about the future.",
            "source_id": "reuters",
            "source_name": "Reuters",
            "source_url": "https://reuters.com",
            "creator": None,
            "pubDate": "2026-02-09 15:00:00",
            "category": "business",
            "country": "usa",
            "language": "english",
            "link": "https://reuters.com/article",
            "image_url": None,
        },
    ])
    path = tmp_path / "test_data.csv"
    df.to_csv(path, index=False, encoding="utf-8")
    carregar_csv(conn, path, "test")
    return conn


# ============================================================================
# TESTES DE TEXT PROCESSING
# ============================================================================

class TestLimparHtml:
    """Testes para limpeza de HTML."""

    def test_remove_tags(self):
        result = limpar_html("<p>Texto <b>simples</b></p>")
        assert "Texto" in result
        assert "simples" in result
        assert "<p>" not in result
        assert "<b>" not in result

    def test_decodifica_entidades(self):
        assert "&" in limpar_html("Tom &amp; Jerry")

    def test_texto_none(self):
        assert limpar_html(None) is None

    def test_texto_vazio(self):
        assert limpar_html("") == ""


class TestLimparCaracteresEspeciais:
    """Testes para normalização de caracteres."""

    def test_normaliza_aspas(self):
        result = limpar_caracteres_especiais('"teste"')
        assert '"' in result

    def test_normaliza_travessoes(self):
        result = limpar_caracteres_especiais("a–b—c")
        assert result == "a-b-c"

    def test_texto_none(self):
        assert limpar_caracteres_especiais(None) is None


class TestNormalizarEspacos:
    """Testes para normalização de espaços."""

    def test_remove_espacos_multiplos(self):
        assert normalizar_espacos("a   b    c") == "a b c"

    def test_trim(self):
        assert normalizar_espacos("  teste  ") == "teste"

    def test_texto_none(self):
        assert normalizar_espacos(None) is None


class TestLimparTextoCompleto:
    """Testes para pipeline completo de limpeza."""

    def test_pipeline_completo(self):
        texto = "  <p>Texto &amp; mais</p>   texto  "
        result = limpar_texto_completo(texto)
        assert "Texto & mais" in result
        assert result == result.strip()

    def test_texto_none(self):
        assert limpar_texto_completo(None) is None


class TestExtrairDominio:
    """Testes para extração de domínio."""

    def test_url_com_www(self):
        assert extrair_dominio("https://www.publico.pt/artigo") == "publico.pt"

    def test_url_sem_www(self):
        assert extrair_dominio("https://sapo.pt/noticia") == "sapo.pt"

    def test_url_none(self):
        assert extrair_dominio(None) is None

    def test_url_invalida(self):
        assert extrair_dominio("not-a-url") is None


class TestContarPalavras:
    """Testes para contagem de palavras."""

    def test_conta_correto(self):
        assert contar_palavras("um dois três quatro cinco") == 5

    def test_texto_vazio(self):
        assert contar_palavras("") == 0

    def test_texto_none(self):
        assert contar_palavras(None) == 0


# ============================================================================
# TESTES DE PARSING DE DATAS
# ============================================================================

class TestParsePubDate:
    """Testes para parsing de datas."""

    def test_formato_padrao(self):
        result = parse_pub_date("2026-02-09 14:30:00")
        assert result["pub_date"] == "2026-02-09"
        assert result["pub_year"] == 2026
        assert result["pub_month"] == 2
        assert result["pub_day"] == 9
        assert result["pub_hour"] == 14

    def test_formato_iso(self):
        result = parse_pub_date("2026-02-09T14:30:00Z")
        assert result["pub_date"] == "2026-02-09"
        assert result["pub_hour"] == 14

    def test_data_none(self):
        result = parse_pub_date(None)
        assert result["pub_date"] is None
        assert result["pub_year"] is None

    def test_data_invalida(self):
        result = parse_pub_date("invalido")
        assert result["pub_date"] is None


# ============================================================================
# TESTES DE NORMALIZAÇÃO
# ============================================================================

class TestNormalizarCategoria:
    """Testes para normalização de categorias."""

    def test_categoria_unica(self):
        result = normalizar_categoria("technology")
        assert result["category_primary"] == "technology"
        assert result["category_count"] == 1

    def test_categorias_multiplas(self):
        result = normalizar_categoria("sports, top")
        assert result["category_primary"] == "sports"
        assert result["category_count"] == 2

    def test_mapeamento_sinonimos(self):
        result = normalizar_categoria("tech")
        assert result["category_primary"] == "technology"

    def test_categoria_none(self):
        result = normalizar_categoria(None)
        assert result["category_primary"] == "general"


class TestValidarUrl:
    """Testes para validação de URLs."""

    def test_url_valida(self):
        result = validar_url("https://publico.pt/artigo")
        assert result["link_valid"] == 1
        assert result["link_domain"] == "publico.pt"

    def test_url_invalida(self):
        result = validar_url("not-a-url")
        assert result["link_valid"] == 0

    def test_url_none(self):
        result = validar_url(None)
        assert result["link_valid"] == 0
        assert result["link_domain"] is None


# ============================================================================
# TESTES DE NLP - SENTIMENTO
# ============================================================================

class TestAnalisarSentimento:
    """Testes para análise de sentimento."""

    def test_texto_positivo(self):
        result = analisar_sentimento("This is great and amazing news!")
        assert result["sentiment_polarity"] > 0
        assert result["sentiment_label"] == "positive"

    def test_texto_negativo(self):
        result = analisar_sentimento("This is terrible and awful news.")
        assert result["sentiment_polarity"] < 0
        assert result["sentiment_label"] == "negative"

    def test_texto_neutro(self):
        result = analisar_sentimento("The meeting is scheduled for 3pm.")
        assert result["sentiment_label"] == "neutral"

    def test_texto_none(self):
        result = analisar_sentimento(None)
        assert result["sentiment_label"] == "neutral"

    def test_texto_curto(self):
        result = analisar_sentimento("Hi")
        assert result["sentiment_label"] == "neutral"


# ============================================================================
# TESTES DE NLP - ENTIDADES
# ============================================================================

class TestExtrairEntidades:
    """Testes para extração de entidades."""

    def test_extrai_locais(self):
        result = extrair_entidades("O evento decorreu em Lisboa, Portugal.")
        locations = json.loads(result["entities_locations"])
        assert len(locations) > 0

    def test_extrai_nomes(self):
        result = extrair_entidades("António Costa reuniu com Maria Silva.")
        persons = json.loads(result["entities_persons"])
        assert len(persons) > 0

    def test_texto_none(self):
        result = extrair_entidades(None)
        assert result["entity_count"] == 0

    def test_conta_entidades(self):
        result = extrair_entidades("Reunião em Lisboa com António Costa sobre Portugal.")
        assert result["entity_count"] >= 2


# ============================================================================
# TESTES DE DETECÇÃO DE LÍNGUA
# ============================================================================

class TestDetectarLingua:
    """Testes para detecção de língua."""

    def test_detecta_portugues(self):
        result = detectar_lingua(
            "Este é um texto em português com várias palavras.",
            "portuguese"
        )
        assert result["language_detected"] == "pt"
        assert result["language_match"] == 1

    def test_detecta_ingles(self):
        result = detectar_lingua(
            "This is a text written in English language.",
            "english"
        )
        assert result["language_detected"] == "en"
        assert result["language_match"] == 1

    def test_texto_curto(self):
        result = detectar_lingua("Hi", "english")
        assert result["language_detected"] is None

    def test_texto_none(self):
        result = detectar_lingua(None, "portuguese")
        assert result["language_detected"] is None


# ============================================================================
# TESTES DE MÉTRICAS DE TEXTO
# ============================================================================

class TestCalcularMetricasTexto:
    """Testes para métricas de texto."""

    def test_calcula_lengths(self):
        result = calcular_metricas_texto("Título", "Descrição do artigo", "Conteúdo")
        assert result["title_length"] == 6
        assert result["description_length"] == 19
        assert result["content_length"] == 8

    def test_conta_palavras(self):
        result = calcular_metricas_texto("Um dois", "três quatro", "cinco")
        assert result["word_count"] == 5

    def test_campos_none(self):
        result = calcular_metricas_texto(None, None, None)
        assert result["title_length"] == 0
        assert result["word_count"] == 0


# ============================================================================
# TESTES DE TRANSFORMAÇÃO COMPLETA
# ============================================================================

class TestTransformarArtigo:
    """Testes para transformação completa de artigo."""

    def test_transforma_campos_basicos(self):
        artigo = {
            "article_id": "test123",
            "title": "  <b>Título</b>  ",
            "description": "Descrição",
            "content": "Conteúdo",
            "pubDate": "2026-02-09 14:00:00",
            "category": "technology",
            "link": "https://teste.pt/artigo",
            "language": "portuguese",
            "source_id": "teste",
            "source_name": "Teste",
            "country": "portugal",
            "endpoint": "test",
        }
        result = transformar_artigo(artigo)

        assert result["article_id"] == "test123"
        assert result["title_clean"] == "Título"
        assert result["pub_date"] == "2026-02-09"
        assert result["category_primary"] == "technology"
        assert result["link_valid"] == 1
        assert result["processed_at"] is not None


# ============================================================================
# TESTES DE PROCESSAMENTO SILVER
# ============================================================================

class TestProcessarSilver:
    """Testes para processamento Silver completo."""

    def test_processa_artigos(self, conn_com_dados):
        n = processar_silver(conn_com_dados, verbose=False)
        assert n == 2

        cursor = conn_com_dados.execute("SELECT COUNT(*) FROM artigos_silver")
        assert cursor.fetchone()[0] == 2

    def test_nao_reprocessa(self, conn_com_dados):
        processar_silver(conn_com_dados, verbose=False)
        n = processar_silver(conn_com_dados, verbose=False)
        assert n == 0  # Já processados

    def test_campos_preenchidos(self, conn_com_dados):
        processar_silver(conn_com_dados, verbose=False)

        cursor = conn_com_dados.execute("""
            SELECT sentiment_label, category_primary, pub_date
            FROM artigos_silver
            WHERE article_id = 'test001'
        """)
        row = cursor.fetchone()

        assert row[0] in ("positive", "negative", "neutral")
        assert row[1] == "sports"
        assert row[2] == "2026-02-09"


class TestCriarTabelaSilver:
    """Testes para criação da tabela silver."""

    def test_cria_tabela(self, conn):
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='artigos_silver'"
        )
        assert cursor.fetchone() is not None

    def test_colunas_principais(self, conn):
        cursor = conn.execute("PRAGMA table_info(artigos_silver)")
        colunas = {row[1] for row in cursor.fetchall()}

        esperadas = {
            "article_id", "title_clean", "sentiment_polarity",
            "sentiment_label", "category_primary", "pub_date",
        }
        assert esperadas.issubset(colunas)

    def test_idempotente(self, conn):
        criar_tabela_silver(conn)  # Segunda vez
        cursor = conn.execute("SELECT COUNT(*) FROM artigos_silver")
        assert cursor.fetchone()[0] == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
