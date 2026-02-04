"""
Testes unitários para o módulo de Web Scraping da Wikipédia.

Utiliza mocks para não fazer requests reais durante os testes.
"""

import json
import sqlite3
import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import Mock, patch, call

from src.bronze.wiki_scraper import (
    pesquisar_por_tema,
    obter_aleatorias,
    extrair_titulos_de_urls,
    extrair_resumo,
    scrape_paginas,
    salvar_json_raw,
    resultados_to_dataframe,
    salvar_csv,
    criar_tabela_wiki,
    carregar_na_db,
    obter_urls,
    escolher_idioma,
    MAX_PAGINAS,
)

# URLs de teste (PT por defeito)
TEST_API_URL = "https://pt.wikipedia.org/w/api.php"
TEST_REST_URL = "https://pt.wikipedia.org/api/rest_v1/page/summary"


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def mock_resumo():
    """Mock de resposta da REST API summary."""
    return {
        "title": "Python (linguagem de programação)",
        "extract": "Python é uma linguagem de programação de alto nível.",
        "pageid": 12345,
        "content_urls": {
            "desktop": {
                "page": "https://pt.wikipedia.org/wiki/Python_(linguagem_de_programa%C3%A7%C3%A3o)"
            }
        },
    }


@pytest.fixture
def mock_resultados():
    """Lista de resultados de scraping."""
    return [
        {
            "titulo": "Python (linguagem de programação)",
            "resumo": "Python é uma linguagem de programação de alto nível.",
            "url": "https://pt.wikipedia.org/wiki/Python",
            "pageid": 12345,
            "timestamp_scrape": "2024-01-15T10:30:00+00:00",
        },
        {
            "titulo": "Linux",
            "resumo": "Linux é um termo utilizado para se referir a sistemas operativos.",
            "url": "https://pt.wikipedia.org/wiki/Linux",
            "pageid": 67890,
            "timestamp_scrape": "2024-01-15T10:30:00+00:00",
        },
    ]


@pytest.fixture
def temp_output(tmp_path):
    """Diretório temporário para output."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    return output_dir


# ============================================================================
# TESTES DE PESQUISA POR TEMA
# ============================================================================

class TestPesquisarPorTema:
    """Testes para pesquisa por tema na Wikipédia."""

    @patch("src.bronze.wiki_scraper.requests.get")
    def test_pesquisa_sucesso(self, mock_get):
        """Testa pesquisa com resultados."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status = Mock()
        mock_response.json.return_value = [
            "python",
            ["Python", "Python (linguagem)"],
            ["", ""],
            ["https://pt.wikipedia.org/wiki/Python", "https://pt.wikipedia.org/wiki/Python_(linguagem)"],
        ]
        mock_get.return_value = mock_response

        titulos = pesquisar_por_tema("python", TEST_API_URL)

        assert len(titulos) == 2
        assert "Python" in titulos

    @patch("src.bronze.wiki_scraper.requests.get")
    def test_pesquisa_sem_resultados(self, mock_get):
        """Testa pesquisa sem resultados."""
        mock_response = Mock()
        mock_response.raise_for_status = Mock()
        mock_response.json.return_value = ["xyzabc123", [], [], []]
        mock_get.return_value = mock_response

        titulos = pesquisar_por_tema("xyzabc123", TEST_API_URL)

        assert titulos == []

    @patch("src.bronze.wiki_scraper.requests.get")
    def test_pesquisa_respeita_limite(self, mock_get):
        """Testa que o limite máximo é respeitado."""
        mock_response = Mock()
        mock_response.raise_for_status = Mock()
        # Simular 15 resultados
        titulos_mock = [f"Página {i}" for i in range(15)]
        mock_response.json.return_value = ["tema", titulos_mock, [], []]
        mock_get.return_value = mock_response

        titulos = pesquisar_por_tema("tema", TEST_API_URL, limite=15)

        assert len(titulos) <= MAX_PAGINAS


# ============================================================================
# TESTES DE PÁGINAS ALEATÓRIAS
# ============================================================================

class TestObterAleatorias:
    """Testes para obtenção de páginas aleatórias."""

    @patch("src.bronze.wiki_scraper.requests.get")
    def test_aleatorias_sucesso(self, mock_get):
        """Testa obtenção de páginas aleatórias."""
        mock_response = Mock()
        mock_response.raise_for_status = Mock()
        mock_response.json.return_value = {
            "query": {
                "random": [
                    {"id": 1, "title": "Artigo A"},
                    {"id": 2, "title": "Artigo B"},
                    {"id": 3, "title": "Artigo C"},
                ]
            }
        }
        mock_get.return_value = mock_response

        titulos = obter_aleatorias(TEST_API_URL, 3)

        assert len(titulos) == 3
        assert "Artigo A" in titulos

    @patch("src.bronze.wiki_scraper.requests.get")
    def test_aleatorias_respeita_limite(self, mock_get):
        """Testa que o limite máximo é respeitado."""
        mock_response = Mock()
        mock_response.raise_for_status = Mock()
        paginas = [{"id": i, "title": f"Artigo {i}"} for i in range(15)]
        mock_response.json.return_value = {"query": {"random": paginas}}
        mock_get.return_value = mock_response

        titulos = obter_aleatorias(TEST_API_URL, 15)

        assert len(titulos) <= MAX_PAGINAS


# ============================================================================
# TESTES DE EXTRAÇÃO DE URLs
# ============================================================================

class TestExtrairTitulosDeUrls:
    """Testes para extração de títulos a partir de URLs."""

    def test_url_simples(self):
        """Testa extração de URL simples."""
        urls = ["https://pt.wikipedia.org/wiki/Python"]
        titulos = extrair_titulos_de_urls(urls)

        assert titulos == ["Python"]

    def test_url_com_underscores(self):
        """Testa que underscores são convertidos em espaços."""
        urls = ["https://pt.wikipedia.org/wiki/Linguagem_de_programa%C3%A7%C3%A3o"]
        titulos = extrair_titulos_de_urls(urls)

        assert titulos[0] == "Linguagem de programação"

    def test_url_invalido(self):
        """Testa que URLs inválidos são ignorados."""
        urls = ["https://example.com/pagina"]
        titulos = extrair_titulos_de_urls(urls)

        assert titulos == []

    def test_limite_maximo(self):
        """Testa que o limite de 10 é respeitado."""
        urls = [f"https://pt.wikipedia.org/wiki/Pagina_{i}" for i in range(15)]
        titulos = extrair_titulos_de_urls(urls)

        assert len(titulos) <= MAX_PAGINAS


# ============================================================================
# TESTES DE EXTRAÇÃO DE RESUMO
# ============================================================================

class TestExtrairResumo:
    """Testes para extração de resumo de uma página."""

    @patch("src.bronze.wiki_scraper.requests.get")
    def test_extrair_sucesso(self, mock_get, mock_resumo):
        """Testa extração bem-sucedida."""
        mock_response = Mock()
        mock_response.raise_for_status = Mock()
        mock_response.json.return_value = mock_resumo
        mock_get.return_value = mock_response

        resultado = extrair_resumo("Python (linguagem de programação)", TEST_REST_URL)

        assert resultado is not None
        assert resultado["titulo"] == "Python (linguagem de programação)"
        assert "alto nível" in resultado["resumo"]

    @patch("src.bronze.wiki_scraper.requests.get")
    def test_extrair_erro(self, mock_get):
        """Testa tratamento de erro na extração."""
        import requests as req
        mock_get.side_effect = req.ConnectionError("Connection error")

        resultado = extrair_resumo("Pagina_Inexistente", TEST_REST_URL)

        assert resultado is None


# ============================================================================
# TESTES DE SCRAPING COMPLETO
# ============================================================================

class TestScrapePaginas:
    """Testes para scraping de múltiplas páginas."""

    @patch("src.bronze.wiki_scraper.extrair_resumo")
    def test_scrape_multiplas(self, mock_extrair):
        """Testa scraping de múltiplas páginas."""
        mock_extrair.return_value = {
            "titulo": "Teste",
            "resumo": "Resumo de teste",
            "url": "https://pt.wikipedia.org/wiki/Teste",
            "pageid": 1,
        }

        resultados = scrape_paginas(["Teste1", "Teste2", "Teste3"], TEST_REST_URL)

        assert len(resultados) == 3
        assert all("timestamp_scrape" in r for r in resultados)

    @patch("src.bronze.wiki_scraper.extrair_resumo")
    def test_scrape_com_falhas(self, mock_extrair):
        """Testa que falhas individuais não bloqueiam o pipeline."""
        mock_extrair.side_effect = [
            {"titulo": "OK", "resumo": "OK", "url": "", "pageid": 1},
            None,  # falha
            {"titulo": "OK2", "resumo": "OK2", "url": "", "pageid": 2},
        ]

        resultados = scrape_paginas(["OK", "Falha", "OK2"], TEST_REST_URL)

        assert len(resultados) == 2

    @patch("src.bronze.wiki_scraper.extrair_resumo")
    def test_scrape_respeita_limite(self, mock_extrair):
        """Testa que o limite de 10 páginas é respeitado."""
        mock_extrair.return_value = {
            "titulo": "T", "resumo": "R", "url": "", "pageid": 1,
        }

        titulos = [f"Pagina_{i}" for i in range(15)]
        resultados = scrape_paginas(titulos, TEST_REST_URL)

        assert len(resultados) <= MAX_PAGINAS


# ============================================================================
# TESTES DE PERSISTÊNCIA
# ============================================================================

class TestSalvarFicheiros:
    """Testes para salvar ficheiros JSON e CSV."""

    def test_salvar_json_raw(self, mock_resultados, temp_output):
        """Testa salvar JSON raw."""
        timestamp = "20240115T103000Z"

        path = salvar_json_raw(mock_resultados, temp_output, timestamp)

        assert path.exists()
        assert path.name == f"wiki_scrape_raw_{timestamp}.json"

        with open(path, "r", encoding="utf-8") as f:
            loaded = json.load(f)
        assert len(loaded) == 2
        assert loaded[0]["titulo"] == "Python (linguagem de programação)"

    def test_salvar_csv(self, mock_resultados, temp_output):
        """Testa salvar CSV."""
        df = pd.DataFrame(mock_resultados)
        timestamp = "20240115T103000Z"

        path = salvar_csv(df, temp_output, timestamp)

        assert path.exists()
        assert path.name == f"wiki_scrape_tabular_{timestamp}.csv"

        loaded = pd.read_csv(path)
        assert len(loaded) == 2


# ============================================================================
# TESTES DE CONVERSÃO PARA DATAFRAME
# ============================================================================

class TestResultadosToDataframe:
    """Testes para conversão de resultados para DataFrame."""

    def test_converte_resultados(self, mock_resultados):
        """Testa conversão de resultados."""
        df = resultados_to_dataframe(mock_resultados)

        assert len(df) == 2
        assert "titulo" in df.columns
        assert "resumo" in df.columns
        assert "url" in df.columns

    def test_resultados_vazios(self):
        """Testa conversão com lista vazia."""
        df = resultados_to_dataframe([])

        assert df.empty


# ============================================================================
# TESTES DE IDIOMA
# ============================================================================

class TestIdioma:
    """Testes para selecção de idioma."""

    def test_obter_urls_pt(self):
        """Testa URLs para português."""
        api_url, rest_url = obter_urls("pt")
        assert "pt.wikipedia.org" in api_url
        assert "pt.wikipedia.org" in rest_url

    def test_obter_urls_en(self):
        """Testa URLs para inglês."""
        api_url, rest_url = obter_urls("en")
        assert "en.wikipedia.org" in api_url
        assert "en.wikipedia.org" in rest_url

    @patch("builtins.input", return_value="1")
    def test_escolher_portugues(self, mock_input):
        """Testa selecção de português."""
        idioma = escolher_idioma()
        assert idioma == "pt"

    @patch("builtins.input", return_value="2")
    def test_escolher_ingles(self, mock_input):
        """Testa selecção de inglês."""
        idioma = escolher_idioma()
        assert idioma == "en"


# ============================================================================
# TESTES DE BASE DE DADOS SQLITE
# ============================================================================

class TestBaseDeDados:
    """Testes para carregamento na wiki.db."""

    @pytest.fixture
    def conn(self):
        """Conexão SQLite em memória."""
        conn = sqlite3.connect(":memory:")
        criar_tabela_wiki(conn)
        yield conn
        conn.close()

    def test_criar_tabela(self, conn):
        """Testa criação da tabela paginas."""
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='paginas'"
        )
        assert cursor.fetchone() is not None

    def test_carregar_resultados(self, conn, mock_resultados):
        """Testa inserção de resultados na DB."""
        inseridos = carregar_na_db(conn, mock_resultados, "tema")

        assert inseridos == 2

        cursor = conn.execute("SELECT COUNT(*) FROM paginas")
        assert cursor.fetchone()[0] == 2

    def test_idempotente(self, conn, mock_resultados):
        """Testa que duplicados são ignorados."""
        carregar_na_db(conn, mock_resultados, "tema")
        carregar_na_db(conn, mock_resultados, "tema")

        cursor = conn.execute("SELECT COUNT(*) FROM paginas")
        assert cursor.fetchone()[0] == 2

    def test_campos_corretos(self, conn, mock_resultados):
        """Testa que os campos são guardados corretamente."""
        carregar_na_db(conn, mock_resultados, "aleatorio")

        cursor = conn.execute("SELECT titulo, modo FROM paginas WHERE pageid='12345'")
        row = cursor.fetchone()
        assert row[0] == "Python (linguagem de programação)"
        assert row[1] == "aleatorio"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
