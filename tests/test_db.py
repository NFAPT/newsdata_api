"""
Testes unitários para o módulo DB (SQLite loader).

Utiliza base de dados em memória para não criar ficheiros durante os testes.
"""

import sqlite3
import pytest
import pandas as pd
from pathlib import Path

from src.db.loader import (
    criar_tabela,
    carregar_csv,
    extrair_endpoint,
)


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def conn():
    """Conexão SQLite em memória com tabela criada."""
    connection = sqlite3.connect(":memory:")
    criar_tabela(connection)
    yield connection
    connection.close()


@pytest.fixture
def csv_file(tmp_path):
    """Cria um ficheiro CSV de teste."""
    df = pd.DataFrame([
        {
            "article_id": "abc123",
            "title": "Notícia de Teste 1",
            "description": "Descrição 1",
            "content": "Conteúdo 1",
            "source_id": "publico",
            "source_name": "Público",
            "source_url": "https://publico.pt",
            "creator": "João Silva",
            "pubDate": "2024-01-15 10:30:00",
            "category": "technology, business",
            "country": "portugal",
            "language": "portuguese",
            "link": "https://publico.pt/noticia1",
            "image_url": "https://publico.pt/img1.jpg",
        },
        {
            "article_id": "def456",
            "title": "Notícia de Teste 2",
            "description": "Descrição 2",
            "content": "Conteúdo 2",
            "source_id": "observador",
            "source_name": "Observador",
            "source_url": "https://observador.pt",
            "creator": None,
            "pubDate": "2024-01-15 11:00:00",
            "category": "sports",
            "country": "portugal",
            "language": "portuguese",
            "link": "https://observador.pt/noticia2",
            "image_url": None,
        },
    ])
    path = tmp_path / "newsdata_latestPT_tabular_20240115T103000Z.csv"
    df.to_csv(path, index=False, encoding="utf-8")
    return path


# ============================================================================
# TESTES DE ESQUEMA
# ============================================================================

class TestCriarTabela:
    """Testes para criação da tabela."""

    def test_cria_tabela(self, conn):
        """Testa que a tabela artigos é criada."""
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='artigos'"
        )
        assert cursor.fetchone() is not None

    def test_colunas_corretas(self, conn):
        """Testa que a tabela tem as colunas esperadas."""
        cursor = conn.execute("PRAGMA table_info(artigos)")
        colunas = {row[1] for row in cursor.fetchall()}

        esperadas = {
            "article_id", "title", "description", "content",
            "source_id", "source_name", "source_url", "creator",
            "pubDate", "category", "country", "language",
            "link", "image_url", "endpoint", "loaded_at",
        }
        assert colunas == esperadas

    def test_idempotente(self, conn):
        """Testa que criar tabela duas vezes não dá erro."""
        criar_tabela(conn)  # Segunda vez
        cursor = conn.execute("SELECT COUNT(*) FROM artigos")
        assert cursor.fetchone()[0] == 0


# ============================================================================
# TESTES DE CARREGAMENTO
# ============================================================================

class TestCarregarCsv:
    """Testes para carregamento de CSV."""

    def test_carrega_registos(self, conn, csv_file):
        """Testa que insere os registos do CSV."""
        inseridos = carregar_csv(conn, csv_file, "latestPT")
        assert inseridos == 2

        cursor = conn.execute("SELECT COUNT(*) FROM artigos")
        assert cursor.fetchone()[0] == 2

    def test_endpoint_preenchido(self, conn, csv_file):
        """Testa que o campo endpoint é preenchido."""
        carregar_csv(conn, csv_file, "latestPT")

        cursor = conn.execute("SELECT DISTINCT endpoint FROM artigos")
        endpoints = [row[0] for row in cursor.fetchall()]
        assert endpoints == ["latestPT"]

    def test_loaded_at_preenchido(self, conn, csv_file):
        """Testa que o campo loaded_at é preenchido."""
        carregar_csv(conn, csv_file, "latestPT")

        cursor = conn.execute("SELECT loaded_at FROM artigos LIMIT 1")
        loaded_at = cursor.fetchone()[0]
        assert loaded_at is not None
        assert len(loaded_at) > 0

    def test_duplicados_ignorados(self, conn, csv_file):
        """Testa que INSERT OR IGNORE evita duplicados."""
        carregar_csv(conn, csv_file, "latestPT")
        carregar_csv(conn, csv_file, "latestPT")  # Segunda vez

        cursor = conn.execute("SELECT COUNT(*) FROM artigos")
        assert cursor.fetchone()[0] == 2  # Não duplicou

    def test_csv_vazio(self, conn, tmp_path):
        """Testa carregamento de CSV vazio."""
        path = tmp_path / "vazio.csv"
        pd.DataFrame().to_csv(path, index=False)

        inseridos = carregar_csv(conn, path, "test")
        assert inseridos == 0


# ============================================================================
# TESTES DE UTILIDADES
# ============================================================================

class TestExtrairEndpoint:
    """Testes para extração do endpoint do nome do ficheiro."""

    def test_latestPT(self):
        assert extrair_endpoint("newsdata_latestPT_tabular_20260202T222456Z.csv") == "latestPT"

    def test_crypto(self):
        assert extrair_endpoint("newsdata_crypto_tabular_20260202T222534Z.csv") == "crypto"

    def test_desconhecido(self):
        assert extrair_endpoint("outro_ficheiro.csv") == "desconhecido"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
