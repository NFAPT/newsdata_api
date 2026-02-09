"""
Testes unitários para a Gold Layer.

Testa agregações de dados Silver -> Gold.
"""

import json
import sqlite3
import pytest

from src.silver.transform import criar_tabela_silver
from src.db.loader import criar_tabela

from src.gold.aggregate import (
    criar_tabelas_gold,
    calcular_daily_summary,
    calcular_source_stats,
    calcular_trending_topics,
    calcular_sentiment_timeline,
    calcular_category_matrix,
    processar_gold,
    extrair_palavras_significativas,
    STOPWORDS,
)


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def conn():
    """Conexão SQLite em memória com todas as tabelas."""
    connection = sqlite3.connect(":memory:")
    criar_tabela(connection)
    criar_tabela_silver(connection)
    criar_tabelas_gold(connection)
    yield connection
    connection.close()


@pytest.fixture
def conn_com_silver(conn):
    """Conexão com dados de teste na tabela silver."""
    # Inserir dados de teste na tabela silver
    dados = [
        # Dia 1 - Fonte A - Categoria sports
        ("id001", "Portugal vence jogo importante", "2026-02-08", "fonte_a", "Fonte A", "sports", 0.5, 0.4, "positive", 100),
        ("id002", "Benfica empata com Porto", "2026-02-08", "fonte_a", "Fonte A", "sports", 0.1, 0.3, "neutral", 80),
        # Dia 1 - Fonte B - Categoria business
        ("id003", "Economia em crescimento", "2026-02-08", "fonte_b", "Fonte B", "business", 0.3, 0.5, "positive", 120),
        # Dia 2 - Fonte A - Categoria technology
        ("id004", "Nova tecnologia revolucionária", "2026-02-09", "fonte_a", "Fonte A", "technology", 0.6, 0.6, "positive", 150),
        ("id005", "Problemas graves na indústria", "2026-02-09", "fonte_a", "Fonte A", "business", -0.4, 0.7, "negative", 90),
        # Dia 2 - Fonte B - Categoria sports
        ("id006", "Sporting perde jogo decisivo", "2026-02-09", "fonte_b", "Fonte B", "sports", -0.2, 0.4, "negative", 110),
    ]

    sql = """
    INSERT INTO artigos_silver (
        article_id, title_clean, pub_date, source_id, source_name,
        category_primary, sentiment_polarity, sentiment_subjectivity,
        sentiment_label, word_count
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    conn.executemany(sql, dados)
    conn.commit()
    return conn


# ============================================================================
# TESTES DE UTILIDADES
# ============================================================================

class TestExtrairPalavrasSignificativas:
    """Testes para extração de palavras significativas."""

    def test_remove_stopwords_pt(self):
        palavras = extrair_palavras_significativas("O presidente da república está em Portugal")
        assert "presidente" in palavras
        assert "república" in palavras
        assert "portugal" in palavras
        assert "o" not in palavras
        assert "da" not in palavras
        assert "em" not in palavras

    def test_remove_stopwords_en(self):
        palavras = extrair_palavras_significativas("The president of the country is here")
        assert "president" in palavras
        assert "country" in palavras
        assert "the" not in palavras
        assert "of" not in palavras
        assert "is" not in palavras

    def test_remove_palavras_curtas(self):
        palavras = extrair_palavras_significativas("a to be or not to be")
        assert all(len(p) > 2 for p in palavras)

    def test_texto_none(self):
        assert extrair_palavras_significativas(None) == []

    def test_texto_vazio(self):
        assert extrair_palavras_significativas("") == []


class TestStopwords:
    """Testes para lista de stopwords."""

    def test_contem_stopwords_pt(self):
        pt_stopwords = ["o", "a", "de", "da", "em", "para", "com", "que", "se"]
        for word in pt_stopwords:
            assert word in STOPWORDS

    def test_contem_stopwords_en(self):
        en_stopwords = ["the", "a", "an", "of", "to", "in", "is", "are", "was"]
        for word in en_stopwords:
            assert word in STOPWORDS


# ============================================================================
# TESTES DE CRIAÇÃO DE TABELAS
# ============================================================================

class TestCriarTabelasGold:
    """Testes para criação das tabelas gold."""

    def test_cria_todas_tabelas(self, conn):
        tabelas_esperadas = [
            "gold_daily_summary",
            "gold_source_stats",
            "gold_trending_topics",
            "gold_sentiment_timeline",
            "gold_category_matrix",
        ]
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'gold_%'"
        )
        tabelas = {row[0] for row in cursor.fetchall()}
        for tabela in tabelas_esperadas:
            assert tabela in tabelas

    def test_idempotente(self, conn):
        criar_tabelas_gold(conn)  # Segunda vez
        # Não deve dar erro


# ============================================================================
# TESTES DE DAILY SUMMARY
# ============================================================================

class TestCalcularDailySummary:
    """Testes para cálculo do resumo diário."""

    def test_calcula_contagens(self, conn_com_silver):
        n = calcular_daily_summary(conn_com_silver)
        assert n > 0

        cursor = conn_com_silver.execute("SELECT COUNT(*) FROM gold_daily_summary")
        assert cursor.fetchone()[0] > 0

    def test_agrupa_por_data_fonte_categoria(self, conn_com_silver):
        calcular_daily_summary(conn_com_silver)

        cursor = conn_com_silver.execute("""
            SELECT summary_date, source_id, category_primary, article_count
            FROM gold_daily_summary
            WHERE source_id = 'fonte_a' AND category_primary = 'sports'
        """)
        row = cursor.fetchone()
        assert row is not None
        assert row[3] == 2  # 2 artigos de fonte_a/sports no dia 1

    def test_calcula_sentimento_medio(self, conn_com_silver):
        calcular_daily_summary(conn_com_silver)

        cursor = conn_com_silver.execute("""
            SELECT avg_sentiment_polarity, positive_count, negative_count
            FROM gold_daily_summary
            WHERE source_id = 'fonte_a' AND category_primary = 'sports'
        """)
        row = cursor.fetchone()
        assert row[0] is not None  # avg_sentiment_polarity
        assert row[1] >= 0  # positive_count
        assert row[2] >= 0  # negative_count

    def test_filtra_por_data(self, conn_com_silver):
        n = calcular_daily_summary(conn_com_silver, "2026-02-08")
        cursor = conn_com_silver.execute("""
            SELECT DISTINCT summary_date FROM gold_daily_summary
        """)
        datas = [row[0] for row in cursor.fetchall()]
        assert "2026-02-08" in datas


# ============================================================================
# TESTES DE SOURCE STATS
# ============================================================================

class TestCalcularSourceStats:
    """Testes para estatísticas por fonte."""

    def test_calcula_volume(self, conn_com_silver):
        n = calcular_source_stats(conn_com_silver)
        assert n > 0

        cursor = conn_com_silver.execute("""
            SELECT source_id, total_articles FROM gold_source_stats
        """)
        stats = {row[0]: row[1] for row in cursor.fetchall()}
        assert stats["fonte_a"] == 4
        assert stats["fonte_b"] == 2

    def test_identifica_categorias(self, conn_com_silver):
        calcular_source_stats(conn_com_silver)

        cursor = conn_com_silver.execute("""
            SELECT category_count, primary_category
            FROM gold_source_stats
            WHERE source_id = 'fonte_a'
        """)
        row = cursor.fetchone()
        assert row[0] >= 2  # Pelo menos 2 categorias

    def test_calcula_sentimento(self, conn_com_silver):
        calcular_source_stats(conn_com_silver)

        cursor = conn_com_silver.execute("""
            SELECT avg_sentiment_polarity FROM gold_source_stats
            WHERE source_id = 'fonte_a'
        """)
        row = cursor.fetchone()
        assert row[0] is not None


# ============================================================================
# TESTES DE TRENDING TOPICS
# ============================================================================

class TestCalcularTrendingTopics:
    """Testes para tópicos em tendência."""

    def test_extrai_palavras(self, conn_com_silver):
        n = calcular_trending_topics(conn_com_silver, "2026-02-09")
        assert n > 0

        cursor = conn_com_silver.execute("""
            SELECT term, frequency FROM gold_trending_topics
            WHERE topic_date = '2026-02-09'
            ORDER BY frequency DESC
            LIMIT 5
        """)
        rows = cursor.fetchall()
        assert len(rows) > 0

    def test_ranking(self, conn_com_silver):
        calcular_trending_topics(conn_com_silver, "2026-02-09")

        cursor = conn_com_silver.execute("""
            SELECT rank, frequency FROM gold_trending_topics
            WHERE topic_date = '2026-02-09'
            ORDER BY rank
        """)
        rows = cursor.fetchall()
        # Verifica que rank 1 tem maior frequência
        if len(rows) >= 2:
            assert rows[0][1] >= rows[1][1]

    def test_sample_titles(self, conn_com_silver):
        calcular_trending_topics(conn_com_silver, "2026-02-09")

        cursor = conn_com_silver.execute("""
            SELECT sample_titles FROM gold_trending_topics
            WHERE topic_date = '2026-02-09'
            LIMIT 1
        """)
        row = cursor.fetchone()
        if row:
            titles = json.loads(row[0])
            assert isinstance(titles, list)


# ============================================================================
# TESTES DE SENTIMENT TIMELINE
# ============================================================================

class TestCalcularSentimentTimeline:
    """Testes para evolução do sentimento."""

    def test_calcula_timeline_diaria(self, conn_com_silver):
        n = calcular_sentiment_timeline(conn_com_silver, "daily")
        assert n > 0

        cursor = conn_com_silver.execute("""
            SELECT timeline_date, avg_polarity, article_count
            FROM gold_sentiment_timeline
            WHERE granularity = 'daily'
        """)
        rows = cursor.fetchall()
        assert len(rows) >= 2  # Pelo menos 2 dias

    def test_calcula_distribuicao(self, conn_com_silver):
        calcular_sentiment_timeline(conn_com_silver)

        cursor = conn_com_silver.execute("""
            SELECT positive_pct, negative_pct, neutral_pct
            FROM gold_sentiment_timeline
            LIMIT 1
        """)
        row = cursor.fetchone()
        # Soma das percentagens deve ser ~100
        total = (row[0] or 0) + (row[1] or 0) + (row[2] or 0)
        assert 99 <= total <= 101

    def test_min_max_polarity(self, conn_com_silver):
        calcular_sentiment_timeline(conn_com_silver)

        cursor = conn_com_silver.execute("""
            SELECT min_polarity, max_polarity
            FROM gold_sentiment_timeline
            LIMIT 1
        """)
        row = cursor.fetchone()
        assert row[0] <= row[1]  # min <= max


# ============================================================================
# TESTES DE CATEGORY MATRIX
# ============================================================================

class TestCalcularCategoryMatrix:
    """Testes para matriz categorias x fontes."""

    def test_calcula_matriz(self, conn_com_silver):
        n = calcular_category_matrix(conn_com_silver)
        assert n > 0

        cursor = conn_com_silver.execute("""
            SELECT source_id, category_primary, article_count
            FROM gold_category_matrix
        """)
        rows = cursor.fetchall()
        assert len(rows) > 0

    def test_percentagens(self, conn_com_silver):
        calcular_category_matrix(conn_com_silver)

        cursor = conn_com_silver.execute("""
            SELECT pct_of_source, pct_of_category
            FROM gold_category_matrix
            WHERE source_id = 'fonte_a' AND category_primary = 'sports'
        """)
        row = cursor.fetchone()
        assert row[0] > 0  # pct_of_source
        assert row[1] > 0  # pct_of_category

    def test_todas_combinacoes(self, conn_com_silver):
        calcular_category_matrix(conn_com_silver)

        # fonte_a tem 3 categorias: sports, business, technology
        cursor = conn_com_silver.execute("""
            SELECT COUNT(DISTINCT category_primary)
            FROM gold_category_matrix
            WHERE source_id = 'fonte_a'
        """)
        assert cursor.fetchone()[0] == 3


# ============================================================================
# TESTES DE PROCESSAMENTO GOLD COMPLETO
# ============================================================================

class TestProcessarGold:
    """Testes para processamento Gold completo."""

    def test_processa_todas_agregacoes(self, conn_com_silver):
        results = processar_gold(conn_com_silver, verbose=False)

        assert results["daily_summary"] > 0
        assert results["source_stats"] > 0
        assert results["trending_topics"] > 0
        assert results["sentiment_timeline"] > 0
        assert results["category_matrix"] > 0

    def test_retorna_contagens(self, conn_com_silver):
        results = processar_gold(conn_com_silver, verbose=False)

        assert isinstance(results, dict)
        assert all(isinstance(v, int) for v in results.values())

    def test_idempotente(self, conn_com_silver):
        # Processar duas vezes deve dar os mesmos resultados
        results1 = processar_gold(conn_com_silver, verbose=False)
        results2 = processar_gold(conn_com_silver, verbose=False)

        # Gold usa INSERT OR REPLACE, então não deve duplicar
        cursor = conn_com_silver.execute("SELECT COUNT(*) FROM gold_daily_summary")
        count = cursor.fetchone()[0]
        assert count == results1["daily_summary"]


# ============================================================================
# TESTES DE EDGE CASES
# ============================================================================

class TestEdgeCases:
    """Testes para casos limite."""

    def test_silver_vazia(self, conn):
        # Sem dados na silver, gold deve retornar 0
        results = processar_gold(conn, verbose=False)
        assert all(v == 0 for v in results.values())

    def test_trending_sem_data(self, conn_com_silver):
        # Deve usar a data mais recente
        n = calcular_trending_topics(conn_com_silver, None)
        assert n > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
