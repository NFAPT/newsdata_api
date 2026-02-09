"""
NewsData.io - Gold Layer Aggregate

Agregacoes de dados Silver -> Gold.

Uso:
    python -m src.gold.aggregate
"""

import argparse
import io
import json
import re
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# Forcar UTF-8 no stdout/stderr (Windows)
if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr.encoding != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")


# ============================================================================
# CONSTANTES
# ============================================================================

# Stopwords para trending topics (PT + EN)
STOPWORDS = {
    # Portugues
    "o", "a", "os", "as", "um", "uma", "uns", "umas", "de", "da", "do", "das", "dos",
    "em", "na", "no", "nas", "nos", "por", "para", "com", "sem", "sob", "sobre",
    "e", "ou", "mas", "que", "se", "como", "mais", "menos", "muito", "pouco",
    "este", "esta", "estes", "estas", "esse", "essa", "esses", "essas",
    "aquele", "aquela", "aqueles", "aquelas", "isto", "isso", "aquilo",
    "eu", "tu", "ele", "ela", "nos", "vos", "eles", "elas", "meu", "teu", "seu",
    "ser", "estar", "ter", "haver", "fazer", "ir", "vir", "ver", "dar", "poder",
    "foi", "foram", "vai", "vao", "tem", "sao", "esta", "ja", "ainda", "so",
    "quando", "onde", "porque", "como", "quanto", "qual", "quais", "quem",
    "apos", "antes", "depois", "durante", "entre", "ate", "desde", "contra",
    # Ingles
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for", "of",
    "with", "by", "from", "as", "is", "was", "are", "were", "been", "be",
    "have", "has", "had", "do", "does", "did", "will", "would", "could", "should",
    "this", "that", "these", "those", "it", "its", "he", "she", "they", "we",
    "i", "you", "my", "your", "his", "her", "their", "our", "who", "what", "which",
    "when", "where", "why", "how", "all", "each", "every", "both", "few", "more",
    "most", "other", "some", "such", "no", "not", "only", "same", "so", "than",
    "too", "very", "just", "also", "now", "new", "first", "last", "long", "great",
    "after", "before", "between", "under", "over", "through", "during",
}


# ============================================================================
# SQL - SCHEMAS GOLD
# ============================================================================

SQL_CRIAR_TABELAS_GOLD = """
-- Resumo diario
CREATE TABLE IF NOT EXISTS gold_daily_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    summary_date DATE NOT NULL,
    source_id TEXT,
    category_primary TEXT,

    article_count INTEGER,
    unique_sources INTEGER,

    avg_sentiment_polarity REAL,
    avg_sentiment_subjectivity REAL,
    positive_count INTEGER,
    negative_count INTEGER,
    neutral_count INTEGER,

    avg_word_count REAL,
    total_word_count INTEGER,

    calculated_at TEXT,

    UNIQUE(summary_date, source_id, category_primary)
);

CREATE INDEX IF NOT EXISTS idx_gold_daily_date ON gold_daily_summary(summary_date);

-- Estatisticas por fonte
CREATE TABLE IF NOT EXISTS gold_source_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id TEXT NOT NULL,
    source_name TEXT,

    period_start DATE,
    period_end DATE,

    total_articles INTEGER,
    articles_per_day REAL,

    categories_covered TEXT,
    category_count INTEGER,
    primary_category TEXT,

    avg_sentiment_polarity REAL,
    sentiment_std_dev REAL,

    avg_title_length REAL,
    avg_content_length REAL,
    articles_with_content INTEGER,
    content_ratio REAL,

    calculated_at TEXT,

    UNIQUE(source_id, period_start, period_end)
);

CREATE INDEX IF NOT EXISTS idx_gold_source ON gold_source_stats(source_id);

-- Trending topics
CREATE TABLE IF NOT EXISTS gold_trending_topics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    topic_date DATE NOT NULL,

    term TEXT NOT NULL,
    term_type TEXT,

    frequency INTEGER,
    article_count INTEGER,

    sample_titles TEXT,
    sources TEXT,
    categories TEXT,

    avg_sentiment REAL,

    rank INTEGER,

    calculated_at TEXT,

    UNIQUE(topic_date, term, term_type)
);

CREATE INDEX IF NOT EXISTS idx_gold_trending_date ON gold_trending_topics(topic_date);
CREATE INDEX IF NOT EXISTS idx_gold_trending_term ON gold_trending_topics(term);

-- Timeline de sentimento
CREATE TABLE IF NOT EXISTS gold_sentiment_timeline (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timeline_date DATE NOT NULL,

    granularity TEXT,

    source_id TEXT,
    category_primary TEXT,

    avg_polarity REAL,
    avg_subjectivity REAL,
    min_polarity REAL,
    max_polarity REAL,
    std_polarity REAL,

    positive_pct REAL,
    negative_pct REAL,
    neutral_pct REAL,

    article_count INTEGER,

    calculated_at TEXT,

    UNIQUE(timeline_date, granularity, source_id, category_primary)
);

CREATE INDEX IF NOT EXISTS idx_gold_timeline_date ON gold_sentiment_timeline(timeline_date);

-- Matriz categorias x fontes
CREATE TABLE IF NOT EXISTS gold_category_matrix (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    period_start DATE,
    period_end DATE,

    source_id TEXT NOT NULL,
    source_name TEXT,
    category_primary TEXT NOT NULL,

    article_count INTEGER,
    pct_of_source REAL,
    pct_of_category REAL,

    avg_sentiment REAL,

    calculated_at TEXT,

    UNIQUE(period_start, period_end, source_id, category_primary)
);

CREATE INDEX IF NOT EXISTS idx_gold_matrix_source ON gold_category_matrix(source_id);
CREATE INDEX IF NOT EXISTS idx_gold_matrix_category ON gold_category_matrix(category_primary);
"""


# ============================================================================
# FUNCOES AUXILIARES
# ============================================================================

def extrair_palavras_significativas(texto: str | None) -> list[str]:
    """
    Extrai palavras significativas de um texto (sem stopwords).

    Args:
        texto: Texto a processar

    Returns:
        Lista de palavras significativas
    """
    if not texto:
        return []

    # Remover pontuacao e converter para minusculas
    texto = re.sub(r"[^\w\s]", " ", texto.lower())

    # Dividir em palavras
    palavras = texto.split()

    # Filtrar stopwords e palavras curtas
    palavras = [p for p in palavras if p not in STOPWORDS and len(p) > 2]

    return palavras


# ============================================================================
# FUNCOES DE AGREGACAO
# ============================================================================

def calcular_daily_summary(conn: sqlite3.Connection, data: str | None = None) -> int:
    """
    Calcula resumo diario por fonte e categoria.

    Args:
        conn: Conexao SQLite
        data: Data especifica (YYYY-MM-DD) ou None para todas

    Returns:
        Numero de registos inseridos
    """
    where_clause = f"WHERE pub_date = '{data}'" if data else ""

    sql = f"""
    INSERT OR REPLACE INTO gold_daily_summary (
        summary_date, source_id, category_primary,
        article_count, unique_sources,
        avg_sentiment_polarity, avg_sentiment_subjectivity,
        positive_count, negative_count, neutral_count,
        avg_word_count, total_word_count,
        calculated_at
    )
    SELECT
        pub_date as summary_date,
        source_id,
        category_primary,
        COUNT(*) as article_count,
        COUNT(DISTINCT source_id) as unique_sources,
        AVG(sentiment_polarity) as avg_sentiment_polarity,
        AVG(sentiment_subjectivity) as avg_sentiment_subjectivity,
        SUM(CASE WHEN sentiment_label = 'positive' THEN 1 ELSE 0 END) as positive_count,
        SUM(CASE WHEN sentiment_label = 'negative' THEN 1 ELSE 0 END) as negative_count,
        SUM(CASE WHEN sentiment_label = 'neutral' THEN 1 ELSE 0 END) as neutral_count,
        AVG(word_count) as avg_word_count,
        SUM(word_count) as total_word_count,
        '{datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")}' as calculated_at
    FROM artigos_silver
    {where_clause}
    GROUP BY pub_date, source_id, category_primary
    """

    cursor = conn.execute(sql)
    conn.commit()
    return cursor.rowcount


def calcular_source_stats(conn: sqlite3.Connection,
                          period_start: str | None = None,
                          period_end: str | None = None) -> int:
    """
    Calcula estatisticas por fonte.

    Args:
        conn: Conexao SQLite
        period_start: Data inicio (YYYY-MM-DD)
        period_end: Data fim (YYYY-MM-DD)

    Returns:
        Numero de registos inseridos
    """
    # Se nao especificado, usar range dos dados
    if not period_start or not period_end:
        cursor = conn.execute("SELECT MIN(pub_date), MAX(pub_date) FROM artigos_silver")
        row = cursor.fetchone()
        period_start = row[0] or datetime.now().strftime("%Y-%m-%d")
        period_end = row[1] or datetime.now().strftime("%Y-%m-%d")

    # Calcular dias no periodo
    from datetime import datetime as dt
    d1 = dt.strptime(period_start, "%Y-%m-%d")
    d2 = dt.strptime(period_end, "%Y-%m-%d")
    dias = max((d2 - d1).days, 1)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Query principal
    sql = f"""
    INSERT OR REPLACE INTO gold_source_stats (
        source_id, source_name,
        period_start, period_end,
        total_articles, articles_per_day,
        categories_covered, category_count, primary_category,
        avg_sentiment_polarity, sentiment_std_dev,
        avg_title_length, avg_content_length,
        articles_with_content, content_ratio,
        calculated_at
    )
    SELECT
        source_id,
        MAX(source_name) as source_name,
        '{period_start}' as period_start,
        '{period_end}' as period_end,
        COUNT(*) as total_articles,
        CAST(COUNT(*) AS REAL) / {dias} as articles_per_day,
        GROUP_CONCAT(DISTINCT category_primary) as categories_covered,
        COUNT(DISTINCT category_primary) as category_count,
        (
            SELECT category_primary
            FROM artigos_silver s2
            WHERE s2.source_id = artigos_silver.source_id
              AND pub_date BETWEEN '{period_start}' AND '{period_end}'
            GROUP BY category_primary
            ORDER BY COUNT(*) DESC
            LIMIT 1
        ) as primary_category,
        AVG(sentiment_polarity) as avg_sentiment_polarity,
        SQRT(AVG(sentiment_polarity * sentiment_polarity) - AVG(sentiment_polarity) * AVG(sentiment_polarity)) as sentiment_std_dev,
        AVG(title_length) as avg_title_length,
        AVG(content_length) as avg_content_length,
        SUM(CASE WHEN content_length > 0 THEN 1 ELSE 0 END) as articles_with_content,
        CAST(SUM(CASE WHEN content_length > 0 THEN 1 ELSE 0 END) AS REAL) / COUNT(*) as content_ratio,
        '{now}' as calculated_at
    FROM artigos_silver
    WHERE pub_date BETWEEN '{period_start}' AND '{period_end}'
    GROUP BY source_id
    """

    cursor = conn.execute(sql)
    conn.commit()
    return cursor.rowcount


def calcular_trending_topics(conn: sqlite3.Connection,
                             data: str | None = None,
                             top_n: int = 20) -> int:
    """
    Identifica topicos em tendencia para um dia.

    Args:
        conn: Conexao SQLite
        data: Data especifica (YYYY-MM-DD) ou None para hoje
        top_n: Numero de topicos a retornar

    Returns:
        Numero de registos inseridos
    """
    if not data:
        cursor = conn.execute("SELECT MAX(pub_date) FROM artigos_silver")
        row = cursor.fetchone()
        data = row[0] if row[0] else datetime.now().strftime("%Y-%m-%d")

    # Buscar titulos do dia
    cursor = conn.execute("""
        SELECT title_clean, source_id, category_primary, sentiment_polarity
        FROM artigos_silver
        WHERE pub_date = ?
    """, (data,))

    rows = cursor.fetchall()

    if not rows:
        return 0

    # Contar palavras
    word_counter = Counter()
    word_articles = {}  # palavra -> lista de (titulo, fonte, categoria, sentimento)

    for title, source, category, sentiment in rows:
        palavras = extrair_palavras_significativas(title)
        for palavra in set(palavras):  # set para contar uma vez por artigo
            word_counter[palavra] += 1
            if palavra not in word_articles:
                word_articles[palavra] = []
            word_articles[palavra].append((title, source, category, sentiment or 0))

    # Top N palavras
    top_words = word_counter.most_common(top_n)

    if not top_words:
        return 0

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Inserir na tabela
    inseridos = 0
    for rank, (term, freq) in enumerate(top_words, 1):
        articles = word_articles[term]

        # Amostras de titulos (max 5)
        sample_titles = [a[0] for a in articles[:5]]
        sources = list(set(a[1] for a in articles if a[1]))
        categories = list(set(a[2] for a in articles if a[2]))
        avg_sentiment = sum(a[3] for a in articles) / len(articles) if articles else 0

        sql = """
        INSERT OR REPLACE INTO gold_trending_topics (
            topic_date, term, term_type, frequency, article_count,
            sample_titles, sources, categories, avg_sentiment, rank, calculated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        conn.execute(sql, (
            data,
            term,
            "word",
            freq,
            len(articles),
            json.dumps(sample_titles, ensure_ascii=False),
            json.dumps(sources),
            json.dumps(categories),
            round(avg_sentiment, 4),
            rank,
            now,
        ))
        inseridos += 1

    conn.commit()
    return inseridos


def calcular_sentiment_timeline(conn: sqlite3.Connection,
                                granularity: str = "daily",
                                source_id: str | None = None,
                                category: str | None = None) -> int:
    """
    Calcula evolucao do sentimento ao longo do tempo.

    Args:
        conn: Conexao SQLite
        granularity: "daily", "weekly", "monthly"
        source_id: Filtrar por fonte (opcional)
        category: Filtrar por categoria (opcional)

    Returns:
        Numero de registos inseridos
    """
    # Construir clausulas WHERE
    conditions = []
    if source_id:
        conditions.append(f"source_id = '{source_id}'")
    if category:
        conditions.append(f"category_primary = '{category}'")

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

    # Group by baseado na granularidade
    if granularity == "weekly":
        date_expr = "strftime('%Y-W%W', pub_date)"
    elif granularity == "monthly":
        date_expr = "strftime('%Y-%m', pub_date)"
    else:
        date_expr = "pub_date"

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    source_val = source_id or "ALL"
    category_val = category or "ALL"

    sql = f"""
    INSERT OR REPLACE INTO gold_sentiment_timeline (
        timeline_date, granularity, source_id, category_primary,
        avg_polarity, avg_subjectivity, min_polarity, max_polarity, std_polarity,
        positive_pct, negative_pct, neutral_pct,
        article_count, calculated_at
    )
    SELECT
        {date_expr} as timeline_date,
        '{granularity}' as granularity,
        '{source_val}' as source_id,
        '{category_val}' as category_primary,
        AVG(sentiment_polarity) as avg_polarity,
        AVG(sentiment_subjectivity) as avg_subjectivity,
        MIN(sentiment_polarity) as min_polarity,
        MAX(sentiment_polarity) as max_polarity,
        SQRT(AVG(sentiment_polarity * sentiment_polarity) - AVG(sentiment_polarity) * AVG(sentiment_polarity)) as std_polarity,
        CAST(SUM(CASE WHEN sentiment_label = 'positive' THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(*) as positive_pct,
        CAST(SUM(CASE WHEN sentiment_label = 'negative' THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(*) as negative_pct,
        CAST(SUM(CASE WHEN sentiment_label = 'neutral' THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(*) as neutral_pct,
        COUNT(*) as article_count,
        '{now}' as calculated_at
    FROM artigos_silver
    {where_clause}
    GROUP BY {date_expr}
    """

    cursor = conn.execute(sql)
    conn.commit()
    return cursor.rowcount


def calcular_category_matrix(conn: sqlite3.Connection,
                             period_start: str | None = None,
                             period_end: str | None = None) -> int:
    """
    Calcula matriz de distribuicao categorias x fontes.

    Args:
        conn: Conexao SQLite
        period_start: Data inicio (YYYY-MM-DD)
        period_end: Data fim (YYYY-MM-DD)

    Returns:
        Numero de registos inseridos
    """
    # Se nao especificado, usar range dos dados
    if not period_start or not period_end:
        cursor = conn.execute("SELECT MIN(pub_date), MAX(pub_date) FROM artigos_silver")
        row = cursor.fetchone()
        period_start = row[0] or datetime.now().strftime("%Y-%m-%d")
        period_end = row[1] or datetime.now().strftime("%Y-%m-%d")

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    sql = f"""
    INSERT OR REPLACE INTO gold_category_matrix (
        period_start, period_end,
        source_id, source_name, category_primary,
        article_count, pct_of_source, pct_of_category,
        avg_sentiment, calculated_at
    )
    SELECT
        '{period_start}' as period_start,
        '{period_end}' as period_end,
        s.source_id,
        s.source_name,
        s.category_primary,
        COUNT(*) as article_count,
        CAST(COUNT(*) AS REAL) * 100 / source_total.total as pct_of_source,
        CAST(COUNT(*) AS REAL) * 100 / category_total.total as pct_of_category,
        AVG(s.sentiment_polarity) as avg_sentiment,
        '{now}' as calculated_at
    FROM artigos_silver s
    JOIN (
        SELECT source_id, COUNT(*) as total
        FROM artigos_silver
        WHERE pub_date BETWEEN '{period_start}' AND '{period_end}'
        GROUP BY source_id
    ) source_total ON s.source_id = source_total.source_id
    JOIN (
        SELECT category_primary, COUNT(*) as total
        FROM artigos_silver
        WHERE pub_date BETWEEN '{period_start}' AND '{period_end}'
        GROUP BY category_primary
    ) category_total ON s.category_primary = category_total.category_primary
    WHERE s.pub_date BETWEEN '{period_start}' AND '{period_end}'
    GROUP BY s.source_id, s.category_primary
    """

    cursor = conn.execute(sql)
    conn.commit()
    return cursor.rowcount


# ============================================================================
# FUNCOES DE BASE DE DADOS
# ============================================================================

def criar_tabelas_gold(conn: sqlite3.Connection) -> None:
    """Cria todas as tabelas gold se nao existirem."""
    conn.executescript(SQL_CRIAR_TABELAS_GOLD)
    conn.commit()


# ============================================================================
# FUNCAO PRINCIPAL
# ============================================================================

def processar_gold(conn: sqlite3.Connection, verbose: bool = True) -> dict:
    """
    Processa todas as agregacoes gold.

    Args:
        conn: Conexao SQLite
        verbose: Se True, imprime progresso

    Returns:
        Dict com contagens por tipo de agregacao
    """
    # Criar tabelas
    criar_tabelas_gold(conn)

    results = {
        "daily_summary": 0,
        "source_stats": 0,
        "trending_topics": 0,
        "sentiment_timeline": 0,
        "category_matrix": 0,
    }

    # 1. Daily Summary
    if verbose:
        print("   [1/5] Calculando daily_summary...")
    results["daily_summary"] = calcular_daily_summary(conn)
    if verbose:
        print(f"         {results['daily_summary']} registos")

    # 2. Source Stats
    if verbose:
        print("   [2/5] Calculando source_stats...")
    results["source_stats"] = calcular_source_stats(conn)
    if verbose:
        print(f"         {results['source_stats']} registos")

    # 3. Trending Topics
    if verbose:
        print("   [3/5] Calculando trending_topics...")
    results["trending_topics"] = calcular_trending_topics(conn)
    if verbose:
        print(f"         {results['trending_topics']} registos")

    # 4. Sentiment Timeline
    if verbose:
        print("   [4/5] Calculando sentiment_timeline...")
    results["sentiment_timeline"] = calcular_sentiment_timeline(conn)
    if verbose:
        print(f"         {results['sentiment_timeline']} registos")

    # 5. Category Matrix
    if verbose:
        print("   [5/5] Calculando category_matrix...")
    results["category_matrix"] = calcular_category_matrix(conn)
    if verbose:
        print(f"         {results['category_matrix']} registos")

    if verbose:
        total = sum(results.values())
        print(f"\n   [OK] Gold Layer concluida: {total} registos totais")

    return results


# ============================================================================
# MAIN STANDALONE
# ============================================================================

def parse_args() -> argparse.Namespace:
    """Parse argumentos da linha de comandos."""
    parser = argparse.ArgumentParser(
        description="NewsData.io - Processar Gold Layer",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Caminho do ficheiro SQLite (default: db/newsdata.db)",
    )
    return parser.parse_args()


def main() -> int:
    """Processa Gold Layer standalone."""
    print("\n" + "=" * 60)
    print("    NEWSDATA.IO - GOLD LAYER")
    print("=" * 60 + "\n")

    args = parse_args()

    project_root = Path(__file__).parent.parent.parent
    db_path = Path(args.db_path) if args.db_path else project_root / "db" / "newsdata.db"

    if not db_path.exists():
        print(f"[ERRO] Base de dados nao encontrada: {db_path}")
        return 1

    try:
        conn = sqlite3.connect(db_path)
        print(f"[DB] Conectado a {db_path}\n")

        print("[GOLD] A calcular agregacoes...")
        results = processar_gold(conn, verbose=True)

        conn.close()
        return 0

    except Exception as e:
        print(f"\n[ERRO] {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
