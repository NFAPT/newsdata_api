"""
NewsData.io - Silver Layer Transform

Transformacoes de dados Bronze -> Silver.

Uso:
    python -m src.silver.transform
"""

import argparse
import io
import json
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
from textblob import TextBlob

# Forcar UTF-8 no stdout/stderr (Windows)
if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr.encoding != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

from src.utils.text_processing import (
    limpar_texto_completo,
    extrair_dominio,
    contar_palavras,
)


# ============================================================================
# CONSTANTES
# ============================================================================

# Mapeamento de categorias para normalizacao
CATEGORIAS_NORMALIZADAS = {
    # Tecnologia
    "technology": "technology",
    "tech": "technology",
    "science": "technology",
    "AI": "technology",
    # Negocios
    "business": "business",
    "economy": "business",
    "finance": "business",
    "money": "business",
    # Desporto
    "sports": "sports",
    "sport": "sports",
    "football": "sports",
    # Politica
    "politics": "politics",
    "world": "politics",
    # Entretenimento
    "entertainment": "entertainment",
    "lifestyle": "entertainment",
    "culture": "entertainment",
    # Saude
    "health": "health",
    # Geral
    "top": "general",
    "breaking": "general",
    "news": "general",
}

# Lista de paises conhecidos (PT/EN)
PAISES_CONHECIDOS = {
    "portugal", "brasil", "brazil", "espanha", "spain", "franca", "france",
    "alemanha", "germany", "italia", "italy", "reino unido", "united kingdom",
    "estados unidos", "united states", "eua", "usa", "china", "russia",
    "japao", "japan", "india", "australia", "canada", "mexico", "argentina",
    "ucrania", "ukraine", "israel", "palestina", "palestine", "gaza",
}

# Lista de cidades conhecidas
CIDADES_CONHECIDAS = {
    "lisboa", "lisbon", "porto", "braga", "coimbra", "faro", "funchal",
    "madrid", "barcelona", "paris", "londres", "london", "berlim", "berlin",
    "roma", "rome", "nova york", "new york", "washington", "los angeles",
    "sao paulo", "rio de janeiro", "brasilia", "buenos aires", "caracas",
    "moscovo", "moscow", "pequim", "beijing", "toquio", "tokyo",
}

# Organizacoes comuns (sufixos)
ORG_SUFIXOS = ["sa", "lda", "inc", "corp", "ltd", "gmbh", "spa", "ag"]


# ============================================================================
# SQL - SCHEMA SILVER
# ============================================================================

SQL_CRIAR_TABELA_SILVER = """
CREATE TABLE IF NOT EXISTS artigos_silver (
    article_id TEXT PRIMARY KEY,

    -- Campos limpos
    title_clean TEXT,
    description_clean TEXT,
    content_clean TEXT,

    -- Data parseada
    pub_date DATE,
    pub_datetime DATETIME,
    pub_year INTEGER,
    pub_month INTEGER,
    pub_day INTEGER,
    pub_hour INTEGER,

    -- Campos originais
    source_id TEXT,
    source_name TEXT,

    -- Categorias normalizadas
    category_primary TEXT,
    category_list TEXT,
    category_count INTEGER,

    -- Validacoes
    link TEXT,
    link_valid INTEGER,
    link_domain TEXT,

    -- Idioma
    language TEXT,
    language_detected TEXT,
    language_match INTEGER,

    -- NLP - Sentimento
    sentiment_polarity REAL,
    sentiment_subjectivity REAL,
    sentiment_label TEXT,

    -- NLP - Entidades
    entities_persons TEXT,
    entities_orgs TEXT,
    entities_locations TEXT,
    entity_count INTEGER,

    -- Metricas de texto
    title_length INTEGER,
    description_length INTEGER,
    content_length INTEGER,
    word_count INTEGER,

    -- Metadados
    country TEXT,
    endpoint TEXT,
    processed_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_silver_pub_date ON artigos_silver(pub_date);
CREATE INDEX IF NOT EXISTS idx_silver_source ON artigos_silver(source_id);
CREATE INDEX IF NOT EXISTS idx_silver_category ON artigos_silver(category_primary);
CREATE INDEX IF NOT EXISTS idx_silver_sentiment ON artigos_silver(sentiment_label);
"""


# ============================================================================
# FUNCOES DE PARSING DE DATAS
# ============================================================================

def parse_pub_date(date_str: str | None) -> dict:
    """
    Converte pubDate string para componentes de data.

    Args:
        date_str: Data no formato "YYYY-MM-DD HH:MM:SS"

    Returns:
        Dict com pub_date, pub_datetime, pub_year, pub_month, pub_day, pub_hour
    """
    result = {
        "pub_date": None,
        "pub_datetime": None,
        "pub_year": None,
        "pub_month": None,
        "pub_day": None,
        "pub_hour": None,
    }

    if not date_str:
        return result

    try:
        # Tentar formato padrao: "2026-02-09 07:38:00"
        dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        result["pub_date"] = dt.date().isoformat()
        result["pub_datetime"] = dt.isoformat()
        result["pub_year"] = dt.year
        result["pub_month"] = dt.month
        result["pub_day"] = dt.day
        result["pub_hour"] = dt.hour
    except ValueError:
        try:
            # Tentar formato ISO: "2026-02-09T07:38:00"
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            result["pub_date"] = dt.date().isoformat()
            result["pub_datetime"] = dt.isoformat()
            result["pub_year"] = dt.year
            result["pub_month"] = dt.month
            result["pub_day"] = dt.day
            result["pub_hour"] = dt.hour
        except ValueError:
            pass

    return result


# ============================================================================
# FUNCOES DE NORMALIZACAO
# ============================================================================

def normalizar_categoria(categoria_str: str | None) -> dict:
    """
    Normaliza categorias.

    Args:
        categoria_str: String com categorias separadas por virgula

    Returns:
        Dict com category_primary, category_list (JSON), category_count
    """
    result = {
        "category_primary": "general",
        "category_list": "[]",
        "category_count": 0,
    }

    if not categoria_str:
        return result

    # Separar categorias
    categorias = [c.strip().lower() for c in categoria_str.split(",")]
    categorias = [c for c in categorias if c]

    if not categorias:
        return result

    # Normalizar cada categoria
    categorias_norm = []
    for cat in categorias:
        cat_norm = CATEGORIAS_NORMALIZADAS.get(cat, cat)
        if cat_norm not in categorias_norm:
            categorias_norm.append(cat_norm)

    result["category_primary"] = categorias_norm[0] if categorias_norm else "general"
    result["category_list"] = json.dumps(categorias_norm)
    result["category_count"] = len(categorias_norm)

    return result


def validar_url(url: str | None) -> dict:
    """
    Valida URL e extrai dominio.

    Args:
        url: URL a validar

    Returns:
        Dict com link_valid, link_domain
    """
    result = {
        "link_valid": 0,
        "link_domain": None,
    }

    if not url:
        return result

    try:
        parsed = urlparse(url)
        if parsed.scheme in ("http", "https") and parsed.netloc:
            result["link_valid"] = 1
            result["link_domain"] = extrair_dominio(url)
    except Exception:
        pass

    return result


# ============================================================================
# FUNCOES DE NLP - SENTIMENTO
# ============================================================================

def analisar_sentimento(texto: str | None) -> dict:
    """
    Analisa sentimento usando TextBlob.

    Args:
        texto: Texto a analisar

    Returns:
        Dict com sentiment_polarity, sentiment_subjectivity, sentiment_label
    """
    result = {
        "sentiment_polarity": 0.0,
        "sentiment_subjectivity": 0.0,
        "sentiment_label": "neutral",
    }

    if not texto or len(texto) < 10:
        return result

    try:
        blob = TextBlob(texto)
        polarity = blob.sentiment.polarity
        subjectivity = blob.sentiment.subjectivity

        result["sentiment_polarity"] = round(polarity, 4)
        result["sentiment_subjectivity"] = round(subjectivity, 4)

        # Classificar
        if polarity > 0.1:
            result["sentiment_label"] = "positive"
        elif polarity < -0.1:
            result["sentiment_label"] = "negative"
        else:
            result["sentiment_label"] = "neutral"

    except Exception:
        pass

    return result


# ============================================================================
# FUNCOES DE NLP - EXTRACAO DE ENTIDADES
# ============================================================================

def extrair_entidades(texto: str | None) -> dict:
    """
    Extrai entidades usando regex e heuristicas.

    Args:
        texto: Texto a analisar

    Returns:
        Dict com entities_persons, entities_orgs, entities_locations, entity_count
    """
    result = {
        "entities_persons": "[]",
        "entities_orgs": "[]",
        "entities_locations": "[]",
        "entity_count": 0,
    }

    if not texto:
        return result

    texto_lower = texto.lower()
    persons = set()
    orgs = set()
    locations = set()

    # Extrair nomes proprios (2-4 palavras capitalizadas consecutivas)
    # Exemplo: "Antonio Costa", "Maria da Silva"
    nome_pattern = r"\b([A-Z][a-záàâãéèêíïóôõöúçñ]+(?:\s+(?:da|de|do|dos|das|e)?\s*[A-Z][a-záàâãéèêíïóôõöúçñ]+){1,3})\b"
    matches = re.findall(nome_pattern, texto)
    for match in matches:
        # Filtrar se for localizacao conhecida
        if match.lower() not in PAISES_CONHECIDOS and match.lower() not in CIDADES_CONHECIDAS:
            persons.add(match)

    # Extrair organizacoes (palavras com sufixos conhecidos)
    for sufixo in ORG_SUFIXOS:
        org_pattern = rf"\b([A-Z][A-Za-záàâãéèêíïóôõöúçñ\s]+\s+{sufixo}\.?)\b"
        matches = re.findall(org_pattern, texto, re.IGNORECASE)
        orgs.update(matches)

    # Extrair localizacoes conhecidas
    for pais in PAISES_CONHECIDOS:
        if pais in texto_lower:
            # Encontrar versao capitalizada no texto original
            pattern = rf"\b({re.escape(pais)})\b"
            match = re.search(pattern, texto, re.IGNORECASE)
            if match:
                locations.add(match.group(1))

    for cidade in CIDADES_CONHECIDAS:
        if cidade in texto_lower:
            pattern = rf"\b({re.escape(cidade)})\b"
            match = re.search(pattern, texto, re.IGNORECASE)
            if match:
                locations.add(match.group(1))

    # Limitar a 10 entidades de cada tipo
    persons = list(persons)[:10]
    orgs = list(orgs)[:10]
    locations = list(locations)[:10]

    result["entities_persons"] = json.dumps(persons)
    result["entities_orgs"] = json.dumps(orgs)
    result["entities_locations"] = json.dumps(locations)
    result["entity_count"] = len(persons) + len(orgs) + len(locations)

    return result


# ============================================================================
# FUNCOES DE DETECAO DE LINGUA
# ============================================================================

def detectar_lingua(texto: str | None, lingua_declarada: str | None) -> dict:
    """
    Detecta lingua e compara com declarada.

    Args:
        texto: Texto a analisar
        lingua_declarada: Lingua declarada nos metadados

    Returns:
        Dict com language_detected, language_match
    """
    result = {
        "language_detected": None,
        "language_match": 0,
    }

    if not texto or len(texto) < 20:
        return result

    try:
        from langdetect import detect
        detected = detect(texto)
        result["language_detected"] = detected

        # Comparar com declarada
        if lingua_declarada:
            # Mapear nomes para codigos
            lang_map = {
                "portuguese": "pt",
                "english": "en",
                "spanish": "es",
                "french": "fr",
                "german": "de",
                "italian": "it",
            }
            declarada_code = lang_map.get(lingua_declarada.lower(), lingua_declarada.lower()[:2])
            if detected == declarada_code:
                result["language_match"] = 1

    except Exception:
        pass

    return result


# ============================================================================
# FUNCOES DE METRICAS DE TEXTO
# ============================================================================

def calcular_metricas_texto(titulo: str | None, descricao: str | None, conteudo: str | None) -> dict:
    """
    Calcula metricas de tamanho de texto.

    Args:
        titulo: Titulo do artigo
        descricao: Descricao do artigo
        conteudo: Conteudo do artigo

    Returns:
        Dict com title_length, description_length, content_length, word_count
    """
    titulo = titulo or ""
    descricao = descricao or ""
    conteudo = conteudo or ""

    # Texto completo para contagem de palavras
    texto_completo = f"{titulo} {descricao} {conteudo}"

    return {
        "title_length": len(titulo),
        "description_length": len(descricao),
        "content_length": len(conteudo),
        "word_count": contar_palavras(texto_completo),
    }


# ============================================================================
# FUNCAO PRINCIPAL DE TRANSFORMACAO
# ============================================================================

def transformar_artigo(row: dict) -> dict:
    """
    Aplica todas as transformacoes a um artigo.

    Args:
        row: Dict com dados do artigo bronze

    Returns:
        Dict com dados transformados para silver
    """
    # Limpar texto
    title_clean = limpar_texto_completo(row.get("title"))
    description_clean = limpar_texto_completo(row.get("description"))
    content_clean = limpar_texto_completo(row.get("content"))

    # Texto combinado para NLP
    texto_nlp = " ".join(filter(None, [title_clean, description_clean, content_clean]))

    # Aplicar transformacoes
    date_info = parse_pub_date(row.get("pubDate"))
    category_info = normalizar_categoria(row.get("category"))
    url_info = validar_url(row.get("link"))
    sentiment_info = analisar_sentimento(texto_nlp)
    entities_info = extrair_entidades(texto_nlp)
    language_info = detectar_lingua(texto_nlp, row.get("language"))
    metrics_info = calcular_metricas_texto(title_clean, description_clean, content_clean)

    # Construir resultado
    return {
        "article_id": row.get("article_id"),
        "title_clean": title_clean,
        "description_clean": description_clean,
        "content_clean": content_clean,
        **date_info,
        "source_id": row.get("source_id"),
        "source_name": row.get("source_name"),
        **category_info,
        "link": row.get("link"),
        **url_info,
        "language": row.get("language"),
        **language_info,
        **sentiment_info,
        **entities_info,
        **metrics_info,
        "country": row.get("country"),
        "endpoint": row.get("endpoint"),
        "processed_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    }


# ============================================================================
# FUNCOES DE BASE DE DADOS
# ============================================================================

def criar_tabela_silver(conn: sqlite3.Connection) -> None:
    """Cria tabela artigos_silver se nao existir."""
    conn.executescript(SQL_CRIAR_TABELA_SILVER)
    conn.commit()


def obter_artigos_pendentes(conn: sqlite3.Connection) -> list[dict]:
    """
    Obtem artigos bronze que ainda nao foram processados para silver.

    Args:
        conn: Conexao SQLite

    Returns:
        Lista de dicts com artigos pendentes
    """
    sql = """
    SELECT a.*
    FROM artigos a
    LEFT JOIN artigos_silver s ON a.article_id = s.article_id
    WHERE s.article_id IS NULL
    """
    cursor = conn.execute(sql)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    return [dict(zip(columns, row)) for row in rows]


def inserir_artigo_silver(conn: sqlite3.Connection, artigo: dict) -> None:
    """Insere artigo transformado na tabela silver."""
    colunas = list(artigo.keys())
    placeholders = ", ".join(["?"] * len(colunas))
    sql = f"INSERT OR REPLACE INTO artigos_silver ({', '.join(colunas)}) VALUES ({placeholders})"
    conn.execute(sql, list(artigo.values()))


# ============================================================================
# FUNCAO PRINCIPAL
# ============================================================================

def processar_silver(conn: sqlite3.Connection, verbose: bool = True) -> int:
    """
    Processa todos os artigos bronze -> silver.

    Args:
        conn: Conexao SQLite
        verbose: Se True, imprime progresso

    Returns:
        Numero de artigos processados
    """
    # Criar tabela se nao existir
    criar_tabela_silver(conn)

    # Obter artigos pendentes
    artigos = obter_artigos_pendentes(conn)

    if not artigos:
        if verbose:
            print("   Nenhum artigo pendente para processar")
        return 0

    if verbose:
        print(f"   {len(artigos)} artigos pendentes")

    # Processar cada artigo
    processados = 0
    for i, artigo in enumerate(artigos, 1):
        try:
            artigo_silver = transformar_artigo(artigo)
            inserir_artigo_silver(conn, artigo_silver)
            processados += 1

            if verbose and i % 10 == 0:
                print(f"   Processados {i}/{len(artigos)}...")

        except Exception as e:
            if verbose:
                print(f"   [ERRO] Artigo {artigo.get('article_id')}: {e}")

    conn.commit()

    if verbose:
        print(f"   [OK] {processados} artigos processados para Silver")

    return processados


# ============================================================================
# MAIN STANDALONE
# ============================================================================

def parse_args() -> argparse.Namespace:
    """Parse argumentos da linha de comandos."""
    parser = argparse.ArgumentParser(
        description="NewsData.io - Processar Silver Layer",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Caminho do ficheiro SQLite (default: db/newsdata.db)",
    )
    return parser.parse_args()


def main() -> int:
    """Processa Silver Layer standalone."""
    print("\n" + "=" * 60)
    print("    NEWSDATA.IO - SILVER LAYER")
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

        print("[SILVER] A processar artigos...")
        n = processar_silver(conn, verbose=True)

        conn.close()

        print(f"\n[OK] Silver Layer concluida: {n} artigos")
        return 0

    except Exception as e:
        print(f"\n[ERRO] {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
