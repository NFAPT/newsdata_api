"""
NewsData.io – Carregamento para SQLite

Carrega os CSV da collection bronze para uma base de dados SQLite.

Uso:
    python -m src.db.loader                    # Carrega todos os CSV
    python -m src.db.loader --db-path outro.db # Caminho personalizado
"""

import argparse
import io
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

# Forçar UTF-8 no stdout/stderr (Windows cp1252 não suporta emojis)
if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr.encoding != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import pandas as pd


# ============================================================================
# ESQUEMA DA BASE DE DADOS
# ============================================================================

SQL_CRIAR_TABELA = """
CREATE TABLE IF NOT EXISTS artigos (
    article_id TEXT PRIMARY KEY,
    title TEXT,
    description TEXT,
    content TEXT,
    source_id TEXT,
    source_name TEXT,
    source_url TEXT,
    creator TEXT,
    pubDate TEXT,
    category TEXT,
    country TEXT,
    language TEXT,
    link TEXT,
    image_url TEXT,
    endpoint TEXT,
    loaded_at TEXT
);
"""


# ============================================================================
# FUNÇÕES DE BASE DE DADOS
# ============================================================================

def criar_tabela(conn: sqlite3.Connection) -> None:
    """Cria a tabela artigos se não existir."""
    conn.execute(SQL_CRIAR_TABELA)
    conn.commit()


def carregar_csv(conn: sqlite3.Connection, csv_path: Path, endpoint: str) -> int:
    """
    Lê um CSV e insere os registos na tabela artigos.

    Usa INSERT OR IGNORE para evitar duplicados (pelo article_id).

    Args:
        conn: Conexão SQLite
        csv_path: Caminho do ficheiro CSV
        endpoint: Nome do endpoint (latestPT, crypto, etc.)

    Returns:
        Número de registos inseridos
    """
    try:
        df = pd.read_csv(csv_path, encoding="utf-8")
    except pd.errors.EmptyDataError:
        print(f"   ⚠️  CSV vazio: {csv_path.name}")
        return 0

    if df.empty:
        print(f"   ⚠️  CSV vazio: {csv_path.name}")
        return 0

    loaded_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    df["endpoint"] = endpoint
    df["loaded_at"] = loaded_at

    colunas = [
        "article_id", "title", "description", "content",
        "source_id", "source_name", "source_url", "creator",
        "pubDate", "category", "country", "language",
        "link", "image_url", "endpoint", "loaded_at",
    ]

    # Garantir que todas as colunas existem
    for col in colunas:
        if col not in df.columns:
            df[col] = None

    df = df[colunas]

    # Converter NaN para None (SQLite NULL)
    df = df.where(df.notna(), None)

    placeholders = ", ".join(["?"] * len(colunas))
    sql = f"INSERT OR IGNORE INTO artigos ({', '.join(colunas)}) VALUES ({placeholders})"

    registos = df.values.tolist()
    cursor = conn.executemany(sql, registos)
    conn.commit()

    inseridos = cursor.rowcount
    print(f"   📥 {csv_path.name}: {inseridos} registos inseridos")
    return inseridos


def extrair_endpoint(nome_ficheiro: str) -> str:
    """
    Extrai o nome do endpoint a partir do nome do ficheiro CSV.

    Exemplo: newsdata_crypto_tabular_20260202T222534Z.csv → crypto
    """
    match = re.match(r"newsdata_(.+?)_tabular_", nome_ficheiro)
    if match:
        return match.group(1)
    return "desconhecido"


def carregar_todos_csv(db_path: Path) -> int:
    """
    Percorre collection/bronze/*.csv e carrega todos na base de dados.

    Args:
        db_path: Caminho do ficheiro SQLite

    Returns:
        Total de registos inseridos
    """
    project_root = Path(__file__).parent.parent.parent
    csv_dir = project_root / "collection" / "bronze"

    ficheiros = sorted(csv_dir.glob("*_tabular_*.csv"))

    if not ficheiros:
        print("⚠️  Nenhum ficheiro CSV encontrado em collection/bronze/")
        return 0

    print(f"📂 Encontrados {len(ficheiros)} ficheiro(s) CSV\n")

    # Criar pasta db se necessário
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    criar_tabela(conn)

    total = 0
    for csv_path in ficheiros:
        endpoint = extrair_endpoint(csv_path.name)
        total += carregar_csv(conn, csv_path, endpoint)

    conn.close()

    print(f"\n✅ Total: {total} registos inseridos em {db_path}")
    return total


# ============================================================================
# MAIN
# ============================================================================

def parse_args() -> argparse.Namespace:
    """Parse argumentos da linha de comandos."""
    parser = argparse.ArgumentParser(
        description="NewsData.io – Carregar CSV para SQLite",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Caminho do ficheiro SQLite (default: db/newsdata.db)",
    )
    return parser.parse_args()


def main() -> int:
    """Carrega todos os CSV da collection bronze para SQLite."""

    print("\n" + "=" * 60)
    print("    🗄️  NEWSDATA.IO – CARREGAR PARA SQLITE")
    print("=" * 60 + "\n")

    args = parse_args()

    project_root = Path(__file__).parent.parent.parent
    db_path = Path(args.db_path) if args.db_path else project_root / "db" / "newsdata.db"

    try:
        total = carregar_todos_csv(db_path)
        if total == 0:
            print("\nNenhum registo novo inserido.")
        return 0
    except Exception as e:
        print(f"\n❌ Erro: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
